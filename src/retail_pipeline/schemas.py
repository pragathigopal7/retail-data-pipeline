"""Event contracts for the orders stream.

Every raw event is validated against these models before it lands in the bronze
layer. Invalid events are routed to a dead letter location with the reason, so
bad producers never silently corrupt downstream tables.

The contract is versioned. `schema_version` selects the model:

* v1: the original contract.
* v2: adds an order level `payment_method` and a per line `discount`. Both are
  additive, so every v1 event can be upcast to v2 losslessly.

Ingest validates against the version the producer declared, then upcasts to the
latest version, so bronze and everything downstream only ever see one shape.
Events that declare a version the registry does not know are rejected to the
DLQ instead of being guessed at.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Annotated, Any, Literal

from pydantic import (
    BaseModel,
    Discriminator,
    Field,
    Tag,
    TypeAdapter,
    field_validator,
    model_validator,
)

LATEST_SCHEMA_VERSION = 2


class OrderStatus(str, Enum):
    PLACED = "PLACED"
    PAID = "PAID"
    SHIPPED = "SHIPPED"
    DELIVERED = "DELIVERED"
    CANCELLED = "CANCELLED"
    REFUNDED = "REFUNDED"


class PaymentMethod(str, Enum):
    CARD = "card"
    WALLET = "wallet"
    GIFT_CARD = "gift_card"
    CASH = "cash"


class LineItem(BaseModel):
    sku: str = Field(min_length=3, max_length=32)
    category: str
    quantity: int = Field(gt=0, le=1000)
    unit_price: Decimal = Field(gt=0, max_digits=10, decimal_places=2)

    @property
    def gross(self) -> Decimal:
        return self.unit_price * self.quantity


class LineItemV2(LineItem):
    discount: Decimal = Field(default=Decimal("0.00"), ge=0, max_digits=10, decimal_places=2)

    @model_validator(mode="after")
    def discount_within_line(self) -> LineItemV2:
        if self.discount > self.gross:
            raise ValueError("discount exceeds line gross")
        return self


class _OrderEventBase(BaseModel):
    event_id: str = Field(min_length=8)
    order_id: str = Field(min_length=4)
    customer_id: str = Field(min_length=4)
    store_id: str
    channel: str = Field(pattern=r"^(web|mobile|store)$")
    status: OrderStatus
    event_ts: datetime
    currency: str = Field(pattern=r"^[A-Z]{3}$")

    @field_validator("event_ts")
    @classmethod
    def ts_must_be_tz_aware(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("event_ts must include a timezone offset")
        return v

    @model_validator(mode="after")
    def unique_skus(self):
        skus = [i.sku for i in self.items]  # type: ignore[attr-defined]
        if len(skus) != len(set(skus)):
            raise ValueError("duplicate sku within one order")
        return self

    @property
    def order_total(self) -> Decimal:
        """Gross merchandise value, before discounts."""
        return sum((i.gross for i in self.items), Decimal(0))  # type: ignore[attr-defined]


class OrderEventV1(_OrderEventBase):
    """One state change of an order, as emitted by the order service (v1)."""

    items: list[LineItem] = Field(min_length=1)
    schema_version: Literal[1] = 1

    def upcast(self) -> OrderEventV2:
        data = self.model_dump()
        data["schema_version"] = 2
        data["payment_method"] = None
        return OrderEventV2.model_validate(data)


class OrderEventV2(_OrderEventBase):
    """v2 adds payment method and line level discounts."""

    items: list[LineItemV2] = Field(min_length=1)
    payment_method: PaymentMethod | None = None
    schema_version: Literal[2] = 2

    def upcast(self) -> OrderEventV2:
        return self

    @property
    def discount_total(self) -> Decimal:
        return sum((i.discount for i in self.items), Decimal(0))


# The latest contract, which is the shape bronze stores.
OrderEvent = OrderEventV2

REGISTRY: dict[int, type[_OrderEventBase]] = {1: OrderEventV1, 2: OrderEventV2}


def _version_tag(value: Any) -> str:
    """Pick the contract from the declared version. Missing means v1 (legacy producers)."""
    version = value.get("schema_version", 1) if isinstance(value, dict) else value.schema_version
    return str(version)


_ANY_VERSION = TypeAdapter(
    Annotated[
        Annotated[OrderEventV1, Tag("1")] | Annotated[OrderEventV2, Tag("2")],
        Discriminator(
            _version_tag,
            custom_error_type="unsupported_schema_version",
            custom_error_message="schema_version is not in the contract registry",
        ),
    ]
)


def validate_event(raw: bytes | str | dict) -> tuple[OrderEventV2, int]:
    """Validate a raw event against its declared version and upcast to the latest.

    Returns the upcast event and the version the producer declared. Raises
    `pydantic.ValidationError` for malformed payloads and unknown versions.
    """
    if isinstance(raw, dict):
        event = _ANY_VERSION.validate_python(raw)
    else:
        event = _ANY_VERSION.validate_json(raw)
    return event.upcast(), event.schema_version


def parse_event(raw: bytes | str | dict) -> OrderEventV2:
    """Validate and upcast, discarding the declared version."""
    return validate_event(raw)[0]
