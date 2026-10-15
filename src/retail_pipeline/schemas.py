"""Event contracts for the orders stream.

Every raw event is validated against these models before it lands in the bronze
layer. Invalid events are routed to a dead letter location with the reason, so
bad producers never silently corrupt downstream tables.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import Enum

from pydantic import BaseModel, Field, field_validator, model_validator


class OrderStatus(str, Enum):
    PLACED = "PLACED"
    PAID = "PAID"
    SHIPPED = "SHIPPED"
    DELIVERED = "DELIVERED"
    CANCELLED = "CANCELLED"
    REFUNDED = "REFUNDED"


class LineItem(BaseModel):
    sku: str = Field(min_length=3, max_length=32)
    category: str
    quantity: int = Field(gt=0, le=1000)
    unit_price: Decimal = Field(gt=0, max_digits=10, decimal_places=2)


class OrderEvent(BaseModel):
    """One state change of an order, as emitted by the order service."""

    event_id: str = Field(min_length=8)
    order_id: str = Field(min_length=4)
    customer_id: str = Field(min_length=4)
    store_id: str
    channel: str = Field(pattern=r"^(web|mobile|store)$")
    status: OrderStatus
    event_ts: datetime
    currency: str = Field(pattern=r"^[A-Z]{3}$")
    items: list[LineItem] = Field(min_length=1)
    schema_version: int = 1

    @field_validator("event_ts")
    @classmethod
    def ts_must_be_tz_aware(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("event_ts must include a timezone offset")
        return v

    @model_validator(mode="after")
    def unique_skus(self) -> OrderEvent:
        skus = [i.sku for i in self.items]
        if len(skus) != len(set(skus)):
            raise ValueError("duplicate sku within one order")
        return self

    @property
    def order_total(self) -> Decimal:
        return sum((i.unit_price * i.quantity for i in self.items), Decimal(0))
