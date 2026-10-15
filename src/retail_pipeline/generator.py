"""Synthetic order event generator.

Produces realistic order lifecycles (PLACED -> PAID -> SHIPPED -> DELIVERED, with
some cancellations and refunds) and deliberately injects the problems a real
stream has: duplicate deliveries, late and out of order events, and malformed
payloads. The pipeline is expected to handle all of them.
"""

from __future__ import annotations

import json
import random
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

CATALOG = {
    "grocery": [("GRC-MILK", 3.49), ("GRC-EGGS", 4.99), ("GRC-BREAD", 2.79), ("GRC-COFFEE", 11.99)],
    "electronics": [("ELC-EARBUDS", 79.00), ("ELC-CHARGER", 24.99), ("ELC-TV55", 449.00)],
    "home": [("HOM-TOWELS", 19.99), ("HOM-LAMP", 34.50), ("HOM-PAN", 27.00)],
    "apparel": [("APP-TSHIRT", 12.00), ("APP-JEANS", 39.99), ("APP-JACKET", 89.00)],
}
STORES = ["ST-0101", "ST-0207", "ST-0315", "ST-0422", "ST-0530"]
CHANNELS = ["web", "mobile", "store"]


@dataclass
class FaultRates:
    duplicate: float = 0.03
    late: float = 0.02
    malformed: float = 0.02


def _iso(ts: datetime) -> str:
    return ts.isoformat()


def _items(rng: random.Random) -> list[dict]:
    picks: dict[str, dict] = {}
    for _ in range(rng.randint(1, 4)):
        category = rng.choice(list(CATALOG))
        sku, price = rng.choice(CATALOG[category])
        picks[sku] = {
            "sku": sku,
            "category": category,
            "quantity": rng.randint(1, 3),
            "unit_price": f"{price:.2f}",
        }
    return list(picks.values())


def _lifecycle(rng: random.Random) -> list[str]:
    roll = rng.random()
    if roll < 0.08:
        return ["PLACED", "CANCELLED"]
    if roll < 0.13:
        return ["PLACED", "PAID", "SHIPPED", "DELIVERED", "REFUNDED"]
    if roll < 0.25:
        return ["PLACED", "PAID", "SHIPPED"]  # still in flight
    return ["PLACED", "PAID", "SHIPPED", "DELIVERED"]


def _malform(event: dict, rng: random.Random) -> dict:
    bad = dict(event)
    choice = rng.randint(0, 4)
    if choice == 0:
        bad.pop("customer_id")
    elif choice == 1:
        bad["items"] = []
    elif choice == 2:
        bad["channel"] = "fax"
    elif choice == 3:
        bad["event_ts"] = bad["event_ts"][:19]  # strip timezone
    else:
        bad["items"] = [dict(bad["items"][0], quantity=-2)]
    return bad


def generate_events(
    n_orders: int,
    start: datetime | None = None,
    seed: int = 42,
    faults: FaultRates | None = None,
) -> Iterator[dict]:
    """Yield raw event dicts in roughly arrival order."""
    rng = random.Random(seed)
    faults = faults or FaultRates()
    start = start or datetime(2026, 10, 1, 8, 0, tzinfo=timezone.utc)

    pending: list[tuple[datetime, dict]] = []
    for n in range(n_orders):
        placed_at = start + timedelta(seconds=n * rng.randint(20, 90))
        order = {
            "order_id": f"ORD-{n:07d}",
            "customer_id": f"CUS-{rng.randint(1, max(10, n_orders // 4)):06d}",
            "store_id": rng.choice(STORES),
            "channel": rng.choice(CHANNELS),
            "currency": "USD",
            "items": _items(rng),
        }
        ts = placed_at
        for status in _lifecycle(rng):
            event = {
                "event_id": uuid.UUID(int=rng.getrandbits(128)).hex,
                **order,
                "status": status,
                "event_ts": _iso(ts),
                "schema_version": 1,
            }
            arrival = ts
            if rng.random() < faults.late:
                arrival = ts + timedelta(hours=rng.randint(2, 30))
            if rng.random() < faults.malformed:
                event = _malform(event, rng)
            pending.append((arrival, event))
            if rng.random() < faults.duplicate:
                pending.append((arrival + timedelta(seconds=rng.randint(1, 120)), dict(event)))
            ts += timedelta(minutes=rng.randint(5, 60 * 24))

    pending.sort(key=lambda pair: pair[0])
    for _, event in pending:
        yield event


def write_jsonl(path: str, events: Iterator[dict]) -> int:
    count = 0
    with open(path, "w", encoding="utf-8") as fh:
        for event in events:
            fh.write(json.dumps(event) + "\n")
            count += 1
    return count
