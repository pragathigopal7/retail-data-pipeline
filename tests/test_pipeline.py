from datetime import datetime, timedelta, timezone

import duckdb
import pytest
from pydantic import ValidationError

from retail_pipeline.generator import FaultRates, generate_events, write_jsonl
from retail_pipeline.ingest import ingest
from retail_pipeline.quality import failed, run_checks
from retail_pipeline.schemas import OrderEvent
from retail_pipeline.sources import FileSource
from retail_pipeline.transform import run_models

T0 = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)


def make_event(status: str, minutes: int, event_id: str | None = None, **overrides) -> dict:
    event = {
        "event_id": event_id or f"evt-{status.lower()}-{minutes:04d}",
        "order_id": "ORD-0000001",
        "customer_id": "CUS-000001",
        "store_id": "ST-0101",
        "channel": "web",
        "status": status,
        "event_ts": (T0 + timedelta(minutes=minutes)).isoformat(),
        "currency": "USD",
        "items": [
            {"sku": "GRC-MILK", "category": "grocery", "quantity": 2, "unit_price": "3.49"},
            {"sku": "ELC-CHARGER", "category": "electronics", "quantity": 1, "unit_price": "24.99"},
        ],
    }
    event.update(overrides)
    return event


def run_file(tmp_path, events: list[dict], name: str = "events.jsonl"):
    path = tmp_path / name
    write_jsonl(str(path), iter(events))
    lake = tmp_path / "lake"
    stats = ingest(FileSource(path, lake / "_checkpoints"), lake, batch_size=3)
    return lake, stats


# schema contract ---------------------------------------------------------------


def test_valid_event_computes_total():
    event = OrderEvent.model_validate(make_event("PLACED", 0))
    assert str(event.order_total) == "31.97"


@pytest.mark.parametrize(
    "override",
    [
        {"channel": "fax"},
        {"items": []},
        {"currency": "usd"},
        {"event_ts": "2026-10-01T12:00:00"},  # no timezone
    ],
)
def test_contract_rejects_bad_events(override):
    with pytest.raises(ValidationError):
        OrderEvent.model_validate(make_event("PLACED", 0, **override))


def test_contract_rejects_duplicate_skus():
    item = {"sku": "GRC-MILK", "category": "grocery", "quantity": 1, "unit_price": "3.49"}
    with pytest.raises(ValidationError):
        OrderEvent.model_validate(make_event("PLACED", 0, items=[item, item]))


# generator ---------------------------------------------------------------------


def test_generator_is_deterministic():
    assert list(generate_events(50, seed=7)) == list(generate_events(50, seed=7))


def test_generator_without_faults_is_all_valid():
    clean = FaultRates(duplicate=0, late=0, malformed=0)
    for raw in generate_events(200, seed=1, faults=clean):
        OrderEvent.model_validate(raw)


# ingestion ---------------------------------------------------------------------


def test_invalid_events_go_to_dlq(tmp_path):
    events = [make_event("PLACED", 0), make_event("PAID", 5, channel="fax")]
    lake, stats = run_file(tmp_path, events)
    assert (stats.valid, stats.invalid) == (1, 1)
    dlq = duckdb.sql(f"SELECT error FROM '{lake}/dlq/order_events/*.parquet'").fetchall()
    assert dlq[0][0].startswith("channel")


def test_checkpoint_makes_reingest_a_noop(tmp_path):
    path = tmp_path / "events.jsonl"
    write_jsonl(str(path), iter([make_event("PLACED", 0), make_event("PAID", 5)]))
    lake = tmp_path / "lake"
    first = ingest(FileSource(path, lake / "_checkpoints"), lake)
    second = ingest(FileSource(path, lake / "_checkpoints"), lake)
    assert first.read == 2 and second.read == 0


# transforms --------------------------------------------------------------------


def test_duplicates_are_removed(tmp_path):
    placed = make_event("PLACED", 0)
    lake, _ = run_file(tmp_path, [placed, dict(placed), dict(placed)])
    counts = run_models(lake)
    assert counts["silver_order_events"] == 1


def test_late_event_does_not_regress_status(tmp_path):
    # PAID arrives last (late), but DELIVERED is later in event time and must win.
    events = [
        make_event("PLACED", 0),
        make_event("SHIPPED", 60),
        make_event("DELIVERED", 600),
        make_event("PAID", 5),
    ]
    lake, _ = run_file(tmp_path, events)
    run_models(lake)
    con = duckdb.connect(str(lake / "warehouse.duckdb"))
    status, has_paid, event_count = con.execute(
        "SELECT current_status, paid_at IS NOT NULL, event_count FROM silver_orders"
    ).fetchone()
    assert status == "DELIVERED"
    assert has_paid and event_count == 4


def test_cancelled_orders_excluded_from_net_revenue(tmp_path):
    events = [make_event("PLACED", 0), make_event("CANCELLED", 10)]
    lake, _ = run_file(tmp_path, events)
    run_models(lake)
    con = duckdb.connect(str(lake / "warehouse.duckdb"))
    gross, net = con.execute(
        "SELECT gross_revenue, COALESCE(net_revenue, 0) FROM gold_daily_store_sales"
    ).fetchone()
    assert float(gross) == pytest.approx(31.97) and float(net) == 0


# end to end --------------------------------------------------------------------


def test_end_to_end_quality_gates_pass(tmp_path):
    path = tmp_path / "events.jsonl"
    write_jsonl(str(path), generate_events(800, seed=3))
    lake = tmp_path / "lake"
    stats = ingest(FileSource(path, lake / "_checkpoints"), lake)
    assert stats.invalid > 0  # faults were injected and caught
    run_models(lake)
    results = run_checks(lake)
    assert not failed(results), [str(r) for r in results]
    assert (lake / "gold" / "daily_store_sales.parquet").exists()


def test_dlq_budget_blocks_bad_feed(tmp_path):
    bad_feed = [make_event("PLACED", i, event_id=f"evt-{i:05d}", channel="fax") for i in range(20)]
    lake, _ = run_file(tmp_path, [make_event("PLACED", 0), *bad_feed])
    run_models(lake)
    names = [r.check.name for r in failed(run_checks(lake, max_dlq_ratio=0.05))]
    assert "dead letter ratio within budget" in names
