import json
from datetime import datetime, timedelta, timezone

import duckdb
import pyarrow.parquet as pq
import pytest
from pydantic import ValidationError

from retail_pipeline import transform
from retail_pipeline.generator import FaultRates, generate_events, write_jsonl
from retail_pipeline.ingest import ingest
from retail_pipeline.quality import failed, history, run_checks
from retail_pipeline.schemas import (
    LATEST_SCHEMA_VERSION,
    OrderEvent,
    parse_event,
    validate_event,
)
from retail_pipeline.sources import FileSource
from retail_pipeline.transform import Model, run_models, run_transform

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
    clean = FaultRates(duplicate=0, late=0, malformed=0, unknown_version=0)
    versions = set()
    for raw in generate_events(200, seed=1, faults=clean):
        versions.add(validate_event(raw)[1])
    assert versions == {1, 2}  # the run spans the v2 producer rollout


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


# schema evolution --------------------------------------------------------------


def v2(event: dict, payment_method: str = "card", discount: str = "1.00") -> dict:
    items = [dict(event["items"][0], discount=discount), *event["items"][1:]]
    return {**event, "schema_version": 2, "payment_method": payment_method, "items": items}


def test_v1_event_is_upcast_to_latest():
    event, declared = validate_event(make_event("PLACED", 0, schema_version=1))
    assert declared == 1 and event.schema_version == LATEST_SCHEMA_VERSION
    assert event.payment_method is None and event.discount_total == 0


def test_v2_event_keeps_new_fields():
    event = parse_event(json.dumps(v2(make_event("PAID", 5), "wallet", "0.98")))
    assert event.payment_method.value == "wallet"
    assert str(event.discount_total) == "0.98"


def test_v2_rejects_discount_above_line_gross():
    with pytest.raises(ValidationError):
        parse_event(v2(make_event("PLACED", 0), discount="99.00"))


def test_unknown_schema_version_goes_to_dlq(tmp_path):
    events = [make_event("PLACED", 0), make_event("PAID", 5, schema_version=3)]
    _, stats = run_file(tmp_path, events)
    assert (stats.valid, stats.invalid) == (1, 1)
    assert stats.errors_by_type == {"schema_version": 1}


def test_mixed_versions_in_one_order(tmp_path):
    # Producer is upgraded mid order: PLACED is v1, PAID onwards is v2.
    events = [make_event("PLACED", 0), v2(make_event("PAID", 5), "gift_card", "2.00")]
    lake, _ = run_file(tmp_path, events)
    run_models(lake)
    con = duckdb.connect(str(lake / "warehouse.duckdb"))
    method, version = con.execute(
        "SELECT payment_method, source_schema_version FROM silver_orders"
    ).fetchone()
    assert (method, version) == ("gift_card", 2)
    # order_total comes from the first event (v1, no discount), so net equals gross
    gross, net = con.execute(
        "SELECT gross_revenue, net_revenue FROM gold_daily_store_sales"
    ).fetchone()
    assert float(gross) == float(net) == pytest.approx(31.97)


def test_bronze_written_before_v2_is_still_readable(tmp_path):
    lake, _ = run_file(tmp_path, [make_event("PLACED", 0)])
    # Rewrite the bronze file as an old writer would have, without the v2 columns.
    files = list((lake / "bronze" / "order_events").rglob("*.parquet"))
    old = pq.read_table(files[0]).drop_columns(
        ["discount_total", "payment_method", "source_schema_version"]
    )
    pq.write_table(old, files[0])
    write_jsonl(str(tmp_path / "more.jsonl"), iter([v2(make_event("PAID", 5))]))
    ingest(FileSource(tmp_path / "more.jsonl", lake / "_checkpoints"), lake)
    counts = run_models(lake)
    assert counts["silver_order_events"] == 2


# incremental models ------------------------------------------------------------


def ingest_in_parts(tmp_path, events: list[dict], parts: int):
    lake = tmp_path / "lake"
    size = -(-len(events) // parts)
    results = []
    for k in range(parts):
        path = tmp_path / f"part{k}.jsonl"
        write_jsonl(str(path), iter(events[k * size : (k + 1) * size]))
        ingest(FileSource(path, lake / "_checkpoints"), lake)
        results.append(run_transform(lake))
    return lake, results


def table_snapshot(lake, tables) -> dict[str, list]:
    con = duckdb.connect(str(lake / "warehouse.duckdb"))
    try:
        # Rows rendered as text in SQL, so timestamps never need converting in Python.
        sql = "SELECT list(r::VARCHAR ORDER BY r::VARCHAR) FROM {} r"
        return {t: con.execute(sql.format(t)).fetchone()[0] for t in tables}
    finally:
        con.close()


def test_incremental_runs_match_full_refresh(tmp_path):
    lake, results = ingest_in_parts(tmp_path, list(generate_events(600, seed=5)), parts=4)
    assert results[0].mode == "full_refresh"
    assert [r.mode for r in results[1:]] == ["incremental"] * 3
    assert all(r.changed_orders < 600 for r in results[1:])
    tables = list(results[-1].counts)
    incremental = table_snapshot(lake, tables)
    run_transform(lake, full_refresh=True)
    assert table_snapshot(lake, tables) == incremental


def test_rerun_without_new_data_is_a_noop(tmp_path):
    lake, _ = run_file(tmp_path, [make_event("PLACED", 0)])
    assert run_transform(lake).mode == "full_refresh"
    assert run_transform(lake).mode == "noop"


def test_late_placed_event_moves_order_to_earlier_day(tmp_path):
    # PAID lands first, so the order is dated by PAID. A late PLACED from the
    # previous day must move it, and the old day must not keep a stale row.
    lake, _ = ingest_in_parts(
        tmp_path, [make_event("PAID", 5), make_event("PLACED", -24 * 60)], parts=2
    )
    con = duckdb.connect(str(lake / "warehouse.duckdb"))
    days = con.execute("SELECT order_date, orders FROM gold_daily_store_sales").fetchall()
    assert [(str(d), n) for d, n in days] == [("2026-09-30", 1)]


def test_failed_transform_rolls_back_everything(tmp_path, monkeypatch):
    lake, _ = run_file(tmp_path, [make_event("PLACED", 0)])
    run_transform(lake)
    write_jsonl(str(tmp_path / "more.jsonl"), iter([make_event("PAID", 5)]))
    ingest(FileSource(tmp_path / "more.jsonl", lake / "_checkpoints"), lake)

    broken = Model.parse("99_gold_broken.sql", "-- materialized: table\nSELECT * FROM nope")
    good = transform.load_models()
    monkeypatch.setattr(transform, "load_models", lambda: [*good, broken])
    with pytest.raises(duckdb.CatalogException):
        run_transform(lake)

    monkeypatch.setattr(transform, "load_models", lambda: good)
    result = run_transform(lake)  # watermark did not move, so PAID is still new
    assert result.mode == "incremental" and result.new_events == 1
    assert result.counts["silver_order_events"] == 2


def test_model_header_validation():
    with pytest.raises(ValueError):
        Model.parse("01_x.sql", "-- materialized: incremental\nSELECT 1")
    with pytest.raises(ValueError):
        Model.parse("01_x.sql", "-- materialized: snapshot\nSELECT 1")


# quality history ---------------------------------------------------------------


def test_quality_runs_are_recorded(tmp_path):
    lake, _ = run_file(tmp_path, [make_event("PLACED", 0), make_event("PAID", 5)])
    run_models(lake)
    run_checks(lake)
    results = run_checks(lake)
    runs = history(lake)
    assert len(runs) == 2 and runs[0][2] == len(results)
    prom = (lake / "metrics" / "quality.prom").read_text()
    assert "retail_quality_blocking_failures 0" in prom
    assert 'retail_quality_check_passed{check="order_totals_are_positive"' in prom
