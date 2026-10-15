"""Bronze ingestion: validate raw events and land them as Parquet.

Valid events go to bronze/order_events, partitioned by event date. Invalid
events go to the dead letter queue with the validation error, so they can be
inspected and replayed after the producer is fixed. Offsets are committed only
after both writes succeed.
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import ValidationError

from .schemas import OrderEvent
from .sources import Source

log = logging.getLogger(__name__)

BRONZE_SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("order_id", pa.string()),
        ("customer_id", pa.string()),
        ("store_id", pa.string()),
        ("channel", pa.string()),
        ("status", pa.string()),
        ("event_ts", pa.timestamp("us", tz="UTC")),
        ("currency", pa.string()),
        ("items_json", pa.string()),
        ("order_total", pa.decimal128(12, 2)),
        ("schema_version", pa.int32()),
        ("source", pa.string()),
        ("source_offset", pa.int64()),
        ("ingested_at", pa.timestamp("us", tz="UTC")),
        ("event_date", pa.string()),
    ]
)

DLQ_SCHEMA = pa.schema(
    [
        ("source", pa.string()),
        ("source_offset", pa.int64()),
        ("raw", pa.string()),
        ("error", pa.string()),
        ("ingested_at", pa.timestamp("us", tz="UTC")),
    ]
)


@dataclass
class IngestStats:
    read: int = 0
    valid: int = 0
    invalid: int = 0
    batches: int = 0
    errors_by_type: dict[str, int] = field(default_factory=dict)


def _bronze_row(event: OrderEvent, source: str, offset: int, now: datetime) -> dict:
    ts = event.event_ts.astimezone(timezone.utc)
    return {
        "event_id": event.event_id,
        "order_id": event.order_id,
        "customer_id": event.customer_id,
        "store_id": event.store_id,
        "channel": event.channel,
        "status": event.status.value,
        "event_ts": ts,
        "currency": event.currency,
        "items_json": json.dumps([i.model_dump(mode="json") for i in event.items]),
        "order_total": event.order_total,
        "schema_version": event.schema_version,
        "source": source,
        "source_offset": offset,
        "ingested_at": now,
        "event_date": ts.date().isoformat(),
    }


def _summarize(err: ValidationError) -> str:
    first = err.errors()[0]
    loc = ".".join(str(p) for p in first["loc"]) or "<root>"
    return f"{loc}: {first['msg']}"


def ingest(source: Source, lake_dir: str | Path, batch_size: int = 500) -> IngestStats:
    lake = Path(lake_dir)
    bronze_dir = lake / "bronze" / "order_events"
    dlq_dir = lake / "dlq" / "order_events"
    stats = IngestStats()

    for batch in source.batches(batch_size):
        now = datetime.now(timezone.utc)
        good, bad = [], []
        for offset, raw in batch:
            stats.read += 1
            try:
                event = OrderEvent.model_validate_json(raw)
                good.append(_bronze_row(event, source.name, offset, now))
            except ValidationError as err:
                reason = _summarize(err)
                field_name = reason.split(":")[0]
                stats.errors_by_type[field_name] = stats.errors_by_type.get(field_name, 0) + 1
                bad.append(
                    {
                        "source": source.name,
                        "source_offset": offset,
                        "raw": raw.decode("utf-8", errors="replace"),
                        "error": reason,
                        "ingested_at": now,
                    }
                )

        batch_id = uuid.uuid4().hex[:12]
        if good:
            pq.write_to_dataset(
                pa.Table.from_pylist(good, schema=BRONZE_SCHEMA),
                root_path=str(bronze_dir),
                partition_cols=["event_date"],
                basename_template=f"part-{batch_id}-{{i}}.parquet",
            )
        if bad:
            dlq_dir.mkdir(parents=True, exist_ok=True)
            pq.write_table(
                pa.Table.from_pylist(bad, schema=DLQ_SCHEMA),
                dlq_dir / f"part-{batch_id}.parquet",
            )

        source.commit(batch[-1][0])
        stats.valid += len(good)
        stats.invalid += len(bad)
        stats.batches += 1
        log.info("batch %s: %d valid, %d to dlq", batch_id, len(good), len(bad))

    return stats
