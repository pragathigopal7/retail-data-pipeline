"""Event sources and sinks.

The pipeline reads from an abstract `Source` that yields (offset, raw_bytes)
pairs. `FileSource` replays a JSONL file and is what tests and the local demo
use. `KafkaSource` reads a real topic (Kafka or Redpanda) and commits offsets
only after a batch has been durably written to bronze, giving at least once
delivery; duplicates are removed downstream by event_id.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Protocol


class Source(Protocol):
    name: str

    def batches(self, batch_size: int) -> Iterator[list[tuple[int, bytes]]]: ...

    def commit(self, last_offset: int) -> None: ...


class FileSource:
    """Replays a JSONL file. Offsets are line numbers, checkpointed to disk."""

    def __init__(self, path: str | Path, checkpoint_dir: str | Path):
        self.path = Path(path)
        self.name = f"file:{self.path.name}"
        self._ckpt = Path(checkpoint_dir) / f"{self.path.name}.offset"
        self._ckpt.parent.mkdir(parents=True, exist_ok=True)

    def _start_offset(self) -> int:
        return int(self._ckpt.read_text()) + 1 if self._ckpt.exists() else 0

    def batches(self, batch_size: int) -> Iterator[list[tuple[int, bytes]]]:
        start = self._start_offset()
        batch: list[tuple[int, bytes]] = []
        with self.path.open("rb") as fh:
            for offset, line in enumerate(fh):
                if offset < start or not line.strip():
                    continue
                batch.append((offset, line.rstrip(b"\n")))
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
        if batch:
            yield batch

    def commit(self, last_offset: int) -> None:
        tmp = self._ckpt.with_suffix(".tmp")
        tmp.write_text(str(last_offset))
        tmp.replace(self._ckpt)  # atomic on POSIX


class KafkaSource:
    """Consumes a Kafka topic with manual offset commits."""

    def __init__(self, bootstrap: str, topic: str, group_id: str, idle_timeout_s: float = 5.0):
        from confluent_kafka import Consumer  # optional dependency

        self.name = f"kafka:{topic}"
        self.topic = topic
        self.idle_timeout_s = idle_timeout_s
        self._consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": group_id,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
            }
        )
        self._consumer.subscribe([topic])

    def batches(self, batch_size: int) -> Iterator[list[tuple[int, bytes]]]:
        while True:
            msgs = self._consumer.consume(num_messages=batch_size, timeout=self.idle_timeout_s)
            msgs = [m for m in msgs if m.error() is None]
            if not msgs:
                return
            yield [(m.offset(), m.value()) for m in msgs]

    def commit(self, last_offset: int) -> None:
        self._consumer.commit(asynchronous=False)

    def close(self) -> None:
        self._consumer.close()


def produce_to_kafka(bootstrap: str, topic: str, events: Iterator[dict]) -> int:
    """Publish events keyed by order_id so one order's events stay in one partition."""
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True})
    count = 0
    for event in events:
        key = str(event.get("order_id", "")).encode()
        producer.produce(topic, key=key, value=json.dumps(event).encode())
        count += 1
        if count % 1000 == 0:
            producer.flush()
    producer.flush()
    return count
