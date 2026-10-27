"""Command line entry point.

    retail-pipeline generate --orders 5000 --out data/events.jsonl
    retail-pipeline ingest   --file data/events.jsonl
    retail-pipeline transform
    retail-pipeline check    --report lake/quality.json
    retail-pipeline run      --orders 5000          # all of the above

Kafka mode:
    retail-pipeline produce --orders 5000 --bootstrap localhost:19092
    retail-pipeline ingest  --kafka localhost:19092
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from .generator import generate_events, write_jsonl
from .ingest import ingest
from .quality import failed, run_checks, write_report
from .sources import FileSource, KafkaSource, produce_to_kafka
from .transform import run_models

TOPIC = "retail.orders.v1"


def _cmd_generate(a: argparse.Namespace) -> int:
    Path(a.out).parent.mkdir(parents=True, exist_ok=True)
    n = write_jsonl(a.out, generate_events(a.orders, seed=a.seed))
    print(f"wrote {n} events to {a.out}")
    return 0


def _cmd_produce(a: argparse.Namespace) -> int:
    n = produce_to_kafka(a.bootstrap, a.topic, generate_events(a.orders, seed=a.seed))
    print(f"produced {n} events to {a.topic}")
    return 0


def _cmd_ingest(a: argparse.Namespace) -> int:
    if a.kafka:
        source = KafkaSource(a.kafka, a.topic, group_id="retail-bronze-ingest")
    else:
        source = FileSource(a.file, Path(a.lake) / "_checkpoints")
    stats = ingest(source, a.lake, batch_size=a.batch_size)
    print(
        f"ingest: read={stats.read} valid={stats.valid} dlq={stats.invalid} batches={stats.batches}"
    )
    for field_name, count in sorted(stats.errors_by_type.items(), key=lambda kv: -kv[1]):
        print(f"  dlq reason {field_name}: {count}")
    return 0


def _cmd_transform(a: argparse.Namespace) -> int:
    for table, rows in run_models(a.lake).items():
        print(f"  {table:<28} {rows:>8} rows")
    return 0


def _cmd_check(a: argparse.Namespace) -> int:
    results = run_checks(a.lake, max_dlq_ratio=a.max_dlq_ratio)
    for r in results:
        print(r)
    if getattr(a, "report", None):
        print(f"wrote quality report to {write_report(results, a.report)}")
    if failed(results):
        print(f"{len(failed(results))} blocking check(s) failed", file=sys.stderr)
        return 1
    return 0


def _cmd_run(a: argparse.Namespace) -> int:
    a.out = str(Path(a.lake) / "landing" / "events.jsonl")
    a.file = a.out
    a.kafka = None
    for step in (_cmd_generate, _cmd_ingest, _cmd_transform, _cmd_check):
        code = step(a)
        if code:
            return code
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="retail-pipeline")
    p.add_argument("--lake", default="lake", help="lakehouse root directory")
    p.add_argument("-v", "--verbose", action="store_true")
    sub = p.add_subparsers(dest="cmd", required=True)

    g = sub.add_parser("generate", help="write synthetic events to JSONL")
    g.add_argument("--orders", type=int, default=5000)
    g.add_argument("--seed", type=int, default=42)
    g.add_argument("--out", default="data/events.jsonl")
    g.set_defaults(fn=_cmd_generate)

    pr = sub.add_parser("produce", help="publish synthetic events to Kafka")
    pr.add_argument("--orders", type=int, default=5000)
    pr.add_argument("--seed", type=int, default=42)
    pr.add_argument("--bootstrap", default="localhost:19092")
    pr.add_argument("--topic", default=TOPIC)
    pr.set_defaults(fn=_cmd_produce)

    i = sub.add_parser("ingest", help="validate events into bronze + dlq")
    src = i.add_mutually_exclusive_group(required=True)
    src.add_argument("--file")
    src.add_argument("--kafka", metavar="BOOTSTRAP")
    i.add_argument("--topic", default=TOPIC)
    i.add_argument("--batch-size", type=int, default=500)
    i.set_defaults(fn=_cmd_ingest)

    t = sub.add_parser("transform", help="build silver and gold models")
    t.set_defaults(fn=_cmd_transform)

    c = sub.add_parser("check", help="run data quality gates")
    c.add_argument("--max-dlq-ratio", type=float, default=0.05)
    c.add_argument("--report", metavar="PATH", help="also write results as JSON")
    c.set_defaults(fn=_cmd_check)

    r = sub.add_parser("run", help="generate, ingest, transform and check end to end")
    r.add_argument("--orders", type=int, default=5000)
    r.add_argument("--seed", type=int, default=42)
    r.add_argument("--batch-size", type=int, default=500)
    r.add_argument("--max-dlq-ratio", type=float, default=0.05)
    r.add_argument("--report", metavar="PATH", help="also write quality results as JSON")
    r.set_defaults(fn=_cmd_run)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
