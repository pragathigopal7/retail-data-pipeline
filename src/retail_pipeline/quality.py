"""Data quality gates.

Each check is a SQL query that returns the number of offending rows. A check
with severity "error" fails the run when it finds any; "warn" checks are
reported but do not block. Thresholded checks compare a ratio instead.

Every run is recorded. Results are appended to the `quality_results` table in
the warehouse, so trends can be queried, and written as a Prometheus textfile
(`metrics/quality.prom`) that a node_exporter textfile collector can scrape and
alert on.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from .transform import connect


@dataclass(frozen=True)
class Check:
    name: str
    sql: str
    severity: str = "error"  # "error" | "warn"
    max_ratio: float | None = None  # when set, sql must return (bad, total)


@dataclass
class CheckResult:
    check: Check
    observed: float
    passed: bool

    def __str__(self) -> str:
        mark = "PASS" if self.passed else ("FAIL" if self.check.severity == "error" else "WARN")
        unit = "ratio" if self.check.max_ratio is not None else "rows"
        return f"[{mark}] {self.check.name}: {self.observed:g} {unit}"


CHECKS: list[Check] = [
    Check(
        "silver_order_events.event_id is unique",
        "SELECT COUNT(*) - COUNT(DISTINCT event_id) FROM silver_order_events",
    ),
    Check(
        "silver_orders.order_id is unique",
        "SELECT COUNT(*) - COUNT(DISTINCT order_id) FROM silver_orders",
    ),
    Check(
        "order totals are positive",
        "SELECT COUNT(*) FROM silver_orders WHERE order_total <= 0",
    ),
    Check(
        "line items reconcile to order total",
        """
        SELECT COUNT(*) FROM (
            SELECT o.order_id
            FROM silver_orders o JOIN silver_order_items i USING (order_id)
            GROUP BY o.order_id, o.order_total
            HAVING ABS(SUM(i.line_total) - o.order_total) > 0.01
        )
        """,
    ),
    Check(
        "delivered orders were shipped first",
        """
        SELECT COUNT(*) FROM silver_orders
        WHERE delivered_at IS NOT NULL AND (shipped_at IS NULL OR shipped_at > delivered_at)
        """,
        severity="warn",
    ),
    Check(
        "gold net revenue never exceeds gross",
        "SELECT COUNT(*) FROM gold_daily_store_sales WHERE net_revenue > gross_revenue",
    ),
    Check(
        "line discounts never exceed line gross",
        "SELECT COUNT(*) FROM silver_order_items WHERE discount < 0 OR discount > line_total",
    ),
    Check(
        "payment mix reconciles with daily sales",
        """
        SELECT COUNT(*) FROM (
            SELECT order_date, channel, SUM(orders) AS orders FROM gold_daily_store_sales
            GROUP BY ALL
        ) s FULL JOIN (
            SELECT order_date, channel, SUM(orders) AS orders FROM gold_payment_mix
            GROUP BY ALL
        ) p USING (order_date, channel)
        WHERE s.orders IS DISTINCT FROM p.orders
        """,
    ),
    Check(
        "orders missing their PLACED event",
        "SELECT COUNT(*) FILTER (WHERE placed_at IS NULL), COUNT(*) FROM silver_orders",
        severity="warn",
        max_ratio=0.05,
    ),
]


def dlq_ratio_check(lake_dir: str | Path, max_ratio: float) -> Check:
    dlq = (Path(lake_dir) / "dlq" / "order_events" / "*.parquet").as_posix()
    has_dlq = any((Path(lake_dir) / "dlq" / "order_events").glob("*.parquet"))
    bad = f"(SELECT COUNT(*) FROM read_parquet('{dlq}'))" if has_dlq else "0"
    return Check(
        "dead letter ratio within budget",
        f"SELECT {bad}, {bad} + (SELECT COUNT(*) FROM bronze_order_events)",
        max_ratio=max_ratio,
    )


RESULTS_DDL = """
CREATE TABLE IF NOT EXISTS quality_results (
    run_id     VARCHAR,
    run_at     TIMESTAMPTZ,
    check_name VARCHAR,
    severity   VARCHAR,
    observed   DOUBLE,
    threshold  DOUBLE,
    passed     BOOLEAN
)
"""


def _metric_name(check_name: str) -> str:
    return "".join(c if c.isalnum() else "_" for c in check_name.lower()).strip("_")


def _prometheus(results: list[CheckResult], run_at: datetime) -> str:
    lines = [
        "# HELP retail_quality_check_observed Offending rows, or ratio for thresholded checks.",
        "# TYPE retail_quality_check_observed gauge",
    ]
    for r in results:
        labels = f'check="{_metric_name(r.check.name)}",severity="{r.check.severity}"'
        lines.append(f"retail_quality_check_observed{{{labels}}} {r.observed:g}")
    lines += [
        "# HELP retail_quality_check_passed 1 when the check passed.",
        "# TYPE retail_quality_check_passed gauge",
    ]
    for r in results:
        labels = f'check="{_metric_name(r.check.name)}",severity="{r.check.severity}"'
        lines.append(f"retail_quality_check_passed{{{labels}}} {int(r.passed)}")
    lines += [
        "# HELP retail_quality_blocking_failures Blocking checks that failed in the last run.",
        "# TYPE retail_quality_blocking_failures gauge",
        f"retail_quality_blocking_failures {len(failed(results))}",
        "# HELP retail_quality_last_run_timestamp_seconds Unix time of the last check run.",
        "# TYPE retail_quality_last_run_timestamp_seconds gauge",
        f"retail_quality_last_run_timestamp_seconds {int(run_at.timestamp())}",
    ]
    return "\n".join(lines) + "\n"


def record_results(
    con, lake_dir: str | Path, results: list[CheckResult], run_at: datetime | None = None
) -> str:
    """Append results to `quality_results` and refresh the Prometheus textfile."""
    run_at = run_at or datetime.now(timezone.utc)
    run_id = uuid.uuid4().hex[:12]
    con.execute(RESULTS_DDL)
    con.executemany(
        "INSERT INTO quality_results VALUES (?, ?::TIMESTAMPTZ, ?, ?, ?, ?, ?)",
        [
            (
                run_id,
                run_at.isoformat(),
                r.check.name,
                r.check.severity,
                float(r.observed),
                r.check.max_ratio,
                r.passed,
            )
            for r in results
        ],
    )
    metrics = Path(lake_dir) / "metrics"
    metrics.mkdir(parents=True, exist_ok=True)
    tmp = metrics / "quality.prom.tmp"
    tmp.write_text(_prometheus(results, run_at), encoding="utf-8")
    tmp.replace(metrics / "quality.prom")  # atomic, so a scrape never sees half a file
    return run_id


def history(lake_dir: str | Path, limit: int = 10) -> list[tuple]:
    """Most recent runs: (run_id, run_at, checks, passed, warnings, blocking failures)."""
    con = connect(lake_dir)
    try:
        con.execute(RESULTS_DDL)
        return con.execute(
            """
            SELECT
                run_id,
                strftime(MIN(run_at) AT TIME ZONE 'UTC', '%Y-%m-%d %H:%M:%S') AS run_at,
                COUNT(*)                                                      AS checks,
                COUNT(*) FILTER (WHERE passed)                                AS passed,
                COUNT(*) FILTER (WHERE NOT passed AND severity = 'warn')      AS warnings,
                COUNT(*) FILTER (WHERE NOT passed AND severity = 'error')     AS failures
            FROM quality_results
            GROUP BY run_id
            ORDER BY MIN(run_at) DESC
            LIMIT ?
            """,
            [limit],
        ).fetchall()
    finally:
        con.close()


def run_checks(
    lake_dir: str | Path, max_dlq_ratio: float = 0.05, record: bool = True
) -> list[CheckResult]:
    from .transform import register_bronze

    con = connect(lake_dir)
    try:
        register_bronze(con, lake_dir)
        results = []
        for check in [*CHECKS, dlq_ratio_check(lake_dir, max_dlq_ratio)]:
            row = con.execute(check.sql).fetchone()
            if check.max_ratio is not None:
                bad, total = row
                observed = round(bad / total, 4) if total else 0.0
                passed = observed <= check.max_ratio
            else:
                observed = row[0]
                passed = observed == 0
            results.append(CheckResult(check, observed, passed))
        if record:
            record_results(con, lake_dir, results)
        return results
    finally:
        con.close()


def failed(results: list[CheckResult]) -> list[CheckResult]:
    return [r for r in results if not r.passed and r.check.severity == "error"]
