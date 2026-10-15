"""Data quality gates.

Each check is a SQL query that returns the number of offending rows. A check
with severity "error" fails the run when it finds any; "warn" checks are
reported but do not block. Thresholded checks compare a ratio instead.
"""

from __future__ import annotations

from dataclasses import dataclass
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


def run_checks(lake_dir: str | Path, max_dlq_ratio: float = 0.05) -> list[CheckResult]:
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
        return results
    finally:
        con.close()


def failed(results: list[CheckResult]) -> list[CheckResult]:
    return [r for r in results if not r.passed and r.check.severity == "error"]
