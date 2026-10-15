"""Silver and gold transforms, run as ordered SQL models in DuckDB.

Each file in `sql/` is one model. Files run in name order, so silver models
(0x) build before gold models (1x). Gold tables are also exported as Parquet
so any engine (Spark, Trino, Athena) can read them.
"""

from __future__ import annotations

import logging
import time
from importlib import resources
from pathlib import Path

import duckdb

log = logging.getLogger(__name__)

GOLD_PREFIX = "gold_"


def _models() -> list[tuple[str, str]]:
    sql_dir = resources.files("retail_pipeline") / "sql"
    files = sorted(p for p in sql_dir.iterdir() if p.name.endswith(".sql"))
    return [(p.name, p.read_text(encoding="utf-8")) for p in files]


def connect(lake_dir: str | Path) -> duckdb.DuckDBPyConnection:
    lake = Path(lake_dir)
    lake.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(lake / "warehouse.duckdb"))


def register_bronze(con: duckdb.DuckDBPyConnection, lake_dir: str | Path) -> None:
    pattern = Path(lake_dir) / "bronze" / "order_events" / "**" / "*.parquet"
    con.execute(
        f"""
        CREATE OR REPLACE VIEW bronze_order_events AS
        SELECT * FROM read_parquet('{pattern.as_posix()}', hive_partitioning = true)
        """
    )


def run_models(lake_dir: str | Path) -> dict[str, int]:
    """Build every model and return row counts per table."""
    con = connect(lake_dir)
    try:
        register_bronze(con, lake_dir)
        counts: dict[str, int] = {}
        for name, sql in _models():
            started = time.perf_counter()
            con.execute(sql)
            table = name.split("_", 1)[1].removesuffix(".sql")
            counts[table] = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            log.info(
                "built %s (%d rows) in %.2fs", table, counts[table], time.perf_counter() - started
            )

        gold_dir = Path(lake_dir) / "gold"
        gold_dir.mkdir(parents=True, exist_ok=True)
        for table in counts:
            if table.startswith(GOLD_PREFIX):
                out = gold_dir / f"{table.removeprefix(GOLD_PREFIX)}.parquet"
                con.execute(f"COPY {table} TO '{out.as_posix()}' (FORMAT parquet)")
        return counts
    finally:
        con.close()
