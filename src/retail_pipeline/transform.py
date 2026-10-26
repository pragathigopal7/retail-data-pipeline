"""Silver and gold transforms, run as ordered SQL models in DuckDB.

Each file in `sql/` is one model: a SELECT plus a small header that says how to
materialize it, in the spirit of dbt.

    -- materialized: append | incremental | table
    -- unique_key: event_id | order_id | order_date

Files run in name order, so silver models (0x) build before gold models (1x).

Runs are incremental by default. A watermark on bronze `ingested_at` marks what
has already been processed. Each run:

1. snapshots the bronze high water mark and exposes only rows above the stored
   watermark as `new_bronze`
2. `append` models insert rows whose key is not already present (dedup across
   runs, keeping the first copy)
3. the set of changed `order_id`s (and the `order_date`s they used to and now
   map to) is exposed as `changed_order_id` / `changed_order_date`
4. `incremental` models delete and rebuild only the rows for changed keys
5. `table` models are small global aggregates and are rebuilt in full

Everything runs in one transaction with the watermark update, so a crash leaves
the warehouse and watermark exactly as they were. `full_refresh=True` rebuilds
every model from all of bronze and must produce the same tables.

Gold tables are also exported as Parquet so any engine (Spark, Trino, Athena)
can read them.
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, field
from importlib import resources
from pathlib import Path

import duckdb

log = logging.getLogger(__name__)

GOLD_PREFIX = "gold_"
STATE_TABLE = "_pipeline_state"
MATERIALIZATIONS = {"append", "incremental", "table"}
SCOPED_KEYS = {"order_id", "order_date"}
_HEADER = re.compile(r"^--\s*(materialized|unique_key)\s*:\s*(\w+)\s*$", re.MULTILINE)


@dataclass(frozen=True)
class Model:
    file: str
    table: str
    sql: str
    materialized: str = "table"
    unique_key: str | None = None

    @classmethod
    def parse(cls, file: str, sql: str) -> Model:
        meta = dict(_HEADER.findall(sql))
        model = cls(
            file=file,
            table=file.split("_", 1)[1].removesuffix(".sql"),
            sql=sql.strip().rstrip(";"),
            materialized=meta.get("materialized", "table"),
            unique_key=meta.get("unique_key"),
        )
        if model.materialized not in MATERIALIZATIONS:
            raise ValueError(f"{file}: unknown materialization {model.materialized!r}")
        if model.materialized != "table" and not model.unique_key:
            raise ValueError(f"{file}: {model.materialized} models need a unique_key")
        if model.materialized == "incremental" and model.unique_key not in SCOPED_KEYS:
            raise ValueError(f"{file}: incremental key must be one of {sorted(SCOPED_KEYS)}")
        return model


@dataclass
class TransformResult:
    mode: str  # "full_refresh" | "incremental" | "noop"
    new_events: int = 0
    changed_orders: int = 0
    changed_dates: int = 0
    counts: dict[str, int] = field(default_factory=dict)


def load_models() -> list[Model]:
    sql_dir = resources.files("retail_pipeline") / "sql"
    files = sorted(p for p in sql_dir.iterdir() if p.name.endswith(".sql"))
    return [Model.parse(p.name, p.read_text(encoding="utf-8")) for p in files]


def connect(lake_dir: str | Path) -> duckdb.DuckDBPyConnection:
    lake = Path(lake_dir)
    lake.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(lake / "warehouse.duckdb"))


def register_bronze(con: duckdb.DuckDBPyConnection, lake_dir: str | Path) -> None:
    # union_by_name lets bronze files written before a schema change (older
    # columns only) be read alongside newer ones; missing columns become NULL.
    pattern = Path(lake_dir) / "bronze" / "order_events" / "**" / "*.parquet"
    con.execute(
        f"""
        CREATE OR REPLACE VIEW bronze_order_events AS
        SELECT * FROM read_parquet(
            '{pattern.as_posix()}', hive_partitioning = true, union_by_name = true
        )
        """
    )


def _table_exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    row = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", [table]
    ).fetchone()
    return row[0] > 0


def _has_watermark(con: duckdb.DuckDBPyConnection) -> bool:
    if not _table_exists(con, STATE_TABLE):
        return False
    row = con.execute(
        f"SELECT COUNT(*) FROM {STATE_TABLE} WHERE name = 'bronze_ingested_at'"
    ).fetchone()
    return row[0] > 0


def _pin_bounds(con: duckdb.DuckDBPyConnection, full: bool) -> None:
    """Pin (low, high] for this run. Timestamps stay in SQL, never round trip via Python."""
    low = (
        "NULL::TIMESTAMPTZ"
        if full
        else f"(SELECT watermark FROM {STATE_TABLE} WHERE name = 'bronze_ingested_at')"
    )
    con.execute(
        f"CREATE OR REPLACE TEMP TABLE _bounds AS SELECT {low} AS low, "
        "(SELECT MAX(ingested_at) FROM bronze_order_events) AS high"
    )


def _advance_watermark(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        f"CREATE TABLE IF NOT EXISTS {STATE_TABLE} "
        "(name VARCHAR PRIMARY KEY, watermark TIMESTAMPTZ, updated_at TIMESTAMPTZ)"
    )
    con.execute(
        f"INSERT OR REPLACE INTO {STATE_TABLE} "
        "SELECT 'bronze_ingested_at', high, now() FROM _bounds WHERE high IS NOT NULL"
    )


_CHANGED_ORDER_DATES = """
        SELECT DISTINCT CAST(COALESCE(placed_at, first_event_at) AS DATE) AS order_date
        FROM silver_orders
        WHERE order_id IN (SELECT order_id FROM changed_order_id)
"""


def _materialize(con: duckdb.DuckDBPyConnection, model: Model, full: bool) -> None:
    exists = _table_exists(con, model.table)
    if full or not exists or model.materialized == "table":
        con.execute(f"CREATE OR REPLACE TABLE {model.table} AS {model.sql}")
    elif model.materialized == "append":
        key = model.unique_key
        con.execute(
            f"INSERT INTO {model.table} BY NAME SELECT * FROM ({model.sql}) m "
            f"WHERE m.{key} NOT IN (SELECT {key} FROM {model.table})"
        )
    else:  # incremental: replace the slice of rows for changed keys
        key = model.unique_key
        con.execute(f"DELETE FROM {model.table} WHERE {key} IN (SELECT {key} FROM changed_{key})")
        con.execute(f"INSERT INTO {model.table} BY NAME {model.sql}")


def run_transform(lake_dir: str | Path, full_refresh: bool = False) -> TransformResult:
    """Build every model, incrementally unless asked for a full refresh."""
    models = load_models()
    con = connect(lake_dir)
    in_txn = False
    try:
        register_bronze(con, lake_dir)
        built = all(_table_exists(con, m.table) for m in models)
        full = full_refresh or not built or not _has_watermark(con)

        result = TransformResult(mode="full_refresh" if full else "incremental")
        con.execute("BEGIN TRANSACTION")
        in_txn = True
        _pin_bounds(con, full)
        con.execute(
            "CREATE OR REPLACE TEMP VIEW new_bronze AS SELECT b.* FROM bronze_order_events b, "
            "_bounds w WHERE b.ingested_at <= w.high AND (w.low IS NULL OR b.ingested_at > w.low)"
        )
        result.new_events = con.execute("SELECT COUNT(*) FROM new_bronze").fetchone()[0]
        if not full and result.new_events == 0:
            con.execute("ROLLBACK")
            in_txn = False
            result.mode = "noop"
            result.counts = {m.table: _count(con, m.table) for m in models}
            return result

        con.execute(
            "CREATE OR REPLACE TEMP TABLE changed_order_id AS "
            "SELECT DISTINCT order_id FROM new_bronze"
        )
        result.changed_orders = _count(con, "changed_order_id")
        # Dates the changed orders mapped to before this run. A late PLACED event
        # can move an order to an earlier day, and both days must be rebuilt.
        con.execute("CREATE OR REPLACE TEMP TABLE changed_order_date (order_date DATE)")
        if not full:
            con.execute(f"INSERT INTO changed_order_date {_CHANGED_ORDER_DATES}")
        dates_final = False

        for model in models:
            if model.unique_key == "order_date" and not dates_final:
                con.execute(
                    "INSERT INTO changed_order_date "
                    f"SELECT order_date FROM ({_CHANGED_ORDER_DATES}) "
                    "EXCEPT SELECT order_date FROM changed_order_date"
                )
                dates_final = True
            started = time.perf_counter()
            _materialize(con, model, full)
            result.counts[model.table] = _count(con, model.table)
            log.info(
                "built %s [%s] (%d rows) in %.2fs",
                model.table,
                "full" if full else model.materialized,
                result.counts[model.table],
                time.perf_counter() - started,
            )
        result.changed_dates = _count(con, "changed_order_date")

        _advance_watermark(con)
        con.execute("COMMIT")
        in_txn = False

        gold_dir = Path(lake_dir) / "gold"
        gold_dir.mkdir(parents=True, exist_ok=True)
        for table in result.counts:
            if table.startswith(GOLD_PREFIX):
                out = gold_dir / f"{table.removeprefix(GOLD_PREFIX)}.parquet"
                con.execute(f"COPY {table} TO '{out.as_posix()}' (FORMAT parquet)")
        return result
    except Exception:
        if in_txn:
            con.execute("ROLLBACK")
        raise
    finally:
        con.close()


def _count(con: duckdb.DuckDBPyConnection, table: str) -> int:
    return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def run_models(lake_dir: str | Path, full_refresh: bool = False) -> dict[str, int]:
    """Build every model and return row counts per table."""
    return run_transform(lake_dir, full_refresh=full_refresh).counts
