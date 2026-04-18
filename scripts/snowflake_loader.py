"""
Loads gold-layer parquet files into Snowflake.

Uses the pandas -> snowflake write_pandas approach instead of a Spark
Snowflake connector. Reasons:
  - no JAR mess
  - gold tables are small (thousands to low millions of rows), fits in memory
  - write_pandas is fast enough (uses parquet internally)

For silver or bronze you'd want the Spark connector, not this.
"""

import argparse
import logging
from pathlib import Path

import pandas as pd
import snowflake.connector
import yaml
from snowflake.connector.pandas_tools import write_pandas

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def load_config(config_path: str = "config/snowflake_config.yaml") -> dict:
    with open(config_path) as f:
        return yaml.safe_load(f)


def get_connection(cfg: dict):
    return snowflake.connector.connect(
        user=cfg["user"],
        password=cfg["password"],
        account=cfg["account"],
        warehouse=cfg["warehouse"],
        database=cfg["database"],
        schema=cfg["schema"],
        role=cfg.get("role"),
        login_timeout=30,  # default is way too long
    )


def run_ddl(conn, ddl_file: str = "sql/ddl_snowflake.sql") -> None:
    """Execute the DDL script. Creates tables if they don't exist."""
    log.info("running DDL from %s", ddl_file)
    with open(ddl_file) as f:
        ddl_sql = f.read()

    cur = conn.cursor()
    # split on semicolons — crude but works for our simple DDL
    for stmt in ddl_sql.split(";"):
        stmt = stmt.strip()
        if not stmt:
            continue
        log.debug("executing: %s", stmt[:80])
        cur.execute(stmt)
    cur.close()
    log.info("DDL complete")


def load_parquet_to_snowflake(conn, parquet_dir: Path, table_name: str,
                              truncate_first: bool = True) -> int:
    """Load a directory of parquet files into a Snowflake table."""
    log.info("loading %s -> %s", parquet_dir, table_name)

    # read all parquet files in dir (handles spark's partitioned output)
    df = pd.read_parquet(parquet_dir)
    log.info("read %d rows from parquet", len(df))

    if len(df) == 0:
        log.warning("empty dataframe, skipping load")
        return 0

    # snowflake is picky about column name case — uppercase everything
    df.columns = [c.upper() for c in df.columns]

    cur = conn.cursor()
    if truncate_first:
        cur.execute(f"TRUNCATE TABLE IF EXISTS {table_name}")
        log.info("truncated %s", table_name)
    cur.close()

    success, nchunks, nrows, _ = write_pandas(
        conn, df, table_name,
        auto_create_table=False,
        overwrite=False,
        quote_identifiers=False,
    )

    if not success:
        raise RuntimeError(f"write_pandas failed for {table_name}")
    log.info("loaded %d rows in %d chunks into %s", nrows, nchunks, table_name)
    return nrows


# mapping of gold parquet dirs to target tables
GOLD_TABLE_MAP = {
    "fct_trips_hourly": "FCT_TRIPS_HOURLY",
    "fct_trips_daily": "FCT_TRIPS_DAILY",
    "agg_top_routes": "AGG_TOP_ROUTES",
    "agg_zone_stats": "AGG_ZONE_STATS",
    "agg_payment_breakdown": "AGG_PAYMENT_BREAKDOWN",
}


def load_all_gold_tables(conn, gold_dir: str = "data/processed/gold") -> dict:
    gold_path = Path(gold_dir)
    results = {}
    for subdir, table_name in GOLD_TABLE_MAP.items():
        parquet_dir = gold_path / subdir
        if not parquet_dir.exists():
            log.warning("skipping %s, directory not found", parquet_dir)
            continue
        rows = load_parquet_to_snowflake(conn, parquet_dir, table_name)
        results[table_name] = rows
    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--setup", action="store_true",
                        help="run DDL to create tables")
    parser.add_argument("--load", action="store_true",
                        help="load gold tables from parquet")
    parser.add_argument("--gold-dir", default="data/processed/gold")
    parser.add_argument("--config", default="config/snowflake_config.yaml")
    args = parser.parse_args()

    if not (args.setup or args.load):
        parser.error("specify --setup and/or --load")

    cfg = load_config(args.config)
    conn = get_connection(cfg)

    try:
        if args.setup:
            run_ddl(conn)
        if args.load:
            results = load_all_gold_tables(conn, args.gold_dir)
            log.info("load summary: %s", results)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
