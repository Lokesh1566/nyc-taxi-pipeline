"""
Monitors the streaming ingestion job.

The streaming job itself runs as a long-running process (not as an Airflow
task — Airflow isn't built for that). This DAG just checks that bronze data
is actually being written and alerts if ingestion has stalled.

Runs every 15 minutes.
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException


PROJECT_DIR = "/opt/pipeline"
BRONZE_DIR = f"{PROJECT_DIR}/data/processed/bronze"
# alert if no new bronze files in this many minutes
STALE_THRESHOLD_MINUTES = 30


default_args = {
    "owner": "lokesh",
    "retries": 0,  # don't retry monitoring failures — that defeats the purpose
    "email_on_failure": True,
}


def check_bronze_freshness(**context):
    """Check that at least one bronze file was modified recently."""
    bronze = Path(BRONZE_DIR)
    if not bronze.exists():
        raise AirflowFailException(f"bronze directory does not exist: {bronze}")

    # walk the dir — bronze is partitioned so files are nested
    parquet_files = list(bronze.rglob("*.parquet"))
    if not parquet_files:
        raise AirflowFailException("no parquet files in bronze directory")

    latest_mtime = max(f.stat().st_mtime for f in parquet_files)
    latest_dt = datetime.fromtimestamp(latest_mtime)
    age_minutes = (datetime.now() - latest_dt).total_seconds() / 60

    print(f"found {len(parquet_files)} parquet files in bronze")
    print(f"most recent file: {latest_dt.isoformat()} ({age_minutes:.1f} min ago)")

    if age_minutes > STALE_THRESHOLD_MINUTES:
        raise AirflowFailException(
            f"bronze data is stale — last file was {age_minutes:.1f} minutes ago "
            f"(threshold: {STALE_THRESHOLD_MINUTES} min). "
            f"Is the streaming job running?"
        )

    print("bronze freshness OK")


def check_bronze_row_count(**context):
    """Sanity check on row counts — catch upstream schema issues."""
    # using pyarrow directly instead of spinning up spark just for a count
    import pyarrow.parquet as pq

    bronze = Path(BRONZE_DIR)
    parquet_files = list(bronze.rglob("*.parquet"))

    if not parquet_files:
        print("no files to count")
        return

    # sample last 10 files to avoid reading everything
    recent_files = sorted(parquet_files, key=lambda f: f.stat().st_mtime)[-10:]
    total_rows = sum(pq.read_metadata(f).num_rows for f in recent_files)
    print(f"last 10 bronze files contain {total_rows} rows")

    if total_rows == 0:
        raise AirflowFailException("recent bronze files are empty")


with DAG(
    dag_id="taxi_ingestion_monitor",
    default_args=default_args,
    description="monitors the streaming ingestion job",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["taxi", "monitoring"],
) as dag:

    freshness_check = PythonOperator(
        task_id="check_bronze_freshness",
        python_callable=check_bronze_freshness,
    )

    row_count_check = PythonOperator(
        task_id="check_bronze_row_count",
        python_callable=check_bronze_row_count,
    )

    freshness_check >> row_count_check
