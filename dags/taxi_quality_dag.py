"""
Dedicated data quality DAG that runs expanded GE checks on gold tables.

The transform DAG runs a minimal check on silver (fail-fast). This one is
more thorough — it runs against the Snowflake gold tables after load,
generates a data docs report, and emails a summary.

Runs at 4am daily, after the transform DAG should be done.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor


PROJECT_DIR = "/opt/pipeline"

default_args = {
    "owner": "lokesh",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": True,
}


with DAG(
    dag_id="taxi_data_quality",
    default_args=default_args,
    description="thorough GE checks on gold/snowflake data",
    schedule_interval="0 4 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["taxi", "data-quality"],
) as dag:

    # wait until the transform pipeline succeeds before running
    wait_for_transform = ExternalTaskSensor(
        task_id="wait_for_transform",
        external_dag_id="taxi_transform_pipeline",
        external_task_id="mark_success",
        execution_delta=timedelta(hours=2),
        timeout=60 * 60,  # give up after an hour
        mode="reschedule",  # don't hog a worker slot
    )

    validate_gold = BashOperator(
        task_id="validate_gold_tables",
        bash_command=f"""
            cd {PROJECT_DIR} && \
            python -m tests.great_expectations.run_validation \
                --dataset gold \
                --generate-docs
        """,
    )

    # snapshot the row counts to a log we can diff later
    snapshot_counts = BashOperator(
        task_id="snapshot_row_counts",
        bash_command=f"""
            cd {PROJECT_DIR} && \
            python -c "
from scripts.snowflake_loader import load_config, get_connection
cfg = load_config()
conn = get_connection(cfg)
cur = conn.cursor()
tables = ['FCT_TRIPS_HOURLY', 'FCT_TRIPS_DAILY', 'AGG_TOP_ROUTES',
          'AGG_ZONE_STATS', 'AGG_PAYMENT_BREAKDOWN']
for t in tables:
    cur.execute(f'SELECT COUNT(*) FROM {{t}}')
    print(f'{{t}}: {{cur.fetchone()[0]}} rows')
cur.close()
conn.close()
            "
        """,
    )

    wait_for_transform >> validate_gold >> snapshot_counts
