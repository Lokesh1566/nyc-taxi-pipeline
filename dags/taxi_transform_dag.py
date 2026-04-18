"""
Daily transform DAG.

Takes bronze data that the streaming job has landed, runs silver + gold
transformations, validates with Great Expectations, and loads to Snowflake.

Runs at 2am daily. Picks up whatever bronze has accumulated since last run.
Not incremental — full refresh on silver/gold. For a dataset this size it's
actually fine and it's way simpler than proper incremental logic.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# project lives outside AIRFLOW_HOME in our setup
PROJECT_DIR = "/opt/pipeline"

default_args = {
    "owner": "lokesh",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["lokeshreddye@iu.edu"],  # replace with your email
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


with DAG(
    dag_id="taxi_transform_pipeline",
    default_args=default_args,
    description="bronze -> silver -> gold -> snowflake",
    schedule_interval="0 2 * * *",  # daily at 2am
    start_date=datetime(2024, 1, 1),
    catchup=False,  # don't backfill every historical day on first run
    max_active_runs=1,
    tags=["taxi", "etl", "spark"],
) as dag:

    start = EmptyOperator(task_id="start")

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=f"""
            cd {PROJECT_DIR} && \
            spark-submit \
                --master local[4] \
                --driver-memory 4g \
                spark_jobs/bronze_to_silver.py \
                --bronze data/processed/bronze \
                --zones data/raw/taxi_zone_lookup.csv \
                --silver-output data/processed/silver \
                --quarantine-output data/processed/quarantine
        """,
    )

    # GE validation sits between silver and gold — fail the pipeline if
    # silver data looks wrong before we spend time on gold aggregations
    validate_silver = BashOperator(
        task_id="validate_silver",
        bash_command=f"""
            cd {PROJECT_DIR} && \
            python -m tests.great_expectations.run_validation \
                --dataset silver \
                --fail-on-error
        """,
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=f"""
            cd {PROJECT_DIR} && \
            spark-submit \
                --master local[4] \
                --driver-memory 4g \
                spark_jobs/silver_to_gold.py \
                --silver data/processed/silver \
                --gold-output data/processed/gold
        """,
    )

    load_snowflake = BashOperator(
        task_id="load_snowflake",
        bash_command=f"""
            cd {PROJECT_DIR} && \
            python scripts/snowflake_loader.py \
                --load \
                --gold-dir data/processed/gold
        """,
    )

    def _log_success(**context):
        """Simple success marker. In prod this would be a metric emission."""
        import logging
        log = logging.getLogger(__name__)
        log.info("pipeline completed for run %s", context["ds"])
        log.info("dag run id: %s", context["run_id"])

    success = PythonOperator(
        task_id="mark_success",
        python_callable=_log_success,
    )

    end = EmptyOperator(task_id="end")

    start >> bronze_to_silver >> validate_silver >> silver_to_gold
    silver_to_gold >> load_snowflake >> success >> end
