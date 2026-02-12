from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from ml_pipeline_phase_wise.pipeline_runner_phase4 import run_phase4_pipeline
from utils.email_callbacks import (
    notify_failure,
    notify_retry,
    notify_success
)

default_args = {
    "owner": "data_science_team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),

    "on_failure_callback": notify_failure,
    "on_retry_callback": notify_retry,
    "on_success_callback": notify_success,
}

with DAG(
    dag_id="Ml_Automation_Phase4",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["ml", "phase4", "grid_search"]
) as dag:

    run_phase4 = PythonOperator(
        task_id="run_phase4_pipeline",
        python_callable=run_phase4_pipeline
    )
