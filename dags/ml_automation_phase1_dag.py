from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from ml_pipeline_phase_wise.pipeline_runner_phase1 import run_phase1_pipeline
from utils.email_callbacks import (
    notify_failure,
    notify_retry,
    notify_success
)

default_args = {
    "owner": "data_science_team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),

    # EMAIL CALLBACKS
    "on_failure_callback": notify_failure,
    "on_retry_callback": notify_retry,
    "on_success_callback": notify_success,
}

with DAG(
    dag_id="Ml_Automation_Phase1",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),  
    schedule_interval=None,
    catchup=False,
    tags=["ml", "phase1"]
) as dag:

    run_phase1 = PythonOperator(
        task_id="run_phase1_pipeline",
        python_callable=run_phase1_pipeline
    )
