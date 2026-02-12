from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from future_prediction_all_phase2.pipeline_orchestrator import run_future_pipeline
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

    # 🔔 EMAIL CALLBACKS
    "on_failure_callback": notify_failure,
    "on_retry_callback": notify_retry,
    "on_success_callback": notify_success,
}

with DAG(
    dag_id="future_pipeline_manual",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),  # ✅ your actual start date
    schedule_interval=None,
    catchup=False,
    tags=["future", "manual"]
) as dag:

    run_phase = PythonOperator(
        task_id="run_future_pipeline",
        python_callable=run_future_pipeline
    )
