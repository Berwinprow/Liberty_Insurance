from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from ml_pipeline_phase_wise.pipeline_runner_phase3 import run_phase3_pipeline


# ======================================================
# DEFAULT ARGS
# ======================================================
default_args = {
    "owner": "data_science_team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}


# ======================================================
# DAG DEFINITION
# ======================================================
with DAG(
    dag_id="Ml_Automation_Phase3",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,     # manual / triggered
    catchup=False,
    tags=["ml", "phase3", "threshold", "7set"]
) as dag:

    run_phase3 = PythonOperator(
        task_id="run_phase3_pipeline",
        python_callable=run_phase3_pipeline
    )
