from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from ml_pipeline_phase_wise.pipeline_runner_phase2 import run_phase2_pipeline


default_args = {
    "owner": "data_science_team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}


with DAG(
    dag_id="Ml_Automation_Phase2",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["ml", "phase2"]
) as dag:

    run_phase2 = PythonOperator(
        task_id="run_phase2_pipeline",
        python_callable=run_phase2_pipeline
    )
