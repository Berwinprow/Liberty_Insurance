from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from ml_pipeline_phase_wise.pipeline_runner_phase1 import run_phase1_pipeline


default_args = {
    "owner": "data_science_team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}


with DAG(
    dag_id="Ml_Automation_Phase_wise",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["ml", "phase1", "phase2"]
) as dag:

    # ================= PHASE 1 =================
    run_phase1 = PythonOperator(
        task_id="run_phase1_pipeline",
        python_callable=run_phase1_pipeline
    )

