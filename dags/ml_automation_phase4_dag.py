from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from ml_pipeline_phase_wise.pipeline_runner_phase4 import run_phase4_pipeline


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
    dag_id="Ml_Automation_Phase4",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,     # manual / triggered
    catchup=False,
    tags=["ml", "phase4", "grid_search"]
) as dag:

    run_phase4 = PythonOperator(
        task_id="run_phase4_pipeline",
        python_callable=run_phase4_pipeline
    )
