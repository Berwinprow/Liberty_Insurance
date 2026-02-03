from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from future_prediction_all_phase2.pipeline_orchestrator import run_future_pipeline



default_args = {
    "owner": "data_science_team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}


with DAG(
    dag_id="future_pipeline_manual",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["future", "manual"]
) as dag:

    # ================= PHASE 1 =================
    run_phase = PythonOperator(
        task_id="run_future_pipeline",
        python_callable=run_future_pipeline
    )

