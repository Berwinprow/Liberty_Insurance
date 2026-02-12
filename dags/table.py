from airflow.providers.postgres.hooks.postgres import PostgresHook
from pathlib import Path
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from schema_table_config import get_schema


# ---------------------------------------------------------------------
# 🔧 Constants
# ---------------------------------------------------------------------
DAGS_DIR = Path(__file__).resolve().parent
JSON_PATH = str(DAGS_DIR / "config" / "schema_config.json")

SOURCE_SCHEMA = get_schema("bi_dwh", JSON_PATH)
CONNECTION_ID = "postgres_cloud_prochurn"
SOURCE_TABLE = "final_policy_features"

# ---------------------------------------------------------------------
# 📌 Read Table
# ---------------------------------------------------------------------
def read_final_policy_features():
    hook = PostgresHook(postgres_conn_id=CONNECTION_ID)
    engine = hook.get_sqlalchemy_engine()

    query = f"""
        SELECT *
        FROM {SOURCE_SCHEMA}.{SOURCE_TABLE}
    """
    df = pd.read_sql(query, engine)
    print(f"Loaded {len(df)} rows from {SOURCE_SCHEMA}.final_policy_features")

with DAG(
    dag_id = "test_run",
    default_args = {"owner":"airflow","start_date":datetime(2024,1,1)},
    schedule_interval = None,
    catchup = False,
    tags = ["test","script"]
)as dag:
    
    test_run_task = PythonOperator(
        task_id = "test", 
        python_callable = read_final_policy_features
    )

 