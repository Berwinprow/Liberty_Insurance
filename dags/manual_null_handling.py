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
TARGET_TABLE = "final_policy_feature_manclean"


# ---------------------------------------------------------------------
# 📌 Read + Clean + Upload
# ---------------------------------------------------------------------
def manual_null_handling():
    hook = PostgresHook(postgres_conn_id=CONNECTION_ID)
    engine = hook.get_sqlalchemy_engine()

    query = f"""
        SELECT *
        FROM {SOURCE_SCHEMA}.{SOURCE_TABLE}
    """
    df = pd.read_sql(query, engine)
    print(f"Loaded {len(df)} rows from {SOURCE_SCHEMA}.{SOURCE_TABLE}")

    # ---------------------------------------------
    # Replace "(blank)" with "unknown"
    # ---------------------------------------------
    df.replace(
        to_replace=r'^\(blank\)$',
        value='unknown',
        regex=True,
        inplace=True
    )

    # ---------------------------------------------
    # Drop NULL customer_id
    # ---------------------------------------------
    df = df.dropna(subset=['customer_id'])

    print(f"Rows after cleaning: {len(df)}")

    # ---------------------------------------------
    # Upload cleaned DF to DB
    # ---------------------------------------------
    df.to_sql(
        name=TARGET_TABLE,
        con=engine,
        schema=SOURCE_SCHEMA,
        if_exists="replace",      # ⬅️ overwrite; use "append" if needed
        index=False
    )

    print(f"Uploaded cleaned DF → {SOURCE_SCHEMA}.{TARGET_TABLE}")
    

# ---------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------
with DAG(
    dag_id="manual_null_handling",
    default_args={"owner": "airflow", "start_date": datetime(2024, 1, 1)},
    schedule_interval=None,
    catchup=False,
    tags=["test", "script"]
) as dag:

    test_run_task = PythonOperator(
        task_id="manual_null",
        python_callable=manual_null_handling
    )
