from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from ml_pipeline_phase_wise.schema_table_config import get_schema

CONNECTION_ID = "postgres_cloud_prochurn"
SOURCE_TABLE = "final_policy_feature_manclean"

def load_and_clean_data():
    schema = get_schema(
        "bi_dwh",
        "/opt/airflow/dags/config/schema_config.json"
    )

    hook = PostgresHook(postgres_conn_id=CONNECTION_ID)
    engine = hook.get_sqlalchemy_engine()

    df = pd.read_sql(
        f"SELECT * FROM {schema}.{SOURCE_TABLE}",
        engine
    )

    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].fillna("unknown")
        else:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df = df[df["policy_status"].isin(["Renewed", "Not Renewed"])]
    df["policy_status"] = df["policy_status"].map(
        {"Renewed": 0, "Not Renewed": 1}
    )

    print(f"DATA LOADED | Rows: {len(df)}")
    return df
