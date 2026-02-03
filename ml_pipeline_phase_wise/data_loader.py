from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import json
from ml_pipeline_phase_wise.schema_table_config import get_schema


CONFIG_PATH = "/opt/airflow/dags/config/connections_table_columns.json"


def load_and_clean_data():
    # ================= LOAD CONFIG =================
    with open(CONFIG_PATH, "r") as f:
        cfg = json.load(f)

    CONNECTION_ID = cfg["connection"]["postgres_conn_id"]
    SOURCE_TABLE = cfg["tables"]["source_table"]

    TARGET_COLUMN = cfg["columns"]["target_column"]
    TARGET_MAPPING = cfg["columns"]["target_mapping"]
    VALID_TARGET_VALUES = list(TARGET_MAPPING.keys())

    # ================= GET SCHEMA =================
    schema = get_schema(
        "bi_dwh",
        "/opt/airflow/dags/config/schema_config.json"
    )

    # ================= DB CONNECTION =================
    hook = PostgresHook(postgres_conn_id=CONNECTION_ID)
    engine = hook.get_sqlalchemy_engine()

    # ================= READ DATA =================
    df = pd.read_sql(
        f"SELECT * FROM {schema}.{SOURCE_TABLE}",
        engine
    )

    # ================= NULL HANDLING =================
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].fillna("unknown")
        else:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # ================= TARGET FILTER + MAP =================
    df = df[df[TARGET_COLUMN].isin(VALID_TARGET_VALUES)]
    df[TARGET_COLUMN] = df[TARGET_COLUMN].map(TARGET_MAPPING)

    print(
        f"DATA LOADED | Rows: {len(df)} | "
        f"Source Table: {SOURCE_TABLE} | "
        f"Target: {TARGET_COLUMN}"
    )

    return df
