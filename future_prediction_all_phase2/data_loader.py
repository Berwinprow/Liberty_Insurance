import json
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

from future_prediction_all_phase2.schema_table_config import get_schema

CONFIG_PATH = "/opt/airflow/dags/config/connections_table_columns.json"
SCHEMA_CONFIG_PATH = "/opt/airflow/dags/config/schema_config.json"


def load_data():
    """
    Loads full source data from Postgres.

    Responsibilities:
    - Read full table
    - Handle nulls (non-target)
    - Normalize target column
    - Keep labeled + Open rows
    - Map labels while keeping Open unchanged
    """

    # ================= LOAD CONFIG =================
    with open(CONFIG_PATH, "r") as f:
        cfg = json.load(f)

    conn_id = cfg["connection"]["postgres_conn_id"]
    table_name = cfg["tables"]["source_table"]

    target_col = cfg["columns"]["target_column"]
    target_mapping = cfg["columns"]["target_mapping"]
    future_values = cfg["future_prediction"]["future_values"]

    # ================= RESOLVE SCHEMA =================
    source_schema = get_schema("bi_dwh", SCHEMA_CONFIG_PATH)
    full_table_name = f"{source_schema}.{table_name}"

    # ================= DB CONNECTION =================
    hook = PostgresHook(postgres_conn_id=conn_id)
    engine = hook.get_sqlalchemy_engine()

    # ================= READ DATA =================
    df = pd.read_sql(f"SELECT * FROM {full_table_name}", engine)

    # ================= NULL HANDLING (NON-TARGET ONLY) =================
    for col in df.columns:
        if col == target_col:
            continue

        if df[col].dtype == "object":
            df[col] = df[col].fillna("unknown")
        else:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # ================= TARGET NORMALIZATION =================
    df[target_col] = df[target_col].astype(str).str.strip()

    label_set = set(target_mapping.keys())
    future_set = {str(v).strip() for v in future_values if v is not None}

    # ================= FILTER LABELED + OPEN =================
    df = df[
        df[target_col].isin(label_set)
        | df[target_col].isin(future_set)
    ].copy()

    # ================= MAP LABELS (KEEP OPEN INTACT) =================
    df[target_col] = df[target_col].replace(target_mapping)
    df[target_col] = df[target_col].replace({"nan": pd.NA})

    print(
        f"DATA LOADED | Rows: {len(df)} | "
        f"Labeled rows: {df[target_col].isin([0,1]).sum()} | "
        f"Open rows: {(df[target_col] == 'Open').sum()}"
    )

    return df
