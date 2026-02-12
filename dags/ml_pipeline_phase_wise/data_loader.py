"""
Data loading and basic cleaning utilities.
"""

import json
import logging

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

from ml_pipeline_phase_wise.schema_table_config import get_schema


CONFIG_PATH = "/opt/airflow/dags/config/connections_table_columns.json"
SCHEMA_CONFIG_PATH = "/opt/airflow/dags/config/schema_config.json"
SCHEMA_KEY = "bi_dwh"

logger = logging.getLogger(__name__)


def load_and_clean_data():
    """
    Load data from Postgres, perform basic null handling,
    filter valid target values, and map target labels.

    Returns:
        pd.DataFrame: Cleaned dataframe
    """
    # ================= LOAD CONFIG =================
    with open(CONFIG_PATH, "r") as file:
        cfg = json.load(file)

    connection_id = cfg["connection"]["postgres_conn_id"]
    source_table = cfg["tables"]["source_table"]

    target_column = cfg["columns"]["target_column"]
    target_mapping = cfg["columns"]["target_mapping"]
    valid_target_values = list(target_mapping.keys())

    # ================= GET SCHEMA =================
    schema = get_schema(
        SCHEMA_KEY,
        SCHEMA_CONFIG_PATH,
    )

    # ================= DB CONNECTION =================
    hook = PostgresHook(postgres_conn_id=connection_id)
    engine = hook.get_sqlalchemy_engine()

    # ================= READ DATA =================
    df = pd.read_sql(
        f"SELECT * FROM {schema}.{source_table}",
        engine,
    )

    # ================= NULL HANDLING =================
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].fillna("unknown")
        else:
            df[col] = (
                pd.to_numeric(df[col], errors="coerce")
                .fillna(0)
            )

    # ================= TARGET FILTER + MAP =================
    df = df[df[target_column].isin(valid_target_values)]
    df[target_column] = df[target_column].map(target_mapping)

    logger.info(
        "DATA LOADED | Rows: %s | Source Table: %s | Target: %s",
        len(df),
        source_table,
        target_column,
    )

    return df
