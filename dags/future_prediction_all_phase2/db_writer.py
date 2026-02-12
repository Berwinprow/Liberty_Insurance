"""
Database writer utilities for Future Prediction Phase-2.

Responsibilities:
- Load DB and schema configuration
- Write CV results
- Write time-based evaluation results
- Write full-train + open prediction outputs
"""

import json
import logging
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

CONFIG_PATH = "/opt/airflow/dags/config/connections_table_columns.json"
SCHEMA_CONFIG_PATH = "/opt/airflow/dags/config/schema_config.json"

logger = logging.getLogger(__name__)


# ======================================================
# LOAD DB + SCHEMA CONFIG
# ======================================================
def _load_db_config():
    """
    Load database connection ID, table mappings,
    and resolved Postgres schema name.
    """
    with open(CONFIG_PATH, "r") as f:
        cfg = json.load(f)

    with open(SCHEMA_CONFIG_PATH, "r") as f:
        schema_cfg = json.load(f)

    conn_id = cfg["connection"]["postgres_conn_id"]
    tables = cfg["tables"]

    # Resolved Postgres schema name
    model_selection_schema = schema_cfg["schema"]["model_selection_schema"]

    return conn_id, tables, model_selection_schema


# ======================================================
# CORE WRITE FUNCTION
# ======================================================
def _write_df(df, full_table_name, conn_id):
    """
    Write DataFrame to Postgres using SQLAlchemy engine.
    """
    if df.empty:
        logger.debug("Skipping DB write: empty DataFrame")
        return

    hook = PostgresHook(postgres_conn_id=conn_id)
    engine = hook.get_sqlalchemy_engine()

    schema, table = full_table_name.split(".")

    df.to_sql(
        name=table,
        schema=schema,
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
    )


# ======================================================
# CV RESULTS
# ======================================================
def write_future_cv_results(rows):
    """
    Write cross-validation results for future prediction.
    """
    if not rows:
        logger.debug("No CV rows to write")
        return

    conn_id, tables, schema = _load_db_config()
    table_name = f"{schema}.{tables['future_cv_results_table']}"

    df = pd.DataFrame(rows)
    _write_df(df, table_name, conn_id)

    logger.info(
        "FUTURE CV RESULTS WRITTEN | Table=%s | Rows=%s",
        table_name,
        len(df),
    )


# ======================================================
# TIME BASED RESULTS
# ======================================================
def write_future_time_based_results(rows):
    """
    Write time-based evaluation results.
    """
    if not rows:
        logger.debug("No time-based rows to write")
        return

    conn_id, tables, schema = _load_db_config()
    table_name = f"{schema}.{tables['future_time_based_results_table']}"

    df = pd.DataFrame(rows)
    _write_df(df, table_name, conn_id)

    logger.info(
        "FUTURE TIME-BASED RESULTS WRITTEN | Table=%s | Rows=%s",
        table_name,
        len(df),
    )


# ======================================================
# FULL TRAIN + OPEN PREDICTION RESULTS
# ======================================================
def write_future_open_results(rows):
    """
    Write full-train + open-policy prediction results.
    """
    if not rows:
        logger.debug("No open-policy rows to write")
        return

    conn_id, tables, schema = _load_db_config()
    table_name = f"{schema}.{tables['future_open_policy_results_table']}"

    df = pd.DataFrame(rows)
    _write_df(df, table_name, conn_id)

    logger.info(
        "FUTURE OPEN POLICY RESULTS WRITTEN | Table=%s | Rows=%s",
        table_name,
        len(df),
    )
