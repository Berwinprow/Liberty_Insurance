import json
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

CONFIG_PATH = "/opt/airflow/dags/config/connections_table_columns.json"
SCHEMA_CONFIG_PATH = "/opt/airflow/dags/config/schema_config.json"


# ======================================================
# LOAD DB + SCHEMA CONFIG
# ======================================================

def _load_db_config():
    with open(CONFIG_PATH, "r") as f:
        cfg = json.load(f)

    with open(SCHEMA_CONFIG_PATH, "r") as f:
        schema_cfg = json.load(f)

    conn_id = cfg["connection"]["postgres_conn_id"]
    tables = cfg["tables"]

    # REAL postgres schema name
    model_selection_schema = schema_cfg["schema"]["model_selection_schema"]

    return conn_id, tables, model_selection_schema


# ======================================================
# CORE WRITE FUNCTION
# ======================================================

def _write_df(df, full_table_name, conn_id):
    if df.empty:
        return

    hook = PostgresHook(postgres_conn_id=conn_id)
    engine = hook.get_sqlalchemy_engine()

    df.to_sql(
        name=full_table_name.split(".")[-1],
        schema=full_table_name.split(".")[0],
        con=engine,
        if_exists="append",
        index=False,
        method="multi"
    )


# ======================================================
# CV RESULTS
# ======================================================

def write_future_cv_results(rows):
    if not rows:
        return

    conn_id, tables, schema = _load_db_config()
    table_name = f"{schema}.{tables['future_cv_results_table']}"

    df = pd.DataFrame(rows)
    _write_df(df, table_name, conn_id)

    print(
        f"FUTURE CV RESULTS WRITTEN | "
        f"Table: {table_name} | Rows: {len(df)}"
    )


# ======================================================
# TIME BASED RESULTS
# ======================================================

def write_future_time_based_results(rows):
    if not rows:
        return

    conn_id, tables, schema = _load_db_config()
    table_name = f"{schema}.{tables['future_time_based_results_table']}"

    df = pd.DataFrame(rows)
    _write_df(df, table_name, conn_id)

    print(
        f"FUTURE TIME-BASED RESULTS WRITTEN | "
        f"Table: {table_name} | Rows: {len(df)}"
    )


# ======================================================
# FULL TRAIN + OPEN PREDICTION RESULTS
# ======================================================

def write_future_open_results(rows):
    if not rows:
        return

    conn_id, tables, schema = _load_db_config()
    table_name = f"{schema}.{tables['future_open_policy_results_table']}"

    df = pd.DataFrame(rows)
    _write_df(df, table_name, conn_id)

    print(
        f"FUTURE OPEN POLICY RESULTS WRITTEN | "
        f"Table: {table_name} | Rows: {len(df)}"
    )
