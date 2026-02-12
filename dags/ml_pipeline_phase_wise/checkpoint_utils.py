"""
Utility functions for checking whether a model configuration
has already been trained and stored in the database.
"""

import json
from airflow.providers.postgres.hooks.postgres import PostgresHook
from ml_pipeline_phase_wise.schema_table_config import get_schema


CONFIG_PATH = "/opt/airflow/dags/config/connections_table_columns.json"
SCHEMA_CONFIG_PATH = "/opt/airflow/dags/config/schema_config.json"
SCHEMA_KEY = "model_selection_schema"


def already_trained(
    feature,
    split,
    sampling,
    model_name,
    params,
    table_name,
    seven_set=None,
):
    """
    Check if a model with the given configuration
    already exists in the database.

    Args:
        feature: Feature set name
        split: Data splitting strategy
        sampling: Sampling method
        model_name: Model name
        params: Model parameters
        table_name: Database table name
        seven_set: Optional 7-set undersampling flag

    Returns:
        bool: True if model already trained, else False
    """
    # ================= LOAD CONFIG =================
    with open(CONFIG_PATH, "r") as file:
        cfg = json.load(file)

    connection_id = cfg["connection"]["postgres_conn_id"]

    # ================= DB CONNECTION =================
    hook = PostgresHook(postgres_conn_id=connection_id)
    engine = hook.get_sqlalchemy_engine()

    # ================= GET SCHEMA =================
    schema = get_schema(
        SCHEMA_KEY,
        SCHEMA_CONFIG_PATH,
    )

    # ================= CHECK QUERY =================
    query = f"""
        SELECT 1
        FROM {schema}.{table_name}
        WHERE
            "Feature" = %(feature)s
            AND "Data_Splitting" = %(split)s
            AND "sampling_method" = %(sampling)s
            AND "Model_name" = %(model)s
            AND "Parameter" = %(params)s
    """

    params_dict = {
        "feature": feature,
        "split": split,
        "sampling": sampling,
        "model": model_name,
        "params": str(params),
    }

    if seven_set is not None:
        query += ' AND "7set_undersampling" = %(seven)s'
        params_dict["seven"] = seven_set

    # ================= EXECUTE =================
    with engine.connect() as conn:
        return (
            conn.execute(query, params_dict).fetchone()
            is not None
        )
