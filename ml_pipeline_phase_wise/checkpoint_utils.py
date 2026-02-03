from airflow.providers.postgres.hooks.postgres import PostgresHook
from ml_pipeline_phase_wise.schema_table_config import get_schema
import json


CONFIG_PATH = "/opt/airflow/dags/config/connections_table_columns.json"


def already_trained(
    feature,
    split,
    sampling,
    model_name,
    params,
    table_name,
    seven_set=None
):
    # ================= LOAD CONFIG =================
    with open(CONFIG_PATH, "r") as f:
        cfg = json.load(f)

    CONNECTION_ID = cfg["connection"]["postgres_conn_id"]

    # ================= DB CONNECTION =================
    hook = PostgresHook(postgres_conn_id=CONNECTION_ID)
    engine = hook.get_sqlalchemy_engine()

    # ================= GET SCHEMA =================
    schema = get_schema(
        "model_selection_schema",
        "/opt/airflow/dags/config/schema_config.json"
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
        "params": str(params)
    }

    if seven_set is not None:
        query += ' AND "7set_undersampling" = %(seven)s'
        params_dict["seven"] = seven_set

    # ================= EXECUTE =================
    with engine.connect() as conn:
        return conn.execute(query, params_dict).fetchone() is not None
