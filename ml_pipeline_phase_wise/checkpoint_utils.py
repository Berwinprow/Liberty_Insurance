from airflow.providers.postgres.hooks.postgres import PostgresHook
from ml_pipeline_phase_wise.schema_table_config import get_schema


def already_trained(
    feature,
    split,
    sampling,
    model_name,
    params,
    table_name,
    seven_set=None
):
    hook = PostgresHook(postgres_conn_id="postgres_cloud_prochurn")
    engine = hook.get_sqlalchemy_engine()

    schema = get_schema(
        "model_selection_schema",
        "/opt/airflow/dags/config/schema_config.json"
    )

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

    with engine.connect() as conn:
        return conn.execute(query, params_dict).fetchone() is not None
