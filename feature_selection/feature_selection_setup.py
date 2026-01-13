from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import logging
import time

import pandas as pd
import numpy as np
import scipy.stats as stats
from scipy.stats import chi2_contingency
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA

from schema_table_config import get_schema

# ---------------------------------------------------------------------
# 🔧 Logging
# ---------------------------------------------------------------------
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# 🔧 Constants
# ---------------------------------------------------------------------
CONNECTION_ID = "postgres_cloud_prochurn"
JSON_PATH = "/opt/airflow/dags/config/schema_config.json"

SOURCE_SCHEMA = get_schema("bi_dwh", JSON_PATH)
SOURCE_TABLE = "final_policy_feature_manclean"

TARGET_SCHEMA = get_schema("target_feature_selection_schema", JSON_PATH)
TARGET_TABLE = "feature_selection_metrics"

TARGET_COL = "policy_status_binary"

PCA_THRESHOLDS = {
    "pca_90_importance": 0.90,
    "pca_95_importance": 0.95,
    "pca_98_importance": 0.98,
}

CHI_SQUARE_MAX_CATEGORIES = 50
IV_NUMERIC_BINS = 10

# ---------------------------------------------------------------------
# 📌 Utilities
# ---------------------------------------------------------------------
def get_engine():
    hook = PostgresHook(postgres_conn_id=CONNECTION_ID)
    return hook.get_sqlalchemy_engine()

# ---------------------------------------------------------------------
# 📌 Metric Functions
# ---------------------------------------------------------------------
def cramers_v(x, y):
    contingency = pd.crosstab(x, y)
    chi2 = stats.chi2_contingency(contingency)[0]
    n = contingency.sum().sum()

    if n == 0:
        return 0

    phi2 = chi2 / n
    r, k = contingency.shape
    denom = min(k - 1, r - 1)

    return np.sqrt(phi2 / denom) if denom > 0 else 0


def calculate_iv(df, feature):
    total_event = (df[TARGET_COL] == 1).sum()
    total_non_event = (df[TARGET_COL] == 0).sum()

    grouped = df.groupby(feature)[TARGET_COL].agg(["count", "sum"])
    grouped.columns = ["total", "event"]

    grouped["non_event"] = grouped["total"] - grouped["event"]
    grouped.replace(0, 0.5, inplace=True)

    dist_event = grouped["event"] / total_event
    dist_non_event = grouped["non_event"] / total_non_event

    return ((dist_event - dist_non_event)
            * np.log(dist_event / dist_non_event)).sum()


def calculate_iv_numeric(df, feature, bins=IV_NUMERIC_BINS):
    try:
        binned = pd.qcut(df[feature], q=bins, duplicates="drop")
        return calculate_iv(df.assign(_bin=binned), "_bin")
    except Exception as exc:
        logger.warning("IV skipped for numeric %s: %s", feature, exc)
        return 0


def target_encode(df, col):
    return df.groupby(col)[TARGET_COL].transform("mean")

# ---------------------------------------------------------------------
# 📌 Main Task
# ---------------------------------------------------------------------
def compute_feature_target_statistics(**context):
    start_time = time.time()
    run_ts = context["logical_date"]

    logger.info("========== FEATURE SELECTION + PCA START ==========")
    logger.info("Run timestamp: %s", run_ts)

    engine = get_engine()

    # ------------------------------
    # Load Data
    # ------------------------------
    df = pd.read_sql(
        f"""
        SELECT *
        FROM {SOURCE_SCHEMA}.{SOURCE_TABLE}
        WHERE policy_status IN ('Renewed', 'Not Renewed')
        """,
        engine,
    )

    df[TARGET_COL] = (df["policy_status"] == "Not Renewed").astype(int)
    df = df.copy()

    logger.info("Data loaded | Rows=%s | Columns=%s", df.shape[0], df.shape[1])

    # ------------------------------
    # Date Expansion
    # ------------------------------
    date_cols = df.select_dtypes(include=["datetime64[ns]"]).columns.tolist()
    date_features = {}

    for col in date_cols:
        date_features[f"{col}_year"] = df[col].dt.year.fillna(0).astype(int)
        date_features[f"{col}_month"] = df[col].dt.month.fillna(0).astype(int)
        date_features[f"{col}_day"] = df[col].dt.day.fillna(0).astype(int)

    if date_features:
        df = pd.concat([df, pd.DataFrame(date_features)], axis=1)
        df.drop(columns=date_cols, inplace=True)

    # ------------------------------
    # Feature Identification
    # ------------------------------
    boolean_cols = df.select_dtypes(include=["bool"]).columns.tolist()
    categorical_cols = df.select_dtypes(include=["object", "category"]).columns.tolist()
    categorical_cols.extend(boolean_cols)

    numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns.tolist()
    numeric_cols = [c for c in numeric_cols if c not in categorical_cols and c != TARGET_COL]

    # ------------------------------
    # Missing Value Handling
    # ------------------------------
    df[categorical_cols] = df[categorical_cols].fillna("missing")
    df[numeric_cols] = df[numeric_cols].fillna(0)

    # ------------------------------
    # Target Encoding
    # ------------------------------
    te_cols = []
    for col in categorical_cols:
        te_col = f"{col}_te"
        df[te_col] = target_encode(df, col)
        te_cols.append(te_col)

    # ------------------------------
    # PCA Feature Importance
    # ------------------------------
    pca_input_cols = numeric_cols + te_cols
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(df[pca_input_cols])

    pca_importance_maps = {}

    for pca_col, threshold in PCA_THRESHOLDS.items():
        pca_tmp = PCA()
        pca_tmp.fit(X_scaled)

        cum_var = np.cumsum(pca_tmp.explained_variance_ratio_)
        n_components = int(np.argmax(cum_var >= threshold) + 1)

        logger.info("PCA %.0f%% variance → n_components=%s",
                    threshold * 100, n_components)

        pca = PCA(n_components=n_components)
        pca.fit(X_scaled)

        loadings = pd.DataFrame(pca.components_, columns=pca_input_cols)
        pca_importance_maps[pca_col] = loadings.abs().sum(axis=0)

    # ------------------------------
    # Build Result Rows
    # ------------------------------
    rows = []

    for col in numeric_cols:
        rows.append({
            "run_timestamp": run_ts,
            "feature_name": col,
            "feature_type": "numeric",
            "pearson_corr": df[col].corr(df[TARGET_COL]),
            "spearman_corr": df[col].corr(df[TARGET_COL], method="spearman"),
            "cramers_v": None,
            "chi_square_p": None,
            "iv_score": calculate_iv_numeric(df, col),
            "pca_90_importance": pca_importance_maps["pca_90_importance"].get(col, 0),
            "pca_95_importance": pca_importance_maps["pca_95_importance"].get(col, 0),
            "pca_98_importance": pca_importance_maps["pca_98_importance"].get(col, 0),
            "created_at": datetime.utcnow(),
        })

    for col in categorical_cols:
        if df[col].nunique() > CHI_SQUARE_MAX_CATEGORIES:
            logger.warning("Skipping chi-square for %s (high cardinality=%s)",
                           col, df[col].nunique())
            chi_p = None
        else:
            contingency = pd.crosstab(df[col], df[TARGET_COL])
            _, chi_p, _, _ = chi2_contingency(contingency)

        te_col = f"{col}_te"

        rows.append({
            "run_timestamp": run_ts,
            "feature_name": col,
            "feature_type": "categorical",
            "pearson_corr": None,
            "spearman_corr": None,
            "cramers_v": cramers_v(df[col], df[TARGET_COL]),
            "chi_square_p": chi_p,
            "iv_score": calculate_iv(df, col),
            "pca_90_importance": pca_importance_maps["pca_90_importance"].get(te_col, 0),
            "pca_95_importance": pca_importance_maps["pca_95_importance"].get(te_col, 0),
            "pca_98_importance": pca_importance_maps["pca_98_importance"].get(te_col, 0),
            "created_at": datetime.utcnow(),
        })

    result_df = pd.DataFrame(rows)

    result_df.to_sql(
        TARGET_TABLE,
        engine,
        schema=TARGET_SCHEMA,
        if_exists="append",
        index=False,
        chunksize=1000,
        method="multi",
    )

    logger.info("========== PIPELINE END | Time: %.2f sec ==========",
                time.time() - start_time)

# ---------------------------------------------------------------------
# 🛠 DAG Definition
# ---------------------------------------------------------------------
with DAG(
    dag_id="feature_selection_setup",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["feature_selection", "pca", "target_encoding"],
) as dag:

    feature_selection_task = PythonOperator(
        task_id="compute_feature_target_statistics",
        python_callable=compute_feature_target_statistics,
    )

    feature_selection_task
