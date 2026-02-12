"""
Feature processing utilities for Future Prediction Phase-2.

Responsibilities:
- Select configured feature columns
- Create date-derived features (year, month, day)
- Drop original date columns
"""

import json
import logging
import pandas as pd

CONFIG_PATH = "/opt/airflow/dags/config/connections_table_columns.json"

logger = logging.getLogger(__name__)


def process_features(df, selected_columns):
    """
    Select features, derive date features,
    and drop raw date columns.

    Parameters
    ----------
    df : pandas.DataFrame
        Input dataframe
    selected_columns : list
        Columns to retain before feature engineering

    Returns
    -------
    pandas.DataFrame
        Processed feature dataframe
    """

    # ================= LOAD CONFIG =================
    with open(CONFIG_PATH, "r") as f:
        cfg = json.load(f)

    date_columns = cfg["columns"]["date_columns"]

    # ================= SELECT REQUIRED COLUMNS =================
    df = df[selected_columns].copy()

    # ================= DATE FEATURE ENGINEERING =================
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

            df[f"{col}_year"] = df[col].dt.year
            df[f"{col}_month"] = df[col].dt.month
            df[f"{col}_day"] = df[col].dt.day

    # ================= DROP RAW DATE COLUMNS =================
    drop_cols = [c for c in date_columns if c in df.columns]
    df = df.drop(columns=drop_cols)

    logger.info(
        "Feature processing completed | "
        "Final columns=%d | Dropped date columns=%s",
        df.shape[1],
        drop_cols
    )

    return df
