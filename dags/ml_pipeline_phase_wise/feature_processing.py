"""
Feature selection and date-based feature engineering utilities.
"""

import json
import logging

import pandas as pd


CONFIG_PATH = "/opt/airflow/dags/config/connections_table_columns.json"

logger = logging.getLogger(__name__)


def process_features(df, selected_columns):
    """
    Select required columns and perform date feature engineering.

    Args:
        df (pd.DataFrame): Input dataframe
        selected_columns (list): Columns to keep

    Returns:
        pd.DataFrame: Processed dataframe
    """
    # ================= LOAD CONFIG =================
    with open(CONFIG_PATH, "r") as file:
        cfg = json.load(file)

    date_columns = cfg["columns"]["date_columns"]

    # ================= SELECT COLUMNS =================
    df = df[selected_columns].copy()

    # ================= DATE FEATURE ENGINEERING =================
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(
                df[col],
                errors="coerce",
            )
            df[f"{col}_day"] = df[col].dt.day
            df[f"{col}_month"] = df[col].dt.month
            df[f"{col}_year"] = df[col].dt.year

    # ================= DROP ORIGINAL DATE COLUMNS =================
    df.drop(
        columns=date_columns,
        inplace=True,
        errors="ignore",
    )

    logger.info(
        "FEATURE ENGINEERING DONE | Columns: %s",
        df.shape[1],
    )

    return df
