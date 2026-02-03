import json
import pandas as pd
from sklearn.model_selection import train_test_split

CONFIG_PATH = "/opt/airflow/dags/config/time_based_config.json"


# ======================================================
# 80 / 20 SPLIT
# ======================================================
def split_80_20(X, y):
    """
    Random 80/20 split with stratification.
    X: feature dataframe
    y: target series (0/1 only)
    """

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        stratify=y,
        random_state=42
    )

    return X_train, X_test, y_train, y_test


# ======================================================
# 70 / 30 SPLIT
# ======================================================
def split_70_30(X, y):
    """
    Random 70/30 split with stratification.
    """

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.3,
        stratify=y,
        random_state=42
    )

    return X_train, X_test, y_train, y_test


# ======================================================
# TIME BASED SPLIT
# ======================================================
def time_based_split(X, y):
    """
    Time-based split driven by time_based_config.json.

    Uses feature-engineered year and month columns
    instead of raw date columns.
    """

    with open(CONFIG_PATH, "r") as f:
        cfg = json.load(f)["time_based_window"]

    year_col = cfg["year_column"]
    month_col = cfg["month_column"]

    val_year = cfg["validation"]["year"]
    val_months = cfg["validation"]["months"]

    if year_col not in X.columns or month_col not in X.columns:
        raise ValueError(
            f"Required time columns missing: {year_col}, {month_col}"
        )

    test_mask = (
        (X[year_col] == val_year) &
        (X[month_col].isin(val_months))
    )

    X_train = X.loc[~test_mask]
    y_train = y.loc[~test_mask]

    X_test = X.loc[test_mask]
    y_test = y.loc[test_mask]

    return X_train, X_test, y_train, y_test


# ======================================================
# FULL TRAIN SPLIT
# ======================================================
def full_train_split(X, y):
    """
    Full data training split for future / open policy prediction.
    No test set.
    """

    return X.copy(), y.copy()
