"""
Data splitting utilities for Future Prediction Phase-2.

Supports:
- Random stratified splits (80/20, 70/30)
- Time-based split driven by configuration
- Full training split for open / future prediction
"""

import json
import logging
from sklearn.model_selection import train_test_split

CONFIG_PATH = "/opt/airflow/dags/config/time_based_config.json"

logger = logging.getLogger(__name__)


# ======================================================
# 80 / 20 SPLIT
# ======================================================
def split_80_20(X, y):
    """
    Random 80/20 split with stratification.

    Parameters
    ----------
    X : pandas.DataFrame
        Feature dataframe.
    y : pandas.Series
        Target series (binary: 0/1).

    Returns
    -------
    tuple
        X_train, X_test, y_train, y_test
    """

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        stratify=y,
        random_state=42,
    )

    logger.info(
        "Split completed | Type=80/20 | Train=%d | Test=%d",
        len(X_train),
        len(X_test),
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
        random_state=42,
    )

    logger.info(
        "Split completed | Type=70/30 | Train=%d | Test=%d",
        len(X_train),
        len(X_test),
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

    with open(CONFIG_PATH, "r") as file:
        cfg = json.load(file)["time_based_window"]

    year_col = cfg["year_column"]
    month_col = cfg["month_column"]

    val_year = cfg["validation"]["year"]
    val_months = cfg["validation"]["months"]

    if year_col not in X.columns or month_col not in X.columns:
        raise ValueError(
            f"Required time columns missing: {year_col}, {month_col}"
        )

    test_mask = (
        (X[year_col] == val_year)
        & (X[month_col].isin(val_months))
    )

    X_train = X.loc[~test_mask]
    y_train = y.loc[~test_mask]

    X_test = X.loc[test_mask]
    y_test = y.loc[test_mask]

    logger.info(
        "Time-based split completed | "
        "Train=%d | Test=%d | Year=%s | Months=%s",
        len(X_train),
        len(X_test),
        val_year,
        val_months,
    )

    return X_train, X_test, y_train, y_test


# ======================================================
# FULL TRAIN SPLIT
# ======================================================
def full_train_split(X, y):
    """
    Full data training split for future / open policy prediction.
    No test set.
    """

    logger.info(
        "Full train split used | Rows=%d",
        len(X),
    )

    return X.copy(), y.copy()
