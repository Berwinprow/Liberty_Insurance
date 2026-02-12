"""
Utility functions for sampling and scaling features.
"""

import logging

from imblearn.over_sampling import (
    RandomOverSampler,
    SMOTE,
)
from imblearn.under_sampling import RandomUnderSampler
from sklearn.preprocessing import StandardScaler


logger = logging.getLogger(__name__)


def apply_sampling(X, y, method):
    """
    Apply sampling technique to handle class imbalance.

    Args:
        X: Feature dataframe
        y: Target series
        method (str): Sampling method
            - "none"
            - "smote"
            - "oversample"
            - "undersample"

    Returns:
        tuple: Resampled (X, y)
    """
    if method == "none":
        return X.copy(), y.copy()

    try:
        sampler = {
            "smote": SMOTE(random_state=42),
            "oversample": RandomOverSampler(random_state=42),
            "undersample": RandomUnderSampler(random_state=42),
        }[method]

        Xr, yr = sampler.fit_resample(X, y)

        logger.info(
            "SAMPLING DONE → %s | Rows: %s",
            method,
            len(Xr),
        )

        return Xr, yr

    except Exception as exc:
        logger.error(
            "SAMPLING FAILED → %s | %s",
            method,
            exc,
        )
        return X.copy(), y.copy()


def apply_scaling(X_train, X_test, enable):
    """
    Apply standard scaling to numerical columns.

    Args:
        X_train: Training features
        X_test: Test features
        enable (bool): Whether scaling is enabled

    Returns:
        tuple: Scaled (X_train, X_test)
    """
    if not enable:
        return X_train, X_test

    scaler = StandardScaler()
    cols = X_train.select_dtypes(
        include=["int64", "float64"]
    ).columns

    X_tr = X_train.copy()
    X_te = X_test.copy()

    X_tr[cols] = scaler.fit_transform(X_tr[cols])
    X_te[cols] = scaler.transform(X_te[cols])

    logger.info("SCALING APPLIED")

    return X_tr, X_te
