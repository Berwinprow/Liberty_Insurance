"""
Scaling utilities for Future Prediction Phase-2.

Provides standard scaling for:
- Train data
- Test data
- Open / future prediction data

Rules:
- Scaler is fit ONLY on training data
- Same scaler applied to test / open
- Inputs are NOT mutated
"""

import logging
from sklearn.preprocessing import StandardScaler

logger = logging.getLogger(__name__)


def apply_standard_scaling(
    X_train,
    X_test=None,
    X_open=None,
):
    """
    Apply standard scaling.

    Parameters
    ----------
    X_train : array-like
        Training feature matrix.
    X_test : array-like, optional
        Test feature matrix.
    X_open : array-like, optional
        Open / future prediction feature matrix.

    Returns
    -------
    tuple
        Scaled (X_train, X_test, X_open)
    """

    scaler = StandardScaler()

    X_tr = X_train.copy()
    X_te = X_test.copy() if X_test is not None else None
    X_op = X_open.copy() if X_open is not None else None

    # Fit ONLY on training data
    X_tr_scaled = scaler.fit_transform(X_tr)

    X_te_scaled = scaler.transform(X_te) if X_te is not None else None
    X_op_scaled = scaler.transform(X_op) if X_op is not None else None

    logger.info("Standard scaling completed")

    return X_tr_scaled, X_te_scaled, X_op_scaled
