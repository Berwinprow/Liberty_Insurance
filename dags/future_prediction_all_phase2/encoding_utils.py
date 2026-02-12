"""
Label encoding utilities for Future Prediction Phase-2.

Supports:
- Train / Test encoding
- Train / Open (future prediction) encoding

Rules:
- Encoders are fit ONLY on training data
- Same mapping applied to test / open
- Unseen categories mapped to max_id + 1
"""

import logging
from sklearn.preprocessing import LabelEncoder

logger = logging.getLogger(__name__)


def apply_label_encoding(
    X_train,
    X_test=None,
    X_open=None,
):
    """
    Apply label encoding to categorical columns.

    Parameters
    ----------
    X_train : pd.DataFrame
        Training feature set.
    X_test : pd.DataFrame, optional
        Test feature set.
    X_open : pd.DataFrame, optional
        Open / future prediction feature set.

    Returns
    -------
    tuple
        Encoded (X_train, X_test, X_open)
    """
    X_tr = X_train.copy()
    X_te = X_test.copy() if X_test is not None else None
    X_op = X_open.copy() if X_open is not None else None

    for col in X_tr.columns:
        if X_tr[col].dtype != "object":
            continue

        le = LabelEncoder()

        # Fit ONLY on training data
        X_tr[col] = le.fit_transform(
            X_tr[col].astype(str)
        )

        mapping = {v: i for i, v in enumerate(le.classes_)}
        max_id = max(mapping.values()) + 1

        # Apply to test data
        if X_te is not None:
            X_te[col] = (
                X_te[col]
                .astype(str)
                .map(mapping)
                .fillna(max_id)
            )

        # Apply to open / future data
        if X_op is not None:
            X_op[col] = (
                X_op[col]
                .astype(str)
                .map(mapping)
                .fillna(max_id)
            )

    logger.info("LABEL ENCODING COMPLETED")

    return X_tr, X_te, X_op
