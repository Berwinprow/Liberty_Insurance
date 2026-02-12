"""
Encoding utilities for categorical features.
"""

import logging

from sklearn.preprocessing import LabelEncoder


logger = logging.getLogger(__name__)


def apply_label_encoding(X_train, X_test):
    """
    Apply label encoding to categorical (object) columns.

    Encoding is fitted on training data and applied to test data.
    Unseen categories in test data are assigned a new label.

    Args:
        X_train: Training feature dataframe
        X_test: Testing feature dataframe

    Returns:
        tuple: Encoded (X_train, X_test)
    """
    X_tr = X_train.copy()
    X_te = X_test.copy()

    for col in X_tr.columns:
        if X_tr[col].dtype == "object":
            le = LabelEncoder()
            X_tr[col] = le.fit_transform(X_tr[col].astype(str))

            mapping = {v: i for i, v in enumerate(le.classes_)}
            max_id = max(mapping.values()) + 1

            X_te[col] = (
                X_te[col]
                .astype(str)
                .map(mapping)
                .fillna(max_id)
            )

    logger.info("LABEL ENCODING COMPLETED")

    return X_tr, X_te
