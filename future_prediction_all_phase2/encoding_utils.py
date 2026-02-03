from sklearn.preprocessing import LabelEncoder


def apply_label_encoding(
    X_train,
    X_test=None,
    X_open=None
):
    """
    Label encoding utility supporting:
    - train / test
    - train / open (future prediction)

    Rules:
    - Fit encoders ONLY on X_train
    - Apply same mapping to X_test / X_open
    - Unseen categories mapped to max_id + 1
    """

    X_tr = X_train.copy()
    X_te = X_test.copy() if X_test is not None else None
    X_op = X_open.copy() if X_open is not None else None

    for col in X_tr.columns:
        if X_tr[col].dtype == "object":
            le = LabelEncoder()

            # fit on training data only
            X_tr[col] = le.fit_transform(
                X_tr[col].astype(str)
            )

            mapping = {v: i for i, v in enumerate(le.classes_)}
            max_id = max(mapping.values()) + 1

            # apply to test data
            if X_te is not None:
                X_te[col] = (
                    X_te[col]
                    .astype(str)
                    .map(mapping)
                    .fillna(max_id)
                )

            # apply to open/future data
            if X_op is not None:
                X_op[col] = (
                    X_op[col]
                    .astype(str)
                    .map(mapping)
                    .fillna(max_id)
                )

    print("LABEL ENCODING COMPLETED")

    return X_tr, X_te, X_op
