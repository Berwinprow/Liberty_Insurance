from sklearn.preprocessing import StandardScaler


def apply_standard_scaling(
    X_train,
    X_test=None,
    X_open=None
):
    """
    Standard scaling utility.

    Rules:
    - Fit scaler ONLY on X_train
    - Apply same scaler to X_test / X_open
    - Return scaled copies (do NOT mutate inputs)
    """

    scaler = StandardScaler()

    X_tr = X_train.copy()
    X_te = X_test.copy() if X_test is not None else None
    X_op = X_open.copy() if X_open is not None else None

    # fit only on training
    X_tr_scaled = scaler.fit_transform(X_tr)

    if X_te is not None:
        X_te_scaled = scaler.transform(X_te)
    else:
        X_te_scaled = None

    if X_op is not None:
        X_op_scaled = scaler.transform(X_op)
    else:
        X_op_scaled = None

    print("STANDARD SCALING COMPLETED")

    return X_tr_scaled, X_te_scaled, X_op_scaled
