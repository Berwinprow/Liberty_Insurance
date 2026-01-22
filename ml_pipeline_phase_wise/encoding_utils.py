from sklearn.preprocessing import LabelEncoder

def apply_label_encoding(X_train, X_test):
    X_tr = X_train.copy()
    X_te = X_test.copy()

    for col in X_tr.columns:
        if X_tr[col].dtype == "object":
            le = LabelEncoder()
            X_tr[col] = le.fit_transform(X_tr[col].astype(str))

            mapping = {v: i for i, v in enumerate(le.classes_)}
            max_id = max(mapping.values()) + 1

            X_te[col] = (
                X_te[col].astype(str)
                .map(mapping)
                .fillna(max_id)
            )

    print("LABEL ENCODING COMPLETED")
    return X_tr, X_te
