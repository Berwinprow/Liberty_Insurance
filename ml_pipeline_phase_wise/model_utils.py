from sklearn.preprocessing import StandardScaler
from imblearn.over_sampling import SMOTE, RandomOverSampler
from imblearn.under_sampling import RandomUnderSampler

def apply_sampling(X, y, method):
    if method == "none":
        return X.copy(), y.copy()

    try:
        sampler = {
            "smote": SMOTE(random_state=42),
            "oversample": RandomOverSampler(random_state=42),
            "undersample": RandomUnderSampler(random_state=42)
        }[method]

        Xr, yr = sampler.fit_resample(X, y)
        print(f"SAMPLING DONE → {method} | Rows: {len(Xr)}")
        return Xr, yr

    except Exception as e:
        print(f"SAMPLING FAILED → {method} | {e}")
        return X.copy(), y.copy()


def apply_scaling(X_train, X_test, enable):
    if not enable:
        return X_train, X_test

    scaler = StandardScaler()
    cols = X_train.select_dtypes(include=["int64", "float64"]).columns

    X_tr = X_train.copy()
    X_te = X_test.copy()

    X_tr[cols] = scaler.fit_transform(X_tr[cols])
    X_te[cols] = scaler.transform(X_te[cols])

    print("SCALING APPLIED")
    return X_tr, X_te
