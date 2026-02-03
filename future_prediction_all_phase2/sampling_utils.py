from imblearn.over_sampling import SMOTE, RandomOverSampler
from imblearn.under_sampling import RandomUnderSampler


def apply_sampling(X, y, method):
    """
    Apply sampling technique on training data.

    Supported methods:
    - none
    - smote
    - oversampling
    - undersampling
    - seven_set
    """

    # ---------- NO SAMPLING ----------
    if method == "none":
        return X.copy(), y.copy()

    # ---------- SEVEN SET UNDERSAMPLING ----------
    if method == "seven_set":
        return seven_set_undersampling(X, y)

    try:
        sampler_map = {
            "smote": SMOTE(random_state=42),
            "oversampling": RandomOverSampler(random_state=42),
            "undersampling": RandomUnderSampler(random_state=42)
        }

        if method not in sampler_map:
            raise ValueError(f"Unsupported sampling method: {method}")

        sampler = sampler_map[method]
        X_res, y_res = sampler.fit_resample(X, y)

        print(f"SAMPLING DONE | Method: {method} | Rows: {len(X_res)}")
        return X_res, y_res

    except Exception as e:
        print(f"SAMPLING FAILED | Method: {method} | Error: {e}")
        return X.copy(), y.copy()


def seven_set_undersampling(X, y):
    """
    Generate 7 different undersampled training sets
    using different random states.

    Returns:
    {
        "train_set_1": (X1, y1),
        ...
        "train_set_7": (X7, y7)
    }
    """

    undersampled_sets = {}

    for i in range(7):
        rus = RandomUnderSampler(random_state=i)
        X_us, y_us = rus.fit_resample(X, y)
        undersampled_sets[f"train_set_{i + 1}"] = (X_us, y_us)

    print("7-SET UNDERSAMPLING COMPLETED")

    return undersampled_sets
