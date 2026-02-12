"""
Sampling utilities for Future Prediction Phase-2.

Supports:
- No sampling
- SMOTE
- Random over-sampling
- Random under-sampling
- Seven-set under-sampling
"""

import logging
from imblearn.over_sampling import SMOTE, RandomOverSampler
from imblearn.under_sampling import RandomUnderSampler

logger = logging.getLogger(__name__)


# ======================================================
# MAIN SAMPLING DISPATCHER
# ======================================================
def apply_sampling(X, y, method):
    """
    Apply sampling technique on training data.

    Supported methods:
    - none
    - smote
    - oversampling
    - undersampling
    - seven_set

    Returns:
    - (X_resampled, y_resampled)
      OR
    - dict[str, tuple(X, y)] for seven_set
    """

    # ---------- NO SAMPLING ----------
    if method == "none":
        logger.info("Sampling skipped | Method=none")
        return X.copy(), y.copy()

    # ---------- SEVEN SET UNDERSAMPLING ----------
    if method == "seven_set":
        logger.info("Seven-set undersampling started")
        return seven_set_undersampling(X, y)

    sampler_map = {
        "smote": SMOTE(random_state=42),
        "oversampling": RandomOverSampler(random_state=42),
        "undersampling": RandomUnderSampler(random_state=42),
    }

    try:
        if method not in sampler_map:
            raise ValueError(f"Unsupported sampling method: {method}")

        sampler = sampler_map[method]
        X_res, y_res = sampler.fit_resample(X, y)

        logger.info(
            "Sampling completed | Method=%s | Rows=%d",
            method,
            len(X_res),
        )

        return X_res, y_res

    except Exception as exc:
        logger.exception(
            "Sampling failed | Method=%s | Falling back to original data",
            method,
        )
        return X.copy(), y.copy()


# ======================================================
# SEVEN SET UNDERSAMPLING
# ======================================================
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

        logger.debug(
            "Seven-set generated | Set=%d | Rows=%d",
            i + 1,
            len(X_us),
        )

    logger.info("Seven-set undersampling completed | Sets=7")

    return undersampled_sets
