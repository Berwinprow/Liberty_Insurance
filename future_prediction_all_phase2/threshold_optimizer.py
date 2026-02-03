import numpy as np


# ======================================================
# APPLY THRESHOLD (JSON OR DEFAULT)
# ======================================================

def apply_threshold(y_prob, threshold):
    """
    Apply a fixed threshold to probability predictions.

    Behavior:
    - threshold is taken from model_cfg["threshold"] if present
    - otherwise caller passes default 0.5

    Parameters
    ----------
    y_prob : array-like
        Probabilities for positive class (class 1)
    threshold : float
        Threshold value (e.g. 0.6 or default 0.5)

    Returns
    -------
    np.ndarray
        Binary predictions (0 or 1)
    """
    return (np.asarray(y_prob) >= threshold).astype(int)
