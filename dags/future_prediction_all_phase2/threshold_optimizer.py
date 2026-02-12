"""
Threshold utilities for Future Prediction Phase-2.

Provides a simple and consistent way to apply
probability thresholds for binary classification.
"""

import logging
import numpy as np

logger = logging.getLogger(__name__)


# ======================================================
# APPLY THRESHOLD
# ======================================================
def apply_threshold(y_prob, threshold):
    """
    Apply a fixed threshold to probability predictions.

    Behavior
    --------
    - Threshold is provided explicitly by the caller
    - Default handling (e.g., 0.5) is done upstream

    Parameters
    ----------
    y_prob : array-like
        Probabilities for the positive class (class 1).
    threshold : float
        Threshold value (e.g. 0.6 or 0.5).

    Returns
    -------
    numpy.ndarray
        Binary predictions (0 or 1).
    """

    preds = (np.asarray(y_prob) >= threshold).astype(int)

    logger.debug(
        "Threshold applied | Threshold=%.3f | Samples=%d",
        threshold,
        len(preds),
    )

    return preds
