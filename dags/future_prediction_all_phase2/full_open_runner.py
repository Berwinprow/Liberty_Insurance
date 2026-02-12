"""
Full train + open prediction runner for Future Prediction Phase-2.

Responsibilities:
- Train final model on full labeled data
- Apply threshold
- Predict outcomes for open customers
- Support single, ensemble, and seven-set strategies
"""

import json
import logging
import numpy as np
from datetime import datetime

from sklearn.metrics import (
    accuracy_score,
    roc_auc_score,
    log_loss,
    precision_score,
    recall_score,
    f1_score,
    confusion_matrix,
)
from imblearn.under_sampling import RandomUnderSampler

from future_prediction_all_phase2.single_model_library import create_single_model
from future_prediction_all_phase2.ensembled_models import (
    build_soft_voting,
    build_stacking,
    build_weighted_ensemble,
    build_bagging,
)
from future_prediction_all_phase2.threshold_optimizer import apply_threshold

logger = logging.getLogger(__name__)


# ======================================================
# TRAIN METRICS
# ======================================================
def compute_train_metrics(y_true, y_prob, threshold):
    """
    Compute training metrics using a fixed threshold.
    """
    y_pred = apply_threshold(y_prob, threshold)
    tn, fp, fn, tp = confusion_matrix(y_true, y_pred).ravel()

    return {
        "accuracy": accuracy_score(y_true, y_pred),
        "roc": roc_auc_score(y_true, y_prob),
        "logloss": log_loss(y_true, y_prob),
        "precision_class1": precision_score(y_true, y_pred, pos_label=1),
        "precision_class0": precision_score(y_true, y_pred, pos_label=0),
        "recall_class1": recall_score(y_true, y_pred, pos_label=1),
        "recall_class0": recall_score(y_true, y_pred, pos_label=0),
        "f1_class1": f1_score(y_true, y_pred, pos_label=1),
        "f1_class0": f1_score(y_true, y_pred, pos_label=0),
        "tp": int(tp),
        "tn": int(tn),
        "fp": int(fp),
        "fn": int(fn),
    }


# ======================================================
# FULL TRAIN + OPEN PREDICTION
# ======================================================
def run_full_open_prediction(
    X_train,
    y_train,
    X_open,
    model_name,
    model_cfg,
    feature_set,
    sampling_method,
):
    """
    Full training + open customer prediction runner.

    Rules:
    - Threshold must be scalar (handled upstream)
    - Supports single, ensemble, and seven-set logic
    """

    logger.info(
        "Full open prediction started | Model=%s | Sampling=%s",
        model_name,
        sampling_method,
    )

    # ================= SAFETY =================
    X_train = X_train.values if hasattr(X_train, "values") else np.asarray(X_train)
    X_open = X_open.values if hasattr(X_open, "values") else np.asarray(X_open)
    y_train = y_train.values if hasattr(y_train, "values") else np.asarray(y_train)

    threshold = model_cfg.get("threshold", 0.5)

    # ======================================================
    # SEVEN SET LOGIC
    # ======================================================
    if sampling_method == "seven_set":
        train_probs = []
        open_probs = []

        for i in range(7):
            rus = RandomUnderSampler(random_state=i)
            X_us, y_us = rus.fit_resample(X_train, y_train)

            if model_cfg["type"] == "single":
                model = create_single_model(
                    model_name,
                    model_cfg.get("params", {}),
                )
                model.fit(X_us, y_us)

                train_probs.append(model.predict_proba(X_train)[:, 1])
                open_probs.append(model.predict_proba(X_open)[:, 1])

            else:
                method = model_cfg["method"]

                if method == "soft_voting":
                    model = build_soft_voting(model_cfg, X_us, X_us, y_us)
                    train_probs.append(model.predict_proba(X_train)[:, 1])
                    open_probs.append(model.predict_proba(X_open)[:, 1])

                elif method == "stacking":
                    model = build_stacking(model_cfg, X_us, X_us, y_us)
                    train_probs.append(model.predict_proba(X_train)[:, 1])
                    open_probs.append(model.predict_proba(X_open)[:, 1])

                elif method == "bagging":
                    model = build_bagging(model_cfg, X_us, X_us, y_us)
                    train_probs.append(model.predict_proba(X_train)[:, 1])
                    open_probs.append(model.predict_proba(X_open)[:, 1])

                elif method == "weighted":
                    model = build_weighted_ensemble(model_cfg, X_us, X_us, y_us)
                    train_probs.append(
                        model.predict_proba((X_train, X_train))[:, 1]
                    )
                    open_probs.append(
                        model.predict_proba((X_open, X_open))[:, 1]
                    )

                else:
                    raise ValueError(f"Unsupported ensemble method: {method}")

        y_tr_prob = np.mean(train_probs, axis=0)
        y_op_prob = np.mean(open_probs, axis=0)

    # ======================================================
    # NORMAL LOGIC
    # ======================================================
    else:
        if model_cfg["type"] == "single":
            model = create_single_model(
                model_name,
                model_cfg.get("params", {}),
            )
            model.fit(X_train, y_train)

            y_tr_prob = model.predict_proba(X_train)[:, 1]
            y_op_prob = model.predict_proba(X_open)[:, 1]

        else:
            method = model_cfg["method"]

            if method == "soft_voting":
                model = build_soft_voting(model_cfg, X_train, X_train, y_train)

            elif method == "stacking":
                model = build_stacking(model_cfg, X_train, X_train, y_train)

            elif method == "bagging":
                model = build_bagging(model_cfg, X_train, X_train, y_train)

            elif method == "weighted":
                model = build_weighted_ensemble(
                    model_cfg,
                    X_train,
                    X_train,
                    y_train,
                )

            else:
                raise ValueError(f"Unsupported ensemble method: {method}")

            y_tr_prob = (
                model.predict_proba((X_train, X_train))[:, 1]
                if method == "weighted"
                else model.predict_proba(X_train)[:, 1]
            )
            y_op_prob = (
                model.predict_proba((X_open, X_open))[:, 1]
                if method == "weighted"
                else model.predict_proba(X_open)[:, 1]
            )

    # ================= TRAIN METRICS =================
    train_m = compute_train_metrics(y_train, y_tr_prob, threshold)

    # ================= OPEN PREDICTIONS =================
    open_pred = apply_threshold(y_op_prob, threshold)

    predicted_not_renewed = int((open_pred == 1).sum())
    predicted_renewed = int((open_pred == 0).sum())

    logger.info(
        "Open prediction completed | Renewed=%d | Not renewed=%d | Total=%d",
        predicted_renewed,
        predicted_not_renewed,
        len(X_open),
    )

    # ================= RESULT ROW =================
    return {
        "run_timestamp": datetime.utcnow(),
        "feature": feature_set,
        "data_splitting": "full",
        "sampling_method": sampling_method,
        "model_name": model_name,
        "parameter": json.dumps(model_cfg.get("params", {})),

        # ---- TRAIN METRICS ----
        "train_accuracy": train_m["accuracy"],
        "train_accuracy_class1": train_m["recall_class1"],
        "train_accuracy_class0": train_m["recall_class0"],
        "train_logloss": train_m["logloss"],
        "train_roc": train_m["roc"],
        "train_precision_class1": train_m["precision_class1"],
        "train_precision_class0": train_m["precision_class0"],
        "train_recall_class1": train_m["recall_class1"],
        "train_recall_class0": train_m["recall_class0"],
        "train_f1_class1": train_m["f1_class1"],
        "train_f1_class0": train_m["f1_class0"],
        "train_truepositive": train_m["tp"],
        "train_truenegative": train_m["tn"],
        "train_falsepositive": train_m["fp"],
        "train_falsenegative": train_m["fn"],

        # ---- OPEN SUMMARY ----
        "predicted_renewed": predicted_renewed,
        "predicted_not_renewed": predicted_not_renewed,
        "total_open_customers": int(len(X_open)),

        "train_size": int(len(X_train)),
        "open_customers_size": int(len(X_open)),
    }
