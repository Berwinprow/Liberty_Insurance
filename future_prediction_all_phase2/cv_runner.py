import json
import numpy as np
from datetime import datetime

from sklearn.model_selection import StratifiedKFold
from sklearn.metrics import (
    accuracy_score,
    roc_auc_score,
    log_loss,
    precision_score,
    recall_score,
    f1_score,
    confusion_matrix
)
from imblearn.under_sampling import RandomUnderSampler

from future_prediction_all_phase2.single_model_library import create_single_model
from future_prediction_all_phase2.ensembled_models import (
    build_soft_voting,
    build_stacking,
    build_weighted_ensemble,
    build_bagging
)
from future_prediction_all_phase2.threshold_optimizer import apply_threshold


# ======================================================
# METRIC CALCULATION
# ======================================================
def compute_metrics(y_true, y_prob, threshold):
    y_pred = apply_threshold(y_prob, threshold)

    tn, fp, fn, tp = confusion_matrix(y_true, y_pred).ravel()

    return {
        "accuracy": accuracy_score(y_true, y_pred),
        "roc_auc": roc_auc_score(y_true, y_prob),
        "log_loss": log_loss(y_true, y_prob),

        "precision_class1": precision_score(y_true, y_pred, pos_label=1),
        "precision_class0": precision_score(y_true, y_pred, pos_label=0),
        "recall_class1": recall_score(y_true, y_pred, pos_label=1),
        "recall_class0": recall_score(y_true, y_pred, pos_label=0),
        "f1_class1": f1_score(y_true, y_pred, pos_label=1),
        "f1_class0": f1_score(y_true, y_pred, pos_label=0),

        "tp": int(tp),
        "tn": int(tn),
        "fp": int(fp),
        "fn": int(fn)
    }


# ======================================================
# CROSS VALIDATION RUNNER
# ======================================================
def run_cross_validation(
    X,
    y,
    model_name,
    model_cfg,
    split_name,
    feature_set,
    sampling_method,
    k_folds=5
):
    """
    STRICT CV RUNNER
    Threshold MUST be passed as scalar via model_cfg["threshold"]
    """

    # ================= SAFETY =================
    X_arr = X.values if hasattr(X, "iloc") else np.asarray(X)
    y_arr = y.values if hasattr(y, "iloc") else np.asarray(y)

    # threshold is already scalar (orchestrator handles list)
    threshold = model_cfg.get("threshold", 0.5)

    skf = StratifiedKFold(
        n_splits=k_folds,
        shuffle=True,
        random_state=42
    )

    train_metrics = []
    test_metrics = []

    for fold, (tr_idx, te_idx) in enumerate(skf.split(X_arr, y_arr), start=1):

        X_tr = X_arr[tr_idx]
        X_te = X_arr[te_idx]
        y_tr = y_arr[tr_idx]
        y_te = y_arr[te_idx]

        # ======================================================
        # SEVEN SET LOGIC
        # ======================================================
        if sampling_method == "seven_set":

            tr_probs = []
            te_probs = []

            for i in range(7):
                rus = RandomUnderSampler(random_state=i)
                X_us, y_us = rus.fit_resample(X_tr, y_tr)

                if model_cfg["type"] == "single":

                    model = create_single_model(
                        model_name,
                        model_cfg.get("params", {})
                    )
                    model.fit(X_us, y_us)

                    tr_probs.append(model.predict_proba(X_tr)[:, 1])
                    te_probs.append(model.predict_proba(X_te)[:, 1])

                else:
                    if model_cfg["method"] == "soft_voting":
                        model = build_soft_voting(
                            model_cfg,
                            X_us,
                            X_us,
                            y_us
                        )
                        tr_probs.append(model.predict_proba(X_tr)[:, 1])
                        te_probs.append(model.predict_proba(X_te)[:, 1])

                    elif model_cfg["method"] == "stacking":
                        model = build_stacking(
                            model_cfg,
                            X_us,
                            X_us,
                            y_us
                        )
                        tr_probs.append(model.predict_proba(X_tr)[:, 1])
                        te_probs.append(model.predict_proba(X_te)[:, 1])

                    elif model_cfg["method"] == "bagging":
                        model = build_bagging(
                            model_cfg,
                            X_us,
                            X_us,
                            y_us
                        )
                        tr_probs.append(model.predict_proba(X_tr)[:, 1])
                        te_probs.append(model.predict_proba(X_te)[:, 1])

                    elif model_cfg["method"] == "weighted":
                        model = build_weighted_ensemble(
                            model_cfg,
                            X_us,
                            X_us,
                            y_us
                        )
                        tr_probs.append(
                            model.predict_proba((X_tr, X_tr))[:, 1]
                        )
                        te_probs.append(
                            model.predict_proba((X_te, X_te))[:, 1]
                        )

                    else:
                        raise ValueError(
                            f"Unsupported ensemble method: {model_cfg['method']}"
                        )

            y_tr_prob = np.mean(tr_probs, axis=0)
            y_te_prob = np.mean(te_probs, axis=0)

        # ======================================================
        # NORMAL LOGIC
        # ======================================================
        else:
            if model_cfg["type"] == "single":

                model = create_single_model(
                    model_name,
                    model_cfg.get("params", {})
                )
                model.fit(X_tr, y_tr)

                y_tr_prob = model.predict_proba(X_tr)[:, 1]
                y_te_prob = model.predict_proba(X_te)[:, 1]

            else:
                if model_cfg["method"] == "soft_voting":
                    model = build_soft_voting(
                        model_cfg,
                        X_tr,
                        X_tr,
                        y_tr
                    )
                    y_tr_prob = model.predict_proba(X_tr)[:, 1]
                    y_te_prob = model.predict_proba(X_te)[:, 1]

                elif model_cfg["method"] == "stacking":
                    model = build_stacking(
                        model_cfg,
                        X_tr,
                        X_tr,
                        y_tr
                    )
                    y_tr_prob = model.predict_proba(X_tr)[:, 1]
                    y_te_prob = model.predict_proba(X_te)[:, 1]

                elif model_cfg["method"] == "bagging":
                    model = build_bagging(
                        model_cfg,
                        X_tr,
                        X_tr,
                        y_tr
                    )
                    y_tr_prob = model.predict_proba(X_tr)[:, 1]
                    y_te_prob = model.predict_proba(X_te)[:, 1]

                elif model_cfg["method"] == "weighted":
                    model = build_weighted_ensemble(
                        model_cfg,
                        X_tr,
                        X_tr,
                        y_tr
                    )
                    y_tr_prob = model.predict_proba((X_tr, X_tr))[:, 1]
                    y_te_prob = model.predict_proba((X_te, X_te))[:, 1]

                else:
                    raise ValueError(
                        f"Unsupported ensemble method: {model_cfg['method']}"
                    )

        train_metrics.append(
            compute_metrics(y_tr, y_tr_prob, threshold)
        )
        test_metrics.append(
            compute_metrics(y_te, y_te_prob, threshold)
        )

    # ======================================================
    # AGGREGATE RESULTS
    # ======================================================
    def mean_metric(metrics, key):
        return float(np.mean([m[key] for m in metrics]))

    row = {
        "run_timestamp": datetime.utcnow(),
        "feature": feature_set,
        "data_splitting": split_name,
        "sampling_method": sampling_method,
        "model_name": model_name,
        "parameter": json.dumps(model_cfg.get("params", {})),

        # ---- CV TRAIN ----
        "cv_mean_train_accuracy": mean_metric(train_metrics, "accuracy"),
        "cv_mean_train_roc_auc": mean_metric(train_metrics, "roc_auc"),
        "cv_mean_train_log_loss": mean_metric(train_metrics, "log_loss"),
        "cv_mean_train_precision_class1": mean_metric(train_metrics, "precision_class1"),
        "cv_mean_train_precision_class0": mean_metric(train_metrics, "precision_class0"),
        "cv_mean_train_recall_class1": mean_metric(train_metrics, "recall_class1"),
        "cv_mean_train_recall_class0": mean_metric(train_metrics, "recall_class0"),
        "cv_mean_train_f1_class1": mean_metric(train_metrics, "f1_class1"),
        "cv_mean_train_f1_class0": mean_metric(train_metrics, "f1_class0"),

        # ---- CV TEST ----
        "cv_mean_test_accuracy": mean_metric(test_metrics, "accuracy"),
        "cv_mean_test_roc_auc": mean_metric(test_metrics, "roc_auc"),
        "cv_mean_test_log_loss": mean_metric(test_metrics, "log_loss"),
        "cv_mean_test_precision_class1": mean_metric(test_metrics, "precision_class1"),
        "cv_mean_test_precision_class0": mean_metric(test_metrics, "precision_class0"),
        "cv_mean_test_recall_class1": mean_metric(test_metrics, "recall_class1"),
        "cv_mean_test_recall_class0": mean_metric(test_metrics, "recall_class0"),
        "cv_mean_test_f1_class1": mean_metric(test_metrics, "f1_class1"),
        "cv_mean_test_f1_class0": mean_metric(test_metrics, "f1_class0"),

        # ---- LAST FOLD ----
        "test_accuracy": test_metrics[-1]["accuracy"],
        "test_roc": test_metrics[-1]["roc_auc"],
        "test_logloss": test_metrics[-1]["log_loss"],
        "test_precision_class1": test_metrics[-1]["precision_class1"],
        "test_precision_class0": test_metrics[-1]["precision_class0"],
        "test_recall_class1": test_metrics[-1]["recall_class1"],
        "test_recall_class0": test_metrics[-1]["recall_class0"],
        "test_f1_class1": test_metrics[-1]["f1_class1"],
        "test_f1_class0": test_metrics[-1]["f1_class0"],
        "test_truepositive": test_metrics[-1]["tp"],
        "test_truenegative": test_metrics[-1]["tn"],
        "test_falsepositive": test_metrics[-1]["fp"],
        "test_falsenegative": test_metrics[-1]["fn"],

        "train_size": int(len(X_arr)),
        "test_size": int(len(X_arr) / k_folds)
    }

    return row
