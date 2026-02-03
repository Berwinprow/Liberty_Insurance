import json
import numpy as np
from datetime import datetime

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
        "fn": int(fn)
    }


# ======================================================
# TIME BASED RUNNER
# ======================================================
def run_time_based(
    X_train,
    y_train,
    X_test,
    y_test,
    model_name,
    model_cfg,
    feature_set,
    sampling_method,
    time_cfg
):
    """
    STRICT TIME-BASED RUNNER
    Threshold MUST be scalar (handled by orchestrator)
    """

    # ================= SAFETY =================
    X_train = X_train.values if hasattr(X_train, "values") else np.asarray(X_train)
    X_test = X_test.values if hasattr(X_test, "values") else np.asarray(X_test)

    y_train = y_train.values if hasattr(y_train, "values") else np.asarray(y_train)
    y_test = y_test.values if hasattr(y_test, "values") else np.asarray(y_test)

    # threshold already scalar
    threshold = model_cfg.get("threshold", 0.5)

    # ======================================================
    # SEVEN SET LOGIC
    # ======================================================
    if sampling_method == "seven_set":

        tr_probs = []
        te_probs = []

        for i in range(7):
            rus = RandomUnderSampler(random_state=i)
            X_us, y_us = rus.fit_resample(X_train, y_train)

            if model_cfg["type"] == "single":

                model = create_single_model(
                    model_name,
                    model_cfg.get("params", {})
                )
                model.fit(X_us, y_us)

                tr_probs.append(model.predict_proba(X_train)[:, 1])
                te_probs.append(model.predict_proba(X_test)[:, 1])

            else:
                if model_cfg["method"] == "soft_voting":
                    model = build_soft_voting(
                        model_cfg,
                        X_us,
                        X_us,
                        y_us
                    )
                    tr_probs.append(model.predict_proba(X_train)[:, 1])
                    te_probs.append(model.predict_proba(X_test)[:, 1])

                elif model_cfg["method"] == "stacking":
                    model = build_stacking(
                        model_cfg,
                        X_us,
                        X_us,
                        y_us
                    )
                    tr_probs.append(model.predict_proba(X_train)[:, 1])
                    te_probs.append(model.predict_proba(X_test)[:, 1])

                elif model_cfg["method"] == "bagging":
                    model = build_bagging(
                        model_cfg,
                        X_us,
                        X_us,
                        y_us
                    )
                    tr_probs.append(model.predict_proba(X_train)[:, 1])
                    te_probs.append(model.predict_proba(X_test)[:, 1])

                elif model_cfg["method"] == "weighted":
                    model = build_weighted_ensemble(
                        model_cfg,
                        X_us,
                        X_us,
                        y_us
                    )
                    tr_probs.append(
                        model.predict_proba((X_train, X_train))[:, 1]
                    )
                    te_probs.append(
                        model.predict_proba((X_test, X_test))[:, 1]
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
            model.fit(X_train, y_train)

            y_tr_prob = model.predict_proba(X_train)[:, 1]
            y_te_prob = model.predict_proba(X_test)[:, 1]

        else:
            if model_cfg["method"] == "soft_voting":
                model = build_soft_voting(
                    model_cfg,
                    X_train,
                    X_train,
                    y_train
                )
                y_tr_prob = model.predict_proba(X_train)[:, 1]
                y_te_prob = model.predict_proba(X_test)[:, 1]

            elif model_cfg["method"] == "stacking":
                model = build_stacking(
                    model_cfg,
                    X_train,
                    X_train,
                    y_train
                )
                y_tr_prob = model.predict_proba(X_train)[:, 1]
                y_te_prob = model.predict_proba(X_test)[:, 1]

            elif model_cfg["method"] == "bagging":
                model = build_bagging(
                    model_cfg,
                    X_train,
                    X_train,
                    y_train
                )
                y_tr_prob = model.predict_proba(X_train)[:, 1]
                y_te_prob = model.predict_proba(X_test)[:, 1]

            elif model_cfg["method"] == "weighted":
                model = build_weighted_ensemble(
                    model_cfg,
                    X_train,
                    X_train,
                    y_train
                )
                y_tr_prob = model.predict_proba((X_train, X_train))[:, 1]
                y_te_prob = model.predict_proba((X_test, X_test))[:, 1]

            else:
                raise ValueError(
                    f"Unsupported ensemble method: {model_cfg['method']}"
                )

    # ================= METRICS =================
    train_m = compute_metrics(y_train, y_tr_prob, threshold)
    test_m = compute_metrics(y_test, y_te_prob, threshold)

    # ================= RESULT ROW =================
    row = {
        "run_timestamp": datetime.utcnow(),
        "feature": feature_set,
        "data_splitting": "time_based",
        "sampling_method": sampling_method,
        "model_name": model_name,
        "parameter": json.dumps(model_cfg.get("params", {})),

        "test_year": time_cfg["validation"]["year"],
        "test_months": ",".join(
            map(str, time_cfg["validation"]["months"])
        ),

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

        # ---- TEST METRICS ----
        "test_accuracy": test_m["accuracy"],
        "test_accuracy_class1": test_m["recall_class1"],
        "test_accuracy_class0": test_m["recall_class0"],
        "test_logloss": test_m["logloss"],
        "test_roc": test_m["roc"],
        "test_precision_class1": test_m["precision_class1"],
        "test_precision_class0": test_m["precision_class0"],
        "test_recall_class1": test_m["recall_class1"],
        "test_recall_class0": test_m["recall_class0"],
        "test_f1_class1": test_m["f1_class1"],
        "test_f1_class0": test_m["f1_class0"],
        "test_truepositive": test_m["tp"],
        "test_truenegative": test_m["tn"],
        "test_falsepositive": test_m["fp"],
        "test_falsenegative": test_m["fn"],

        "train_size": int(len(X_train)),
        "test_size": int(len(X_test))
    }

    return row
