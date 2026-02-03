import json
import numpy as np
import pandas as pd
from datetime import datetime

from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    roc_auc_score,
    log_loss,
    confusion_matrix,
    roc_curve
)

from airflow.providers.postgres.hooks.postgres import PostgresHook
from ml_pipeline_phase_wise.schema_table_config import get_schema
from ml_pipeline_phase_wise.checkpoint_utils import already_trained


CONFIG_PATH = "/opt/airflow/dags/config/connections_table_columns.json"


# ======================================================
# SAFE DIVISION
# ======================================================
def safe_div(n, d):
    return n / d if d != 0 else 0.0


# ======================================================
# THRESHOLD OPTIMIZATION (CHECKPOINT ENABLED)
# ======================================================
def run_threshold_optimization(
    fs_name,
    split_name,
    sampling_name,
    model_name,
    params,
    y_true,
    y_prob
):
    # ---------------- LOAD CONFIG ----------------
    with open(CONFIG_PATH) as f:
        cfg = json.load(f)

    hook = PostgresHook(
        postgres_conn_id=cfg["connection"]["postgres_conn_id"]
    )
    engine = hook.get_sqlalchemy_engine()

    schema = get_schema(
        "model_selection_schema",
        "/opt/airflow/dags/config/schema_config.json"
    )

    OUTPUT_TABLE = cfg["tables"]["threshold_optimization_results"]

    # ======================================================
    # 🔒 CHECKPOINT (CRITICAL)
    # ======================================================
    if already_trained(
        feature=fs_name,
        split=split_name,
        sampling=sampling_name,
        model_name=model_name,
        params=params,
        table_name=OUTPUT_TABLE
    ):
        print(
            f"⏭️ SKIPPED THRESHOLD OPT → "
            f"{model_name} | {sampling_name}"
        )
        return

    # ======================================================
    # ROC CURVE
    # ======================================================
    fpr, tpr, thresholds = roc_curve(y_true, y_prob)
    spec = 1 - fpr
    obs_prev = np.mean(y_true)

    # ======================================================
    # THRESHOLD SELECTION (SENIOR LOGIC)
    # ======================================================
    idx_sens_spec = np.argmin(np.abs(tpr - spec))
    idx_youden = np.argmax(tpr - fpr)

    accs = [
        accuracy_score(y_true, (y_prob >= t).astype(int))
        for t in thresholds
    ]
    idx_maxpcc = np.argmax(accs)

    pred_prev = [np.mean(y_prob >= t) for t in thresholds]
    idx_predprev = np.argmin(np.abs(np.array(pred_prev) - obs_prev))

    roc_dist = np.sqrt((1 - tpr) ** 2 + fpr ** 2)
    idx_minrocdist = np.argmin(roc_dist)

    threshold_map = {
        "Sens=Spec": thresholds[idx_sens_spec],
        "MaxSens+Spec": thresholds[idx_youden],
        "MaxPCC": thresholds[idx_maxpcc],
        "PredPrev=ObsPrev": thresholds[idx_predprev],
        "MinROCdist": thresholds[idx_minrocdist]
    }

    # ======================================================
    # METRICS PER SELECTED THRESHOLD
    # ======================================================
    rows = []

    for method, thr in threshold_map.items():

        y_pred = (y_prob >= thr).astype(int)
        tn, fp, fn, tp = confusion_matrix(y_true, y_pred).ravel()

        rows.append({
            # ---------- IDENTIFIERS ----------
            "Feature": fs_name,
            "Data_Splitting": split_name,
            "sampling_method": sampling_name,
            "Model_name": model_name,
            "Parameter": json.dumps(params, sort_keys=True),

            # ---------- THRESHOLD ----------
            "threshold_method": method,
            "threshold_value": float(thr),
            "is_selected": True,

            # ---------- OVERALL ----------
            "Accuracy": accuracy_score(y_true, y_pred),
            "ROC_AUC": roc_auc_score(y_true, y_prob),
            "LogLoss": log_loss(y_true, y_prob),
            "PCC": safe_div(tp + tn, tp + tn + fp + fn),

            # ---------- CLASS 1 ----------
            "Precision_class1": safe_div(tp, tp + fp),
            "Recall_class1": safe_div(tp, tp + fn),
            "F1_class1": f1_score(y_true, y_pred, pos_label=1, zero_division=0),
            "Accuracy_class1": safe_div(tp, tp + fn),

            # ---------- CLASS 0 ----------
            "Precision_class0": safe_div(tn, tn + fn),
            "Recall_class0": safe_div(tn, tn + fp),
            "F1_class0": f1_score(y_true, y_pred, pos_label=0, zero_division=0),
            "Accuracy_class0": safe_div(tn, tn + fp),

            # ---------- CONFUSION ----------
            "true_positive": int(tp),
            "true_negative": int(tn),
            "false_positive": int(fp),
            "false_negative": int(fn),

            "timestamp": datetime.now()
        })

    # ======================================================
    # STORE RESULTS (ONE MODEL = ONE WRITE)
    # ======================================================
    pd.DataFrame(rows).to_sql(
        OUTPUT_TABLE,
        engine,
        schema=schema,
        if_exists="append",
        index=False,
        method="multi"
    )

    print(
        f"✅ THRESHOLD OPT STORED → "
        f"{model_name} | {sampling_name} | {len(rows)} rows"
    )
