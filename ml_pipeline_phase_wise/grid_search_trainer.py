# grid_search_trainer.py
import json
import pandas as pd
from datetime import datetime
from sklearn.base import clone
from sklearn.metrics import (
    accuracy_score, roc_auc_score, log_loss,
    confusion_matrix, precision_score,
    recall_score, f1_score
)

from airflow.providers.postgres.hooks.postgres import PostgresHook
from ml_pipeline_phase_wise.schema_table_config import get_schema
from ml_pipeline_phase_wise.checkpoint_utils import already_trained


CONFIG_PATH = "/opt/airflow/dags/config/connections_table_columns.json"


def run_grid_model(
    fs_name,
    split_name,
    sampling,
    model_name,
    base_model,
    params,
    X_tr,
    X_te,
    y_tr,
    y_te
):
    with open(CONFIG_PATH) as f:
        cfg = json.load(f)

    hook = PostgresHook(cfg["connection"]["postgres_conn_id"])
    engine = hook.get_sqlalchemy_engine()

    schema = get_schema(
        "model_selection_schema",
        "/opt/airflow/dags/config/schema_config.json"
    )

    OUTPUT_TABLE = cfg["tables"]["grid_search_results"]

    param_str = json.dumps(params, sort_keys=True)

    # ================= CHECKPOINT =================
    if already_trained(
        feature=fs_name,
        split=split_name,
        sampling=sampling,
        model_name=model_name,
        params=param_str,
        table_name=OUTPUT_TABLE
    ):
        print(f"⏭️ GRID SKIPPED | {model_name} | {param_str}")
        return

    print(f"▶️ GRID START | {model_name} | {param_str}")

    model = clone(base_model)
    model.set_params(**params)
    model.fit(X_tr, y_tr)

    # ---------- TRAIN ----------
    y_train_pred = model.predict(X_tr)
    y_train_prob = model.predict_proba(X_tr)[:, 1]
    tn_tr, fp_tr, fn_tr, tp_tr = confusion_matrix(
        y_tr, y_train_pred
    ).ravel()

    # ---------- TEST ----------
    y_test_pred = model.predict(X_te)
    y_test_prob = model.predict_proba(X_te)[:, 1]
    tn, fp, fn, tp = confusion_matrix(
        y_te, y_test_pred
    ).ravel()

    row = {
        "Feature": fs_name,
        "Data_Splitting": split_name,
        "sampling_method": sampling,
        "Model_name": model_name,
        "Parameter": param_str,

        # ===== TRAIN =====
        "Train_Accuracy": accuracy_score(y_tr, y_train_pred),
        "Train_Accuracy_class1": recall_score(y_tr, y_train_pred, pos_label=1),
        "Train_Accuracy_class0": recall_score(y_tr, y_train_pred, pos_label=0),
        "Train_logloss": log_loss(y_tr, y_train_prob),
        "Train_roc": roc_auc_score(y_tr, y_train_prob),

        "train_precision_class1": precision_score(y_tr, y_train_pred, pos_label=1),
        "train_precision_class0": precision_score(y_tr, y_train_pred, pos_label=0),
        "train_recall_class1": recall_score(y_tr, y_train_pred, pos_label=1),
        "train_recall_class0": recall_score(y_tr, y_train_pred, pos_label=0),
        "train_f1_class1": f1_score(y_tr, y_train_pred, pos_label=1),
        "train_f1_class0": f1_score(y_tr, y_train_pred, pos_label=0),

        "train_truepositive": int(tp_tr),
        "train_truenegative": int(tn_tr),
        "train_falsepositive": int(fp_tr),
        "train_falsenegative": int(fn_tr),

        # ===== TEST =====
        "Test_Accuracy": accuracy_score(y_te, y_test_pred),
        "Test_Accuracy_class1": recall_score(y_te, y_test_pred, pos_label=1),
        "Test_Accuracy_class0": recall_score(y_te, y_test_pred, pos_label=0),
        "Test_logloss": log_loss(y_te, y_test_prob),
        "Test_roc": roc_auc_score(y_te, y_test_prob),

        "test_precision_class1": precision_score(y_te, y_test_pred, pos_label=1),
        "test_precision_class0": precision_score(y_te, y_test_pred, pos_label=0),
        "test_recall_class1": recall_score(y_te, y_test_pred, pos_label=1),
        "test_recall_class0": recall_score(y_te, y_test_pred, pos_label=0),
        "test_f1_class1": f1_score(y_te, y_test_pred, pos_label=1),
        "test_f1_class0": f1_score(y_te, y_test_pred, pos_label=0),

        "test_truepositive": int(tp),
        "test_truenegative": int(tn),
        "test_falsepositive": int(fp),
        "test_falsenegative": int(fn),

        "timestamp": datetime.now()
    }

    pd.DataFrame([row]).to_sql(
        OUTPUT_TABLE,
        engine,
        schema=schema,
        if_exists="append",
        index=False,
        method="multi"
    )

    print(f"✅ GRID STORED | {model_name}")
