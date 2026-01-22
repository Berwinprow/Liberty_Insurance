import json
import pandas as pd
from datetime import datetime

from sklearn.metrics import (
    accuracy_score,
    roc_auc_score,
    log_loss,
    confusion_matrix,
    precision_score,
    recall_score,
    f1_score
)

from airflow.providers.postgres.hooks.postgres import PostgresHook
from ml_pipeline_phase_wise.schema_table_config import get_schema
from ml_pipeline_phase_wise.checkpoint_utils import already_trained


def run_ensemble(
    ensemble_name,
    model,
    X_train,
    X_test,
    y_train,
    y_test,
    meta,
    cfg
):
    hook = PostgresHook(postgres_conn_id="postgres_cloud_prochurn")
    engine = hook.get_sqlalchemy_engine()

    schema = get_schema(
        "model_selection_schema",
        "/opt/airflow/dags/config/schema_config.json"
    )

    param_str = json.dumps(cfg, sort_keys=True)

    # ================= CHECKPOINT =================
    if already_trained(
        feature=meta["feature_set"],
        split=meta["split"],
        sampling=meta["sampling"],
        model_name=ensemble_name,
        params=param_str,
        table_name="ml_automation_ensembled_result"
    ):
        print(f"⏭️ SKIPPED ENSEMBLE | {ensemble_name}")
        return

    print(f"▶️ STARTED ENSEMBLE | {ensemble_name}")

    # ================= TRAIN METRICS =================
    y_train_pred = model.predict(X_train)
    y_train_prob = model.predict_proba(X_train)[:, 1]

    tn_tr, fp_tr, fn_tr, tp_tr = confusion_matrix(
        y_train, y_train_pred
    ).ravel()

    # ================= TEST METRICS =================
    y_test_pred = model.predict(X_test)
    y_test_prob = model.predict_proba(X_test)[:, 1]

    tn, fp, fn, tp = confusion_matrix(
        y_test, y_test_pred
    ).ravel()

    result = {
        "Feature": meta["feature_set"],
        "Data_Splitting": meta["split"],
        "sampling_method": meta["sampling"],
        "Model_name": ensemble_name,
        "Parameter": param_str,

        # -------- TRAIN --------
        "Train_Accuracy": accuracy_score(y_train, y_train_pred),
        "Train_Accuracy_class1": recall_score(y_train, y_train_pred, pos_label=1),
        "Train_Accuracy_class0": recall_score(y_train, y_train_pred, pos_label=0),
        "Train_logloss": log_loss(y_train, y_train_prob),
        "Train_roc": roc_auc_score(y_train, y_train_prob),

        "train_precision_class1": precision_score(y_train, y_train_pred, pos_label=1),
        "train_precision_class0": precision_score(y_train, y_train_pred, pos_label=0),
        "train_recall_class1": recall_score(y_train, y_train_pred, pos_label=1),
        "train_recall_class0": recall_score(y_train, y_train_pred, pos_label=0),
        "train_f1_class1": f1_score(y_train, y_train_pred, pos_label=1),
        "train_f1_class0": f1_score(y_train, y_train_pred, pos_label=0),

        "train_truepositive": int(tp_tr),
        "train_truenegative": int(tn_tr),
        "train_falsepositive": int(fp_tr),
        "train_falsenegative": int(fn_tr),

        # -------- TEST --------
        "Test_Accuracy": accuracy_score(y_test, y_test_pred),
        "Test_Accuracy_class1": recall_score(y_test, y_test_pred, pos_label=1),
        "Test_Accuracy_class0": recall_score(y_test, y_test_pred, pos_label=0),
        "Test_logloss": log_loss(y_test, y_test_prob),
        "Test_roc": roc_auc_score(y_test, y_test_prob),

        "test_precision_class1": precision_score(y_test, y_test_pred, pos_label=1),
        "test_precision_class0": precision_score(y_test, y_test_pred, pos_label=0),
        "test_recall_class1": recall_score(y_test, y_test_pred, pos_label=1),
        "test_recall_class0": recall_score(y_test, y_test_pred, pos_label=0),
        "test_f1_class1": f1_score(y_test, y_test_pred, pos_label=1),
        "test_f1_class0": f1_score(y_test, y_test_pred, pos_label=0),

        "test_truepositive": int(tp),
        "test_truenegative": int(tn),
        "test_falsepositive": int(fp),
        "test_falsenegative": int(fn),

        "timestamp": datetime.now()
    }

    pd.DataFrame([result]).to_sql(
        "ml_automation_ensembled_result",
        engine,
        schema=schema,
        if_exists="append",
        index=False,
        method="multi"
    )

    print(f"✅ ENSEMBLE COMPLETED | {ensemble_name}")
