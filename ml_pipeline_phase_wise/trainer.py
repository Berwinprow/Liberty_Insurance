from datetime import datetime
import pandas as pd

from sklearn.base import clone
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


def run_single_model(
    model_name,
    base_model,
    params,
    X_train,
    X_test,
    y_train,
    y_test,
    meta
):
    hook = PostgresHook(postgres_conn_id="postgres_cloud_prochurn")
    engine = hook.get_sqlalchemy_engine()
    schema = get_schema(
        "model_selection_schema",
        "/opt/airflow/dags/config/schema_config.json"
    )

    # ================= CHECKPOINT =================
    if already_trained(
        feature=meta["feature_set"],
        split=meta["split"],
        sampling=meta["sampling"],
        model_name=model_name,
        params=params,
        table_name="ml_automation_models_output_final_table"
    ):
        print(f"⏭️ SKIPPED | {model_name} | Params: {params}")
        return

    print(f"▶️ STARTED | {model_name} | Params: {params}")

    try:
        model = clone(base_model)
        if params:
            model.set_params(**params)

        # ---------------- TRAIN ----------------
        model.fit(X_train, y_train)

        y_train_pred = model.predict(X_train)
        y_train_prob = model.predict_proba(X_train)[:, 1]

        y_pred = model.predict(X_test)
        y_prob = model.predict_proba(X_test)[:, 1]

        tn_tr, fp_tr, fn_tr, tp_tr = confusion_matrix(
            y_train, y_train_pred
        ).ravel()

        tn, fp, fn, tp = confusion_matrix(
            y_test, y_pred
        ).ravel()

        result = {
            "Feature": meta["feature_set"],
            "Data_Splitting": meta["split"],
            "sampling_method": meta["sampling"],
            "Model_name": model_name,
            "Parameter": str(params),

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

            "test_precision_class1": precision_score(y_test, y_pred, pos_label=1),
            "test_precision_class0": precision_score(y_test, y_pred, pos_label=0),
            "test_recall_class1": recall_score(y_test, y_pred, pos_label=1),
            "test_recall_class0": recall_score(y_test, y_pred, pos_label=0),
            "test_f1_class1": f1_score(y_test, y_pred, pos_label=1),
            "test_f1_class0": f1_score(y_test, y_pred, pos_label=0),

            "train_truepositive": int(tp_tr),
            "train_truenegative": int(tn_tr),
            "train_falsepositive": int(fp_tr),
            "train_falsenegative": int(fn_tr),

            "test_truepositive": int(tp),
            "test_truenegative": int(tn),
            "test_falsepositive": int(fp),
            "test_falsenegative": int(fn),

            "Test_Accuracy": accuracy_score(y_test, y_pred),
            "Test_Accuracy_class1": recall_score(y_test, y_pred, pos_label=1),
            "Test_Accuracy_class0": recall_score(y_test, y_pred, pos_label=0),
            "Test_logloss": log_loss(y_test, y_prob),
            "Test_roc": roc_auc_score(y_test, y_prob),

            "timestamp": datetime.now()
        }

        pd.DataFrame([result]).to_sql(
            "ml_automation_models_output_final_table",
            engine,
            schema=schema,
            if_exists="append",
            index=False,
            method="multi"
        )

        print(f"✅ COMPLETED | {model_name} | Params: {params}")

    except Exception as e:
        print(f"❌ FAILED | {model_name} | Params: {params} | Error: {e}")
