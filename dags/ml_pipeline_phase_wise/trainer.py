"""
Single model training and result persistence utility.
"""

import json
import logging
from datetime import datetime

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sklearn.base import clone
from sklearn.metrics import (
    accuracy_score,
    confusion_matrix,
    f1_score,
    log_loss,
    precision_score,
    recall_score,
    roc_auc_score,
)

from ml_pipeline_phase_wise.checkpoint_utils import already_trained
from ml_pipeline_phase_wise.schema_table_config import get_schema


CONFIG_PATH = "/opt/airflow/dags/config/connections_table_columns.json"
SCHEMA_CONFIG_PATH = "/opt/airflow/dags/config/schema_config.json"
SCHEMA_KEY = "model_selection_schema"

logger = logging.getLogger(__name__)


def run_single_model(
    model_name,
    base_model,
    params,
    X_train,
    X_test,
    y_train,
    y_test,
    meta,
):
    """
    Train a single model with given parameters and store metrics.

    Args:
        model_name: Name of the model
        base_model: Base model instance
        params: Model parameters
        X_train: Training features
        X_test: Test features
        y_train: Training target
        y_test: Test target
        meta: Metadata dictionary
    """
    # ================= LOAD CONFIG =================
    with open(CONFIG_PATH, "r") as file:
        cfg = json.load(file)

    connection_id = cfg["connection"]["postgres_conn_id"]
    output_table = cfg["tables"]["single_model_output_table"]

    # ================= DB CONNECTION =================
    hook = PostgresHook(
        postgres_conn_id=connection_id
    )
    engine = hook.get_sqlalchemy_engine()

    schema = get_schema(
        SCHEMA_KEY,
        SCHEMA_CONFIG_PATH,
    )

    # ================= CHECKPOINT =================
    if already_trained(
        feature=meta["feature_set"],
        split=meta["split"],
        sampling=meta["sampling"],
        model_name=model_name,
        params=params,
        table_name=output_table,
    ):
        logger.info(
            "SKIPPED | %s | Params: %s",
            model_name,
            params,
        )
        return

    logger.info(
        "STARTED | %s | Params: %s",
        model_name,
        params,
    )

    try:
        model = clone(base_model)
        if params:
            model.set_params(**params)

        # ---------------- TRAIN ----------------
        model.fit(X_train, y_train)

        y_train_pred = model.predict(X_train)
        y_train_prob = model.predict_proba(X_train)[:, 1]

        y_test_pred = model.predict(X_test)
        y_test_prob = model.predict_proba(X_test)[:, 1]

        tn_tr, fp_tr, fn_tr, tp_tr = confusion_matrix(
            y_train,
            y_train_pred,
        ).ravel()

        tn, fp, fn, tp = confusion_matrix(
            y_test,
            y_test_pred,
        ).ravel()

        result = {
            "Feature": meta["feature_set"],
            "Data_Splitting": meta["split"],
            "sampling_method": meta["sampling"],
            "Model_name": model_name,
            "Parameter": str(params),

            # -------- TRAIN --------
            "Train_Accuracy": accuracy_score(
                y_train,
                y_train_pred,
            ),
            "Train_Accuracy_class1": recall_score(
                y_train,
                y_train_pred,
                pos_label=1,
            ),
            "Train_Accuracy_class0": recall_score(
                y_train,
                y_train_pred,
                pos_label=0,
            ),
            "Train_logloss": log_loss(
                y_train,
                y_train_prob,
            ),
            "Train_roc": roc_auc_score(
                y_train,
                y_train_prob,
            ),

            "train_precision_class1": precision_score(
                y_train,
                y_train_pred,
                pos_label=1,
            ),
            "train_precision_class0": precision_score(
                y_train,
                y_train_pred,
                pos_label=0,
            ),
            "train_recall_class1": recall_score(
                y_train,
                y_train_pred,
                pos_label=1,
            ),
            "train_recall_class0": recall_score(
                y_train,
                y_train_pred,
                pos_label=0,
            ),
            "train_f1_class1": f1_score(
                y_train,
                y_train_pred,
                pos_label=1,
            ),
            "train_f1_class0": f1_score(
                y_train,
                y_train_pred,
                pos_label=0,
            ),

            "train_truepositive": int(tp_tr),
            "train_truenegative": int(tn_tr),
            "train_falsepositive": int(fp_tr),
            "train_falsenegative": int(fn_tr),

            # -------- TEST --------
            "test_precision_class1": precision_score(
                y_test,
                y_test_pred,
                pos_label=1,
            ),
            "test_precision_class0": precision_score(
                y_test,
                y_test_pred,
                pos_label=0,
            ),
            "test_recall_class1": recall_score(
                y_test,
                y_test_pred,
                pos_label=1,
            ),
            "test_recall_class0": recall_score(
                y_test,
                y_test_pred,
                pos_label=0,
            ),
            "test_f1_class1": f1_score(
                y_test,
                y_test_pred,
                pos_label=1,
            ),
            "test_f1_class0": f1_score(
                y_test,
                y_test_pred,
                pos_label=0,
            ),

            "test_truepositive": int(tp),
            "test_truenegative": int(tn),
            "test_falsepositive": int(fp),
            "test_falsenegative": int(fn),

            "Test_Accuracy": accuracy_score(
                y_test,
                y_test_pred,
            ),
            "Test_Accuracy_class1": recall_score(
                y_test,
                y_test_pred,
                pos_label=1,
            ),
            "Test_Accuracy_class0": recall_score(
                y_test,
                y_test_pred,
                pos_label=0,
            ),
            "Test_logloss": log_loss(
                y_test,
                y_test_prob,
            ),
            "Test_roc": roc_auc_score(
                y_test,
                y_test_prob,
            ),

            "timestamp": datetime.now(),
        }

        pd.DataFrame([result]).to_sql(
            output_table,
            engine,
            schema=schema,
            if_exists="append",
            index=False,
            method="multi",
        )

        logger.info(
            "COMPLETED | %s | Params: %s",
            model_name,
            params,
        )

    except Exception as exc:
        logger.error(
            "FAILED | %s | Params: %s | Error: %s",
            model_name,
            params,
            exc,
        )
