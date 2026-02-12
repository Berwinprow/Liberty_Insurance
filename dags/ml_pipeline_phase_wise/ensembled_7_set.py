"""
Phase-3 7-set undersampled ensemble training.
"""

import json
import logging
from datetime import datetime

import numpy as np
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from imblearn.under_sampling import RandomUnderSampler
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
from sklearn.preprocessing import StandardScaler

from ml_pipeline_phase_wise.checkpoint_utils import already_trained
from ml_pipeline_phase_wise.model_library import MODEL_GROUPS
from ml_pipeline_phase_wise.schema_table_config import get_schema


CONFIG_PATH = "/opt/airflow/dags/config/connections_table_columns.json"
PHASE3_CONFIG = "/opt/airflow/dags/config/seven_set_threshold_config.json"
SCHEMA_CONFIG_PATH = "/opt/airflow/dags/config/schema_config.json"
SCHEMA_KEY = "model_selection_schema"

logger = logging.getLogger(__name__)


# ======================================================
# MODEL REGISTRY
# ======================================================
MODEL_REGISTRY = {}
for group in MODEL_GROUPS:
    MODEL_REGISTRY.update(group)


# ======================================================
# 7-SET ENSEMBLED TRAINING
# ======================================================
def run_ensembled_7_set(
    fs_name,
    split_name,
    X_enc_tr,
    X_enc_te,
    y_tr,
    y_te,
):
    """
    Run 7-set undersampled ensemble training and evaluation.

    Args:
        fs_name: Feature set name
        split_name: Split strategy name
        X_enc_tr: Encoded training features
        X_enc_te: Encoded test features
        y_tr: Training target
        y_te: Test target
    """
    # ================= LOAD CONFIG =================
    with open(CONFIG_PATH, "r") as file:
        cfg = json.load(file)

    with open(PHASE3_CONFIG, "r") as file:
        phase3_cfg = json.load(file)

    models_cfg = (
        phase3_cfg[fs_name]["sampling"]["7_set"]
        ["seven_set_ensemble"]["models"]
    )

    hook = PostgresHook(
        postgres_conn_id=cfg["connection"]["postgres_conn_id"]
    )
    engine = hook.get_sqlalchemy_engine()

    schema = get_schema(
        SCHEMA_KEY,
        SCHEMA_CONFIG_PATH,
    )

    output_table = cfg["tables"]["seven_set_ensembled_results"]

    num_cols = X_enc_tr.select_dtypes(
        include=["int64", "float64"]
    ).columns

    # ======================================================
    # CREATE 7 UNDERSAMPLED SETS (ONCE)
    # ======================================================
    undersampled_sets = {}
    for i in range(7):
        rus = RandomUnderSampler(random_state=i)
        X_us, y_us = rus.fit_resample(X_enc_tr, y_tr)
        undersampled_sets[f"train_set_{i + 1}"] = (X_us, y_us)

    # ======================================================
    # MODEL LOOP
    # ======================================================
    for model_name, mcfg in models_cfg.items():

        base_model = MODEL_REGISTRY[model_name]
        params = mcfg.get("params", {})
        scaling = mcfg.get("scaling", False)

        # ---------- CHECK FULL ENSEMBLE ----------
        if already_trained(
            feature=fs_name,
            split=split_name,
            sampling="7_set",
            model_name=model_name,
            params=params,
            table_name=output_table,
            seven_set="ensemble_7set",
        ):
            logger.info(
                "SKIPPED FULL ENSEMBLE → %s",
                model_name,
            )
            continue

        scaler = None
        if scaling:
            scaler = StandardScaler().fit(
                X_enc_tr[num_cols]
            )

        ensemble_train_probs = []
        ensemble_train_probs_full = []
        ensemble_test_probs = []

        # ======================================================
        # PER UNDERSAMPLED SET
        # ======================================================
        for set_name, (X_us, y_us) in undersampled_sets.items():

            if already_trained(
                feature=fs_name,
                split=split_name,
                sampling="7_set",
                model_name=model_name,
                params=params,
                table_name=output_table,
                seven_set=set_name,
            ):
                logger.info(
                    "SKIPPED → %s | %s",
                    model_name,
                    set_name,
                )
                continue

            X_tr = X_us.copy()
            X_te = X_enc_te.copy()
            X_tr_full = X_enc_tr.copy()

            # ---------- SCALING (AFTER SAMPLING) ----------
            if scaling:
                X_tr[num_cols] = scaler.transform(
                    X_tr[num_cols]
                )
                X_te[num_cols] = scaler.transform(
                    X_te[num_cols]
                )
                X_tr_full[num_cols] = scaler.transform(
                    X_tr_full[num_cols]
                )

            model = clone(base_model)
            if params:
                model.set_params(**params)

            model.fit(X_tr, y_us)

            # ---------- TRAIN (UNDERSAMPLED) ----------
            y_tr_pred = model.predict(X_tr)
            y_tr_prob = model.predict_proba(X_tr)[:, 1]

            # ---------- TRAIN (FULL) ----------
            y_tr_full_prob = model.predict_proba(
                X_tr_full
            )[:, 1]

            # ---------- TEST ----------
            y_te_pred = model.predict(X_te)
            y_te_prob = model.predict_proba(X_te)[:, 1]

            ensemble_train_probs.append(y_tr_prob)
            ensemble_train_probs_full.append(
                y_tr_full_prob
            )
            ensemble_test_probs.append(y_te_prob)

            tn_tr, fp_tr, fn_tr, tp_tr = confusion_matrix(
                y_us,
                y_tr_pred,
            ).ravel()

            tn, fp, fn, tp = confusion_matrix(
                y_te,
                y_te_pred,
            ).ravel()

            row = {
                "Feature": fs_name,
                "Data_Splitting": split_name,
                "sampling_method": "7_set",
                "Model_name": model_name,
                "Parameter": json.dumps(
                    params,
                    sort_keys=True,
                ),
                "7set_undersampling": set_name,

                "Train_Accuracy": accuracy_score(
                    y_us,
                    y_tr_pred,
                ),
                "Train_Accuracy_class1": recall_score(
                    y_us,
                    y_tr_pred,
                    pos_label=1,
                ),
                "Train_Accuracy_class0": recall_score(
                    y_us,
                    y_tr_pred,
                    pos_label=0,
                ),
                "Train_logloss": log_loss(
                    y_us,
                    y_tr_prob,
                ),
                "Train_roc": roc_auc_score(
                    y_us,
                    y_tr_prob,
                ),

                "train_precision_class1": precision_score(
                    y_us,
                    y_tr_pred,
                    pos_label=1,
                ),
                "train_precision_class0": precision_score(
                    y_us,
                    y_tr_pred,
                    pos_label=0,
                ),
                "train_recall_class1": recall_score(
                    y_us,
                    y_tr_pred,
                    pos_label=1,
                ),
                "train_recall_class0": recall_score(
                    y_us,
                    y_tr_pred,
                    pos_label=0,
                ),
                "train_f1_class1": f1_score(
                    y_us,
                    y_tr_pred,
                    pos_label=1,
                ),
                "train_f1_class0": f1_score(
                    y_us,
                    y_tr_pred,
                    pos_label=0,
                ),
                "train_truepositive": int(tp_tr),
                "train_truenegative": int(tn_tr),
                "train_falsepositive": int(fp_tr),
                "train_falsenegative": int(fn_tr),

                "Test_Accuracy": accuracy_score(
                    y_te,
                    y_te_pred,
                ),
                "Test_Accuracy_class1": recall_score(
                    y_te,
                    y_te_pred,
                    pos_label=1,
                ),
                "Test_Accuracy_class0": recall_score(
                    y_te,
                    y_te_pred,
                    pos_label=0,
                ),
                "Test_logloss": log_loss(
                    y_te,
                    y_te_prob,
                ),
                "Test_roc": roc_auc_score(
                    y_te,
                    y_te_prob,
                ),

                "test_precision_class1": precision_score(
                    y_te,
                    y_te_pred,
                    pos_label=1,
                ),
                "test_precision_class0": precision_score(
                    y_te,
                    y_te_pred,
                    pos_label=0,
                ),
                "test_recall_class1": recall_score(
                    y_te,
                    y_te_pred,
                    pos_label=1,
                ),
                "test_recall_class0": recall_score(
                    y_te,
                    y_te_pred,
                    pos_label=0,
                ),
                "test_f1_class1": f1_score(
                    y_te,
                    y_te_pred,
                    pos_label=1,
                ),
                "test_f1_class0": f1_score(
                    y_te,
                    y_te_pred,
                    pos_label=0,
                ),
                "test_truepositive": int(tp),
                "test_truenegative": int(tn),
                "test_falsepositive": int(fp),
                "test_falsenegative": int(fn),

                "timestamp": datetime.now(),
            }

            pd.DataFrame([row]).to_sql(
                output_table,
                engine,
                schema=schema,
                if_exists="append",
                index=False,
                method="multi",
            )

        # ======================================================
        # FINAL ENSEMBLE (MEAN PROBABILITY)
        # ======================================================
        final_train_prob = np.mean(
            np.vstack(ensemble_train_probs_full),
            axis=0,
        )
        final_test_prob = np.mean(
            np.vstack(ensemble_test_probs),
            axis=0,
        )

        final_train_pred = (
            final_train_prob >= 0.5
        ).astype(int)
        final_test_pred = (
            final_test_prob >= 0.5
        ).astype(int)

        tn_tr, fp_tr, fn_tr, tp_tr = confusion_matrix(
            y_tr,
            final_train_pred,
        ).ravel()

        tn, fp, fn, tp = confusion_matrix(
            y_te,
            final_test_pred,
        ).ravel()

        ensemble_row = {
            "Feature": fs_name,
            "Data_Splitting": split_name,
            "sampling_method": "7_set",
            "Model_name": model_name,
            "Parameter": json.dumps(
                params,
                sort_keys=True,
            ),
            "7set_undersampling": "ensemble_7set",

            "Train_Accuracy": accuracy_score(
                y_tr,
                final_train_pred,
            ),
            "Train_Accuracy_class1": recall_score(
                y_tr,
                final_train_pred,
                pos_label=1,
            ),
            "Train_Accuracy_class0": recall_score(
                y_tr,
                final_train_pred,
                pos_label=0,
            ),
            "Train_logloss": log_loss(
                y_tr,
                final_train_prob,
            ),
            "Train_roc": roc_auc_score(
                y_tr,
                final_train_prob,
            ),

            "train_precision_class1": precision_score(
                y_tr,
                final_train_pred,
                pos_label=1,
            ),
            "train_precision_class0": precision_score(
                y_tr,
                final_train_pred,
                pos_label=0,
            ),
            "train_recall_class1": recall_score(
                y_tr,
                final_train_pred,
                pos_label=1,
            ),
            "train_recall_class0": recall_score(
                y_tr,
                final_train_pred,
                pos_label=0,
            ),
            "train_f1_class1": f1_score(
                y_tr,
                final_train_pred,
                pos_label=1,
            ),
            "train_f1_class0": f1_score(
                y_tr,
                final_train_pred,
                pos_label=0,
            ),
            "train_truepositive": int(tp_tr),
            "train_truenegative": int(tn_tr),
            "train_falsepositive": int(fp_tr),
            "train_falsenegative": int(fn_tr),

            "Test_Accuracy": accuracy_score(
                y_te,
                final_test_pred,
            ),
            "Test_Accuracy_class1": recall_score(
                y_te,
                final_test_pred,
                pos_label=1,
            ),
            "Test_Accuracy_class0": recall_score(
                y_te,
                final_test_pred,
                pos_label=0,
            ),
            "Test_logloss": log_loss(
                y_te,
                final_test_prob,
            ),
            "Test_roc": roc_auc_score(
                y_te,
                final_test_prob,
            ),

            "test_precision_class1": precision_score(
                y_te,
                final_test_pred,
                pos_label=1,
            ),
            "test_precision_class0": precision_score(
                y_te,
                final_test_pred,
                pos_label=0,
            ),
            "test_recall_class1": recall_score(
                y_te,
                final_test_pred,
                pos_label=1,
            ),
            "test_recall_class0": recall_score(
                y_te,
                final_test_pred,
                pos_label=0,
            ),
            "test_f1_class1": f1_score(
                y_te,
                final_test_pred,
                pos_label=1,
            ),
            "test_f1_class0": f1_score(
                y_te,
                final_test_pred,
                pos_label=0,
            ),
            "test_truepositive": int(tp),
            "test_truenegative": int(tn),
            "test_falsepositive": int(fp),
            "test_falsenegative": int(fn),

            "timestamp": datetime.now(),
        }

        pd.DataFrame([ensemble_row]).to_sql(
            output_table,
            engine,
            schema=schema,
            if_exists="append",
            index=False,
            method="multi",
        )

    logger.info(
        "PHASE-3 | 7-SET ENSEMBLING COMPLETED"
    )
