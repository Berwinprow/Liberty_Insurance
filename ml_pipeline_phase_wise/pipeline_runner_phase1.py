# ======================================================
# GLOBAL SEED + TF LOG SILENCE
# ======================================================
import os
import random
import numpy as np
import tensorflow as tf
import logging

SEED = 42
os.environ["PYTHONHASHSEED"] = str(SEED)
random.seed(SEED)
np.random.seed(SEED)
tf.random.set_seed(SEED)

os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"
os.environ["TF_DETERMINISTIC_OPS"] = "1"
logging.getLogger("tensorflow").setLevel(logging.ERROR)

from tensorflow.keras.utils import disable_interactive_logging
disable_interactive_logging()

# ======================================================
# WARNINGS
# ======================================================
import warnings
from sklearn.exceptions import ConvergenceWarning
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=ConvergenceWarning)

# ======================================================
# IMPORTS
# ======================================================
import json
import itertools
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    accuracy_score,
    roc_auc_score,
    log_loss,
    confusion_matrix,
    precision_score,
    recall_score,
    f1_score
)

from tensorflow.keras.optimizers import Adam
from airflow.providers.postgres.hooks.postgres import PostgresHook

from ml_pipeline_phase_wise.data_loader import load_and_clean_data
from ml_pipeline_phase_wise.feature_processing import process_features
from ml_pipeline_phase_wise.encoding_utils import apply_label_encoding
from ml_pipeline_phase_wise.model_utils import apply_sampling, apply_scaling
from ml_pipeline_phase_wise.model_library import (
    MODEL_GROUPS,
    MODEL_FIXED_PARAMS,
    DL_MODEL_BUILDERS,
    DL_MODEL_PARAM_GRIDS
)
from ml_pipeline_phase_wise.trainer import run_single_model
from ml_pipeline_phase_wise.checkpoint_utils import already_trained
from ml_pipeline_phase_wise.schema_table_config import get_schema


# ======================================================
# HELPERS
# ======================================================
def reshape_for_rnn(X):
    arr = X.values.astype("float32")
    return arr.reshape(arr.shape[0], arr.shape[1], 1)


def run_model_param_queue(
    model_name,
    base_model,
    params_list,
    X_train,
    X_test,
    y_train,
    y_test,
    meta,
    max_parallel=2
):
    pending = list(params_list)
    running = set()

    print(f"\n🚀 MODEL START → {model_name} | Params: {len(params_list)}")

    with ThreadPoolExecutor(max_workers=max_parallel) as executor:
        while pending or running:

            while pending and len(running) < max_parallel:
                params = pending.pop(0)
                future = executor.submit(
                    run_single_model,
                    model_name,
                    base_model,
                    params,
                    X_train,
                    X_test,
                    y_train,
                    y_test,
                    meta
                )
                running.add(future)

            if not running:
                break

            _, running = wait(running, return_when=FIRST_COMPLETED)

    print(f"🏁 MODEL DONE → {model_name}")


# ======================================================
# PHASE-1 PIPELINE
# ======================================================
def run_phase1_pipeline():

    base_df = load_and_clean_data()

    feature_sets = json.load(
        open("/opt/airflow/dags/config/selected_columns.json")
    )
    rules = json.load(
        open("/opt/airflow/dags/config/encode_strategy.json")
    )

    fs_name = "set_1"
    split_name = "80_20"

    df = process_features(base_df, feature_sets[fs_name])
    X = df.drop("policy_status", axis=1)
    y = df["policy_status"]

    X_tr, X_te, y_tr, y_te = train_test_split(
        X,
        y,
        test_size=0.2,
        stratify=y,
        random_state=42
    )

    # ================= ENCODING (ONCE) =================
    X_tr_enc, X_te_enc = apply_label_encoding(X_tr, X_te)

    hook = PostgresHook(postgres_conn_id="postgres_cloud_prochurn")
    engine = hook.get_sqlalchemy_engine()
    schema = get_schema(
        "model_selection_schema",
        "/opt/airflow/dags/config/schema_config.json"
    )

    # ================= SAMPLING LOOP =================
    for sampling in ["none", "smote", "oversample", "undersample"]:

        print(f"\nPHASE-1 | SAMPLING → {sampling}")

        X_s, y_s = apply_sampling(X_tr_enc, y_tr, sampling)

        X_tr_ns = X_s
        X_te_ns = X_te_enc

        X_tr_sc, X_te_sc = apply_scaling(X_s, X_te_enc, enable=True)

        # ================= ML MODELS (UNCHANGED) =================
        for group in MODEL_GROUPS:
            for model_name, base_model in group.items():

                rule = rules[model_name]
                params_list = MODEL_FIXED_PARAMS.get(model_name, [{}])

                X_train, X_test = (
                    (X_tr_sc, X_te_sc)
                    if rule["scaling"]
                    else (X_tr_ns, X_te_ns)
                )

                meta = {
                    "feature_set": fs_name,
                    "split": split_name,
                    "sampling": sampling,
                    "encoding": "label",
                    "scaling": rule["scaling"]
                }

                run_model_param_queue(
                    model_name,
                    base_model,
                    params_list,
                    X_train,
                    X_test,
                    y_s,
                    y_te,
                    meta,
                    max_parallel=2
                )

        # ================= DL MODELS =================
        print("\nPHASE-1 | DL MODELS STARTED")

        X_dl_tr = reshape_for_rnn(X_tr_sc)
        X_dl_te = reshape_for_rnn(X_te_sc)
        input_shape = (X_dl_tr.shape[1], X_dl_tr.shape[2])

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = []

            for model_name, build_fn in DL_MODEL_BUILDERS.items():
                for grid in DL_MODEL_PARAM_GRIDS[model_name]:
                    for combo in itertools.product(*grid.values()):
                        params = dict(zip(grid.keys(), combo))

                        if already_trained(
                            feature=fs_name,
                            split=split_name,
                            sampling=sampling,
                            model_name=model_name,
                            params=params,
                            table_name="ml_automation_models_output_final_table"
                        ):
                            print(f"⏭️ SKIPPED DL | {model_name} | Params: {params}")
                            continue

                        def run_dl(p=params, name=model_name, bf=build_fn):
                            print(f"▶️ STARTED DL | {name} | Params: {p}")

                            model = bf(input_shape)
                            model.compile(
                                optimizer=Adam(),
                                loss="binary_crossentropy",
                                metrics=["accuracy"]
                            )

                            # TRAIN
                            model.fit(
                                X_dl_tr,
                                y_s.values,
                                epochs=p["epochs"],
                                batch_size=p["batch_size"],
                                verbose=0
                            )

                            # TRAIN METRICS
                            y_train_prob = model.predict(X_dl_tr, verbose=0).ravel()
                            y_train_pred = (y_train_prob >= 0.5).astype(int)
                            tn_tr, fp_tr, fn_tr, tp_tr = confusion_matrix(
                                y_s, y_train_pred
                            ).ravel()

                            # TEST METRICS
                            y_test_prob = model.predict(X_dl_te, verbose=0).ravel()
                            y_test_pred = (y_test_prob >= 0.5).astype(int)
                            tn, fp, fn, tp = confusion_matrix(
                                y_te, y_test_pred
                            ).ravel()

                            result = {
                                "Feature": fs_name,
                                "Data_Splitting": split_name,
                                "sampling_method": sampling,
                                "Model_name": name,
                                "Parameter": str(p),

                                "Train_Accuracy": accuracy_score(y_s, y_train_pred),
                                "Train_Accuracy_class1": recall_score(y_s, y_train_pred, pos_label=1),
                                "Train_Accuracy_class0": recall_score(y_s, y_train_pred, pos_label=0),
                                "Train_logloss": log_loss(y_s, y_train_prob),
                                "Train_roc": roc_auc_score(y_s, y_train_prob),

                                "train_precision_class1": precision_score(y_s, y_train_pred, pos_label=1),
                                "train_precision_class0": precision_score(y_s, y_train_pred, pos_label=0),
                                "train_recall_class1": recall_score(y_s, y_train_pred, pos_label=1),
                                "train_recall_class0": recall_score(y_s, y_train_pred, pos_label=0),
                                "train_f1_class1": f1_score(y_s, y_train_pred, pos_label=1),
                                "train_f1_class0": f1_score(y_s, y_train_pred, pos_label=0),
                                "train_truepositive": int(tp_tr),
                                "train_truenegative": int(tn_tr),
                                "train_falsepositive": int(fp_tr),
                                "train_falsenegative": int(fn_tr),

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

                            pd.DataFrame([result]).to_sql(
                                "ml_automation_models_output_final_table",
                                engine,
                                schema=schema,
                                if_exists="append",
                                index=False,
                                method="multi"
                            )

                            print(f"✅ COMPLETED DL | {name} | Params: {p}")

                        futures.append(executor.submit(run_dl))

            for f in futures:
                f.result()

    print("✅ PHASE-1 COMPLETED SUCCESSFULLY")
