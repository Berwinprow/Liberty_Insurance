# pipeline_runner_phase4.py
# ======================================================
# PHASE-4 GRID SEARCH (ML + DL)
# ======================================================

import json
import itertools
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from datetime import datetime

import numpy as np
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
    DL_MODEL_BUILDERS,
    DL_MODEL_PARAM_GRIDS
)
from ml_pipeline_phase_wise.model_param_grid import MODEL_PARAM_GRIDS
from ml_pipeline_phase_wise.grid_search_trainer import run_grid_model
from ml_pipeline_phase_wise.schema_table_config import get_schema
from ml_pipeline_phase_wise.checkpoint_utils import already_trained


# ======================================================
# CONFIG PATHS
# ======================================================
GRID_CFG = "/opt/airflow/dags/config/grid_search_config.json"
SELECTED_COLS = "/opt/airflow/dags/config/selected_columns.json"
CONN_CFG = "/opt/airflow/dags/config/connections_table_columns.json"
ENCODE_CFG = "/opt/airflow/dags/config/encode_strategy.json"


# ======================================================
# MODEL REGISTRY (ML ONLY)
# ======================================================
MODEL_REGISTRY = {}
for g in MODEL_GROUPS:
    MODEL_REGISTRY.update(g)


# ======================================================
# HELPERS
# ======================================================
def parse_split(split):
    _, test_pct = split.split("_")
    return int(test_pct) / 100


def reshape_for_rnn(X):
    arr = X.values.astype("float32")
    return arr.reshape(arr.shape[0], arr.shape[1], 1)


# ======================================================
# PARALLEL GRID EXECUTION (ML)
# ======================================================
def run_grid_param_queue(
    fs_name,
    split_name,
    sampling,
    model_name,
    base_model,
    param_list,
    X_tr,
    X_te,
    y_tr,
    y_te,
    max_parallel=2
):
    pending = list(param_list)
    running = set()

    print(
        f"\n🚀 GRID START → {model_name} | "
        f"Params={len(param_list)} | Parallel={max_parallel}"
    )

    with ThreadPoolExecutor(max_workers=max_parallel) as executor:
        while pending or running:

            while pending and len(running) < max_parallel:
                params = pending.pop(0)

                future = executor.submit(
                    run_grid_model,
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
                )

                running.add(future)

            if not running:
                break

            _, running = wait(running, return_when=FIRST_COMPLETED)

    print(f"🏁 GRID DONE → {model_name}")


# ======================================================
# PHASE-4 PIPELINE
# ======================================================
def run_phase4_pipeline():

    print("\n🚀 PHASE-4 | GRID SEARCH (ML + DL) STARTED")

    # ---------- LOAD DATA ----------
    base_df = load_and_clean_data()

    # ---------- LOAD CONFIGS ----------
    grid_cfg = json.load(open(GRID_CFG))
    feature_sets = json.load(open(SELECTED_COLS))
    conn_cfg = json.load(open(CONN_CFG))
    encode_rules = json.load(open(ENCODE_CFG))

    TARGET = conn_cfg["columns"]["target_column"]

    hook = PostgresHook(conn_cfg["connection"]["postgres_conn_id"])
    engine = hook.get_sqlalchemy_engine()

    schema = get_schema(
        "model_selection_schema",
        "/opt/airflow/dags/config/schema_config.json"
    )

    OUTPUT_TABLE = conn_cfg["tables"]["grid_search_results"]

    # ======================================================
    # FEATURE SET LOOP
    # ======================================================
    for fs_name, fs_cfg in grid_cfg.items():

        if not fs_cfg["enabled"]:
            continue

        print(f"\n📌 FEATURE SET → {fs_name}")

        df = process_features(base_df, feature_sets[fs_name])
        X = df.drop(TARGET, axis=1)
        y = df[TARGET]

        # ======================================================
        # SPLIT LOOP
        # ======================================================
        for split_name, split_cfg in fs_cfg["splits"].items():

            test_size = parse_split(split_name)

            X_tr, X_te, y_tr, y_te = train_test_split(
                X,
                y,
                test_size=test_size,
                stratify=y,
                random_state=42
            )

            # ---------- ENCODING ----------
            X_tr_enc, X_te_enc = apply_label_encoding(X_tr, X_te)

            # ======================================================
            # SAMPLING LOOP
            # ======================================================
            for sampling, rule in split_cfg["sampling"].items():

                print(f"\n⚙️ SPLIT={split_name} | SAMPLING={sampling}")

                # ---------- SAMPLING ----------
                X_s, y_s = apply_sampling(X_tr_enc, y_tr, sampling)

                # ---------- SCALING (ONCE) ----------
                X_s_sc, X_te_sc = apply_scaling(
                    X_s,
                    X_te_enc,
                    enable=True
                )

                # ======================================================
                # ML GRID SEARCH
                # ======================================================
                for model_name in rule["grid_models"]:

                    base_model = MODEL_REGISTRY[model_name]
                    scale_required = encode_rules[model_name]["scaling"]

                    X_tr_final, X_te_final = (
                        (X_s_sc, X_te_sc)
                        if scale_required
                        else (X_s, X_te_enc)
                    )

                    param_list = []
                    for grid in MODEL_PARAM_GRIDS[model_name]:
                        keys, values = zip(*grid.items())
                        for combo in itertools.product(*values):
                            param_list.append(dict(zip(keys, combo)))

                    run_grid_param_queue(
                        fs_name=fs_name,
                        split_name=split_name,
                        sampling=sampling,
                        model_name=model_name,
                        base_model=base_model,
                        param_list=param_list,
                        X_tr=X_tr_final,
                        X_te=X_te_final,
                        y_tr=y_s,
                        y_te=y_te,
                        max_parallel=2
                    )

                # ======================================================
                # DL GRID SEARCH (JSON CONTROLLED)
                # ======================================================
                dl_models = rule.get("dl_models", [])

                if not dl_models:
                    continue   # 🔑 NO DL unless explicitly mentioned

                print("\n🧠 DL GRID SEARCH STARTED")

                X_dl_tr = reshape_for_rnn(X_s_sc)
                X_dl_te = reshape_for_rnn(X_te_sc)
                input_shape = (X_dl_tr.shape[1], X_dl_tr.shape[2])

                for dl_name in dl_models:

                    build_fn = DL_MODEL_BUILDERS[dl_name]

                    for grid in DL_MODEL_PARAM_GRIDS[dl_name]:
                        keys, values = zip(*grid.items())

                        for combo in itertools.product(*values):
                            params = dict(zip(keys, combo))
                            param_str = json.dumps(params, sort_keys=True)

                            # ---------- CHECKPOINT ----------
                            if already_trained(
                                feature=fs_name,
                                split=split_name,
                                sampling=sampling,
                                model_name=dl_name,
                                params=param_str,
                                table_name=OUTPUT_TABLE
                            ):
                                print(f"⏭️ SKIPPED DL | {dl_name} | {param_str}")
                                continue

                            print(f"▶️ STARTED DL | {dl_name} | {param_str}")

                            model = build_fn(input_shape)
                            model.compile(
                                optimizer=Adam(),
                                loss="binary_crossentropy",
                                metrics=["accuracy"]
                            )

                            model.fit(
                                X_dl_tr,
                                y_s.values,
                                epochs=params["epochs"],
                                batch_size=params["batch_size"],
                                verbose=0
                            )

                            # ---------- TRAIN ----------
                            y_tr_prob = model.predict(X_dl_tr, verbose=0).ravel()
                            y_tr_pred = (y_tr_prob >= 0.5).astype(int)

                            tn_tr, fp_tr, fn_tr, tp_tr = confusion_matrix(
                                y_s, y_tr_pred
                            ).ravel()

                            # ---------- TEST ----------
                            y_te_prob = model.predict(X_dl_te, verbose=0).ravel()
                            y_te_pred = (y_te_prob >= 0.5).astype(int)

                            tn, fp, fn, tp = confusion_matrix(
                                y_te, y_te_pred
                            ).ravel()

                            row = {
                                "Feature": fs_name,
                                "Data_Splitting": split_name,
                                "sampling_method": sampling,
                                "Model_name": dl_name,
                                "Parameter": param_str,

                                # ===== TRAIN =====
                                "Train_Accuracy": accuracy_score(y_s, y_tr_pred),
                                "Train_Accuracy_class1": recall_score(y_s, y_tr_pred, pos_label=1),
                                "Train_Accuracy_class0": recall_score(y_s, y_tr_pred, pos_label=0),
                                "Train_logloss": log_loss(y_s, y_tr_prob),
                                "Train_roc": roc_auc_score(y_s, y_tr_prob),

                                "train_precision_class1": precision_score(y_s, y_tr_pred, pos_label=1),
                                "train_precision_class0": precision_score(y_s, y_tr_pred, pos_label=0),
                                "train_recall_class1": recall_score(y_s, y_tr_pred, pos_label=1),
                                "train_recall_class0": recall_score(y_s, y_tr_pred, pos_label=0),
                                "train_f1_class1": f1_score(y_s, y_tr_pred, pos_label=1),
                                "train_f1_class0": f1_score(y_s, y_tr_pred, pos_label=0),
                                "train_truepositive": int(tp_tr),
                                "train_truenegative": int(tn_tr),
                                "train_falsepositive": int(fp_tr),
                                "train_falsenegative": int(fn_tr),

                                # ===== TEST =====
                                "Test_Accuracy": accuracy_score(y_te, y_te_pred),
                                "Test_Accuracy_class1": recall_score(y_te, y_te_pred, pos_label=1),
                                "Test_Accuracy_class0": recall_score(y_te, y_te_pred, pos_label=0),
                                "Test_logloss": log_loss(y_te, y_te_prob),
                                "Test_roc": roc_auc_score(y_te, y_te_prob),

                                "test_precision_class1": precision_score(y_te, y_te_pred, pos_label=1),
                                "test_precision_class0": precision_score(y_te, y_te_pred, pos_label=0),
                                "test_recall_class1": recall_score(y_te, y_te_pred, pos_label=1),
                                "test_recall_class0": recall_score(y_te, y_te_pred, pos_label=0),
                                "test_f1_class1": f1_score(y_te, y_te_pred, pos_label=1),
                                "test_f1_class0": f1_score(y_te, y_te_pred, pos_label=0),
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

                            print(f"✅ COMPLETED DL | {dl_name} | {param_str}")

    print("\n✅ PHASE-4 | GRID SEARCH (ML + DL) COMPLETED")
