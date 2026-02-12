"""
Phase-1 pipeline runner:
- Data loading
- Feature processing
- Encoding
- Sampling
- ML & DL model training
"""

# ======================================================
# GLOBAL SEED + TF LOG SILENCE
# ======================================================
import os
import random
import logging
import warnings
import json
import itertools
from datetime import datetime
from concurrent.futures import (
    ThreadPoolExecutor,
    wait,
    FIRST_COMPLETED,
)

import numpy as np
import pandas as pd
import tensorflow as tf
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sklearn.exceptions import ConvergenceWarning
from sklearn.metrics import (
    accuracy_score,
    confusion_matrix,
    f1_score,
    log_loss,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.utils import disable_interactive_logging

from ml_pipeline_phase_wise.checkpoint_utils import already_trained
from ml_pipeline_phase_wise.data_loader import load_and_clean_data
from ml_pipeline_phase_wise.encoding_utils import apply_label_encoding
from ml_pipeline_phase_wise.feature_processing import process_features
from ml_pipeline_phase_wise.model_library import (
    DL_MODEL_BUILDERS,
    DL_MODEL_PARAM_GRIDS,
    MODEL_FIXED_PARAMS,
    MODEL_GROUPS,
)
from ml_pipeline_phase_wise.model_utils import (
    apply_sampling,
    apply_scaling,
)
from ml_pipeline_phase_wise.schema_table_config import get_schema
from ml_pipeline_phase_wise.trainer import run_single_model


# ======================================================
# SEED & ENV
# ======================================================
SEED = 42
os.environ["PYTHONHASHSEED"] = str(SEED)
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"
os.environ["TF_DETERMINISTIC_OPS"] = "1"

random.seed(SEED)
np.random.seed(SEED)
tf.random.set_seed(SEED)

logging.getLogger("tensorflow").setLevel(logging.ERROR)
disable_interactive_logging()

warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=ConvergenceWarning)

logger = logging.getLogger(__name__)


CONFIG_PATH = "/opt/airflow/dags/config/connections_table_columns.json"
SCHEMA_CONFIG_PATH = "/opt/airflow/dags/config/schema_config.json"
SCHEMA_KEY = "model_selection_schema"


# ======================================================
# HELPERS
# ======================================================
def reshape_for_rnn(X):
    """
    Reshape dataframe for RNN/LSTM/GRU input.
    """
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
    max_parallel=2,
):
    """
    Run parameter grid for a single model using threading.
    """
    pending = list(params_list)
    running = set()

    logger.info(
        "MODEL START → %s | Params: %s",
        model_name,
        len(params_list),
    )

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
                    meta,
                )
                running.add(future)

            if not running:
                break

            _, running = wait(
                running,
                return_when=FIRST_COMPLETED,
            )

    logger.info(
        "MODEL DONE → %s",
        model_name,
    )


# ======================================================
# PHASE-1 PIPELINE
# ======================================================
def run_phase1_pipeline():
    """
    Execute Phase-1 ML & DL training pipeline.
    """
    # ================= LOAD CONFIG =================
    with open(CONFIG_PATH, "r") as file:
        cfg = json.load(file)

    connection_id = cfg["connection"]["postgres_conn_id"]
    target_column = cfg["columns"]["target_column"]
    output_table = cfg["tables"]["single_model_output_table"]

    # ================= LOAD DATA =================
    base_df = load_and_clean_data()

    with open(
        "/opt/airflow/dags/config/selected_columns.json",
        "r",
    ) as file:
        feature_sets = json.load(file)

    with open(
        "/opt/airflow/dags/config/encode_strategy.json",
        "r",
    ) as file:
        rules = json.load(file)

    fs_name = "set_1"
    split_name = "80_20"

    df = process_features(
        base_df,
        feature_sets[fs_name],
    )

    X = df.drop(target_column, axis=1)
    y = df[target_column]

    X_tr, X_te, y_tr, y_te = train_test_split(
        X,
        y,
        test_size=0.2,
        stratify=y,
        random_state=SEED,
    )

    # ================= ENCODING =================
    X_tr_enc, X_te_enc = apply_label_encoding(
        X_tr,
        X_te,
    )

    hook = PostgresHook(
        postgres_conn_id=connection_id
    )
    engine = hook.get_sqlalchemy_engine()

    schema = get_schema(
        SCHEMA_KEY,
        SCHEMA_CONFIG_PATH,
    )

    # ================= SAMPLING LOOP =================
    for sampling in [
        "none",
        "smote",
        "oversample",
        "undersample",
    ]:
        logger.info(
            "PHASE-1 | SAMPLING → %s",
            sampling,
        )

        X_s, y_s = apply_sampling(
            X_tr_enc,
            y_tr,
            sampling,
        )

        X_tr_ns = X_s
        X_te_ns = X_te_enc

        X_tr_sc, X_te_sc = apply_scaling(
            X_s,
            X_te_enc,
            enable=True,
        )

        # ================= ML MODELS =================
        for group in MODEL_GROUPS:
            for model_name, base_model in group.items():

                rule = rules[model_name]
                params_list = MODEL_FIXED_PARAMS.get(
                    model_name,
                    [{}],
                )

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
                    "scaling": rule["scaling"],
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
                    max_parallel=2,
                )

        # ================= DL MODELS =================
        logger.info(
            "PHASE-1 | DL MODELS STARTED"
        )

        X_dl_tr = reshape_for_rnn(X_tr_sc)
        X_dl_te = reshape_for_rnn(X_te_sc)
        input_shape = (
            X_dl_tr.shape[1],
            X_dl_tr.shape[2],
        )

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = []

            for model_name, build_fn in DL_MODEL_BUILDERS.items():
                for grid in DL_MODEL_PARAM_GRIDS[model_name]:
                    for combo in itertools.product(
                        *grid.values()
                    ):
                        params = dict(
                            zip(grid.keys(), combo)
                        )

                        if already_trained(
                            feature=fs_name,
                            split=split_name,
                            sampling=sampling,
                            model_name=model_name,
                            params=params,
                            table_name=output_table,
                        ):
                            logger.info(
                                "SKIPPED DL | %s | Params: %s",
                                model_name,
                                params,
                            )
                            continue

                        def run_dl(
                            p=params,
                            name=model_name,
                            bf=build_fn,
                        ):
                            logger.info(
                                "STARTED DL | %s | Params: %s",
                                name,
                                p,
                            )

                            model = bf(input_shape)
                            model.compile(
                                optimizer=Adam(),
                                loss="binary_crossentropy",
                                metrics=["accuracy"],
                            )

                            model.fit(
                                X_dl_tr,
                                y_s.values,
                                epochs=p["epochs"],
                                batch_size=p["batch_size"],
                                verbose=0,
                            )

                            y_train_prob = (
                                model.predict(
                                    X_dl_tr,
                                    verbose=0,
                                ).ravel()
                            )
                            y_train_pred = (
                                y_train_prob >= 0.5
                            ).astype(int)

                            tn_tr, fp_tr, fn_tr, tp_tr = (
                                confusion_matrix(
                                    y_s,
                                    y_train_pred,
                                ).ravel()
                            )

                            y_test_prob = (
                                model.predict(
                                    X_dl_te,
                                    verbose=0,
                                ).ravel()
                            )
                            y_test_pred = (
                                y_test_prob >= 0.5
                            ).astype(int)

                            tn, fp, fn, tp = (
                                confusion_matrix(
                                    y_te,
                                    y_test_pred,
                                ).ravel()
                            )

                            result = {
                                "Feature": fs_name,
                                "Data_Splitting": split_name,
                                "sampling_method": sampling,
                                "Model_name": name,
                                "Parameter": str(p),

                                "Train_Accuracy": accuracy_score(
                                    y_s,
                                    y_train_pred,
                                ),
                                "Train_Accuracy_class1": recall_score(
                                    y_s,
                                    y_train_pred,
                                    pos_label=1,
                                ),
                                "Train_Accuracy_class0": recall_score(
                                    y_s,
                                    y_train_pred,
                                    pos_label=0,
                                ),
                                "Train_logloss": log_loss(
                                    y_s,
                                    y_train_prob,
                                ),
                                "Train_roc": roc_auc_score(
                                    y_s,
                                    y_train_prob,
                                ),

                                "train_precision_class1": precision_score(
                                    y_s,
                                    y_train_pred,
                                    pos_label=1,
                                ),
                                "train_precision_class0": precision_score(
                                    y_s,
                                    y_train_pred,
                                    pos_label=0,
                                ),
                                "train_recall_class1": recall_score(
                                    y_s,
                                    y_train_pred,
                                    pos_label=1,
                                ),
                                "train_recall_class0": recall_score(
                                    y_s,
                                    y_train_pred,
                                    pos_label=0,
                                ),
                                "train_f1_class1": f1_score(
                                    y_s,
                                    y_train_pred,
                                    pos_label=1,
                                ),
                                "train_f1_class0": f1_score(
                                    y_s,
                                    y_train_pred,
                                    pos_label=0,
                                ),

                                "train_truepositive": int(tp_tr),
                                "train_truenegative": int(tn_tr),
                                "train_falsepositive": int(fp_tr),
                                "train_falsenegative": int(fn_tr),

                                "Test_Accuracy": accuracy_score(
                                    y_te,
                                    y_test_pred,
                                ),
                                "Test_Accuracy_class1": recall_score(
                                    y_te,
                                    y_test_pred,
                                    pos_label=1,
                                ),
                                "Test_Accuracy_class0": recall_score(
                                    y_te,
                                    y_test_pred,
                                    pos_label=0,
                                ),
                                "Test_logloss": log_loss(
                                    y_te,
                                    y_test_prob,
                                ),
                                "Test_roc": roc_auc_score(
                                    y_te,
                                    y_test_prob,
                                ),

                                "test_precision_class1": precision_score(
                                    y_te,
                                    y_test_pred,
                                    pos_label=1,
                                ),
                                "test_precision_class0": precision_score(
                                    y_te,
                                    y_test_pred,
                                    pos_label=0,
                                ),
                                "test_recall_class1": recall_score(
                                    y_te,
                                    y_test_pred,
                                    pos_label=1,
                                ),
                                "test_recall_class0": recall_score(
                                    y_te,
                                    y_test_pred,
                                    pos_label=0,
                                ),
                                "test_f1_class1": f1_score(
                                    y_te,
                                    y_test_pred,
                                    pos_label=1,
                                ),
                                "test_f1_class0": f1_score(
                                    y_te,
                                    y_test_pred,
                                    pos_label=0,
                                ),

                                "test_truepositive": int(tp),
                                "test_truenegative": int(tn),
                                "test_falsepositive": int(fp),
                                "test_falsenegative": int(fn),

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
                                "COMPLETED DL | %s | Params: %s",
                                name,
                                p,
                            )

                        futures.append(
                            executor.submit(run_dl)
                        )

            for future in futures:
                future.result()

    logger.info(
        "PHASE-1 COMPLETED SUCCESSFULLY"
    )
