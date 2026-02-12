"""
Phase-3 pipeline runner:
- Threshold optimization
- 7-set undersampling ensemble
"""

# ======================================================
# GLOBAL SEED + TF LOG SILENCE
# ======================================================
import os
import random
import json
import logging
import warnings

import numpy as np
import tensorflow as tf
from sklearn.exceptions import ConvergenceWarning
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.base import clone

from tensorflow.keras.utils import disable_interactive_logging

from ml_pipeline_phase_wise.data_loader import load_and_clean_data
from ml_pipeline_phase_wise.feature_processing import process_features
from ml_pipeline_phase_wise.encoding_utils import apply_label_encoding
from ml_pipeline_phase_wise.model_utils import apply_sampling
from ml_pipeline_phase_wise.threshold_optimization import (
    run_threshold_optimization,
)
from ml_pipeline_phase_wise.ensembled_7_set import run_ensembled_7_set
from ml_pipeline_phase_wise.model_library import MODEL_GROUPS


# ======================================================
# SEED & LOGGING
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


# ======================================================
# CONFIG PATHS
# ======================================================
PHASE3_CONFIG = "/opt/airflow/dags/config/seven_set_threshold_config.json"
SELECTED_COLS = "/opt/airflow/dags/config/selected_columns.json"
CONN_CFG = "/opt/airflow/dags/config/connections_table_columns.json"


# ======================================================
# MODEL REGISTRY
# ======================================================
MODEL_REGISTRY = {}
for group in MODEL_GROUPS:
    MODEL_REGISTRY.update(group)


# ======================================================
# SPLIT PARSER
# ======================================================
def parse_split(split_str: str) -> float:
    """
    Convert split string (e.g. '80_20') to test_size float.
    """
    _, test_pct = split_str.split("_")
    return int(test_pct) / 100


# ======================================================
# PHASE-3 PIPELINE
# ======================================================
def run_phase3_pipeline():
    """
    Execute Phase-3 pipeline:
    - Threshold optimization
    - Optional 7-set ensemble
    """
    logger.info("PHASE-3 PIPELINE STARTED")

    # ================= LOAD DATA =================
    base_df = load_and_clean_data()

    with open(PHASE3_CONFIG, "r") as file:
        phase3_cfg = json.load(file)

    with open(SELECTED_COLS, "r") as file:
        feature_sets = json.load(file)

    with open(CONN_CFG, "r") as file:
        conn_cfg = json.load(file)

    target_col = conn_cfg["columns"]["target_column"]

    # ======================================================
    # FEATURE SET LOOP
    # ======================================================
    for fs_name, fs_cfg in phase3_cfg.items():

        if not fs_cfg.get("enabled", False):
            continue

        logger.info(
            "PHASE-3 | FEATURE SET → %s",
            fs_name,
        )

        # ================= FEATURE ENGINEERING =================
        df = process_features(
            base_df,
            feature_sets[fs_name],
        )
        X = df.drop(target_col, axis=1)
        y = df[target_col]

        # ================= SPLIT =================
        test_size = parse_split(fs_cfg["split"])

        X_tr, X_te, y_tr, y_te = train_test_split(
            X,
            y,
            test_size=test_size,
            stratify=y,
            random_state=SEED,
        )

        # ================= ENCODING =================
        X_tr_enc, X_te_enc = apply_label_encoding(
            X_tr,
            X_te,
        )

        # ======================================================
        # THRESHOLD OPTIMIZATION
        # ======================================================
        for sampling_name, sampling_cfg in fs_cfg["sampling"].items():

            if "Threshold_optimization" not in sampling_cfg:
                continue

            logger.info(
                "THRESHOLD OPTIMIZATION | Sampling=%s",
                sampling_name,
            )

            # ---------- SAMPLING (TRAIN ONLY) ----------
            X_s, y_s = apply_sampling(
                X_tr_enc,
                y_tr,
                sampling_name,
            )

            # ---------- NON-SCALED ----------
            X_s_ns = X_s
            X_te_ns = X_te_enc

            # ---------- SCALED ----------
            scaler = StandardScaler().fit(X_s)
            X_s_sc = scaler.transform(X_s)
            X_te_sc = scaler.transform(X_te_enc)

            # ---------- MODEL LOOP ----------
            for model_name, model_cfg in (
                sampling_cfg["Threshold_optimization"]["models"].items()
            ):
                base_model = MODEL_REGISTRY[model_name]
                params_list = model_cfg.get("params", [{}])
                scaling = model_cfg.get("scaling", False)

                if not isinstance(params_list, list):
                    raise TypeError(
                        "Phase-3 expects params as list[dict], "
                        f"got {type(params_list)} for model {model_name}"
                    )

                # ---------- PARAM LOOP ----------
                for idx, params in enumerate(params_list):

                    logger.info(
                        "Phase-3 | Model=%s | Param %s/%s",
                        model_name,
                        idx + 1,
                        len(params_list),
                    )

                    model = clone(base_model)
                    model.set_params(**params)

                    if scaling:
                        X_train = X_s_sc
                        X_test = X_te_sc
                    else:
                        X_train = X_s_ns
                        X_test = X_te_ns

                    model.fit(X_train, y_s)
                    y_prob = model.predict_proba(X_test)[:, 1]

                    run_threshold_optimization(
                        fs_name=fs_name,
                        split_name=fs_cfg["split"],
                        sampling_name=sampling_name,
                        model_name=model_name,
                        params=params,
                        y_true=y_te,
                        y_prob=y_prob,
                    )

        # ======================================================
        # 7-SET ENSEMBLE
        # ======================================================
        if "7_set" in fs_cfg["sampling"]:

            logger.info(
                "7-SET UNDERSAMPLING ENSEMBLE STARTED"
            )

            run_ensembled_7_set(
                fs_name=fs_name,
                split_name=fs_cfg["split"],
                X_enc_tr=X_tr_enc,
                X_enc_te=X_te_enc,
                y_tr=y_tr,
                y_te=y_te,
            )

    logger.info(
        "PHASE-3 PIPELINE COMPLETED SUCCESSFULLY"
    )
