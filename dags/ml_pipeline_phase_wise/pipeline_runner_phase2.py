"""
Phase-2 pipeline runner:
- Feature processing
- Encoding
- Sampling
- Ensemble model training
"""

# ======================================================
# GLOBAL SEED (NON-DEEP LEARNING)
# ======================================================
import os
import random
import logging
import warnings
import json

import numpy as np
from sklearn.exceptions import ConvergenceWarning
from sklearn.model_selection import train_test_split

from ml_pipeline_phase_wise.data_loader import load_and_clean_data
from ml_pipeline_phase_wise.feature_processing import process_features
from ml_pipeline_phase_wise.encoding_utils import apply_label_encoding
from ml_pipeline_phase_wise.model_utils import (
    apply_sampling,
    apply_scaling,
)
from ml_pipeline_phase_wise.ensembled_library import (
    build_soft_voting,
    build_bagging,
    build_stacking,
    WeightedEnsembleClassifier,
)
from ml_pipeline_phase_wise.ensemble_trainer import run_ensemble


# ======================================================
# SEED & LOGGING
# ======================================================
SEED = 42
os.environ["PYTHONHASHSEED"] = str(SEED)
random.seed(SEED)
np.random.seed(SEED)

logging.getLogger().setLevel(logging.ERROR)
logging.getLogger("py.warnings").setLevel(logging.ERROR)
logging.getLogger("sklearn").setLevel(logging.ERROR)
logging.getLogger("joblib").setLevel(logging.ERROR)

warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=ConvergenceWarning)

logger = logging.getLogger(__name__)


CONFIG_PATH = "/opt/airflow/dags/config/connections_table_columns.json"


# ======================================================
# PHASE-2 PIPELINE
# ======================================================
def run_phase2_pipeline():
    """
    Execute Phase-2 ensemble model training pipeline.
    """
    logger.info("PHASE-2 PIPELINE STARTED")

    # ================= LOAD MAIN CONFIG =================
    with open(CONFIG_PATH, "r") as file:
        cfg_main = json.load(file)

    target_column = cfg_main["columns"]["target_column"]

    # ================= LOAD PIPELINE CONFIGS =================
    with open(
        "/opt/airflow/dags/config/ensembled_config.json",
        "r",
    ) as file:
        config = json.load(file)

    with open(
        "/opt/airflow/dags/config/selected_columns.json",
        "r",
    ) as file:
        feature_sets = json.load(file)

    base_df = load_and_clean_data()

    # ================= FEATURE SET LOOP =================
    for fs_name, fs_cfg in config.items():
        if not fs_cfg["enabled"]:
            continue

        logger.info(
            "PHASE-2 | FEATURE SET → %s",
            fs_name,
        )

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

        X_tr_enc, X_te_enc = apply_label_encoding(
            X_tr,
            X_te,
        )

        # ================= SAMPLING LOOP =================
        for sampling, methods in fs_cfg["sampling"].items():

            logger.info(
                "PHASE-2 | SAMPLING → %s",
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

            meta = {
                "feature_set": fs_name,
                "split": fs_cfg["split"],
                "sampling": sampling,
            }

            # ================= ENSEMBLE LOOP =================
            for name, cfg in methods.items():

                logger.info(
                    "ENSEMBLE START → %s",
                    name.upper(),
                )

                # ---------------- BUILD MODEL ----------------
                if name == "voting":
                    model = build_soft_voting(
                        cfg,
                        X_tr_sc,
                        X_tr_ns,
                        y_s,
                    )
                    X_train = X_tr_sc
                    X_test = X_te_sc

                elif name == "bagging":
                    model = build_bagging(
                        cfg,
                        X_tr_sc,
                        X_tr_ns,
                        y_s,
                    )
                    X_train = X_tr_ns
                    X_test = X_te_ns

                elif name == "stacking":
                    model = build_stacking(
                        cfg,
                        X_tr_sc,
                        X_tr_ns,
                        y_s,
                    )
                    X_train = X_tr_sc
                    X_test = X_te_sc

                elif name == "weighted_ensemble":
                    model = WeightedEnsembleClassifier(
                        cfg["models"],
                        cfg["weightage"],
                    ).fit(
                        X_tr_sc,
                        X_tr_ns,
                        y_s,
                    )
                    X_train = (X_tr_sc, X_tr_ns)
                    X_test = (X_te_sc, X_te_ns)

                else:
                    continue

                # ---------------- TRAIN + STORE ----------------
                run_ensemble(
                    ensemble_name=name,
                    model=model,
                    X_train=X_train,
                    X_test=X_test,
                    y_train=y_s,
                    y_test=y_te,
                    meta=meta,
                    cfg=cfg,
                )

                logger.info(
                    "ENSEMBLE DONE → %s",
                    name.upper(),
                )

    logger.info(
        "PHASE-2 COMPLETED SUCCESSFULLY"
    )
