# ======================================================
# PHASE-3 PIPELINE
# - Threshold Optimization
# - 7-Set Undersampling Ensemble
# ======================================================

import json
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.base import clone

from ml_pipeline_phase_wise.data_loader import load_and_clean_data
from ml_pipeline_phase_wise.feature_processing import process_features
from ml_pipeline_phase_wise.encoding_utils import apply_label_encoding
from ml_pipeline_phase_wise.model_utils import apply_sampling

from ml_pipeline_phase_wise.threshold_optimization import run_threshold_optimization
from ml_pipeline_phase_wise.ensembled_7_set import run_ensembled_7_set
from ml_pipeline_phase_wise.model_library import MODEL_GROUPS


PHASE3_CONFIG = "/opt/airflow/dags/config/seven_set_threshold_config.json"
SELECTED_COLS = "/opt/airflow/dags/config/selected_columns.json"
CONN_CFG = "/opt/airflow/dags/config/connections_table_columns.json"


# ======================================================
# MODEL REGISTRY
# ======================================================
MODEL_REGISTRY = {}
for g in MODEL_GROUPS:
    MODEL_REGISTRY.update(g)


# ======================================================
# SPLIT PARSER
# ======================================================
def parse_split(split_str: str) -> float:
    """
    Converts '80_20' → 0.2 (test_size)
    """
    _, test_pct = split_str.split("_")
    return int(test_pct) / 100


# ======================================================
# PHASE-3 RUNNER
# ======================================================
def run_phase3_pipeline():

    print("\n🚀 PHASE-3 PIPELINE STARTED")

    # ================= LOAD DATA =================
    base_df = load_and_clean_data()

    phase3_cfg = json.load(open(PHASE3_CONFIG))
    feature_sets = json.load(open(SELECTED_COLS))
    conn_cfg = json.load(open(CONN_CFG))

    TARGET = conn_cfg["columns"]["target_column"]

    # ======================================================
    # FEATURE SET LOOP
    # ======================================================
    for fs_name, fs_cfg in phase3_cfg.items():

        if not fs_cfg.get("enabled", False):
            continue

        print(f"\n📌 PHASE-3 | FEATURE SET → {fs_name}")

        # ================= FEATURE ENGINEERING =================
        df = process_features(base_df, feature_sets[fs_name])
        X = df.drop(TARGET, axis=1)
        y = df[TARGET]

        # ================= SPLIT (CONFIG DRIVEN) =================
        test_size = parse_split(fs_cfg["split"])

        X_tr, X_te, y_tr, y_te = train_test_split(
            X,
            y,
            test_size=test_size,
            stratify=y,
            random_state=42
        )

        # ======================================================
        # ENCODING (ONCE — REUSED EVERYWHERE)
        # ======================================================
        X_tr_enc, X_te_enc = apply_label_encoding(X_tr, X_te)

        # ======================================================
        # THRESHOLD OPTIMIZATION
        # ======================================================
        for sampling_name, sampling_cfg in fs_cfg["sampling"].items():

            if "Threshold_optimization" not in sampling_cfg:
                continue

            print(
                f"\n🎯 THRESHOLD OPTIMIZATION | "
                f"Sampling={sampling_name}"
            )

            # --------------------------------------------------
            # 1️⃣ SAMPLING (TRAIN ONLY)
            # --------------------------------------------------
            X_s, y_s = apply_sampling(
                X_tr_enc,
                y_tr,
                sampling_name
            )

            # --------------------------------------------------
            # 2️⃣ CACHE NON-SCALED DATA
            # --------------------------------------------------
            X_s_ns = X_s
            X_te_ns = X_te_enc

            # --------------------------------------------------
            # 3️⃣ CACHE SCALED DATA
            # --------------------------------------------------
            scaler = StandardScaler().fit(X_s)
            X_s_sc = scaler.transform(X_s)
            X_te_sc = scaler.transform(X_te_enc)

            # --------------------------------------------------
            # 4️⃣ MODEL LOOP
            # --------------------------------------------------
            for model_name, model_cfg in (
                sampling_cfg["Threshold_optimization"]["models"].items()
            ):

                base_model = MODEL_REGISTRY[model_name]
                params_list = model_cfg.get("params", [{}])
                scaling = model_cfg.get("scaling", False)

                if not isinstance(params_list, list):
                    raise TypeError(
                        f"Phase-3 expects params as list[dict], "
                        f"got {type(params_list)} for model {model_name}"
                    )

                # ======================================================
                # 🔁 PARAM LOOP (NEW MODEL PER PARAM — FIX)
                # ======================================================
                for param_idx, params in enumerate(params_list):

                    print(
                        f"🔁 Phase-3 | Model={model_name} | "
                        f"Param {param_idx + 1}/{len(params_list)}"
                    )

                    # ✅ IMPORTANT FIX:
                    # clone() gives a FRESH unfitted model every time
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
                        y_prob=y_prob
                    )

        # ======================================================
        # 7-SET ENSEMBLE (OPTIONAL)
        # ======================================================
        if "7_set" in fs_cfg["sampling"]:

            print("\n🔁 7-SET UNDERSAMPLING ENSEMBLE STARTED")

            run_ensembled_7_set(
                fs_name=fs_name,
                split_name=fs_cfg["split"],
                X_enc_tr=X_tr_enc,
                X_enc_te=X_te_enc,
                y_tr=y_tr,
                y_te=y_te
            )

    print("\n✅ PHASE-3 PIPELINE COMPLETED SUCCESSFULLY")
