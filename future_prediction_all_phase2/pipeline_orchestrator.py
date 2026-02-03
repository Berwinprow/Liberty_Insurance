import json
import numpy as np

from future_prediction_all_phase2.pipeline_runner_future import run_pipeline
from future_prediction_all_phase2.splitters import (
    split_80_20,
    split_70_30,
    time_based_split,
    full_train_split
)

from future_prediction_all_phase2.encoding_utils import apply_label_encoding
from future_prediction_all_phase2.sampling_utils import (
    apply_sampling,
    seven_set_undersampling
)
from future_prediction_all_phase2.scaling_utils import apply_standard_scaling

from future_prediction_all_phase2.cv_runner import run_cross_validation
from future_prediction_all_phase2.time_based_runner import run_time_based
from future_prediction_all_phase2.full_open_runner import run_full_open_prediction

from future_prediction_all_phase2.db_writer import (
    write_future_cv_results,
    write_future_time_based_results,
    write_future_open_results
)

PIPELINE_CONTROL_PATH = "/opt/airflow/dags/config/pipeline_control.json"
TIME_BASED_CONFIG_PATH = "/opt/airflow/dags/config/time_based_config.json"


# ======================================================
# SEVEN SET AGGREGATION
# ======================================================
def aggregate_rows(rows):
    """
    Aggregate seven-set rows into ONE row.
    Numeric fields -> mean
    """
    base = rows[0].copy()

    numeric_keys = [
        k for k, v in base.items()
        if isinstance(v, (int, float))
    ]

    for key in numeric_keys:
        base[key] = float(np.mean([r[key] for r in rows]))

    return base


# ======================================================
# MAIN PIPELINE
# ======================================================
def run_future_pipeline():

    # ================= LOAD CONFIG =================
    with open(PIPELINE_CONTROL_PATH) as f:
        cfg = json.load(f)

    with open(TIME_BASED_CONFIG_PATH) as f:
        time_cfg = json.load(f)["time_based_window"]

    feature_sets = cfg["feature_sets"]
    pipelines = cfg["pipelines"]

    # ================= LOAD DATA =================
    X_labeled, y_labeled, X_open = run_pipeline()
    y_labeled = y_labeled.astype(int)

    # ================= FEATURE SET LOOP =================
    for feature_set in feature_sets:

        # ================= SPLIT LOOP =================
        for split_name in ["80_20", "70_30", "time_based", "full"]:

            if split_name == "80_20" and cfg["splits"]["80_20"]["cv"]:
                X_tr, X_te, y_tr, y_te = split_80_20(X_labeled, y_labeled)
                mode = "cv"

            elif split_name == "70_30" and cfg["splits"]["70_30"]["cv"]:
                X_tr, X_te, y_tr, y_te = split_70_30(X_labeled, y_labeled)
                mode = "cv"

            elif split_name == "time_based":
                X_tr, X_te, y_tr, y_te = time_based_split(X_labeled, y_labeled)
                mode = "time"

            elif split_name == "full":
                X_tr, y_tr = full_train_split(X_labeled, y_labeled)
                X_te, y_te = None, None
                mode = "full"

            else:
                continue

            # ================= ENCODING =================
            X_tr_enc, X_te_enc, X_op_enc = apply_label_encoding(
                X_tr,
                X_test=X_te,
                X_open=X_open if mode == "full" else None
            )

            # ================= PIPELINE LOOP =================
            for pipeline in pipelines:

                sampling_method = pipeline["sampling"]

                # cache for non-seven-set sampling
                sampled_cache = {}

                # ==================================================
                # HANDLE SINGLE MODELS
                # ==================================================
                for model_name, model_cfg in pipeline.get("models", {}).items():

                    params_list = model_cfg.get("params", [])
                    if isinstance(params_list, dict):
                        params_list = [params_list]

                    for params in params_list:

                        for threshold in model_cfg.get("threshold", [0.5]):

                            model_cfg_run = model_cfg.copy()
                            model_cfg_run["params"] = params
                            model_cfg_run["threshold"] = threshold

                            # ================= SEVEN SET =================
                            if sampling_method == "seven_set":

                                seven_sets = seven_set_undersampling(X_tr_enc, y_tr)
                                collected_rows = []

                                for _, (X_s, y_s) in seven_sets.items():

                                    if model_cfg_run.get("scaling", False):
                                        X_s_scaled, X_te_scaled, X_op_scaled = apply_standard_scaling(
                                            X_s,
                                            X_test=X_te_enc,
                                            X_open=X_op_enc
                                        )
                                    else:
                                        X_s_scaled = X_s
                                        X_te_scaled = X_te_enc
                                        X_op_scaled = X_op_enc

                                    if mode == "cv":
                                        row = run_cross_validation(
                                            X_s_scaled, y_s,
                                            model_name, model_cfg_run,
                                            split_name, feature_set,
                                            sampling_method
                                        )

                                    elif mode == "time":
                                        row = run_time_based(
                                            X_s_scaled, y_s,
                                            X_te_scaled, y_te,
                                            model_name, model_cfg_run,
                                            feature_set, sampling_method,
                                            time_cfg
                                        )

                                    elif mode == "full":
                                        row = run_full_open_prediction(
                                            X_s_scaled, y_s,
                                            X_op_scaled,
                                            model_name, model_cfg_run,
                                            feature_set, sampling_method
                                        )

                                    collected_rows.append(row)

                                final_row = aggregate_rows(collected_rows)

                                if mode == "cv":
                                    write_future_cv_results([final_row])
                                elif mode == "time":
                                    write_future_time_based_results([final_row])
                                elif mode == "full":
                                    write_future_open_results([final_row])

                            # ================= NORMAL SAMPLING =================
                            else:
                                if sampling_method not in sampled_cache:
                                    X_s, y_s = apply_sampling(X_tr_enc, y_tr, sampling_method)
                                    sampled_cache[sampling_method] = (X_s, y_s)
                                else:
                                    X_s, y_s = sampled_cache[sampling_method]

                                if model_cfg_run.get("scaling", False):
                                    X_s_scaled, X_te_scaled, X_op_scaled = apply_standard_scaling(
                                        X_s,
                                        X_test=X_te_enc,
                                        X_open=X_op_enc
                                    )
                                else:
                                    X_s_scaled = X_s
                                    X_te_scaled = X_te_enc
                                    X_op_scaled = X_op_enc

                                if mode == "cv":
                                    row = run_cross_validation(
                                        X_s_scaled, y_s,
                                        model_name, model_cfg_run,
                                        split_name, feature_set,
                                        sampling_method
                                    )
                                    write_future_cv_results([row])

                                elif mode == "time":
                                    row = run_time_based(
                                        X_s_scaled, y_s,
                                        X_te_scaled, y_te,
                                        model_name, model_cfg_run,
                                        feature_set, sampling_method,
                                        time_cfg
                                    )
                                    write_future_time_based_results([row])

                                elif mode == "full":
                                    row = run_full_open_prediction(
                                        X_s_scaled, y_s,
                                        X_op_scaled,
                                        model_name, model_cfg_run,
                                        feature_set, sampling_method
                                    )
                                    write_future_open_results([row])

                # ==================================================
                # HANDLE ENSEMBLED MODELS
                # ==================================================
                for model_name, model_cfg in pipeline.get("ensembled", {}).items():

                    for threshold in model_cfg.get("threshold", [0.5]):

                        model_cfg_run = model_cfg.copy()
                        model_cfg_run["threshold"] = threshold

                        # ================= SEVEN SET =================
                        if sampling_method == "seven_set":

                            seven_sets = seven_set_undersampling(X_tr_enc, y_tr)
                            collected_rows = []

                            for _, (X_s, y_s) in seven_sets.items():

                                X_s_scaled = X_s
                                X_te_scaled = X_te_enc
                                X_op_scaled = X_op_enc

                                if mode == "cv":
                                    row = run_cross_validation(
                                        X_s_scaled, y_s,
                                        model_name, model_cfg_run,
                                        split_name, feature_set,
                                        sampling_method
                                    )

                                elif mode == "time":
                                    row = run_time_based(
                                        X_s_scaled, y_s,
                                        X_te_scaled, y_te,
                                        model_name, model_cfg_run,
                                        feature_set, sampling_method,
                                        time_cfg
                                    )

                                elif mode == "full":
                                    row = run_full_open_prediction(
                                        X_s_scaled, y_s,
                                        X_op_scaled,
                                        model_name, model_cfg_run,
                                        feature_set, sampling_method
                                    )

                                collected_rows.append(row)

                            final_row = aggregate_rows(collected_rows)

                            if mode == "cv":
                                write_future_cv_results([final_row])
                            elif mode == "time":
                                write_future_time_based_results([final_row])
                            elif mode == "full":
                                write_future_open_results([final_row])

                        # ================= NORMAL SAMPLING =================
                        else:
                            if sampling_method not in sampled_cache:
                                X_s, y_s = apply_sampling(X_tr_enc, y_tr, sampling_method)
                                sampled_cache[sampling_method] = (X_s, y_s)
                            else:
                                X_s, y_s = sampled_cache[sampling_method]

                            X_s_scaled = X_s
                            X_te_scaled = X_te_enc
                            X_op_scaled = X_op_enc

                            if mode == "cv":
                                row = run_cross_validation(
                                    X_s_scaled, y_s,
                                    model_name, model_cfg_run,
                                    split_name, feature_set,
                                    sampling_method
                                )
                                write_future_cv_results([row])

                            elif mode == "time":
                                row = run_time_based(
                                    X_s_scaled, y_s,
                                    X_te_scaled, y_te,
                                    model_name, model_cfg_run,
                                    feature_set, sampling_method,
                                    time_cfg
                                )
                                write_future_time_based_results([row])

                            elif mode == "full":
                                row = run_full_open_prediction(
                                    X_s_scaled, y_s,
                                    X_op_scaled,
                                    model_name, model_cfg_run,
                                    feature_set, sampling_method
                                )
                                write_future_open_results([row])

    print("PIPELINE COMPLETED SUCCESSFULLY")
