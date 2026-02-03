import json

from future_prediction_all_phase2.data_loader import load_data
from future_prediction_all_phase2.feature_processing import process_features

CONFIG_PATH = "/opt/airflow/dags/config/connections_table_columns.json"
SELECTED_COLUMNS_PATH = "/opt/airflow/dags/config/selected_columns.json"
PIPELINE_CONTROL_PATH = "/opt/airflow/dags/config/pipeline_control.json"


def run_pipeline():
    """
    DATA PREPARATION ONLY.

    Responsibilities:
    - Read feature_set from pipeline_control.json
    - Load full data
    - Apply feature processing
    - Split labeled vs open
    - RETURN prepared data

    NO SPLITTING LOGIC
    NO MODEL LOGIC
    """

    # ================= LOAD PIPELINE CONTROL =================
    with open(PIPELINE_CONTROL_PATH, "r") as f:
        pipe_cfg = json.load(f)

    feature_set = pipe_cfg["feature_sets"][0]

    # ================= LOAD SELECTED COLUMNS =================
    with open(SELECTED_COLUMNS_PATH, "r") as f:
        selected_columns = json.load(f)[feature_set]

    # ================= LOAD MAIN CONFIG =================
    with open(CONFIG_PATH, "r") as f:
        cfg = json.load(f)

    target_col = cfg["columns"]["target_column"]

    # ================= LOAD DATA =================
    df = load_data()

    # ================= FEATURE PROCESSING =================
    df = process_features(df, selected_columns)

    # ================= SPLIT LABELED / OPEN =================
    labeled_df = df[df[target_col].isin([0, 1])].copy()
    open_df = df[df[target_col] == "Open"].copy()

    X_labeled = labeled_df.drop(columns=[target_col])
    y_labeled = labeled_df[target_col]

    X_open = open_df.drop(columns=[target_col])

    print(
        f"PIPELINE DATA READY | "
        f"Feature set: {feature_set} | "
        f"Labeled rows: {len(X_labeled)} | "
        f"Open rows: {len(X_open)}"
    )

    return X_labeled, y_labeled, X_open
