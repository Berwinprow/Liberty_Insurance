import json
import pandas as pd

CONFIG_PATH = "/opt/airflow/dags/config/connections_table_columns.json"


def process_features(df, selected_columns):
    """
    Selects features, creates date-derived features,
    and DROPS raw date columns.
    """

    # ================= LOAD CONFIG =================
    with open(CONFIG_PATH, "r") as f:
        cfg = json.load(f)

    date_columns = cfg["columns"]["date_columns"]

    # ================= SELECT REQUIRED COLUMNS =================
    df = df[selected_columns].copy()

    # ================= DATE FEATURE ENGINEERING =================
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

            df[f"{col}_year"] = df[col].dt.year
            df[f"{col}_month"] = df[col].dt.month
            df[f"{col}_day"] = df[col].dt.day

    # ================= DROP RAW DATE COLUMNS =================
    drop_cols = [c for c in date_columns if c in df.columns]
    df = df.drop(columns=drop_cols)

    print(
        f"FEATURE PROCESSING DONE | "
        f"Final columns: {df.shape[1]} | "
        f"Dropped date columns: {drop_cols}"
    )

    return df
