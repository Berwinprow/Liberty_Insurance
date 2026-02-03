import json
import pandas as pd

CONFIG_PATH = "/opt/airflow/dags/config/connections_table_columns.json"


def process_features(df, selected_columns):
    # ================= LOAD CONFIG =================
    with open(CONFIG_PATH, "r") as f:
        cfg = json.load(f)

    date_columns = cfg["columns"]["date_columns"]

    # ================= SELECT COLUMNS =================
    df = df[selected_columns].copy()

    # ================= DATE FEATURE ENGINEERING =================
    for c in date_columns:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
            df[f"{c}_day"] = df[c].dt.day
            df[f"{c}_month"] = df[c].dt.month
            df[f"{c}_year"] = df[c].dt.year

    # ================= DROP ORIGINAL DATE COLUMNS =================
    df.drop(
        columns=date_columns,
        inplace=True,
        errors="ignore"
    )

    print(f"FEATURE ENGINEERING DONE | Columns: {df.shape[1]}")
    return df
