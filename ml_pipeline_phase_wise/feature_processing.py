import pandas as pd

def process_features(df, selected_columns):
    df = df[selected_columns].copy()

    for c in ["policy_start_date", "policy_end_date"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
            df[f"{c}_day"] = df[c].dt.day
            df[f"{c}_month"] = df[c].dt.month
            df[f"{c}_year"] = df[c].dt.year

    df.drop(
        columns=["policy_start_date", "policy_end_date"],
        inplace=True,
        errors="ignore"
    )

    print(f"FEATURE ENGINEERING DONE | Columns: {df.shape[1]}")
    return df
