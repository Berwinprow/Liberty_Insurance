import pandas as pd
import re
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.types import String  # ✅ Fix for `policy_number` type issue

# ✅ Define variables for source & target
SOURCE_SCHEMA = "stage"
SOURCE_TABLE = "pr_24"  # Change for different PR files
TARGET_SCHEMA = "bi_dwh"
LOG_SCHEMA = "log"
NET_PREMIUM_COLUMN = "net_premium"
POLICY_START_COLUMN = "policy_start_date"
POLICY_END_COLUMN = "policy_end_date"
POLICY_ISSUE_COLUMN = "policy_issue_date"
POLICY_NUMBER_COLUMN = "policy_no"
NOP_COLUMN = "nop"
GST_PERCENTAGE = 0.18  # ✅ GST percentage

# ✅ Column Mapping
COLUMN_MAPPING = {
    "policy_no.": "policy_no",
    "policy_number": "policy_no",
    "net_tp_premium_/_war_&_srcc": "total_tp_premium",
    "net_od_premium": "total_od_premium",
    "model_name": "model",
    "manufacturer/make": "manufacturer",
    "make_name": "manufacturer",
    "variant": "model_variant",
    "age": "vehicle_age",
    "reg_no": "veh_reg_no",
    "chassis_number": "chassis_no",
    "previous_year_ncb_%": "previous_year_ncb_percentage",
    "ncb_%_previous_year": "previous_year_ncb_percentage",
    "new_branch_name__2": "new_branch_name_2",
    "location": "new_branch_name_2",
    "system_channel": "new_vertical",
    "engine_number": "engine_no",
    "biztype": "business_type",
    "sum_insured": "vehicle_idv",
    "product_name__2": "product_name_2",
    "enginenumber": "engine_no",
    "before_gst_add-on_gwp":"before_gst_add_on_gwp",
    "current_year_ncb_amount":"ncb_amount",
    "current_year_ncb_%":"applicable_discount_with_ncb",
    "add_on_cover_premium":"before_gst_add_on_gwp",
    "state2":"state",
    #"office_name":"new_branch_name_2",
    "total_sum_insured":"vehicle_idv",
    "channel":"new_vertical"
}
# ✅ Fixed Text Cleaning Function
def clean_text(value):
    """Cleans text fields by:
    - Removing extra spaces
    - Removing special characters
    - Converting to lowercase
    - Removing spaces between words (for insured_name)
    """
    if isinstance(value, pd.Series):  # ✅ Ensure it's not a Series
        return value.apply(clean_text)

    if pd.isna(value) or value is None:
        return None  # ✅ Keeps NaN values as is
    value = str(value).strip()
    value = re.sub(r"[^A-Za-z0-9\s]", "", value)  # Remove non-alphanumeric characters
    value = re.sub(r"\s+", " ", value).strip()  # Remove multiple spaces
    value = value.lower()

    return value.replace(" ", "")  # Remove all spaces for insured_name

def clean_policy_number(value):
    """Ensures `policy_number` is numeric & removes any leading `'`."""
    if pd.isna(value):
        return None
    value = str(value).strip().lstrip("'")  # Remove leading `'`
    return value if value.isdigit() else None  # Keep valid numbers

def clean_and_load_pr_data():
    """Cleans PR file data, logs removed rows, and writes logs to Airflow."""
    postgres_hook = PostgresHook(postgres_conn_id="postgres")
    engine = postgres_hook.get_sqlalchemy_engine()
    pipeline_run_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    print(f"🛠 Starting Data Processing at `{pipeline_run_time}`")

    # ✅ Ensure schemas exist
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};"))
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {LOG_SCHEMA};"))

    # ✅ Extract data from `SOURCE_TABLE`
    query = f"SELECT * FROM {SOURCE_SCHEMA}.\"{SOURCE_TABLE}\""
    df = pd.read_sql(query, engine)
    initial_count = len(df)
    print(f"📂 Extracted {initial_count} records from `{SOURCE_SCHEMA}.{SOURCE_TABLE}`.")

    # ✅ Apply Column Mapping
    original_columns = set(df.columns)
    df.rename(columns=COLUMN_MAPPING, inplace=True)
    mapped_columns = set(COLUMN_MAPPING.keys()).intersection(original_columns)
    updated_columns = {col: COLUMN_MAPPING[col] for col in mapped_columns}

    print(f"🔄 Column Mapping Applied: {len(mapped_columns)} columns changed.")
    for old_col, new_col in updated_columns.items():
        print(f"   🔹 `{old_col}` → `{new_col}`")

    columns_to_clean = {
        "chassis_no": "cleaned_chassis_no",
        "engine_no": "cleaned_engine_no",
        "insured_name": "cleaned_insured_name",  # Remove spaces for insured_name
        "veh_reg_no": "cleaned_veh_reg_no",
        "new_branch_name_2": "cleaned_new_branch_name",
        "model":"cleaned_model"
    }

    for original_col, cleaned_col in columns_to_clean.items():
        if original_col in df.columns:
            df[cleaned_col] = df[original_col].astype(str).apply(clean_text)

    print("🧼 Cleaned chassis, engine, insured name, vehicle reg no, and branch name.")

    # ✅ Step 2: Concatenate Chassis & Engine Numbers
    if "cleaned_chassis_no" in df.columns and "cleaned_engine_no" in df.columns:
        df["chassis_engine_no"] = df["cleaned_chassis_no"] + "_" + df["cleaned_engine_no"]
        print("🔗 Concatenated chassis_no & engine_no into `concat_chassis_engine`.")


    # ✅ Step 3: Clean & Convert `policy_number`
    removed_invalid_policy = df[df[POLICY_NUMBER_COLUMN].apply(lambda x: clean_policy_number(x) is None)]
    df[POLICY_NUMBER_COLUMN] = df[POLICY_NUMBER_COLUMN].apply(clean_policy_number)
    df = df[df[POLICY_NUMBER_COLUMN].notna()]
    removed_invalid_policy["removal_reason"] = "Invalid policy number"
    print(f"🔍 Removed {len(removed_invalid_policy)} invalid policy numbers.")

    # ✅ Step 4: Filter `Nop=1`
    
    # ✅ Step 4: Filter `Nop=1` (Only if `NOP_COLUMN` exists)
    if NOP_COLUMN in df.columns:
        removed_nop = df[df[NOP_COLUMN] != 1]  # Capture rows to be removed
        df = df[df[NOP_COLUMN] == 1]  # Keep only rows where `nop` is 1
        removed_nop["removal_reason"] = "`nop` != 1"

        print(f"📌 Removed {len(removed_nop)} rows where `nop` != 1.")
    else:
        removed_nop = pd.DataFrame()  # Create an empty DataFrame if column doesn't exist
        print("⚠️ `nop` column not found, skipping `nop` filtering step.")

    # ✅ Step 4: Remove invalid `total_premium`
    df[NET_PREMIUM_COLUMN] = pd.to_numeric(df[NET_PREMIUM_COLUMN], errors="coerce")
    removed_premium_issues = df[df[NET_PREMIUM_COLUMN].isna() | (df[NET_PREMIUM_COLUMN] <= 0)]
    df = df[df[NET_PREMIUM_COLUMN].notna() & (df[NET_PREMIUM_COLUMN] > 0)]
    removed_premium_issues["removal_reason"] = "Invalid total_premium"
    print(f"💰 Removed {len(removed_premium_issues)} rows with invalid `total_premium`.")

    # ✅ Step 5: Remove policies with duration <= 10 months
    df[POLICY_START_COLUMN] = pd.to_datetime(df[POLICY_START_COLUMN], errors="coerce")
    df[POLICY_END_COLUMN] = pd.to_datetime(df[POLICY_END_COLUMN], errors="coerce")
    df["policy_duration_months"] = ((df[POLICY_END_COLUMN] - df[POLICY_START_COLUMN]).dt.days / 30).astype(int)
    removed_short_policies = df[df["policy_duration_months"] <= 10]
    df = df[df["policy_duration_months"] > 10]
    removed_short_policies["removal_reason"] = "Policy duration <= 10 months"
    print(f"📉 Removed {len(removed_short_policies)} policies with duration <= 10 months.")

    # ✅ Step 6: Deduplicate based on `policy_key`
    df["policy_key"] = df[POLICY_START_COLUMN].astype(str) + "_" + df[POLICY_END_COLUMN].astype(str) + "_" + df[POLICY_NUMBER_COLUMN].astype(str)

    removed_duplicate_policies = pd.DataFrame()
    before_dedup = len(df)
    df = df.sort_values(POLICY_ISSUE_COLUMN, ascending=False).drop_duplicates(subset=["policy_key"], keep="first")
    removed_duplicate_policies = df.iloc[before_dedup - len(df):]
    removed_duplicate_policies["removal_reason"] = "Duplicate policy, keeping latest issue date"
    print(f"📊 Removed {len(removed_duplicate_policies)} duplicate policies.")
	
	# ✅ Step 7: Calculate GST & Total Premium Payable
    if NET_PREMIUM_COLUMN in df.columns:
        df[NET_PREMIUM_COLUMN] = pd.to_numeric(df[NET_PREMIUM_COLUMN], errors="coerce")
        df["gst"] = df[NET_PREMIUM_COLUMN] * GST_PERCENTAGE
        df["total_premium_payable"] = df[NET_PREMIUM_COLUMN] + df["gst"]
        print(f"💰 Calculated gst and total_premium_payable.")

    # ✅ Final count
    final_count = len(df)
    print(f"✅ Final record count after cleansing: {final_count} (Removed {initial_count - final_count} total rows).")

    # ✅ Step 7: Log Removed Rows into `log.removed_<SOURCE_TABLE>`
    removed_data = pd.concat([
        removed_invalid_policy,
        removed_nop,
        removed_premium_issues,
        removed_short_policies,
        removed_duplicate_policies
    ])
    
    if not removed_data.empty:
        removed_data["pipeline_run_time"] = pipeline_run_time
        log_table = f"removed_{SOURCE_TABLE}"
        removed_data.to_sql(name=log_table, schema=LOG_SCHEMA, con=engine, if_exists="replace", index=False)
        print(f"⚠️ Removed rows logged into `{LOG_SCHEMA}.{log_table}` with timestamp `{pipeline_run_time}`.")

    # ✅ Step 8: Load cleaned data into `bi_dwh`
    target_table = f"processed_{SOURCE_TABLE}"
    df.to_sql(name=target_table, schema=TARGET_SCHEMA, con=engine, if_exists="replace", index=False, dtype={POLICY_NUMBER_COLUMN: String})
    print(f"✅ Data successfully loaded into `{TARGET_SCHEMA}.{target_table}`.")

# ✅ Define DAG
with DAG(dag_id="clean_pr_file_data", default_args={"owner": "airflow", "start_date": datetime(2024, 2, 10)}, schedule_interval=None, catchup=False) as dag:
    clean_pr_data = PythonOperator(task_id="clean_pr_file_data", python_callable=clean_and_load_pr_data)
    clean_pr_data
