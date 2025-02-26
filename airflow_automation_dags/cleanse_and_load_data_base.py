import pandas as pd
import re
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from sqlalchemy import text

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
    "office_name":"new_branch_name_2",
    "total_sum_insured":"vehicle_idv",
    "channel":"new_vertical"
}

# ✅ Define Source & Target Tables
SOURCE_SCHEMA = "stage"
SOURCE_TABLE = "base_24"
TARGET_SCHEMA = "bi_dwh"
LOG_SCHEMA = "log"
TARGET_TABLE = f"processed_{SOURCE_TABLE}"
REMOVED_TABLE = f"removed_{SOURCE_TABLE}"

# ✅ Fixed Text Cleaning Function
def clean_text(value):
    """Cleans text fields by:
    - Removing extra spaces
    - Removing special characters
    - Converting to lowercase
    - Removing spaces between words (for insured_name)
    """
    if pd.isna(value) or value is None:
        return None  # Keeps NaN values as is
    
    value = str(value).strip()
    value = re.sub(r"[^A-Za-z0-9\s]", "", value)  # Remove non-alphanumeric characters
    value = re.sub(r"\s+", " ", value).strip()  # Remove multiple spaces
    value = value.lower()

    return value.replace(" ", "")  # Remove all spaces for insured_name

# ✅ Function to Clean & Load Data
def cleanse_and_load_data_base():
    """Cleans and loads Base data into `bi_dwh`, while logging removed rows into `log` schema."""
    postgres_hook = PostgresHook(postgres_conn_id="postgres")
    engine = postgres_hook.get_sqlalchemy_engine()
    pipeline_run_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # ✅ Ensure Schemas Exist
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};"))
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {LOG_SCHEMA};"))

    # ✅ Extract Data
    query = f"SELECT * FROM {SOURCE_SCHEMA}.{SOURCE_TABLE}"
    df = pd.read_sql(query, engine)
    initial_count = len(df)
    print(f"📂 Extracted {initial_count} records from `{SOURCE_SCHEMA}.{SOURCE_TABLE}`.")

    # ✅ Step 1: Apply Column Mapping & Log Changes
    original_columns = set(df.columns)
    df.rename(columns=COLUMN_MAPPING, inplace=True)

    # Find mapped columns
    mapped_columns = set(original_columns).intersection(COLUMN_MAPPING.keys())
    updated_columns = {col: COLUMN_MAPPING[col] for col in mapped_columns if col in COLUMN_MAPPING}

    print(f"🔄 Column Mapping Applied: {len(updated_columns)} columns changed.")
    for old_col, new_col in updated_columns.items():
        print(f"   🔹 `{old_col}` → `{new_col}`")

    # ✅ Clean specific columns
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

    # ✅ Initialize removed rows DataFrame
    removed_rows = pd.DataFrame()

    # ✅ Step 3: Remove Duplicate Rows (Log Removed)
    before_dedup = len(df)
    duplicate_rows = df[df.duplicated()]
    df.drop_duplicates(inplace=True)
    duplicate_rows["removal_reason"] = "Duplicate row"
    removed_rows = pd.concat([removed_rows, duplicate_rows])
    print(f"🚀 Removed {len(duplicate_rows)} duplicate rows.")

    # ✅ Step 4: Remove Invalid `total_premium_payable` (Log Removed)
    if "total_premium_payable" in df.columns:
        df["total_premium_payable"] = pd.to_numeric(df["total_premium_payable"], errors="coerce")
        invalid_premium = df[df["total_premium_payable"].isna() | (df["total_premium_payable"] <= 0)]
        df = df[df["total_premium_payable"].notna() & (df["total_premium_payable"] > 0)]
        invalid_premium["removal_reason"] = "Invalid total_premium_payable"
        removed_rows = pd.concat([removed_rows, invalid_premium])
        print(f"💰 Removed {len(invalid_premium)} rows with invalid `total_premium_payable`.")

    # ✅ Step 5: Remove Invalid `policy_start_date` & `policy_end_date` (Log Removed)
    if "policy_start_date" in df.columns and "policy_end_date" in df.columns:
        df["policy_start_date"] = pd.to_datetime(df["policy_start_date"], errors="coerce")
        df["policy_end_date"] = pd.to_datetime(df["policy_end_date"], errors="coerce")
        invalid_dates = df[df["policy_start_date"].isna() | df["policy_end_date"].isna()]
        df = df[df["policy_start_date"].notna() & df["policy_end_date"].notna()]
        invalid_dates["removal_reason"] = "Invalid policy_start_date/policy_end_date"
        removed_rows = pd.concat([removed_rows, invalid_dates])
        print(f"📅 Removed {len(invalid_dates)} invalid policy date rows.")

    # ✅ Step 6: Deduplicate Policies (Keep Highest `total_premium_payable`)
    if "policy_no" in df.columns and "total_premium_payable" in df.columns:
        before_dedup = len(df)
        df = df.sort_values("total_premium_payable", ascending=False).drop_duplicates(subset=["policy_no"], keep="first")
        print(f"📊 Removed {before_dedup - len(df)} duplicate policies, keeping highest `total_premium_payable`.")

    # ✅ Log Removed Rows into `log` Table
    if not removed_rows.empty:
        removed_rows["pipeline_run_time"] = pipeline_run_time
        removed_rows.to_sql(name=REMOVED_TABLE, schema=LOG_SCHEMA, con=engine, if_exists="replace", index=False)
        print(f"⚠️ Removed rows logged into `{LOG_SCHEMA}.{REMOVED_TABLE}`.")

    # ✅ Load Cleaned Data into `bi_dwh`
    df.to_sql(name=TARGET_TABLE, schema=TARGET_SCHEMA, con=engine, if_exists="replace", index=False)
    print(f"✅ Data successfully loaded into `{TARGET_SCHEMA}.{TARGET_TABLE}`.")

# ✅ Define DAG
with DAG(
    dag_id="cleanse_and_load_data_base",
    default_args={"owner": "airflow", "start_date": datetime(2024, 2, 10)},
    schedule_interval=None,
    catchup=False
) as dag:

    cleanse_and_load_task = PythonOperator(
        task_id="cleanse_and_load_data_base",
        python_callable=cleanse_and_load_data_base
    )

    cleanse_and_load_task
