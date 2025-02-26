import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from sqlalchemy import text
from fuzzywuzzy import fuzz

# ✅ Define Source & Target Tables
SOURCE_SCHEMA = "bi_dwh"
BASE_TABLE = "finalwith_2024_base"
PR_TABLE = "processed_pr_24"
TARGET_SCHEMA = "bi_dwh"
TARGET_TABLE = "finalwith_2024_pr"
LOG_TABLE = "removed_duplicate_policies"
log_schema='log'
FINAL_TABLE = "final_renewed_policies_test"
# ✅ Define Column Names
MANUFACTURER_COLUMN = "manufacturer"
REG_NO_COLUMN = "cleaned_veh_reg_no"
MODEL_COLUMN = "cleaned_model"
CHASSIS_COLUMN = "cleaned_chassis_no"
ENGINE_COLUMN = "cleaned_engine_no"
INSURED_NAME_COLUMN = "cleaned_insured_name"
MONTH_COLUMN = "month"
POLICY_START_COLUMN = "policy_start_date"
POLICY_END_COLUMN = "policy_end_date"
POLICY_NUMBER_COLUMN = "policy_no"
BRANCH_COLUMN="cleaned_new_branch_name"
CORRECTED_CHASSIS_ENGINE_NO="corrected_chassis_no"
CORRECT_INSURANCE_NAME="corrected_name"


def convert_month_format(value):
    """Converts month format to YYYY-MM-DD"""
    try:
        if pd.isna(value):
            return None
        value = str(value).strip()
        if "-" in value:  # Already in date format
            return pd.to_datetime(value).strftime("%Y-%m-%d")
        elif "'" in value:  # Format like Apr'21
            return pd.to_datetime(value, format="%b'%y").strftime("%Y-%m-%d")
        else:  # Format like Apr 22
            return pd.to_datetime(value, format="%b %y").strftime("%Y-%m-%d")
    except Exception:
        return None  # Handle invalid values

def append_base_pr():
    """Appends `base` and `pr` data, identifies common & different columns, and performs cleaning."""
    postgres_hook = PostgresHook(postgres_conn_id="postgres")
    engine = postgres_hook.get_sqlalchemy_engine()

    # ✅ Ensure Target Schema Exists
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};"))

    # ✅ Extract Data from `base` and `pr`
    query_base = f"SELECT * FROM {SOURCE_SCHEMA}.{BASE_TABLE}"
    query_pr = f"SELECT * FROM {SOURCE_SCHEMA}.{PR_TABLE}"
    
    df_base = pd.read_sql(query_base, engine)
    df_pr = pd.read_sql(query_pr, engine)
    
    print(f"📂 Extracted {len(df_base)} records from `{SOURCE_SCHEMA}.{BASE_TABLE}`.")
    print(f"📂 Extracted {len(df_pr)} records from `{SOURCE_SCHEMA}.{PR_TABLE}`.")

    # ✅ Identify Common & Different Columns
    # base_columns = set(df_base.columns)
    # pr_columns = set(df_pr.columns)

    # # ✅ Ensure all `base` columns exist in `pr`
    # missing_in_pr = base_columns - pr_columns  # Columns present in `base` but missing in `pr`

    # if missing_in_pr:
    #     raise ValueError(f"❌ ERROR: The following columns from `base` are missing in `pr`: {missing_in_pr}. Process aborted!")

    # print(f"✅ All required columns from `base` are present in `pr`.")
    # print(f"🔍 Found {len(base_columns)} matching columns between `base` and `pr`.")
    common_columns = set(df_base.columns).intersection(set(df_pr.columns))
    print(f"🔍 Found {len(common_columns)} common columns between base and pr.")
    # original_columns = set(df_pr.columns)
    # mapped_columns = set(common_columns.keys()).intersection(original_columns)
    # updated_columns = {col: common_columns[col] for col in mapped_columns}

    # print(f"🔄 common_columns: {len(mapped_columns)} .")
    # for old_col, new_col in updated_columns.items():
    #     print(f"   🔹 `{old_col}` → `{new_col}`")

    # ✅ Ensure `file_source` column exists
    if "file_source" not in df_base.columns:
        df_base["file_source"] = BASE_TABLE

    if "file_source" not in df_pr.columns:
        df_pr["file_source"] = PR_TABLE

    # ✅ Ensure `booked` column exists in both tables before appending
    if "booked" not in df_base.columns:
        df_base["booked"] = None  # Or ""

    if "booked" not in df_pr.columns:
        df_pr["booked"] = None  # Or ""

    # ✅ Append `base` and `pr` Data
    df = pd.concat([df_base[list(common_columns) + ["file_source", "booked"]],
                    df_pr[list(common_columns) + ["file_source", "booked"]]], ignore_index=True)
    print(f"📌 Appended `base` and `pr` data. Total records: {len(df)}")
    
    removed_nop = df[df[CHASSIS_COLUMN] =='']  # Capture rows to be removed
    df = df[df[CHASSIS_COLUMN] != '']  # Keep only rows where `nop` is 1
    removed_nop["removal_reason"] = "chassis no is blank"

    print(f"📌 Removed {len(removed_nop)} rows where `nop` != 1.")

    # ✅ Step 2: Ensure Chassis & Engine Columns are Strings
    df[CHASSIS_COLUMN] = df[CHASSIS_COLUMN].astype(str).fillna("")
    df[ENGINE_COLUMN] = df[ENGINE_COLUMN].astype(str).fillna("")

    # ✅ Step 3: Create Lookup Tables for Chassis & Engine Numbers
    print("🔍 Creating lookup dictionaries for faster updates...")

    model_lookup = (
        df[df[REG_NO_COLUMN] != "new"]  # Only consider records where `veh_reg_no` is NOT "new"
        .groupby([REG_NO_COLUMN])[MODEL_COLUMN]
        .apply(lambda x: max(x.dropna(), key=len) if x.dropna().any() else "")  # Get longest chassis number per group
        .to_dict()
    )
    def update_model(row):
        """Update chassis number only if `veh_reg_no` is NOT 'new'."""
        if row[REG_NO_COLUMN] == "new":
            return row[MODEL_COLUMN]  # Keep as-is
        return model_lookup.get((row[REG_NO_COLUMN], row[MODEL_COLUMN]))
    df[MODEL_COLUMN] = df.apply(update_model, axis=1)
    
    chassis_lookup = (
        df[df[REG_NO_COLUMN] != "new"]
        .groupby([REG_NO_COLUMN, MODEL_COLUMN])[CHASSIS_COLUMN]
        .apply(lambda x: max(x.dropna(), key=len) if x.dropna().any() else "")
        .to_dict()
    )

    engine_lookup = (
        df[df[REG_NO_COLUMN] != "new"]
        .groupby([REG_NO_COLUMN, MODEL_COLUMN])[ENGINE_COLUMN]
        .apply(lambda x: max(x.dropna(), key=len) if x.dropna().any() else "")
        .to_dict()
    )

    # ✅ Step 4: Efficiently Update Chassis & Engine Numbers & model

    def update_chassis(row):
        """Update chassis number only if `veh_reg_no` is NOT 'new'."""
        if row[REG_NO_COLUMN] == "new":
            return row[CHASSIS_COLUMN]  # Keep as-is
        return chassis_lookup.get((row[REG_NO_COLUMN], row[MODEL_COLUMN]), row[CHASSIS_COLUMN])

    def update_engine(row):
        """Update engine number only if `veh_reg_no` is NOT 'new'."""
        if row[REG_NO_COLUMN] == "new":
            return row[ENGINE_COLUMN]  # Keep as-is
        return engine_lookup.get((row[REG_NO_COLUMN], row[MODEL_COLUMN]), row[ENGINE_COLUMN])

    df[CHASSIS_COLUMN] = df.apply(update_chassis, axis=1)
    df[ENGINE_COLUMN] = df.apply(update_engine, axis=1)

    df["cleaned_chassis_engine_no"] = df[CHASSIS_COLUMN].astype(str) + "_" + df[ENGINE_COLUMN].astype(str)

    print("✅ Chassis & Engine numbers updated successfully (excluding 'new' vehicles).")

    # ✅ Convert Month Column
    df["formatted_month"] = df[MONTH_COLUMN].apply(convert_month_format)

    df["cleaned_chassis_engine_no"] = df[CHASSIS_COLUMN].astype(str) + "_" + df[ENGINE_COLUMN].astype(str)

    # ✅ Generate Policy & Chassis Keys
    df["policy_key"] = df[POLICY_NUMBER_COLUMN].astype(str) + "_" + df[POLICY_START_COLUMN].astype(str) + "_" + df[POLICY_END_COLUMN].astype(str)
    #df["chassis_key"] = df[CHASSIS_COLUMN].astype(str) + "_" + df[ENGINE_COLUMN].astype(str) + "_" + df[POLICY_START_COLUMN].astype(str) + "_" + df[POLICY_END_COLUMN].astype(str)


    # ✅ **Step: Fuzzy Matching for Insured Names**
    prev_name = None
    prev_chassis = None
    corrected_names = []
    similarity_scores = []

    print("📝 Correcting insured names using fuzzy matching...")

    for index, row in df.iterrows():
        current_name = row[INSURED_NAME_COLUMN]
        chassis_engine_key = row["cleaned_chassis_engine_no"]

        if prev_name and prev_chassis == chassis_engine_key:
            similarity = fuzz.ratio(prev_name, current_name)
            corrected_names.append(prev_name if similarity >= 70 else current_name)
        else:
            corrected_names.append(current_name)

        similarity_scores.append(fuzz.ratio(corrected_names[-1], current_name))
        prev_name = corrected_names[-1]
        prev_chassis = chassis_engine_key

    df["corrected_name"] = corrected_names
    df["name_similarity"] = similarity_scores

    print("✅ Name correction process completed.")
    # ✅ Initialize Previous Values
    prev_chassis = None
    prev_name = None

    corrected_chassis_numbers = []
    similarity_scores = []

    print("🔍 Correcting chassis numbers using fuzzy logic...")
    df = df.sort_values(
        by=["cleaned_chassis_engine_no",CORRECT_INSURANCE_NAME, POLICY_START_COLUMN, POLICY_END_COLUMN], 
        ascending=[True, True, True, True]
    )
    # ✅ Iterate Over Rows Sequentially
    for index, row in df.iterrows():
        current_name = row["corrected_name"]
        chassis_engine_key = row["cleaned_chassis_engine_no"]

        if prev_name and prev_name == current_name:
            similarity = fuzz.ratio(prev_chassis, chassis_engine_key)

            if similarity >= 80:  # ✅ If similarity is 80% or more, replace with previous chassis number
                corrected_chassis_numbers.append(prev_chassis)
            else:
                corrected_chassis_numbers.append(chassis_engine_key)
        else:
            corrected_chassis_numbers.append(chassis_engine_key)  # ✅ First record for this name keeps its chassis

        similarity_scores.append(fuzz.ratio(corrected_chassis_numbers[-1], chassis_engine_key))

        # ✅ Update Previous Values
        prev_name = current_name
        prev_chassis = corrected_chassis_numbers[-1]

    # ✅ Add Corrected Columns to DataFrame
    df["corrected_chassis_no"] = corrected_chassis_numbers
    df["chassis_similarity"] = similarity_scores

    print("✅ Chassis number correction process completed.")

    df["chassis_key"] = df[CORRECTED_CHASSIS_ENGINE_NO].astype(str) + "_" + df[POLICY_START_COLUMN].astype(str) + "_" + df[POLICY_END_COLUMN].astype(str)

# ✅ **Step: Order Data Before Processing**
    df = df.sort_values(by=["chassis_key", POLICY_START_COLUMN], ascending=[True, True])

# ✅ Remove Duplicates by `chassis_key`, keeping latest
    before_dedup = len(df)
    duplicate_chassis = df[df.duplicated(subset=["chassis_key"], keep="first")]
    df = df.drop_duplicates(subset=["chassis_key"], keep="first")
    removed_chassis_count = before_dedup - len(df)
    print(f"📊 Removed {removed_chassis_count} duplicate chassis, keeping latest month.")


    # ✅ Remove Duplicates by `policy_key`, keeping latest
    before_dedup = len(df)
    duplicate_policies = df[df.duplicated(subset=["policy_key"], keep="first")]
    df = df.drop_duplicates(subset=["policy_key"], keep="first")
    removed_count = before_dedup - len(df)
    print(f"📊 Removed {removed_count} duplicate policies, keeping latest month.")

    # ✅ Log Removed Duplicates
    removed_duplicates = pd.concat([duplicate_policies, duplicate_chassis])
    if not removed_duplicates.empty:
        removed_duplicates.to_sql(name=LOG_TABLE, schema=TARGET_SCHEMA, con=engine, if_exists="replace", index=False)
        print(f"⚠️ Logged {len(removed_duplicates)} removed duplicates into `{log_schema}.{LOG_TABLE}`.")

 # ✅ Load Cleaned Data into Target Table
    df.to_sql(name=TARGET_TABLE, schema=TARGET_SCHEMA, con=engine, if_exists="replace", index=False)
    print(f"✅ Appended data successfully loaded into `{TARGET_SCHEMA}.{TARGET_TABLE}`.")


def identify_renewed_policies_and_calculate_tenure():
        
 
    postgres_hook = PostgresHook(postgres_conn_id="postgres")
    engine = postgres_hook.get_sqlalchemy_engine()

    query = f"""
        SELECT * FROM {TARGET_SCHEMA}.{TARGET_TABLE} 
        ORDER BY corrected_chassis_no, corrected_name, policy_start_date limit 1000
    """
    df = pd.read_sql(query, engine)

    if df.empty:
        print("⚠️ No records found!")
        return

    print(f"📂 Loaded {len(df)} records from `{TARGET_SCHEMA}.{TARGET_TABLE}`.")

    print("🔍 Extracting appended data...")
    query = f"""
        SELECT * FROM {TARGET_SCHEMA}.{TARGET_TABLE}  
        ORDER BY cleaned_chassis_engine_no, corrected_name, policy_start_date 
        
    """
    df = pd.read_sql(query, engine)

    if df.empty:
        print("⚠️ No records found!")
        return

    print(f"📂 Loaded {len(df)} records from `{TARGET_SCHEMA}.{TARGET_TABLE}`.")

    # ✅ Convert Date Columns to Datetime Format
    df[POLICY_START_COLUMN] = pd.to_datetime(df[POLICY_START_COLUMN], errors="coerce")
    df[POLICY_END_COLUMN] = pd.to_datetime(df[POLICY_END_COLUMN], errors="coerce")

    # ✅ Step 1: Generate Customer ID
    df["customer_id_Base"] = df[CORRECT_INSURANCE_NAME].astype(str) + "_" + df[BRANCH_COLUMN].astype(str)
    df["customer_id"] = (df.groupby("customer_id_Base").ngroup() + 1000001).astype(str)

    # Convert dates to datetime
    df["policy_start_date"] = pd.to_datetime(df["policy_start_date"], errors="coerce")
    df["policy_end_date"] = pd.to_datetime(df["policy_end_date"], errors="coerce")

    # Sort DataFrame
    df = df.sort_values(
        by=[CORRECTED_CHASSIS_ENGINE_NO, POLICY_START_COLUMN, POLICY_END_COLUMN,CORRECT_INSURANCE_NAME], 
        ascending=[True, True, True, True]
    )


    # ✅ Shift previous row values for comparison
    df["prev_chassis"] = df[CORRECTED_CHASSIS_ENGINE_NO].shift(1)
    df["prev_name"] = df[CORRECT_INSURANCE_NAME].shift(1)
    df["prev_start"] = df[POLICY_START_COLUMN].shift(1)
    df["prev_end"] = df[POLICY_END_COLUMN].shift(1)
    df["prev_customer_id"] = df["customer_id"].shift(1)

    # ✅ Mask where records need correction
    mask = (
        (df[CORRECTED_CHASSIS_ENGINE_NO] == df["prev_chassis"]) &  # Same chassis
        (df[POLICY_START_COLUMN] == df["prev_start"]) &  # Same start date
        (df[POLICY_END_COLUMN] != df["prev_end"])  # Different end date
    )

    # ✅ Propagate `customer_id` and `corrected_name`
    df.loc[mask, "customer_id"] = df.loc[mask, "prev_customer_id"]
    df.loc[mask, CORRECT_INSURANCE_NAME] = df.loc[mask, "prev_name"]  # Fixed name update

    # ✅ Drop temporary columns
    df.drop(columns=["prev_chassis", "prev_name", "prev_start", "prev_end", "prev_customer_id"], inplace=True)

    print("✅ Customer ID & Name successfully propagated for duplicate policies!")


    # ✅ Step 1: Sort the Data (If Not Sorted Already)
    df = df.sort_values(by=["corrected_chassis_no", "corrected_name"]).reset_index(drop=True)

    # ✅ Step 2: Forward Fill Customer ID Where `corrected_chassis_no` & `corrected_name` Match
    df["customer_id"] = df.groupby(["corrected_chassis_no", "corrected_name"])["customer_id"].transform("first")

    print("✅ Customer ID successfully propagated!")

    # ✅ Sort Data for Correct Processing
    df = df.sort_values(
        by=[CORRECTED_CHASSIS_ENGINE_NO, CORRECT_INSURANCE_NAME, POLICY_START_COLUMN, POLICY_END_COLUMN], 
        ascending=[True, True, True, True]
    )

    # ✅ Initialize Columns
    df["renewed_flag"] = 0
    df["renewed_policy_date"] = None
    df["policy_renew_days_difference"] = None
    df["old_policy_no"] = None
    df["initial_policy_no"] = None

    # ✅ Identify Renewed Policies & Mark Previous One
    for _, group in df.groupby([CORRECTED_CHASSIS_ENGINE_NO, CORRECT_INSURANCE_NAME]):
        previous_index = None
        previous_end_date = None
        initial_policy_no = None

        for index, row in group.iterrows():
            start_date = row[POLICY_START_COLUMN]
            end_date = row[POLICY_END_COLUMN]

            # ✅ Check if policy is still active (end date in the future)
            today = pd.Timestamp.today()
            if end_date > today:
                df.at[index, "renewed_flag"] = 2  # ✅ Mark as "Open Renewal"
            else:
                df.at[index, "renewed_flag"] = 0  # ✅ Default: Not Renewed

            # ✅ If there's a previous policy, check renewal condition
            if previous_end_date is not None:
                days_difference = (start_date - previous_end_date).days
                
                if 0 <= days_difference <= 5:  # ✅ Renewed within 5 days
                    df.at[previous_index, "renewed_flag"] = 1  # ✅ Mark previous policy as renewed
                    df.at[previous_index, "renewed_policy_date"] = start_date
                    df.at[previous_index, "policy_renew_days_difference"] = days_difference

                    # ✅ Assign old policy number
                    df.at[index, "old_policy_no"] = df.at[previous_index, POLICY_NUMBER_COLUMN]

                    # ✅ Assign initial policy number (first policy in renewal chain)
                    if initial_policy_no is None:
                        initial_policy_no = df.at[previous_index, POLICY_NUMBER_COLUMN]
                    df.at[index, "initial_policy_no"] = initial_policy_no
                else:
                    df.at[index, "initial_policy_no"] = row[POLICY_NUMBER_COLUMN]

            # ✅ Update previous policy details for next iteration
            previous_index = index
            previous_end_date = end_date

            # ✅ Ensure the first policy in a chain has its own initial policy number
            if initial_policy_no is None:
                df.at[index, "initial_policy_no"] = row[POLICY_NUMBER_COLUMN]

    # ✅ Remove values for policies marked as "Open" (renewed_flag = 2)
    df.loc[df["renewed_flag"] == 2, ["renewed_policy_date", "policy_renew_days_difference"]] = None

    print("✅ Policy renewal identification completed.")

    # ✅ Step 3: Calculate policy_tenure
    df["policy_tenure_month"] = ((df[POLICY_END_COLUMN].dt.year - df[POLICY_START_COLUMN].dt.year) * 12 +
                                (df[POLICY_END_COLUMN].dt.month - df[POLICY_START_COLUMN].dt.month))

    df["policy_tenure"] = (df["policy_tenure_month"] / 12).round(0)

    # ✅ Step 4: Extract start_year & end_year
    df["start_year"] = df[POLICY_START_COLUMN].dt.year
    df["end_year"] = df[POLICY_END_COLUMN].dt.year

    # ✅ Step 5: Calculate Yearly & Cumulative Tenure
    yearly_tenure = (
        df.groupby(["customer_id", "start_year"])
        .agg({POLICY_START_COLUMN: "min", POLICY_END_COLUMN: "max"})
        .reset_index()
    )

    yearly_tenure["yearly_tenure_months"] = (
        (yearly_tenure[POLICY_END_COLUMN].dt.year - yearly_tenure[POLICY_START_COLUMN].dt.year) * 12 +
        (yearly_tenure[POLICY_END_COLUMN].dt.month - yearly_tenure[POLICY_START_COLUMN].dt.month)
    )

    yearly_tenure["cumulative_tenure_months"] = (
        yearly_tenure.groupby("customer_id")["yearly_tenure_months"]
        .cumsum()
    )

    yearly_tenure["tenure_decimal"] = yearly_tenure["cumulative_tenure_months"] / 12
    yearly_tenure["customer_tenure"] = yearly_tenure["tenure_decimal"].round(0)

    df = df.drop(columns=["cumulative_tenure_months", "customer_tenure", "tenure_decimal"], errors="ignore")

    # ✅ Step 6: Merge Tenure Data Back to Main DataFrame
    tenure_mapping = yearly_tenure[["customer_id", "start_year", "cumulative_tenure_months", "tenure_decimal", "customer_tenure"]]
    df = df.merge(tenure_mapping, on=["customer_id", "start_year"], how="left")

    # ✅ Step 7: Identify New Customers
    df["firstyearpolicy"] = df.groupby("customer_id")["start_year"].transform("min")
    df["new_customer"] = df.apply(
        lambda row: f"{row['firstyearpolicy']}_{row['customer_id']}" if row["start_year"] == row["firstyearpolicy"] else "",
        axis=1
    )
    df["New Customers"] = df["new_customer"].apply(lambda x: "Yes" if x else "No")

    print("✅ customer_tenure & renewal status calculated.")


    # ✅ Load Cleaned Data into Target Table
    df.to_sql(name=FINAL_TABLE, schema=TARGET_SCHEMA, con=engine, if_exists="replace", index=False)
    print(f"✅ Appended data successfully loaded into `{TARGET_SCHEMA}.{FINAL_TABLE}`.")

# ✅ Define DAG
with DAG(
    dag_id="append_base_pr_identify_columns",
    default_args={"owner": "airflow", "start_date": datetime(2024, 2, 10)},
    schedule_interval=None,
    catchup=False
) as dag:

    append_task = PythonOperator(task_id="append_base_pr", python_callable=append_base_pr)
    renewal_task = PythonOperator(task_id="identify_renewed_policies", python_callable=identify_renewed_policies_and_calculate_tenure)

    append_task >> renewal_task
