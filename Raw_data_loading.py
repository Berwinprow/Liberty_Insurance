import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import String, Integer, Float, DateTime, Boolean
import logging
from urllib.parse import quote
from config import DB_CONFIG, DIRECTORY_PATH, LOG_FILE
import psycopg2
import openpyxl

# URL encode the password
encoded_password = quote(DB_CONFIG["db_password"])
db_name = "liberty"
schema_name = "stage"  # Schema set to 'stage'

# Set up logging
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Ensure database and schema exist
def ensure_db_and_schema_exists():
    """Ensures that the database and schema exist in PostgreSQL."""
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user=DB_CONFIG["db_user"],
            password=DB_CONFIG["db_password"],
            host=DB_CONFIG["db_host"],
            port=DB_CONFIG["db_port"],
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Check if the database exists
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (db_name,))
        exists = cursor.fetchone()

        if not exists:
            cursor.execute(f"CREATE DATABASE {db_name} WITH ENCODING 'UTF8' TEMPLATE template0;")
            logging.info(f"Database '{db_name}' created.")

        cursor.close()
        conn.close()

        # Now connect to the 'liberty' database
        engine = create_engine(f"postgresql+psycopg2://{DB_CONFIG['db_user']}:{encoded_password}@{DB_CONFIG['db_host']}:{DB_CONFIG['db_port']}/{db_name}")
        
        # Ensure schema exists
        with engine.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
            logging.info(f"Ensured schema '{schema_name}' exists in database '{db_name}'.")

        return engine

    except Exception as e:
        logging.error(f"Error ensuring database and schema existence: {e}")
        raise

# Function to clean and deduplicate column names
def clean_column_names(df):
    """Cleans column names by removing special characters, spaces, and ensuring uniqueness."""
    df.columns = (
        df.columns.astype(str)  # Ensure all column names are strings
        .str.strip()
        .str.replace(" ", "_", regex=True)
        .str.replace("[()\[\]{}]", "", regex=True)
        .str.lower()
    )
    
    # Deduplicate column names
    seen = set()
    new_columns = []
    for col in df.columns:
        new_col = col
        count = 1
        while new_col in seen:
            new_col = f"{col}_{count}"
            count += 1
        seen.add(new_col)
        new_columns.append(new_col)
    df.columns = new_columns
    return df

# Function to process Excel files and upload them
def process_excel_to_postgres(file_path, engine):
    try:
        logging.info(f"Processing Excel file: {file_path}")
        df_dict = pd.read_excel(file_path, sheet_name=None, engine="openpyxl")  # Reads all sheets
        
        for sheet_name, df in df_dict.items():
            table_name = os.path.splitext(os.path.basename(file_path))[0] + "_" + sheet_name.replace(" ", "_").lower()
            df = clean_column_names(df)  # Clean column names
            
            logging.info(f"Columns detected in sheet '{sheet_name}': {list(df.columns)}")  # Debugging

            if df.empty:
                logging.warning(f"Skipping empty sheet '{sheet_name}' in file '{file_path}'.")
                continue

            dtype_mapping = {}
            for col in df.columns:
                if isinstance(df[col], pd.Series):
                    col_dtype = df[col].dtype
                else:
                    logging.warning(f"Column '{col}' is not a Series but a DataFrame. Skipping.")
                    continue
                
                if pd.api.types.is_integer_dtype(col_dtype):
                    dtype_mapping[col] = Integer
                elif pd.api.types.is_float_dtype(col_dtype):
                    dtype_mapping[col] = Float
                elif pd.api.types.is_bool_dtype(col_dtype):
                    dtype_mapping[col] = Boolean
                elif pd.api.types.is_datetime64_any_dtype(col_dtype):
                    dtype_mapping[col] = DateTime
                else:
                    dtype_mapping[col] = String

            df.to_sql(table_name, engine, schema=schema_name, if_exists="replace", index=False, dtype=dtype_mapping)
            logging.info(f"Sheet '{sheet_name}' uploaded to table '{schema_name}.{table_name}' successfully.")
    
    except Exception as e:
        logging.error(f"Error processing Excel file {file_path}: {e}")

# Batch process all Excel files in the directory
def batch_process(directory_path):
    files = [f for f in os.listdir(directory_path) if f.endswith((".xlsx", ".xlsb"))]
    total_files = len(files)
    logging.info(f"Found {total_files} files to process.")

    engine = ensure_db_and_schema_exists()  # Ensure database and schema exist before processing files
    
    for i, file_name in enumerate(files, start=1):
        file_path = os.path.join(directory_path, file_name)
        logging.info(f"Processing file {i} of {total_files}: {file_name}")
        process_excel_to_postgres(file_path, engine)
        logging.info(f"Finished processing file {i} of {total_files}: {file_name}")

if __name__ == "__main__":
    try:
        batch_process(DIRECTORY_PATH)
        logging.info("Batch processing completed successfully.")
    except Exception as e:
        logging.error(f"Critical error during batch processing: {e}")
