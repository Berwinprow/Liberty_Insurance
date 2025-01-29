import os
import pandas as pd
from sqlalchemy import create_engine
import logging
from urllib.parse import quote

# PostgreSQL connection settings
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "Berwin@97"  # Your password with special characters
DB_HOST = "localhost"
DB_PORT = "5432"  # Default PostgreSQL port

# URL encode the password
encoded_password = quote(DB_PASSWORD)

# Directory containing Excel files
DIRECTORY_PATH = r"D:\liberty_automation"

# Set up logging
LOG_FILE = "excel_to_postgres.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Connect to PostgreSQL
engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# Function to process Excel files and upload them
def process_excel_to_postgres(file_path):
    try:
        # Read the Excel file into a DataFrame
        logging.info(f"Processing file: {file_path}")
        df = pd.read_excel(file_path, sheet_name=None)  # Reads all sheets
        for sheet_name, data in df.items():
            # Replace spaces in sheet names with underscores
            table_name = os.path.splitext(os.path.basename(file_path))[0] + "_" + sheet_name.replace(" ", "_")
            # Load data into PostgreSQL (automatically creates table structure)
            data.to_sql(table_name, engine, if_exists='replace', index=False)
            logging.info(f"Sheet '{sheet_name}' uploaded to table '{table_name}' successfully.")
    except Exception as e:
        logging.error(f"Error processing file {file_path}: {e}")

# Batch process Excel files in the directory
def batch_process(directory_path):
    files = [f for f in os.listdir(directory_path) if f.endswith((".xlsx", ".xlsb"))]
    total_files = len(files)
    logging.info(f"Found {total_files} files to process.")
    
    for i, file_name in enumerate(files, start=1):
        file_path = os.path.join(directory_path, file_name)
        logging.info(f"Processing file {i} of {total_files}: {file_name}")
        process_excel_to_postgres(file_path)
        logging.info(f"Finished processing file {i} of {total_files}: {file_name}")

if __name__ == "__main__":
    try:
        batch_process(DIRECTORY_PATH)
        logging.info("Batch processing completed successfully.")
    except Exception as e:
        logging.error(f"Critical error during batch processing: {e}")
