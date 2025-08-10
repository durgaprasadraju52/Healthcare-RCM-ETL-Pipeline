# PHASE 2: DATA EXTRACTION (Production Version)
import pandas as pd
from sqlalchemy import create_engine, text
import logging
import os
import glob

#Configure Logging and Global Variables ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [DataExtractor] - %(message)s')

DB_CONFIG = {
    'hospital_a': {'user': 'root', 'password': 'root', 'host': '127.0.0.1', 'port': '3306', 'db': 'hospital_a_db'},
    'hospital_b': {'user': 'root', 'password': 'root', 'host': '127.0.0.1', 'port': '3306', 'db': 'hospital_b_db'}
}
CLAIMS_FOLDER = './Data/claims' 

# Define the Data Extractor Class ---
class DataExtractor:
    """A toolkit for connecting to and extracting data from various sources."""
    def __init__(self, db_configs):
        self.engines = {}
        for db_name, config in db_configs.items():
            try:
                connection_str = (f"mysql+mysqlconnector://{config['user']}:{config['password']}"f"@{config['host']}:{config['port']}/{config['db']}")
                self.engines[db_name] = create_engine(connection_str)
                logging.info(f"Database engine for '{db_name}' created successfully.")
            except Exception as e:
                logging.error(f"Failed to create database engine for '{db_name}'. Error: {e}", exc_info=True)
                self.engines[db_name] = None
    def extract_from_mysql(self, db_name, table_name):
        if self.engines.get(db_name) is None: return None
        query = f"SELECT * FROM {table_name};"
        logging.info(f"Extracting data from '{db_name}.{table_name}'...")
        try:
            with self.engines[db_name].connect() as connection:
                df = pd.read_sql(text(query), connection)
                logging.info(f"  > Success: Retrieved {len(df)} rows from '{db_name}.{table_name}'.")
                return df
        except Exception as e:
            logging.error(f"  > FAILED to extract data from '{db_name}.{table_name}'. Error: {e}")
            return None
    def extract_from_csv(self, file_path):
        logging.info(f"Reading data from CSV: '{file_path}'...")
        try:
            df = pd.read_csv(file_path)
            logging.info(f"  > Success: Retrieved {len(df)} rows from '{os.path.basename(file_path)}'.")
            return df
        except Exception as e:
            logging.error(f"  > FAILED to read CSV file '{file_path}'. Error: {e}")
            return None

#Define the Main Orchestrator Function ---
def run_extraction():
    """
    Main entry point for the extraction and integration phase.
    This function will be called by our master pipeline script.
    """
    logging.info("========================================")
    logging.info("  RUNNING DATA EXTRACTION SUB-PIPELINE")
    logging.info("========================================")
    
    extractor = DataExtractor(DB_CONFIG)
    
    # Extraction ---
    tables_to_extract = ['departments', 'encounters', 'patients', 'providers', 'transactions']
    
    logging.info("--- Extracting all tables from all databases...")
    data_hospital_a = {tbl: extractor.extract_from_mysql('hospital_a', tbl) for tbl in tables_to_extract}
    data_hospital_b = {tbl: extractor.extract_from_mysql('hospital_b', tbl) for tbl in tables_to_extract}

    logging.info("--- Extracting all claims from all CSV files...")
    claim_files = glob.glob(os.path.join(CLAIMS_FOLDER, '*.csv'))
    claims_dfs_list = [extractor.extract_from_csv(f) for f in claim_files]
    
    # Standardization & Integration ---
    logging.info("--- Standardizing and integrating all data sources...")
    
    # Standardize in 'patients' table columns.
    if 'patients' in data_hospital_b and data_hospital_b['patients'] is not None:
        data_hospital_b['patients'].rename(columns={
            'ID': 'PatientID', 'F_Name': 'FirstName', 'L_Name': 'LastName', 'M_Name': 'MiddleName'
        }, inplace=True)
        logging.info("  > Standardized column names for 'hospital_b.patients'.")

    # Integrate the database tables.
    integrated_db_data = {}
    for table_name in tables_to_extract:
        df_a, df_b = data_hospital_a.get(table_name), data_hospital_b.get(table_name)
        if df_a is not None and df_b is not None:
            df_a['source_hospital'] = 'hospital_a'
            df_b['source_hospital'] = 'hospital_b'
            integrated_db_data[table_name] = pd.concat([df_a, df_b], ignore_index=True)
    
    # Create the unified_patient_id on the now-integrated patients table.
    if 'patients' in integrated_db_data and not integrated_db_data['patients'].empty:
         integrated_db_data['patients']['unified_patient_id'] = integrated_db_data['patients']['source_hospital'].str.replace('hospital_', '').str.upper() + '-' + integrated_db_data['patients']['PatientID'].astype(str)
         logging.info("  > Created 'unified_patient_id' to uniquely identify all patients.")
    
    # Integrate the claims CSV files (schemas are identical, so this is straightforward).
    integrated_claims_df = pd.DataFrame()
    if claims_dfs_list:
        valid_claims_dfs = [df for i, df in enumerate(claims_dfs_list) if df is not None]
        for i, df in enumerate(valid_claims_dfs):
            # We determine the source from the filename as a best practice
            source = 'hospital_a' if 'hospital1' in claim_files[i].lower() else 'hospital_b'
            df['source_hospital'] = source
        integrated_claims_df = pd.concat(valid_claims_dfs, ignore_index=True)
        logging.info(f"  > Successfully integrated {len(integrated_claims_df)} claim records from {len(valid_claims_dfs)} files.")
    
    # The function returns the two key data structures for the next phase.
    return integrated_db_data, integrated_claims_df

# Isolated Test Block ---
if __name__ == "__main__":
    db_data, claims_data = run_extraction()

    print("\n\n--- EXTRACTION SCRIPT TEST RUN COMPLETE ---")
    
    if db_data:
        for name, df in db_data.items():
            print(f"\n--- Unified '{name.title()}' Table (Shape: {df.shape}) ---")
            print(df.head())
    
    if not claims_data.empty:
        print(f"\n--- Unified 'Claims' Table (Shape: {claims_data.shape}) ---")
        print(claims_data.head())