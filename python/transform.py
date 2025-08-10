# PHASE 3: DATA TRANSFORMATION (Master Orchestrator for Phase 2 & 3)
# Import Necessary Libraries ---
import pandas as pd
import numpy as np
import logging
from datetime import datetime

# Import the REAL data handoff from our extraction script
from extraction import run_extraction

#Configure Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(filename)s] - %(message)s')

#Define the Data Transformer Class ---
class DataTransformer:
    """A collection of functions for cleaning and enriching our healthcare data."""
    # (The contents of this class are perfect and need no changes)
    def clean_and_enrich_patients(self, patients_df: pd.DataFrame) -> pd.DataFrame:
        if patients_df.empty: return patients_df
        logging.info(f"Transforming {len(patients_df)} patient records...")
        for col in ['FirstName', 'LastName', 'MiddleName']:
            patients_df[col] = patients_df[col].str.title().fillna('Unknown')
        gender_map = {'M': 'Male', 'F': 'Female', 'O': 'Other'}
        patients_df['Gender'] = patients_df['Gender'].str.upper().map(gender_map).fillna('Unknown')
        patients_df['PhoneNumber'] = patients_df['PhoneNumber'].astype(str).str.replace(r'\D', '', regex=True).fillna('')
        patients_df['DOB'] = pd.to_datetime(patients_df['DOB'], errors='coerce')
        current_date = datetime.now()
        patients_df['age'] = patients_df['DOB'].apply(
            lambda dob: current_date.year - dob.year - ((current_date.month, current_date.day) < (dob.month, dob.day)) if pd.notnull(dob) else None
        )
        return patients_df

    def clean_and_enrich_claims(self, claims_df: pd.DataFrame) -> pd.DataFrame:
        if claims_df.empty: return claims_df
        logging.info(f"Transforming {len(claims_df)} claim records...")
        date_cols = ['ServiceDate', 'ClaimDate', 'InsertDate', 'ModifiedDate']
        for col in date_cols:
            if col in claims_df.columns:
                claims_df[col] = pd.to_datetime(claims_df[col], errors='coerce')
        claims_df['coverage_percentage'] = ((claims_df['PaidAmount'] / claims_df['ClaimAmount'].replace(0, np.nan)) * 100).fillna(0).round(2)
        claims_df['days_to_payment'] = (claims_df['ModifiedDate'] - claims_df['ServiceDate']).dt.days
        claims_df['claim_year'] = claims_df['ServiceDate'].dt.year
        claims_df['claim_month'] = claims_df['ServiceDate'].dt.month
        return claims_df

    def generate_surrogate_keys(self, database_data: dict) -> dict:
        logging.info("Generating surrogate keys...")
        dimensions_and_keys = {'patients': ['unified_patient_id'], 'providers': ['ProviderID', 'source_hospital'], 'departments': ['DeptID', 'source_hospital']}
        for name, natural_key in dimensions_and_keys.items():
            if name in database_data and not database_data[name].empty:
                df = database_data[name]
                unique_members = df[natural_key].drop_duplicates().reset_index(drop=True)
                surrogate_key_col = f"{name.rstrip('s')}_sk"
                unique_members[surrogate_key_col] = unique_members.index
                database_data[name] = pd.merge(df, unique_members, on=natural_key, how='left')
        return database_data

def run_all_transformations(extracted_db_data: dict, extracted_claims_data: pd.DataFrame) -> (dict, pd.DataFrame):
    """
    Main orchestrator function for the transformation phase logic.
    """
    transformer = DataTransformer()
    if 'patients' in extracted_db_data:
        extracted_db_data['patients'] = transformer.clean_and_enrich_patients(extracted_db_data['patients'])
    transformed_claims = transformer.clean_and_enrich_claims(extracted_claims_data)
    final_db_data_with_keys = transformer.generate_surrogate_keys(extracted_db_data)
    return final_db_data_with_keys, transformed_claims

# --- Step 5: Main Execution Block ---
if __name__ == "__main__":
    logging.info("  STARTING CONSOLIDATED EXTRACTION & TRANSFORMATION")
    try:
        # EXTRACTION ---
        # First, we call the imported function to get the REAL raw data.
        logging.info("--- [STEP 1] Executing Data Extraction ---")
        raw_db_data, raw_claims_data = run_extraction()
        logging.info("--- [STEP 1] Data Extraction Complete ---")
        
        # A safety check to ensure we have data before continuing.
        if raw_db_data is None or raw_claims_data is None:
            raise ValueError("Data extraction failed. Halting pipeline.")
            
        #TRANSFORMATION 
        # Second, we pass the REAL data into our transformation logic.
        logging.info("--- [STEP 2] Executing Data Transformation ---")
        transformed_db_data, transformed_claims_data = run_all_transformations(
            raw_db_data, 
            raw_claims_data
        )
        logging.info("--- [STEP 2] Data Transformation Complete ---")

        # --- FINAL OUTPUT ---
        print("\n\n" + "="*80)
        print(" SUCCESS: DATA IS TRANSFORMED AND READY FOR MODELING ")
        print("="*80)
        
        for name, df in transformed_db_data.items():
            if not df.empty:
                print(f"\n--- Transformed '{name.title()}' Table (Shape: {df.shape}) ---")
                print(df.head())
        
        if not transformed_claims_data.empty:
            print(f"\n--- Transformed 'Claims' Table (Shape: {transformed_claims_data.shape}) ---")
            print(transformed_claims_data.head())

    except Exception as e:
        logging.error("<<<<<<<<<< PIPELINE FAILED >>>>>>>>>>", exc_info=True)