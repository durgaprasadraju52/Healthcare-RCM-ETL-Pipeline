#PHASE 5
import pandas as pd
from datetime import datetime, timedelta
import logging
import os

# --- Assuming these modules exist in the same directory ---
from extraction import run_extraction
from transform import run_all_transformations
from dimensional_modeling import run_modeling

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(filename)s] - %(message)s')
STAGING_DIR = r'C:\Users\durga\OneDrive\Desktop\HealthCare Revenue Recycle\Data\staging' # Using absolute path for clarity

def apply_scd_type2(new_dim_patients: pd.DataFrame, existing_dim_patients: pd.DataFrame) -> pd.DataFrame:
    """A robust function to apply SCD Type 2 logic for the patient dimension."""
    logging.info("Applying SCD Type 2 logic to the patient dimension...")
    attributes_to_track = ['Address', 'LastName']

    # --- First Run Scenario ---
    if existing_dim_patients.empty:
        logging.info("  > This is the first run. Initializing dimension with version history.")
        new_dim_patients['version'] = 1
        new_dim_patients['effective_date'] = datetime.now().date()
        new_dim_patients['expiry_date'] = pd.NaT
        new_dim_patients['is_current'] = True
        new_dim_patients.reset_index(drop=True, inplace=True)
        new_dim_patients['patient_sk'] = new_dim_patients.index
        return new_dim_patients

    # --- Subsequent Runs ---
    merged = pd.merge(
        existing_dim_patients[existing_dim_patients['is_current']],
        new_dim_patients,
        on='unified_patient_id',
        how='outer',
        suffixes=('_old', '_new'),
        indicator=True
    )

    new_cols = [c for c in merged.columns if c.endswith('_new')]
    old_cols = [c for c in merged.columns if c.endswith('_old')]

    # --- 1. Detect Records with Changes ---
    changed_mask = (merged['_merge'] == 'both')
    change_detected_mask = False
    for attr in attributes_to_track:
        change_detected_mask |= (merged[f'{attr}_old'].fillna('') != merged[f'{attr}_new'].fillna(''))
    
    changed_df = merged[changed_mask & change_detected_mask].copy()

    expired_records = pd.DataFrame()
    new_versions = pd.DataFrame()

    if not changed_df.empty:
        logging.info(f"  > Found {len(changed_df)} patients with updated attributes.")
        expired_records = changed_df[['unified_patient_id'] + old_cols].copy()
        expired_records.columns = ['unified_patient_id'] + [c.replace('_old', '') for c in old_cols]
        expired_records['is_current'] = False
        expired_records['expiry_date'] = datetime.now().date() - timedelta(days=1)
        
        new_versions = changed_df[['unified_patient_id'] + new_cols].copy()
        new_versions.columns = ['unified_patient_id'] + [c.replace('_new', '') for c in new_cols]
        new_versions['version'] = changed_df['version_old'].values + 1
        new_versions['effective_date'] = datetime.now().date()
        new_versions['expiry_date'] = pd.NaT
        new_versions['is_current'] = True

    # --- 2. Isolate Brand New Records ---
    new_records_df = merged[merged['_merge'] == 'right_only'].copy()
    new_records = pd.DataFrame()
    if not new_records_df.empty:
        logging.info(f"  > Found {len(new_records_df)} new patient records.")
        new_records = new_records_df[['unified_patient_id'] + new_cols].copy()
        new_records.columns = ['unified_patient_id'] + [c.replace('_new', '') for c in new_cols]
        new_records['version'] = 1
        new_records['effective_date'] = datetime.now().date()
        new_records['expiry_date'] = pd.NaT
        new_records['is_current'] = True

    # --- 3. Keep Unaffected Records ---
    keys_of_changed_patients = changed_df['unified_patient_id'].tolist()
    final_unchanged = existing_dim_patients[
        ~existing_dim_patients['unified_patient_id'].isin(keys_of_changed_patients)
    ].copy()

    # --- 4. Assemble and Finalize the Dimension ---
    final_dimension = pd.concat([
        final_unchanged,
        expired_records,
        new_versions,
        new_records
    ], ignore_index=True)

    final_dimension.sort_values(by=['unified_patient_id', 'version'], inplace=True)
    final_dimension.reset_index(drop=True, inplace=True)
    final_dimension['patient_sk'] = final_dimension.index

    return final_dimension

if __name__ == "__main__":
    try:
        logging.info("<<<<<<<<<< STARTING FULL DATA PROCESSING PIPELINE (Phases 2-5) >>>>>>>>>>")
        
        raw_db_data, raw_claims_data = run_extraction()
        transformed_db_data, transformed_claims_data = run_all_transformations(raw_db_data, raw_claims_data)
        final_dimensions, final_facts = run_modeling(transformed_db_data, transformed_claims_data)
        
        new_dim_patients_from_pipeline = final_dimensions['dim_patients']
        
        existing_dim_path = os.path.join(STAGING_DIR, 'dim_patients.parquet')
        if os.path.exists(existing_dim_path):
            logging.info(f"Found existing patient dimension at: {existing_dim_path}")
            existing_dim_patients = pd.read_parquet(existing_dim_path)
        else:
            logging.info("No existing patient dimension found. This will be the first run.")
            existing_dim_patients = pd.DataFrame()
            
        final_dim_patients_with_history = apply_scd_type2(new_dim_patients_from_pipeline, existing_dim_patients)
        final_dimensions['dim_patients'] = final_dim_patients_with_history
        
        logging.info(f"--- Saving all final data models to: {STAGING_DIR} ---")
        os.makedirs(STAGING_DIR, exist_ok=True)
        for name, df in final_dimensions.items():
            path = os.path.join(STAGING_DIR, f"{name}.parquet")
            df.to_parquet(path, index=False)
            logging.info(f"  > Saved {name} with {len(df)} rows.")
        for name, df in final_facts.items():
            path = os.path.join(STAGING_DIR, f"{name}.parquet")
            df.to_parquet(path, index=False)
            logging.info(f"  > Saved {name} with {len(df)} rows.")

        print("\n" + "="*80)
        print("✅  SUCCESS: DATA PROCESSING COMPLETE. ALL FINAL TABLES SAVED TO STAGING. ")
        print("="*80)
        
    except Exception as e:
        logging.error("<<<<<<<<<< PIPELINE FAILED >>>>>>>>>>", exc_info=True)