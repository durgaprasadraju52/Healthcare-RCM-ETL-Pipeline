# PHASE 4: DIMENSIONAL MODELING (Master Orchestrator for Phases 2, 3, & 4)
import pandas as pd
import logging

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(filename)s] - %(message)s')

# Define the Dimensional Modeler Class
class DimensionalModeler:
    """A toolkit for creating a star schema from our transformed RCM data."""
    
    def create_dimension_tables(self, transformed_db_data: dict) -> dict:
        """Task 4.1: Creates all required dimension tables."""
        logging.info("Assembling all dimension tables...")
        dimensions = {}

        # dim_patients
        patients_df = transformed_db_data['patients']
        dim_cols = ['patient_sk', 'unified_patient_id', 'FirstName', 'LastName', 'Gender', 'age', 'Address', 'source_hospital']
        dimensions['dim_patients'] = patients_df[dim_cols].copy()

        # dim_providers
        providers_df = transformed_db_data['providers']
        depts_df = transformed_db_data['departments']
        providers_with_dept = pd.merge(providers_df, depts_df[['DeptID', 'Name', 'source_hospital']], on=['DeptID', 'source_hospital'], how='left').rename(columns={'Name': 'DepartmentName'})
        dim_cols = ['provider_sk', 'ProviderID', 'FirstName', 'LastName', 'Specialization', 'DepartmentName', 'NPI', 'source_hospital']
        dimensions['dim_providers'] = providers_with_dept[dim_cols].copy()
        
        # dim_procedures
        if 'transactions' in transformed_db_data and not transformed_db_data['transactions'].empty:
            unique_proc_codes = transformed_db_data['transactions']['ProcedureCode'].dropna().unique()
            dim_procedures = pd.DataFrame({'ProcedureCode': unique_proc_codes})
            dim_procedures['procedure_sk'] = dim_procedures.index
            dim_procedures['ProcedureDescription'] = 'Desc for Code ' + dim_procedures['ProcedureCode'].astype(str)
            dimensions['dim_procedures'] = dim_procedures

        # dim_date
        all_dates = pd.concat([pd.to_datetime(transformed_db_data['transactions']['ServiceDate']), pd.to_datetime(transformed_db_data['encounters']['EncounterDate'])]).dropna().unique()
        dim_date = pd.DataFrame({'full_date': all_dates}).sort_values('full_date').reset_index(drop=True)
        dim_date['date_sk'] = dim_date.index
        dim_date['year'] = dim_date['full_date'].dt.year
        dim_date['month'] = dim_date['full_date'].dt.month
        dim_date['quarter'] = dim_date['full_date'].dt.quarter
        dim_date['day_of_week'] = dim_date['full_date'].dt.day_name()
        dimensions['dim_date'] = dim_date
        
        logging.info("  > All dimension tables created successfully.")
        return dimensions

    def create_fact_tables(self, transformed_db_data: dict, transformed_claims_df: pd.DataFrame, dimensions: dict) -> dict:
        """Task 4.2: Creates all required fact tables."""
        logging.info("Assembling all fact tables...")
        facts = {}
        
        patients_lookup, providers_lookup, date_lookup, procedures_lookup = dimensions['dim_patients'][['unified_patient_id', 'patient_sk']], dimensions['dim_providers'][['ProviderID', 'source_hospital', 'provider_sk']], dimensions['dim_date'][['full_date', 'date_sk']], dimensions['dim_procedures'][['ProcedureCode', 'procedure_sk']]

        # fact_transactions
        trans_df = transformed_db_data['transactions'].copy()
        trans_df['unified_patient_id'] = trans_df['source_hospital'].str.replace('hospital_', '').str.upper() + '-' + trans_df['PatientID']
        merged_trans = pd.merge(trans_df, patients_lookup, on='unified_patient_id', how='left')
        merged_trans = pd.merge(merged_trans, providers_lookup, on=['ProviderID', 'source_hospital'], how='left')
        merged_trans = pd.merge(merged_trans, procedures_lookup, on='ProcedureCode', how='left')
        merged_trans['ServiceDate'] = pd.to_datetime(merged_trans['ServiceDate'])
        merged_trans = pd.merge(merged_trans, date_lookup, left_on='ServiceDate', right_on='full_date', how='left')
        fact_cols = ['TransactionID', 'EncounterID', 'patient_sk', 'provider_sk', 'procedure_sk', 'date_sk', 'Amount', 'PaidAmount']
        facts['fact_transactions'] = merged_trans[fact_cols].copy()

        # fact_claims
        claims_df = transformed_claims_df.copy()
        trans_lookup = merged_trans[['TransactionID', 'patient_sk', 'source_hospital']]
        merged_claims = pd.merge(claims_df, trans_lookup, on=['TransactionID', 'source_hospital'], how='left')
        merged_claims['ServiceDate'] = pd.to_datetime(merged_claims['ServiceDate'])
        merged_claims = pd.merge(merged_claims, date_lookup, left_on='ServiceDate', right_on='full_date', how='left')
        fact_cols = ['ClaimID', 'TransactionID', 'patient_sk', 'date_sk', 'ClaimAmount', 'PaidAmount', 'ClaimStatus', 'PayorType', 'Deductible', 'Coinsurance', 'Copay', 'days_to_payment']
        facts['fact_claims'] = merged_claims[fact_cols].copy()

        logging.info("  > All fact tables created successfully.")
        return facts

    def validate_schema(self, facts: dict, dimensions: dict):
        """Task 4.3: Performs validation checks on the star schema."""
        logging.info("Performing data validation on the new star schema...")
        fact_trans = facts.get('fact_transactions', pd.DataFrame())
        dim_patients = dimensions.get('dim_patients', pd.DataFrame())

        if not fact_trans.empty and not dim_patients.empty:
            valid_patient_keys = dim_patients['patient_sk'].unique()
            orphaned_patients = fact_trans[~fact_trans['patient_sk'].isin(valid_patient_keys)]
            if len(orphaned_patients) > 0:
                logging.error(f"  > VALIDATION FAILED: Found {len(orphaned_patients)} orphaned patient records!")
            else:
                logging.info("  >  Referential Integrity Check PASSED: No orphaned patient records found.")
            
            invalid_amounts = fact_trans[fact_trans['Amount'] <= 0]
            if len(invalid_amounts) > 0:
                logging.warning(f"  > VALIDATION WARNING: Found {len(invalid_amounts)} transactions with a zero or negative amount.")
            else:
                logging.info("  >  Business Rule Check PASSED: All transaction amounts are positive.")
        else:
            logging.warning("Skipping validation as fact or dimension tables are missing.")


def run_modeling(transformed_db_data, transformed_claims_data):
    """Main orchestrator function for the modeling phase logic."""
    modeler = DimensionalModeler()
    dimensions = modeler.create_dimension_tables(transformed_db_data)
    facts = modeler.create_fact_tables(transformed_db_data, transformed_claims_data, dimensions)
    modeler.validate_schema(facts, dimensions) # Run validation at the end
    return dimensions, facts

# --- Step 4: Main Execution Block ---
if __name__ == "__main__":
    logging.info("  STARTING CONSOLIDATED PIPELINE (PHASES 2, 3, & 4)")
    try:
        from extraction import run_extraction
        from transform import run_all_transformations

        #EXTRACTION
        logging.info("--- [STEP 1] Executing Data Extraction ---")
        raw_db_data, raw_claims_data = run_extraction()
        
        # TRANSFORMATION
        logging.info("--- [STEP 2] Executing Data Transformation ---")
        transformed_db_data, transformed_claims_data = run_all_transformations(
            raw_db_data, 
            raw_claims_data
        )
        
        # DIMENSIONAL MODELING
        logging.info("--- [STEP 3] Executing Dimensional Modeling ---")
        final_dimensions, final_facts = run_modeling(
            transformed_db_data,
            transformed_claims_data
        )
        
        # FINAL OUTPUT
        print("\n\n" + "="*80)
        print("✅  SUCCESS: STAR SCHEMA IS BUILT AND READY FOR SCD & LOADING  ✅")
        print("="*80)
        
        for name, df in final_dimensions.items():
            if not df.empty: print(f"\n--- Dimension Table: '{name}' (Shape: {df.shape}) ---\n{df.head()}")
        for name, df in final_facts.items():
            if not df.empty: print(f"\n--- Fact Table: '{name}' (Shape: {df.shape}) ---\n{df.head()}")

    except Exception as e:
        logging.error("<<<<<<<<<< PIPELINE FAILED >>>>>>>>>>", exc_info=True)