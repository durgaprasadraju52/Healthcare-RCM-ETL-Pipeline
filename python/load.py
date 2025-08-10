import pandas as pd
import logging
import os
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(filename)s] - %(message)s')

PROJECT_ID = 'healthcare-rcm-project'
DATASET_ID = 'healthcare_rcm'
KEY_FILE_PATH = r"C:\Users\durga\OneDrive\Pictures\Screenshots\healthcare-rcm-project-9b2d25a8f32a.json"
STAGING_DIR = r'C:\Users\durga\OneDrive\Desktop\HealthCare Revenue Recycle\Data\staging'

if __name__ == "__main__":
    try:
        logging.info("========================================")
        logging.info("  STARTING FINAL LOAD TO BIGQUERY")
        logging.info("========================================")
        
        logging.info("Authenticating with Google Cloud...")
        client = bigquery.Client.from_service_account_json(KEY_FILE_PATH, project=PROJECT_ID)
        
        dataset_ref = client.dataset(DATASET_ID)
        try:
            client.get_dataset(dataset_ref)
        except NotFound:
            client.create_dataset(dataset_ref)
        logging.info(f"Successfully connected and ensured dataset '{DATASET_ID}' exists.")
        
        # --- Define Schemas that EXACTLY match your Parquet files ---
        schemas = {
            'dim_patients': [
                bigquery.SchemaField("patient_sk", "INTEGER"), bigquery.SchemaField("unified_patient_id", "STRING"),
                bigquery.SchemaField("FirstName", "STRING"), bigquery.SchemaField("LastName", "STRING"),
                bigquery.SchemaField("Gender", "STRING"), bigquery.SchemaField("age", "FLOAT"),
                bigquery.SchemaField("Address", "STRING"), bigquery.SchemaField("source_hospital", "STRING"),
                bigquery.SchemaField("version", "INTEGER"), bigquery.SchemaField("effective_date", "DATE"),
                bigquery.SchemaField("expiry_date", "DATE"), bigquery.SchemaField("is_current", "BOOLEAN")
            ],
            'dim_providers': [
                bigquery.SchemaField("provider_sk", "INTEGER"), bigquery.SchemaField("ProviderID", "STRING"),
                bigquery.SchemaField("FirstName", "STRING"), bigquery.SchemaField("LastName", "STRING"),
                bigquery.SchemaField("Specialization", "STRING"), bigquery.SchemaField("DepartmentName", "STRING"),
                bigquery.SchemaField("NPI", "INTEGER"), bigquery.SchemaField("source_hospital", "STRING")
            ],
            'dim_procedures': [
                bigquery.SchemaField("procedure_sk", "INTEGER"), bigquery.SchemaField("ProcedureCode", "INTEGER"),
                bigquery.SchemaField("ProcedureDescription", "STRING")
            ],
            'dim_date': [
                bigquery.SchemaField("date_sk", "INTEGER"), bigquery.SchemaField("full_date", "DATE"),
                bigquery.SchemaField("year", "INTEGER"), bigquery.SchemaField("month", "INTEGER"),
                bigquery.SchemaField("quarter", "INTEGER"), bigquery.SchemaField("day_of_week", "STRING")
            ],
            'dim_departments': [
                bigquery.SchemaField("department_sk", "INTEGER"), bigquery.SchemaField("DeptID", "STRING"),
                bigquery.SchemaField("Name", "STRING"), bigquery.SchemaField("source_hospital", "STRING"),
            ],
            'fact_transactions': [
                bigquery.SchemaField("TransactionID", "STRING"), bigquery.SchemaField("EncounterID", "STRING"),
                bigquery.SchemaField("patient_sk", "INTEGER"), bigquery.SchemaField("provider_sk", "INTEGER"),
                bigquery.SchemaField("procedure_sk", "INTEGER"), bigquery.SchemaField("date_sk", "INTEGER"),
                bigquery.SchemaField("Amount", "FLOAT"), bigquery.SchemaField("PaidAmount", "FLOAT")
                # REMOVED: The 'ServiceDate' column, as it does not exist in the file.
            ],
            'fact_claims': [
                bigquery.SchemaField("ClaimID", "STRING"), bigquery.SchemaField("TransactionID", "STRING"),
                bigquery.SchemaField("patient_sk", "INTEGER"), bigquery.SchemaField("date_sk", "INTEGER"),
                bigquery.SchemaField("ClaimAmount", "FLOAT"), bigquery.SchemaField("PaidAmount", "FLOAT"),
                bigquery.SchemaField("ClaimStatus", "STRING"), bigquery.SchemaField("PayorType", "STRING"),
                bigquery.SchemaField("Deductible", "FLOAT"), bigquery.SchemaField("Coinsurance", "FLOAT"),
                bigquery.SchemaField("Copay", "FLOAT"), bigquery.SchemaField("days_to_payment", "FLOAT")
                # REMOVED: The 'ServiceDate' column, as it does not exist in the file.
            ]
        }
        
        files_to_load = [f for f in os.listdir(STAGING_DIR) if f.endswith('.parquet')]
        logging.info(f"Found {len(files_to_load)} Parquet files to load: {files_to_load}")

        for filename in files_to_load:
            table_name = filename.replace('.parquet', '')
            file_path = os.path.join(STAGING_DIR, filename)
            
            logging.info(f"--- Processing: {filename} -> {table_name} ---")
            
            if table_name not in schemas:
                logging.warning(f"  > SKIPPING: No schema defined for table '{table_name}'.")
                continue

            df = pd.read_parquet(file_path)

            full_table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
            job_config = bigquery.LoadJobConfig(
                schema=schemas[table_name],
                write_disposition="WRITE_TRUNCATE"
            )
            
            # --- Configure partitioning and clustering ---
            # REMOVED: Time partitioning because the date column does not exist.
            if table_name in ['fact_transactions', 'fact_claims']:
                job_config.clustering_fields = ["patient_sk"]
            elif table_name == 'dim_patients':
                job_config.clustering_fields = ["unified_patient_id", "is_current"]
            
            for schema_field in schemas[table_name]:
                if schema_field.name in df.columns and schema_field.field_type == 'DATE':
                    df[schema_field.name] = pd.to_datetime(df[schema_field.name], errors='coerce')
            
            df.replace({pd.NaT: None}, inplace=True)

            logging.info(f"  > Loading {len(df)} rows into BigQuery table '{full_table_id}'...")
            job = client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
            job.result()
            
            table_info = client.get_table(full_table_id)
            if len(df) == table_info.num_rows:
                logging.info(f"  >  SUCCESS: Validated {table_info.num_rows} rows in {table_name}.")
            else:
                 logging.error(f"  > FAILED ROW COUNT VALIDATION for {table_name}: Expected {len(df)}, Found {table_info.num_rows}")
            
        logging.info("<< FINAL LOAD TO BIGQUERY COMPLETE >>")
        print("\n\n Congratulations, buddy! The entire pipeline is complete and all data is in BigQuery! ✅")
        
    except Exception as e:
        logging.error("<< PIPELINE FAILED >>", exc_info=True)