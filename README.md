Healthcare Revenue Cycle Management (RCM) Data Engineering Challenge

Project Overview:

This project simulates a real-world data engineering challenge within a healthcare network. The primary goal is to design and implement an end-to-end Extract, Transform, Load (ETL) pipeline that consolidates disparate healthcare data sources, ensures data quality, tracks historical changes, and prepares a unified dataset for actionable analytics in Google BigQuery.

Business Challenge:

A regional healthcare network operates two hospitals, each with its own separate data systems. This fragmentation leads to:

Data Silos: Inability to perform cross-hospital analysis.

Manual Processes: Delays in extracting insights, especially for claims.

Historical Tracking Deficiencies: No system to track patient information changes over time.

Revenue Leakage: Difficulty identifying patterns in claim denials and payment delays.

Compliance Risks: Challenges in maintaining audit trails.

This project addresses these challenges by building a robust data pipeline.

Technical Stack:

Languages: Python, SQL

Databases: MySQL (local)

Cloud Platform: Google Cloud Platform (GCP)

Cloud Data Warehouse: Google BigQuery

Python Libraries: pandas, sqlalchemy, mysql-connector-python, google-cloud-bigquery, pyarrow, pandas-gbq

Project Phases & Data Engineering Concepts

This project is structured into several interconnected phases, each applying core data engineering principles:

Phase 1: Environment Setup

Concept: Setting up development and execution environments.

Description: Installation of MySQL, creation of databases (hospital_a_db, hospital_b_db), configuration of Google Cloud Project (BigQuery API, Service Account, JSON key), and setup of a Python virtual environment with necessary libraries.

Phase 1.5: Data Exploration

Concept: Source system analysis, schema discovery, data profiling.

Script: data_exploration.py

Description: A dedicated script to connect to raw data sources (MySQL tables and CSV files), identify table names, column names, data types, and critical schema differences between sources. This phase informs the design of subsequent ETL steps.

Phase 2: Data Extraction

Concept: Data integration, error handling.

Script: extraction.py

Description: Extracts data from two distinct MySQL databases (5 tables each) and two separate claims CSV files. Crucially, it performs initial schema standardization (e.g., renaming ID to PatientID in Hospital B's patients table, and unifying claims CSV structures) and adds source_hospital identifiers.

Phase 3: Data Transformation

Concept: Data cleansing, data enrichment, business logic implementation, common data model (CDM), surrogate keys.

Script: transform.py

Description: Takes the raw, integrated DataFrames. Cleans and standardizes patient information, calculates new metrics (e.g., patient age, claim coverage_percentage, days_to_payment), and generates numeric surrogate keys for dimensional entities (patients, providers, departments) to prepare for dimensional modeling.

Phase 4: Dimensional Modeling

Concept: Star schema design, fact table construction, dimension table creation.

Script: dimensional_modeling.py

Description: Assembles the transformed data into a star schema. Creates dedicated dimension tables (dim_patients, dim_providers, dim_departments, dim_procedures, dim_date) and fact tables (fact_transactions, fact_claims) optimized for analytical queries. It ensures foreign key relationships through surrogate keys.

Phase 5: Slowly Changing Dimension (SCD) Type 2 Implementation

Concept: Historical data tracking, version control, audit trails.

Script: scd_implementation.py

Description: Implements SCD Type 2 logic for the dim_patients table. It compares the latest patient data with the existing dimension (persisted in a staging file), identifies changes in tracked attributes (e.g., Address, LastName), expires old records, and inserts new versioned records to maintain a complete history of patient information.

Phase 6: BigQuery Integration

Concept: Cloud data warehousing, partitioning, clustering, data loading, data validation.

Script: bigquery_loader.py

Description: The final loading phase. It reads all the finalized star schema tables (.parquet files) from the local staging directory and loads them into corresponding tables in Google BigQuery. Tables are created with appropriate schemas, partitioning (for fact tables), and clustering for optimal performance. Includes post-load row count validation.

Project Structure

Healthcare Revenue Recycle/
├── Data/
│   ├── claims/
│   │   ├── hospital1_claim_data.csv    (Input)
│   │   └── hospital2_claim_data.csv    (Input)
│   └── staging/
│       ├── dim_patients.parquet        (Output of SCD, Input for Loader)
│       ├── dim_providers.parquet       (Output of Dimensional Modeling, Input for Loader)
│       ├── dim_departments.parquet     (Output of Dimensional Modeling, Input for Loader)
│       ├── dim_procedures.parquet      (Output of Dimensional Modeling, Input for Loader)
│       ├── dim_date.parquet            (Output of Dimensional Modeling, Input for Loader)
│       ├── fact_transactions.parquet   (Output of Dimensional Modeling, Input for Loader)
│       └── fact_claims.parquet         (Output of Dimensional Modeling, Input for Loader)
├── sql/
│   ├── hospital1_db         (MySQL setup script for Hospital A)
│   └── hospital2_db        (MySQL setup script for Hospital B)
├── python/
│   └── extraction/
│       ├── extraction.py               (Phase 2 logic)
│       ├── transform.py                (Phase 3 logic)
│       ├── dimensional_modeling.py     (Phase 4 logic)
│       ├── scd_implementation.py       (Phase 5 logic - The master processing script)
│       └── loader.py                   (Phase 6 logic - The master loading script)
│    
└── README.md                           (This file)

The pipeline is designed to be run in two main steps:

Step 1: Run the Data Processing (Extraction, Transformation, Modeling, SCD)

This script will extract, clean, transform, model, and apply SCD logic. It will save all the final tables as .parquet files in your Data/staging/ directory.

Open your Administrator Terminal (with (venv) activated).

Execute the scd_implementation.py script:

code
Bash
download
content_copy
expand_less
IGNORE_WHEN_COPYING_START
IGNORE_WHEN_COPYING_END
python "python/extraction/scd_implementation.py"

Wait for it to complete. You should see a success message: ✅ SUCCESS: DATA PROCESSING COMPLETE. ALL FINAL TABLES SAVED TO STAGING. ✅

Step 2: Run the BigQuery Loader

This script will read the .parquet files from the Data/staging/ directory and upload them to your BigQuery dataset.

Use the SAME Administrator Terminal (with (venv) activated).

Execute the bigquery_loader.py script:

code
Bash
download
content_copy
expand_less
IGNORE_WHEN_COPYING_START
IGNORE_WHEN_COPYING_END
python "python/extraction/bigquery_loader.py"

Wait for it to complete. You should see a final success message: ✅✅✅ PROJECT COMPLETE! ALL TABLES LOADED TO BIGQUERY! ✅✅✅

Validation

After the pipeline completes successfully, you can verify the data in your BigQuery project:

Go to the Google Cloud Console and navigate to BigQuery.

Expand your project and the healthcare_rcm dataset. You should see all the dimension (dim_...) and fact (fact_...) tables populated.

You can run SQL queries to inspect the data, check row counts, and observe the SCD Type 2 history in dim_patients.

Future Enhancements / Next Steps

RCM Analytics: Write SQL queries in BigQuery to calculate KPIs, analyze revenue trends, claims performance, and operational efficiency (as outlined in Phase 7 of the project PDF).

Dashboarding: Connect BigQuery to a BI tool (e.g., Looker Studio, Tableau, Power BI) to create interactive dashboards based on the star schema.

Orphaned Records: Investigate and fix the 36 orphaned patient records identified during validation (this suggests PatientID in transactions without a matching PatientID in the patients table, or a unified_patient_id mismatch).

Incremental Loading: Implement logic in bigquery_loader.py to load only new or changed data instead of truncating and loading the entire table each time.

Error Reporting & Monitoring: Set up automated alerts for pipeline failures.

