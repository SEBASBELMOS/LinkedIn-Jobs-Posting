import logging
import os
from datetime import datetime

import kagglehub
import pandas as pd
from sqlalchemy import text
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

@task
def create_postgres_schemas(conn_id: str):
    log.info("Starting task: create_postgres_schemas")
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = None # Initialize conn to None
    try:
        conn = hook.get_conn()
        with conn.cursor() as cursor:
            log.info("Creating schema: raw (if not exists)")
            cursor.execute("CREATE SCHEMA IF NOT EXISTS raw")
            
            log.info("Creating schema: cleaned (if not exists)")
            cursor.execute("CREATE SCHEMA IF NOT EXISTS cleaned")
            
            log.info("Creating schema: cleaned (if not exists)")
            cursor.execute("CREATE SCHEMA IF NOT EXISTS dimensional_model")


            # Verification
            cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('raw', 'cleaned', 'dimensional_model')")
            schemas = [row[0] for row in cursor.fetchall()]
            log.info(f"Found schemas after creation attempt: {schemas}")
            if 'raw' not in schemas or 'cleaned' not in schemas:
                raise ValueError(f"Failed to verify schema creation. Found: {schemas}")

        conn.commit()
        log.info("Task create_postgres_schemas finished successfully.")
    except Exception as e:
        if conn:
            conn.rollback()
        log.error(f"Error in create_postgres_schemas: {str(e)}", exc_info=True)
        raise
    finally:
        if conn:
            conn.close()

@task
def load_kaggle_data_to_raw(conn_id: str, kaggle_dataset: str = "arshkon/linkedin-job-postings"):
    log.info("Starting task: load_kaggle_data_to_raw")
    hook = PostgresHook(postgres_conn_id=conn_id)
    # Get engine for pandas.to_sql
    engine = hook.get_sqlalchemy_engine()
    conn = None # Initialize conn to None

    try:
        log.info(f"Downloading dataset '{kaggle_dataset}' from Kaggle Hub.")
        dataset_path = kagglehub.dataset_download(kaggle_dataset)
        log.info(f"Dataset downloaded to path: {dataset_path}")
        csv_files = {
            'jobs': os.path.join(dataset_path, "postings.csv"),
            'benefits': os.path.join(dataset_path, "jobs", "benefits.csv"),
            'salaries': os.path.join(dataset_path, "jobs", "salaries.csv"),
            'employee_counts': os.path.join(dataset_path, "companies", "employee_counts.csv"),
            'industries': os.path.join(dataset_path, "companies", "company_industries.csv"),
            'companies': os.path.join(dataset_path, "companies", "companies.csv"),
            'skills_industries': os.path.join(dataset_path, "mappings", "skills.csv")
        }

        index_definitions = {
            'jobs': [
                "CREATE INDEX IF NOT EXISTS idx_jobs_job_id ON raw.jobs(job_id)",
                "CREATE INDEX IF NOT EXISTS idx_jobs_original_listed_time ON raw.jobs(original_listed_time)"
            ],
            'salaries': [
                "CREATE INDEX IF NOT EXISTS idx_salaries_job_id ON raw.salaries(job_id)"
            ],
            'benefits': [
                "CREATE INDEX IF NOT EXISTS idx_benefits_job_id ON raw.benefits(job_id)"
            ],
            'employee_counts': [
                "CREATE INDEX IF NOT EXISTS idx_employee_counts_company_id ON raw.employee_counts(company_id)"
            ],
            'industries': [
                "CREATE INDEX IF NOT EXISTS idx_industries_company_id ON raw.industries(company_id)"
            ],
            'skills_industries': [
                "CREATE INDEX IF NOT EXISTS idx_skills_industries_skill_abr ON raw.skills_industries(skill_abr)" # Ensure this column name is correct
            ],
            'companies': [
                "CREATE INDEX IF NOT EXISTS idx_companies_company_id ON raw.companies(company_id)"
            ]
        }

        for table, file_path in csv_files.items():
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"CSV file not found at expected path: {file_path}")

            log.info(f"Processing table 'raw.{table}' from file '{file_path}'")

            chunk_size = 10000
            first_chunk = True
            for i, chunk in enumerate(pd.read_csv(file_path, chunksize=chunk_size)):
                log.info(f"Loading chunk {i+1} for table 'raw.{table}' ({len(chunk)} rows)")
                chunk.to_sql(
                    name=table,
                    con=engine,
                    schema='raw',
                    if_exists='replace' if first_chunk else 'append',
                    index=False,
                    method='multi'
                )
                first_chunk = False
            log.info(f"Finished loading data for 'raw.{table}'")

        log.info("Starting index creation for raw schema tables.")
        conn = hook.get_conn() 
        with conn.cursor() as cursor:
            for table, indexes in index_definitions.items():
                if table in csv_files: 
                    log.info(f"Creating indexes for table 'raw.{table}'")
                    for index_sql in indexes:
                        try:
                            cursor.execute(index_sql)
                            log.info(f"Executed: {index_sql}")
                        except Exception as index_e:
                            log.error(f"Failed to execute index statement '{index_sql}': {index_e}", exc_info=True)
        conn.commit() 
        log.info("Finished index creation.")

        log.info("Task load_kaggle_data_to_raw finished successfully.")

    except Exception as e:
        if conn:
             conn.rollback() # Rollback if index creation failed within transaction
        log.error(f"Error in load_kaggle_data_to_raw: {str(e)}", exc_info=True)
        raise
    finally:
        if conn:
            conn.close() # Ensure connection is closed