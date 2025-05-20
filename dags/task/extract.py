### extract.py
import logging
import os
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import kagglehub

from settings import POSTGRES_CONN_ID

log = logging.getLogger(__name__)

@task
def create_postgres_schemas(conn_id: str = POSTGRES_CONN_ID):
    log.info("Starting task: create_postgres_schemas")
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = None
    try:
        conn = hook.get_conn()
        with conn.cursor() as cursor:
            log.info("Creating schemas if not exist")
            cursor.execute("CREATE SCHEMA IF NOT EXISTS raw")
            cursor.execute("CREATE SCHEMA IF NOT EXISTS cleaned")
            cursor.execute("CREATE SCHEMA IF NOT EXISTS dimensional_model")

            cursor.execute(
                "SELECT schema_name FROM information_schema.schemata "
                "WHERE schema_name IN ('raw','cleaned','dimensional_model')"
            )
            schemas = [row[0] for row in cursor.fetchall()]
            log.info(f"Schemas found: {schemas}")
            if not all(s in schemas for s in ('raw','cleaned','dimensional_model')):
                raise ValueError(f"Schema creation failed. Found: {schemas}")

        conn.commit()
        log.info("create_postgres_schemas completed successfully.")
    except Exception:
        if conn:
            conn.rollback()
        log.exception("Error in create_postgres_schemas")
        raise
    finally:
        if conn:
            conn.close()

@task
def load_kaggle_data_to_raw(
    conn_id: str = POSTGRES_CONN_ID,
    kaggle_dataset: str = "arshkon/linkedin-job-postings"
):
    log.info("Starting task: load_kaggle_data_to_raw")
    hook = PostgresHook(postgres_conn_id=conn_id)
    engine = hook.get_sqlalchemy_engine()
    conn = None

    try:
        log.info(f"Downloading dataset '{kaggle_dataset}' from Kaggle Hub.")
        dataset_path = kagglehub.dataset_download(kaggle_dataset)
        log.info(f"Dataset downloaded to: {dataset_path}")

        csv_files = {
            'jobs': os.path.join(dataset_path, "postings.csv"),
            'benefits': os.path.join(dataset_path, "jobs", "benefits.csv"),
            'salaries': os.path.join(dataset_path, "jobs", "salaries.csv"),
            'employee_counts': os.path.join(dataset_path, "companies", "employee_counts.csv"),
            'industries': os.path.join(dataset_path, "companies", "company_industries.csv"),
            'companies': os.path.join(dataset_path, "companies", "companies.csv"),
            'skills_industries': os.path.join(dataset_path, "mappings", "skills.csv"),
        }

        index_definitions = {
            'jobs': [
                "CREATE INDEX IF NOT EXISTS idx_jobs_job_id ON raw.jobs(job_id)",
                "CREATE INDEX IF NOT EXISTS idx_jobs_original_listed_time ON raw.jobs(original_listed_time)",
            ],
            'salaries': ["CREATE INDEX IF NOT EXISTS idx_salaries_job_id ON raw.salaries(job_id)"],
            'benefits': ["CREATE INDEX IF NOT EXISTS idx_benefits_job_id ON raw.benefits(job_id)"],
            'employee_counts': ["CREATE INDEX IF NOT EXISTS idx_employee_counts_company_id ON raw.employee_counts(company_id)"],
            'industries': ["CREATE INDEX IF NOT EXISTS idx_industries_company_id ON raw.industries(company_id)"],
            'skills_industries': ["CREATE INDEX IF NOT EXISTS idx_skills_industries_skill_abr ON raw.skills_industries(skill_abr)"],
            'companies': ["CREATE INDEX IF NOT EXISTS idx_companies_company_id ON raw.companies(company_id)"],
        }

        # Load CSV chunks
        for table, file_path in csv_files.items():
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"CSV not found: {file_path}")
            log.info(f"Loading table raw.{table}")
            first = True
            for chunk in pd.read_csv(file_path, chunksize=10000):
                chunk.to_sql(
                    name=table,
                    con=engine,
                    schema='raw',
                    if_exists='replace' if first else 'append',
                    index=False,
                    method='multi'
                )
                first = False
        log.info("Raw data loading complete.")

        # Create indexes
        conn = hook.get_conn()
        with conn.cursor() as cursor:
            for stmts in index_definitions.values():
                for sql in stmts:
                    try:
                        cursor.execute(sql)
                    except Exception:
                        log.exception(f"Failed index SQL: {sql}")
        conn.commit()
        log.info("Index creation complete.")
    except Exception:
        if conn:
            conn.rollback()
        log.exception("Error in load_kaggle_data_to_raw")
        raise
    finally:
        if conn:
            conn.close()