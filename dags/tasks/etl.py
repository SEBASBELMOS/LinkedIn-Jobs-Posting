import sys
import os
import pickle
import pandas as pd
import requests
import logging
from datetime import datetime
import time
import glob
from sqlalchemy import create_engine, text

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)

from src.extract.extract_db import extracting_db_data
from src.transform.transform_dwh import transform_data
from src.load.load_dwh import load_to_dwh
from src.transform.merge import merge_data
from src.database.db_connection import create_docker_engine, dispose_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%d/%m/%Y %I:%M:%S %p"
)

def create_schemas_task(**kwargs):
    logging.info("DEBUG: create_schemas_task has been triggered.")
    try:
        logging.info("Starting create_schemas_task: Creating raw and cleaned schemas.")
        engine = create_docker_engine()
        
        with engine.connect() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
            logging.info("Created raw schema (if it didn't exist).")
            
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS cleaned"))
            logging.info("Created cleaned schema (if it didn't exist).")
            
            result = conn.execute(text("SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('raw', 'cleaned')"))
            schemas = [row[0] for row in result]
            if 'raw' not in schemas or 'cleaned' not in schemas:
                raise ValueError(f"Failed to create schemas. Found: {schemas}")
        
        logging.info("create_schemas_task completed successfully.")
    except Exception as e:
        logging.error(f"Error in create_schemas_task: {str(e)}")
        raise

def load_initial_data_task(**kwargs):
    logging.info("DEBUG: load_initial_data_task has been triggered.")
    try:
        logging.info("Starting load_initial_data_task: Loading initial data into raw schema.")
        engine = create_docker_engine()
        
        table_files = {
            'jobs': '/opt/airflow/data/postings.csv',
            'salaries': '/opt/airflow/data/jobs/salaries.csv',
            'benefits': '/opt/airflow/data/jobs/benefits.csv',
            'employee_counts': '/opt/airflow/data/companies/employee_counts.csv',
            'industries': '/opt/airflow/data/companies/company_industries.csv',
            'skills_industries': '/opt/airflow/data/mappings/skills.csv',
            'companies': '/opt/airflow/data/companies/companies.csv'
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
                "CREATE INDEX IF NOT EXISTS idx_skills_industries_skill_abr ON raw.skills_industries(skill_abr)"  # Updated to skill_abr
            ],
            'companies': [
                "CREATE INDEX IF NOT EXISTS idx_companies_company_id ON raw.companies(company_id)"
            ]
        }
        
        with engine.connect() as conn:
            with conn.begin() as transaction:
                for table, file_path in table_files.items():
                    if not os.path.exists(file_path):
                        logging.error(f"CSV file not found: {file_path}")
                        raise FileNotFoundError(f"CSV file not found: {file_path}")
                    
                    logging.info(f"Loading data from {file_path} into raw.{table}")
                    df = pd.read_csv(file_path)
                    df.to_sql(table, engine, schema='raw', if_exists='replace', index=False)
                    logging.info(f"Successfully loaded {len(df)} rows into raw.{table}")
                    
                    for index_sql in index_definitions.get(table, []):
                        logging.info(f"Creating index for raw.{table}: {index_sql}")
                        conn.execute(text(index_sql))
                        logging.info(f"Created index for raw.{table}")
        
        logging.info("load_initial_data_task completed successfully.")
    except Exception as e:
        logging.error(f"Error in load_initial_data_task: {str(e)}")
        raise

def extract_db_task(**kwargs):
    logging.info("DEBUG: extract_db_task has been triggered.")
    try:
        logging.info("Starting extract_db_task: Extracting data from the raw schema.")
        dataframes = extracting_db_data()
        temp_dir = "/opt/airflow/temp"
        os.makedirs(temp_dir, exist_ok=True)
        temp_file = os.path.join(temp_dir, f"extracted_db_data_{kwargs['run_id']}.pkl")
        with open(temp_file, 'wb') as f:
            pickle.dump(dataframes, f)
        logging.info(f"Extracted database data saved to {temp_file}")
        return temp_file
    except Exception as e:
        logging.error(f"Error in extract_db_task: {str(e)}")
        raise

# def extract_api_task(**kwargs):
#     logging.info("DEBUG: extract_api_task has been triggered.")
#     try:
#         logging.info("Starting extract_api_task: Extracting data from Glassdoor Data API.")
#         ti = kwargs['ti']
#         temp_file = ti.xcom_pull(task_ids='extract_db')
#         
#         with open(temp_file, 'rb') as f:
#             dataframes = pickle.load(f)
#         
#         if 'jobs' not in dataframes:
#             raise ValueError("Jobs DataFrame not found in extracted data.")
#         
#         jobs_df = dataframes['jobs']
#         
#         max_retries = 3
#         retry_delay = 60
#         try:
#             reviews_df = extract_api_data(jobs_df, kwargs['run_id'])
#         except requests.exceptions.HTTPError as http_err:
#             if http_err.response.status_code == 429:
#                 logging.warning("Rate limit hit (429). Retrying...")
#                 for attempt in range(max_retries):
#                     logging.info(f"Retry attempt {attempt + 1}/{max_retries} after {retry_delay} seconds.")
#                     time.sleep(retry_delay)
#                     try:
#                         reviews_df = extract_api_data(jobs_df, kwargs['run_id'])
#                         break
#                     except requests.exceptions.HTTPError as retry_err:
#                         if retry_err.response.status_code != 429 or attempt == max_retries - 1:
#                             logging.error(f"Failed to fetch API data after {max_retries} retries: {str(retry_err)}")
#                             raise
#             else:
#                 raise http_err
#         
#         if 'reviews_df' not in locals():
#             logging.warning("API extraction failed. Attempting to load from backup.")
#             backup_dir = os.path.join(project_root, "backups")
#             backup_files = glob.glob(os.path.join(backup_dir, "glassdoor_reviews_*.csv"))
#             if backup_files:
#                 latest_backup = max(backup_files, key=os.path.getctime)
#                 logging.info(f"Loading Glassdoor data from backup: {latest_backup}")
#                 reviews_df = pd.read_csv(latest_backup)
#             else:
#                 logging.error("No backup CSV found in backups/ directory.")
#                 raise ValueError("API extraction failed and no backup CSV available.")
#         
#         temp_dir = os.path.join(project_root, "temp")
#         os.makedirs(temp_dir, exist_ok=True)
#         api_extract_file = os.path.join(temp_dir, f"extracted_api_data_{kwargs['run_id']}.pkl")
#         with open(api_extract_file, 'wb') as f:
#             pickle.dump(reviews_df, f)
#         
#         logging.info(f"API extracted data saved to {api_extract_file}")
#         return {'extract_db_file': temp_file, 'extract_api_file': api_extract_file}
#     except Exception as e:
#         logging.error(f"Error in extract_api_task: {str(e)}")
#         raise

def transform_db_task_for_table(table_name, temp_file, run_id):
    logging.info(f"DEBUG: transform_db_task for {table_name} has been triggered.")
    try:
        logging.info(f"Starting transform_db_task for {table_name}: Transforming database data.")
        with open(temp_file, 'rb') as f:
            dataframes = pickle.load(f)
        
        if table_name not in dataframes:
            raise ValueError(f"Table {table_name} not found in extracted data.")
        
        df = dataframes[table_name]
        transformed_df = transform_data({table_name: df})[table_name]
        
        temp_dir = "/opt/airflow/temp"
        os.makedirs(temp_dir, exist_ok=True)
        transformed_file = os.path.join(temp_dir, f"transformed_db_{table_name}_{run_id}.pkl")
        with open(transformed_file, 'wb') as f:
            pickle.dump(transformed_df, f)
        
        logging.info(f"Transformed {table_name} data saved to {transformed_file}")
        return {table_name: transformed_file}
    except Exception as e:
        logging.error(f"Error in transform_db_task for {table_name}: {str(e)}")
        raise

# def transform_api_task(**kwargs):
#     logging.info("DEBUG: transform_api_task has been triggered.")
#     try:
#         logging.info("Starting transform_api_task: Transforming Glassdoor API data.")
#         ti = kwargs['ti']
#         files = ti.xcom_pull(task_ids='transform_db')
#         api_extract_file = files['extract_api_file']
#         
#         with open(api_extract_file, 'rb') as f:
#             reviews_df = pickle.load(f)
#         
#         transformed_reviews_df = transform_api_data(reviews_df)
#         
#         temp_dir = os.path.join(project_root, "temp")
#         os.makedirs(temp_dir, exist_ok=True)
#         transformed_api_file = os.path.join(temp_dir, f"transformed_api_data_{kwargs['run_id']}.pkl")
#         with open(transformed_api_file, 'wb') as f:
#             pickle.dump(transformed_reviews_df, f)
#         
#         logging.info(f"Transformed API data saved to {transformed_api_file}")
#         return {'transform_db_file': files['transform_db_file'], 'transform_api_file': transformed_api_file}
#     except Exception as e:
#         logging.error(f"Error in transform_api_task: {str(e)}")
#         raise

def merge_task(**kwargs):
    logging.info("DEBUG: merge_task has been triggered.")
    try:
        logging.info("Starting merge_task: Merging database data.")
        ti = kwargs['ti']
        tables = [
            'jobs',
            'salaries',
            'benefits',
            'employee_counts',
            'industries',
            'skills_industries',
            'companies'
        ]
        
        merged_dataframes = {}
        for table in tables:
            transform_file = ti.xcom_pull(task_ids=f'transform_db_{table}')
            with open(transform_file[table], 'rb') as f:
                df = pickle.load(f)
            merged_dataframes[table] = df
        
        temp_dir = "/opt/airflow/temp"
        os.makedirs(temp_dir, exist_ok=True)
        merged_file = os.path.join(temp_dir, f"merged_data_{kwargs['run_id']}.pkl")
        with open(merged_file, 'wb') as f:
            pickle.dump(merged_dataframes, f)
        
        logging.info(f"Merged data saved to {merged_file}")
        return merged_file
    except Exception as e:
        logging.error(f"Error in merge_task: {str(e)}")
        raise

def load_task(**kwargs):
    logging.info("DEBUG: load_task has been triggered.")
    try:
        logging.info("Starting load_task: Loading data into the cleaned schema.")
        ti = kwargs['ti']
        temp_file = ti.xcom_pull(task_ids='merge')
        with open(temp_file, 'rb') as f:
            dataframes = pickle.load(f)
        load_to_dwh(dataframes)
        logging.info("load_task completed successfully.")
        return temp_file
    except Exception as e:
        logging.error(f"Error in load_task: {str(e)}")
        raise

def validate_task(**kwargs):
    logging.info("DEBUG: validate_task has been triggered.")
    try:
        logging.info("Starting validate_task: Validating data distribution.")
        ti = kwargs['ti']
        temp_file = ti.xcom_pull(task_ids='load')
        with open(temp_file, 'rb') as f:
            dataframes = pickle.load(f)
        if 'jobs' in dataframes:
            jobs_df = dataframes['jobs']
            logging.info("Jobs Data Distribution:")
            # Use the correct column name 'normalized_salary'
            logging.info(jobs_df[['normalized_salary', 'original_listed_time']].describe().to_string())
            logging.info("\nDate Distribution:")
            logging.info(jobs_df['original_listed_time'].value_counts().sort_index().to_string())
            logging.info(f"\nTotal unique jobs: {jobs_df['job_id'].nunique()}")
            logging.info(f"Total rows: {len(jobs_df)}")
        logging.info("validate_task completed successfully.")
    except Exception as e:
        logging.error(f"Error in validate_task: {str(e)}")
        raise

def cleanup_task(**kwargs):
    logging.info("DEBUG: cleanup_task has been triggered.")
    try:
        logging.info("Starting cleanup_task: Removing temporary files.")
        ti = kwargs['ti']
        extract_db_file = ti.xcom_pull(task_ids='extract_db')
        tables = [
            'jobs', 'salaries', 'benefits', 'employee_counts',
            'industries', 'skills_industries', 'companies'
        ]
        transform_files = []
        for table in tables:
            transform_file = ti.xcom_pull(task_ids=f'transform_db_{table}')
            if transform_file:
                transform_files.append(transform_file[table])
        merged_file = ti.xcom_pull(task_ids='merge')
        load_file = ti.xcom_pull(task_ids='load')
        
        for temp_file in [extract_db_file, *transform_files, merged_file, load_file]:
            if temp_file and os.path.exists(temp_file):
                os.remove(temp_file)
                logging.info(f"Removed temporary file: {temp_file}")
        logging.info("cleanup_task completed successfully.")
    except Exception as e:
        logging.error(f"Error in cleanup_task: {str(e)}")
        raise

def dispose_engine_task(**kwargs):
    logging.info("DEBUG: dispose_engine_task has been triggered.")
    try:
        logging.info("Starting dispose_engine_task: Disposing of database engine.")
        dispose_engine()
        logging.info("dispose_engine_task completed successfully.")
    except Exception as e:
        logging.error(f"Error in dispose_engine_task: {str(e)}")
        raise