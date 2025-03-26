import sys
import os
import json
import pandas as pd
import logging
from datetime import datetime, timedelta
from airflow.decorators import dag, task

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(project_root)

#from tasks.etl import *
import tasks.etl as etl

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%d/%m/%Y %I:%M:%S %p"
)

default_args = {
    'owner': 'sebasbelmos',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


@dag(
    default_args=default_args,
    description='ETL pipeline for LinkedIn Job Postings data',
    schedule_interval=timedelta(days=1),  # Run daily
    start_date=datetime(2025, 3, 26),  # Start date (today)
    catchup=False,
    max_active_runs=1,
)
def linkedin_etl_pipeline():
    """
    This DAG executes the ETL pipeline for the LinkedIn Job Postings project.
    It extracts data from the raw schema, transforms it, loads it into the cleaned schema,
    and validates the data distribution.
    """

    @task
    def extract():
        return etl.extract_task()

    @task
    def transform(df_json):
        return etl.transform_task(df_json)

    @task
    def load(df_json):
        return etl.load_task(df_json)

    @task
    def validate(df_json):
        return etl.validate_task(df_json)
    
    #extracted_data = extract_task()
    #transformed_data = transform_task(extracted_data)
    #loaded_data = load_task(transformed_data)
    #validate_task(loaded_data)
    
    extracted_data = extract()
    transformed_data = transform(extracted_data)
    loaded_data = load(transformed_data)
    validate(loaded_data)
    
linkedin_dag = linkedin_etl_pipeline()