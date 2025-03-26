from datetime import datetime, timedelta
from airflow.decorators import dag, task

from tasks.etl import extract_task, transform_task, load_task

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
    It extracts data from the raw schema, transforms it, and loads it into the cleaned schema.
    """

    extracted_data = extract_task()

    transformed_data = transform_task(extracted_data)

    load_task(transformed_data)

linkedin_dag = linkedin_etl_pipeline()