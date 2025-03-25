from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.extract.extract_db import extract_data
from src.transform.transform_dwh import transform_data
from src.load.load_dwh import load_to_dwh

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'linkedin_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for LinkedIn Job Postings',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 3, 25),
    catchup=False,
) as dag:

    # Task 1: Extract
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
    )

    # Task 2: Transform
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    # Task 3: Load
    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_to_dwh,
        op_kwargs={'dataframes_to_load': transform_task.output},
    )

    # Define task dependencies
    extract_task >> transform_task >> load_task