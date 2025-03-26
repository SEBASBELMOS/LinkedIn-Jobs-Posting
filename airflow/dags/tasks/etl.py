from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.extract.extract_db import extract_data
from src.transform.transform_dwh import transform_data
from src.load.load_dwh import load_to_dwh

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'linkedin_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for LinkedIn Job Postings on GCP',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 3, 25),
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_to_dwh,
        op_kwargs={'dataframes_to_load': transform_task.output},
    )

    extract_task >> transform_task >> load_task