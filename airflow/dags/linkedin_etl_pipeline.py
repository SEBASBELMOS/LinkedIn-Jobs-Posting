from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from tasks.etl import extract_task, transform_task, load_task, validate_task, cleanup_task

with DAG(
    'linkedin_etl_pipeline',
    start_date=datetime(2025, 3, 28),
    schedule_interval='@daily',
    catchup=False,
) as dag:
    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_task,
        provide_context=True,
    )
    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_task,
        provide_context=True,
    )
    load = PythonOperator(
        task_id='load',
        python_callable=load_task,
        provide_context=True,
    )
    validate = PythonOperator(
        task_id='validate',
        python_callable=validate_task,
        provide_context=True,
    )
    cleanup = PythonOperator(
        task_id='cleanup',
        python_callable=cleanup_task,
        provide_context=True,
    )

    extract >> transform >> load >> validate >> cleanup