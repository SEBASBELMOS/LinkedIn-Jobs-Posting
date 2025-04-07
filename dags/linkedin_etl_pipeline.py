from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from tasks.etl import (
                       create_schemas_task,
                       load_initial_data_task,
                       extract_db_task, 
                       #extract_api_task, 
                       transform_db_task_for_table, 
                       #transform_api_task, 
                       merge_task, 
                       load_task, 
                       validate_task, 
                       cleanup_task,
                       dispose_engine_task
                       )

with DAG(
    'linkedin_etl_pipeline',
    start_date=datetime(2025, 4, 7),
    schedule_interval='@daily',
    catchup=False,
) as dag:
    create_schemas = PythonOperator(
        task_id='create_schemas',
        python_callable=create_schemas_task,
        provide_context=True,
    )
    load_initial_data = PythonOperator(
        task_id='load_initial_data',
        python_callable=load_initial_data_task,
        provide_context=True,
    )
    extract_db = PythonOperator(
        task_id='extract_db',
        python_callable=extract_db_task,
        provide_context=True,
    )
    # extract_api = PythonOperator(
    #     task_id='extract_api',
    #     python_callable=extract_api_task,
    #     provide_context=True,
    # )
    tables = [
        'jobs',
        'salaries',
        'benefits',
        'employee_counts',
        'industries',
        'skills_industries',
        'companies'
    ]
    transform_tasks = {}
    for table in tables:
        transform_tasks[table] = PythonOperator(
            task_id=f'transform_db_{table}',
            python_callable=transform_db_task_for_table,
            op_kwargs={
                'table_name': table,
                'temp_file': '{{ ti.xcom_pull(task_ids="extract_db") }}',
                'run_id': '{{ run_id }}',
            },
            provide_context=True,
        )
    # transform_api = PythonOperator(
    #     task_id='transform_api',
    #     python_callable=transform_api_task,
    #     provide_context=True,
    # )
    merge = PythonOperator(
        task_id='merge',
        python_callable=merge_task,
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
    dispose_engine = PythonOperator(
        task_id='dispose_engine',
        python_callable=dispose_engine_task,
        provide_context=True,
    )

    create_schemas >> load_initial_data >> extract_db
    for table in tables:
        extract_db >> transform_tasks[table]
    [transform_tasks[table] for table in tables] >> merge >> load >> validate >> cleanup >> dispose_engine