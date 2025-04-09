from __future__ import annotations

import pendulum
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from task.extract import create_postgres_schemas, load_kaggle_data_to_raw
from task.cleaned import clean_raw_data
from task.dimensional_model import transform_to_dimensional_model
from task.control import (
    check_if_extraction_needed,
    branch_based_on_result,
    check_if_cleaning_needed,
    branch_based_on_cleaning_result
)

POSTGRES_CONN_ID = "project_etl" # Connection ID for PostgreSQL

@dag(
    dag_id="etl_kaggle_to_postgres_conditional_load",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["etl", "kaggle", "postgres", "conditional_load", "transform", "clean"]
)
def etl_process_conditional_dag():
    create_schemas = create_postgres_schemas(conn_id=POSTGRES_CONN_ID)

    check_extraction = check_if_extraction_needed(conn_id=POSTGRES_CONN_ID)

    branch_op = BranchPythonOperator(
        task_id="branch_extraction_decision",
        python_callable=branch_based_on_result,
        op_kwargs={
            "extraction_task_id": "load_raw_data",
            "skip_extraction_task_id": "skip_raw_data_load"
        }
    )

    load_raw_data = load_kaggle_data_to_raw(conn_id=POSTGRES_CONN_ID)

    skip_raw_data_load = EmptyOperator(task_id="skip_raw_data_load")

    join_after_load = EmptyOperator(
        task_id="join_after_load_branch",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    check_cleaning = check_if_cleaning_needed(conn_id=POSTGRES_CONN_ID)

    branch_cleaning_op = BranchPythonOperator(
        task_id="branch_cleaning_decision",
        python_callable=branch_based_on_cleaning_result,
        op_kwargs={
            "cleaning_task_id": "clean_raw_data",
            "skip_cleaning_task_id": "skip_cleaning"
        }
    )
    clean_data_task = clean_raw_data(conn_id=POSTGRES_CONN_ID)


    skip_cleaning = EmptyOperator(task_id="skip_cleaning")


    join_after_cleaning = EmptyOperator(
        task_id="join_after_cleaning_branch",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    transform_data_task = transform_to_dimensional_model(
        conn_id=POSTGRES_CONN_ID,
        trigger_rule=TriggerRule.ONE_SUCCESS
    )
    
    #AIRFLOW
    create_schemas >> check_extraction >> branch_op
    branch_op >> [load_raw_data, skip_raw_data_load] >> join_after_load
    join_after_load >> check_cleaning >> branch_cleaning_op
    branch_cleaning_op >> [clean_data_task, skip_cleaning] >> join_after_cleaning
    join_after_cleaning >> transform_data_task

etl_process_conditional_dag()