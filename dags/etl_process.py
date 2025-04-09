from __future__ import annotations

import pendulum
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

# Importar las funciones de tarea (ajusta las rutas según tu estructura)
from task.extract import create_postgres_schemas, load_kaggle_data_to_raw
from task.cleaned import clean_raw_data
from task.dimensional_model import transform_to_dimensional_model
from task.control import (
    check_if_extraction_needed,
    branch_based_on_result,
    check_if_cleaning_needed,
    branch_based_on_cleaning_result
)

POSTGRES_CONN_ID = "project_etl"

@dag(
    dag_id="etl_kaggle_to_postgres_conditional_load",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["etl", "kaggle", "postgres", "conditional_load", "transform", "clean"]
)
def etl_process_conditional_dag():
    # 1. Crear esquemas - siempre se ejecuta
    create_schemas = create_postgres_schemas(conn_id=POSTGRES_CONN_ID)

    # 2. Verificar si se necesita extracción
    check_extraction = check_if_extraction_needed(conn_id=POSTGRES_CONN_ID)

    # 3. Bifurcación basada en la necesidad de extracción
    branch_op = BranchPythonOperator(
        task_id="branch_extraction_decision",
        python_callable=branch_based_on_result,
        op_kwargs={
            "extraction_task_id": "load_raw_data",
            "skip_extraction_task_id": "skip_raw_data_load"
        }
    )

    # 4a. Cargar datos crudos si es necesario
    load_raw_data = load_kaggle_data_to_raw(conn_id=POSTGRES_CONN_ID)

    # 4b. Saltar carga si no es necesario
    skip_raw_data_load = EmptyOperator(task_id="skip_raw_data_load")

    # 5. Punto de unión después de la bifurcación de extracción
    join_after_load = EmptyOperator(
        task_id="join_after_load_branch",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    # 6. Verificar si se necesita limpieza
    check_cleaning = check_if_cleaning_needed(conn_id=POSTGRES_CONN_ID)

    # 7. Bifurcación basada en la necesidad de limpieza
    branch_cleaning_op = BranchPythonOperator(
        task_id="branch_cleaning_decision",
        python_callable=branch_based_on_cleaning_result,
        op_kwargs={
            "cleaning_task_id": "clean_raw_data",
            "skip_cleaning_task_id": "skip_cleaning"
        }
    )

    # 8a. Limpiar datos si es necesario
    clean_data_task = clean_raw_data(conn_id=POSTGRES_CONN_ID)

    # 8b. Saltar limpieza si no es necesario
    skip_cleaning = EmptyOperator(task_id="skip_cleaning")

    # 9. Punto de unión después de la bifurcación de limpieza
    join_after_cleaning = EmptyOperator(
        task_id="join_after_cleaning_branch",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    # 10. Transformar a modelo dimensional
    transform_data_task = transform_to_dimensional_model(
        conn_id=POSTGRES_CONN_ID,
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    # Definir el flujo del DAG
    create_schemas >> check_extraction >> branch_op
    branch_op >> [load_raw_data, skip_raw_data_load] >> join_after_load
    join_after_load >> check_cleaning >> branch_cleaning_op
    branch_cleaning_op >> [clean_data_task, skip_cleaning] >> join_after_cleaning
    join_after_cleaning >> transform_data_task

# Instanciar el DAG
etl_process_conditional_dag()