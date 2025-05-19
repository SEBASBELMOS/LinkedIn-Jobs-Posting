from __future__ import annotations
import os, sys

DAGS_FOLDER = os.path.dirname(__file__)
PROJECT_ROOT = os.path.abspath(os.path.join(DAGS_FOLDER, os.pardir))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import pendulum
from airflow.decorators import dag

from settings import POSTGRES_CONN_ID
from task.schema import create_project_etl_schema
from task.extract import load_kaggle_data_to_raw
from task.cleaned import clean_raw_data
from task.dimensional_model import transform_to_dimensional_model

@dag(
    dag_id="etl_kaggle",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["etl", "kaggle", "postgres", "conditional_load", "transform", "clean"],
)
def etl_process_conditional_dag():
    # 1) Crear base de datos y esquemas
    init_schema     = create_project_etl_schema()

    # 2) Cargar datos RAW desde Kaggle
    load_raw_data   = load_kaggle_data_to_raw(conn_id=POSTGRES_CONN_ID)

    # 3) Limpiar datos
    clean_data_task = clean_raw_data(conn_id=POSTGRES_CONN_ID)

    # 4) Transformar a modelo dimensional
    transform_task  = transform_to_dimensional_model(conn_id=POSTGRES_CONN_ID)

    # Definición de dependencias
    init_schema >> load_raw_data >> clean_data_task >> transform_task

etl_process_conditional_dag()
