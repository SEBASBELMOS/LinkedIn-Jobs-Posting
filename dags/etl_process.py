from __future__ import annotations
import os
import sys

import pendulum
from airflow.decorators import dag


DAG_DIR = os.path.dirname(os.path.abspath(__file__)) 
PROJECT_ROOT_DIR = os.path.abspath(os.path.join(DAG_DIR, os.pardir))

if PROJECT_ROOT_DIR not in sys.path:
   sys.path.insert(0, PROJECT_ROOT_DIR)

from settings import POSTGRES_CONN_ID
from task.schema import create_project_etl_schema
from task.extract import load_kaggle_data_to_raw
from task.extract_api import extract_api
from task.cleaned import clean_raw_data
from task.transform_api import transform_api
from task.merge import merge
from task.dimensional_model import dimensional_model
from task.kafka_tasks import start_kafka_infrastructure_task, run_kafka_producer_task

@dag(
    dag_id="linkedin",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["linkedin", "etl", "kafka", "project_structure"],
)
def linkedin_etl():
    # --- Task Definitions ---
    init_db_schema = create_project_etl_schema()

    extract_kaggle_data = load_kaggle_data_to_raw(conn_id=POSTGRES_CONN_ID)
    extract_api_data = extract_api(conn_id=POSTGRES_CONN_ID)

    clean_kaggle_data = clean_raw_data(conn_id=POSTGRES_CONN_ID)
    transform_api_data = transform_api(conn_id=POSTGRES_CONN_ID)

    merge_sources = merge(conn_id=POSTGRES_CONN_ID)

    build_dim_model = dimensional_model(conn_id=POSTGRES_CONN_ID)
    
    start_kafka_services = start_kafka_infrastructure_task(
        task_id="start_kafka_services" 
    )
    
    publish_to_kafka = run_kafka_producer_task(
        task_id="publish_to_kafka"
    )

    # --- Workflow Dependencies ---
    init_db_schema >> [extract_kaggle_data, extract_api_data]
    
    extract_kaggle_data >> clean_kaggle_data
    extract_api_data >> transform_api_data
    
    [clean_kaggle_data, transform_api_data] >> merge_sources
    
    merge_sources >> build_dim_model
    merge_sources >> start_kafka_services
    
    start_kafka_services >> publish_to_kafka

# Instantiate the DAG
dag_instance = linkedin_etl()