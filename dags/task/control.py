import logging
from typing import List

from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

@task
def check_if_extraction_needed(conn_id: str, tables: List[str] = None) -> bool:

    log.info("Starting task: check_if_extraction_needed")

    if tables is None:
        tables = ['jobs', 'benefits', 'salaries', 'employee_counts', 
                  'industries', 'companies', 'skills_industries']

    hook = PostgresHook(postgres_conn_id=conn_id)
    with hook.get_conn() as conn, conn.cursor() as cursor:
        cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'raw'")
        if not cursor.fetchone():
            log.info("Schema 'raw' does not exist, extraction is needed")
            return True

        for table in tables:
            cursor.execute(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'raw' AND table_name = '{table}'
                )
            """)
            table_exists = cursor.fetchone()[0]
            if not table_exists:
                log.info(f"Table 'raw.{table}' does not exist, extraction is needed")
                return True

            cursor.execute(f"SELECT EXISTS (SELECT 1 FROM raw.{table} LIMIT 1)")
            has_data = cursor.fetchone()[0]
            if not has_data:
                log.info(f"Table 'raw.{table}' exists but is empty, extraction is needed")
                return True

        log.info("All required tables exist and have data, extraction not needed")
        return False

def branch_based_on_result(ti, extraction_task_id: str, skip_extraction_task_id: str) -> str:
    """
    Función para BranchPythonOperator que decide qué rama seguir.

    Args:
        ti: Task Instance
        extraction_task_id: ID de la tarea si se necesita extracción
        skip_extraction_task_id: ID de la tarea si no se necesita extracción

    Returns:
        str: ID de la tarea a ejecutar
    """
    extraction_needed = ti.xcom_pull(task_ids="check_if_extraction_needed")
    if extraction_needed is None:
        log.error("No XCom value received from 'check_if_extraction_needed'")
        raise ValueError("Failed to retrieve extraction_needed from XCom")
    log.info(f"Extraction needed: {extraction_needed}")
    return extraction_task_id if extraction_needed else skip_extraction_task_id

@task
def check_if_cleaning_needed(conn_id: str, tables: List[str] = None) -> bool:

    log.info("Starting task: check_if_cleaning_needed")
    if tables is None:
        tables = ['jobs', 'salaries', 'benefits', 'employee_counts', 'industries', 'companies']
    hook = PostgresHook(postgres_conn_id=conn_id)
    with hook.get_conn() as conn, conn.cursor() as cursor:
        cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'cleaned'")
        if not cursor.fetchone():
            log.info("Schema 'cleaned' does not exist, cleaning is needed")
            return True
        for table in tables:
            cursor.execute(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'cleaned' AND table_name = '{table}'
                )
            """)
            table_exists = cursor.fetchone()[0]
            if not table_exists:
                log.info(f"Table 'cleaned.{table}' does not exist, cleaning is needed")
                return True
            cursor.execute(f"SELECT EXISTS (SELECT 1 FROM cleaned.{table} LIMIT 1)")
            has_data = cursor.fetchone()[0]
            if not has_data:
                log.info(f"Table 'cleaned.{table}' exists but is empty, cleaning is needed")
                return True
        log.info("All required tables in 'cleaned' exist and have data, cleaning not needed")
        return False

def branch_based_on_cleaning_result(ti) -> str:
    cleaning_needed = ti.xcom_pull(task_ids="check_if_cleaning_needed")
    if cleaning_needed is None:
        log.error("No XCom value received from 'check_if_cleaning_needed'")
        raise ValueError("Failed to retrieve cleaning_needed from XCom")
    log.info(f"Cleaning needed: {cleaning_needed}")
    return "clean_raw_data" if cleaning_needed else "skip_cleaning"