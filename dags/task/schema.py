import logging
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from airflow.decorators import task
from settings import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    ETL_DATABASE,
)

log = logging.getLogger(__name__)

@task
def create_project_etl_schema() -> None:
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname="postgres",
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    with conn.cursor() as cur:
        log.info(f"Comprobando existencia de BD '{ETL_DATABASE}'…")
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (ETL_DATABASE,))
        if not cur.fetchone():
            cur.execute(f'CREATE DATABASE "{ETL_DATABASE}"')
            log.info(f"Base de datos '{ETL_DATABASE}' creada.")
        else:
            log.info(f"Base de datos '{ETL_DATABASE}' ya existe.")
    conn.close()

    conn2 = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname=ETL_DATABASE,
    )
    conn2.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    with conn2.cursor() as cur:
        log.info("Creando esquemas raw, cleaned y dimensional_model…")
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
        cur.execute("CREATE SCHEMA IF NOT EXISTS cleaned;")
        cur.execute("CREATE SCHEMA IF NOT EXISTS dimensional_model;")
        cur.execute("CREATE SCHEMA IF NOT EXISTS merge;")
    conn2.close()
    log.info("Tarea create_project_etl_schema completada.")
