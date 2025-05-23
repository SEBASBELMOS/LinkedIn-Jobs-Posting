import os
import logging
import pandas as pd

from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

CURRENT_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

PROJECT_ROOT_DIR_INSIDE_TASK = os.path.abspath(os.path.join(CURRENT_SCRIPT_DIR, os.pardir, os.pardir))


@task
def load_kaggle_data_to_raw(conn_id: str) -> None:
    log.info(f"Starting task: load_raw_data to 'raw' schema. PROJECT_ROOT_DIR_INSIDE_TASK: {PROJECT_ROOT_DIR_INSIDE_TASK}")
    hook = PostgresHook(postgres_conn_id=conn_id)
    engine = hook.get_sqlalchemy_engine()

    data_dir = os.path.join(PROJECT_ROOT_DIR_INSIDE_TASK, 'data')

    files = {
        'postings':       os.path.join(data_dir, 'postings.csv'),
        'benefits':       os.path.join(data_dir, 'jobs', 'benefits.csv'),
        'job_industries': os.path.join(data_dir, 'jobs', 'job_industries.csv'),
        'job_skills':     os.path.join(data_dir, 'jobs', 'job_skills.csv'),
        'salaries':       os.path.join(data_dir, 'jobs', 'salaries.csv'),
        'companies':      os.path.join(data_dir, 'companies', 'companies.csv'),
        'employee_counts':os.path.join(data_dir, 'companies', 'employee_counts.csv'),
        'industries':     os.path.join(data_dir, 'mappings', 'industries.csv'),
        'skills':         os.path.join(data_dir, 'mappings', 'skills.csv')
    }

    for table, path in files.items():
        log.info(f"Attempting to load CSV from: {path}")
        if not os.path.isfile(path):
            log.error(f"CSV not found: {path}. Current CWD (likely not relevant for absolute paths): {os.getcwd()}")
            raise FileNotFoundError(f"CSV not found: {path}")

        log.info(f"Loading raw.{table} from {path}")
        df = pd.read_csv(path)
        df.to_sql(
            name=table,
            con=engine,
            schema='raw',
            if_exists='replace',
            index=False
        )
        log.info(f"Finished loading raw.{table}")

    log.info("All CSVs loaded into raw schema successfully.")