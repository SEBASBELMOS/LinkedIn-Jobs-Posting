import os
import logging
import pandas as pd
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

CURRENT_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT_DIR_INSIDE_TASK = os.path.abspath(os.path.join(CURRENT_SCRIPT_DIR, os.pardir, os.pardir))

@task
def extract_api(conn_id: str) -> None:
    log.info(f"Starting task: extract_api. PROJECT_ROOT_DIR_INSIDE_TASK: {PROJECT_ROOT_DIR_INSIDE_TASK}")
    hook = PostgresHook(postgres_conn_id=conn_id)
    engine = hook.get_sqlalchemy_engine()

    data_api_dir = os.path.join(PROJECT_ROOT_DIR_INSIDE_TASK, 'data_api')
    path = os.path.join(data_api_dir, 'usajobs_data.csv')
    
    log.info(f"Attempting to load API CSV from: {path}")
    if not os.path.isfile(path):
        log.error(f"CSV not found: {path}")
        raise FileNotFoundError(f"CSV not found: {path}")

    df = pd.read_csv(path)
    log.info(f"Loaded {len(df)} rows from {path}")
    
    df.columns = (
        df.columns
          .str.strip()
          .str.lower()          
          .str.replace(' ', '_', regex=False)
    )

    target_schema = 'merge'
    target_table = 'api_raw'

    full_target_table = f"{target_schema}.{target_table}"

    hook.run(f"DROP TABLE IF EXISTS {full_target_table};") 
    df.head(0).to_sql(
        name=target_table,
        con=engine,
        schema=target_schema,
        if_exists='replace', 
        index=False
    )
    hook.insert_rows(
        table=full_target_table,
        rows=df.replace({pd.NA: None}).values.tolist(),
        target_fields=list(df.columns),
        commit_every=5000
    )
    log.info(f"extract_api completed. Data written to {full_target_table}")