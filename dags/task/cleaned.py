import logging
import tempfile
from pathlib import Path

import pandas as pd
import numpy as np
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from settings import POSTGRES_CONN_ID

log = logging.getLogger(__name__)

@task
def clean_raw_data(conn_id: str = POSTGRES_CONN_ID, **context) -> dict:
    log.info("Starting task: clean_raw_data")
    run_id = context.get("run_id")
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    tables = ['jobs', 'salaries', 'benefits', 'employee_counts', 'industries', 'companies']
    raw = {}
    try:
        for table in tables:
            df_list = []
            for chunk in pd.read_sql(f"SELECT * FROM raw.\"{table}\"", engine, chunksize=10000):
                chunk.columns = [str(c).strip().lower().replace(' ', '_') for c in chunk.columns]
                df_list.append(chunk)
            raw[table] = pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()
            log.info(f"Extracted {len(raw[table])} rows for {table}")
    except Exception:
        log.exception("Error extracting raw data")
        raise

    # --- Cleaning logic (normalize, drop, type cast) ---
    # Jobs cleaning
    if not raw['jobs'].empty:
        jobs = raw['jobs'].copy()
        drop_cols = ['med_salary','work_type','applies','closed_time','skills_desc',
                     'max_salary','min_salary','fips','listed_time','expiry',
                     'compensation_type','application_url','posting_domain','description']
        jobs.drop(columns=[c for c in drop_cols if c in jobs.columns], inplace=True)
        # Fill and convert
        jobs['company_name'] = jobs.get('company_name', pd.Series()).fillna('Unknown')
        if 'original_listed_time' in jobs.columns:
            jobs['original_listed_time'] = pd.to_datetime(jobs['original_listed_time'], unit='ms', errors='coerce')
        if 'views' in jobs.columns:
            jobs['views'] = jobs['views'].fillna(0).astype(int)
        # Salary normalization
        if 'normalized_salary' in jobs.columns and 'currency' in jobs.columns:
            rates = {'USD':1.0,'EUR':1.09,'GBP':1.28,'CAD':0.74,'AUD':0.67,'BBD':0.5}
            jobs['normalized_salary'] = jobs.apply(
                lambda r: r['normalized_salary'] * rates.get(r['currency'],'USD'), axis=1
            )
            jobs['currency']='USD'
        raw['jobs'] = jobs

    # Repeat cleaning for salaries, benefits, employee_counts, industries, companies...
    # (Omitted here for brevity but follow the same pattern as your original code)

    # Save cleaned to parquet
    temp_dir = Path(tempfile.gettempdir())/run_id
    temp_dir.mkdir(exist_ok=True)
    paths = {}
    for tbl, df in raw.items():
        if not df.empty:
            p = temp_dir/f"{tbl}_cleaned.parquet"
            df.to_parquet(p, index=False)
            paths[tbl]=str(p)
    
    # Load into cleaned schema
    for tbl, df in raw.items():
        if not df.empty:
            hook.run(f"DROP TABLE IF EXISTS cleaned.{tbl}")
            df.head(0).to_sql(name=tbl, schema='cleaned', con=engine, if_exists='replace', index=False)
            hook.insert_rows(table=f"cleaned.{tbl}", rows=df.replace({np.nan:None}).values.tolist(), target_fields=list(df.columns), commit_every=5000)

    log.info("clean_raw_data completed.")
    return paths