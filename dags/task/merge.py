import logging
import pandas as pd
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from settings import POSTGRES_CONN_ID

log = logging.getLogger(__name__)

@task
def merge(conn_id: str = POSTGRES_CONN_ID) -> None:
    log.info("Starting task: merge")
    hook = PostgresHook(postgres_conn_id=conn_id)
    engine = hook.get_sqlalchemy_engine()

    df = pd.read_sql_table('postings', schema='cleaned', con=engine)
    df['original_listed_month'] = df['original_listed_month'].str.lower()

    df_benefits    = pd.read_sql_table('benefits', schema='cleaned', con=engine)
    df_ind_cat     = pd.read_sql_table('job_industries_category', schema='cleaned', con=engine)
    df_skills_list = pd.read_sql_table('job_skills_list', schema='cleaned', con=engine)
    df_companies   = pd.read_sql_table('companies', schema='cleaned', con=engine)
    df_emp_counts  = pd.read_sql_table('employee_counts', schema='cleaned', con=engine)
    df_api         = pd.read_sql_table('api', schema='merge', con=engine)

    df_merged = df.merge(df_benefits[['job_id','has_benefits','benefits_count']],
                         on='job_id', how='left').fillna({'has_benefits':0,'benefits_count':0})
    df_merged = df_merged.merge(df_ind_cat[['job_id','industry_category']],
                                on='job_id', how='left').fillna({'industry_category':'unknown'})
    df_merged = df_merged.merge(df_skills_list[['job_id','skills_list']],
                                on='job_id', how='left').fillna({'skills_list':''})
    df_merged = df_merged.merge(df_companies[['company_id','company_size']],
                                on='company_id', how='left').fillna({'company_size':0}).astype({'company_size':int})
    df_merged = df_merged.merge(df_emp_counts[['company_id','employee_count','follower_count']],
                                on='company_id', how='left')\
                         .fillna({'employee_count':0,'follower_count':0})\
                         .astype({'employee_count':int,'follower_count':int})

    df_final = pd.concat([df_merged, df_api], ignore_index=True)
    df_final = df_final.drop_duplicates(subset=['job_id'], keep='first')

    text_cols = [c for c in df_final.select_dtypes(include='object').columns if c != 'state_only']
    for col in text_cols:
        df_final[col] = df_final[col].fillna('').str.lower()

    hook.run("DROP TABLE IF EXISTS merge.merge;")
    df_final.head(0).to_sql(name='merge', schema='merge', con=engine,
                            if_exists='replace', index=False)
    hook.insert_rows(
        table='merge.merge',
        rows=df_final.replace({pd.NA: None}).values.tolist(),
        target_fields=list(df_final.columns),
        commit_every=5000
    )

    log.info("merge completed.")
