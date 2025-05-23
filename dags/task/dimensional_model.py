import logging
import pandas as pd
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from settings import POSTGRES_CONN_ID

log = logging.getLogger(__name__)

@task
def dimensional_model(conn_id: str = POSTGRES_CONN_ID) -> None:
    hook = PostgresHook(postgres_conn_id=conn_id)
    engine = hook.get_sqlalchemy_engine()

    SCHEMA_SRC = 'merge'
    SCHEMA_DEST = 'dimensional_model'
    TABLE_SRC = 'merge'

    df = pd.read_sql_table(TABLE_SRC, schema=SCHEMA_SRC, con=engine)


    dim_work_type = (
        df[['formatted_work_type']]
          .dropna().drop_duplicates().reset_index(drop=True)
    )
    dim_work_type['work_type_id'] = dim_work_type.index + 1

    dim_application_type = (
        df[['application_type']]
          .dropna().drop_duplicates().reset_index(drop=True)
    )
    dim_application_type['application_type_id'] = dim_application_type.index + 1

    dim_experience_level = (
        df[['formatted_experience_level']]
          .dropna().drop_duplicates().reset_index(drop=True)
    )
    dim_experience_level['experience_level_id'] = dim_experience_level.index + 1

    dim_state = (
        df[['state_only']]
          .dropna().drop_duplicates().reset_index(drop=True)
    )
    dim_state['state_id'] = dim_state.index + 1

    month_map = {
        'january':1, 'february':2, 'march':3, 'april':4,
        'may':5, 'june':6, 'july':7, 'august':8,
        'september':9, 'october':10, 'november':11, 'december':12
    }
    df_time = (
        df[['original_listed_year','original_listed_month']]
          .dropna().drop_duplicates().reset_index(drop=True)
    )
    df_time['month_num'] = df_time['original_listed_month']\
        .str.lower().map(month_map).astype(int)
    df_time['time_id'] = (
        df_time['original_listed_year'].astype(int) * 100 + df_time['month_num']
    )
    dim_time = df_time[['time_id','original_listed_year','original_listed_month']]

    dim_company = (
        df[['company_id','company_name']]
          .dropna(subset=['company_id','company_name'])
          .drop_duplicates().reset_index(drop=True)
    )
    dim_company['company_dim_id'] = dim_company.index + 1

    dim_industry = (
        df[['industry_category']]
          .dropna().drop_duplicates().reset_index(drop=True)
    )
    dim_industry['industry_dim_id'] = dim_industry.index + 1

    skills_expanded = (
        df[['job_id','skills_list']]
          .dropna(subset=['skills_list'])
          .assign(skills=lambda d: d['skills_list'].str.split(','))
          .explode('skills')
          .assign(skill=lambda d: d['skills'].str.strip())
          .drop(columns=['skills_list'])
    )
    unique_skills = skills_expanded['skill'].unique()
    dim_skill = pd.DataFrame({
        'skill_id': range(1, len(unique_skills)+1),
        'skill_name': unique_skills
    })
    fact_job_skills = (
        skills_expanded
          .merge(dim_skill, left_on='skill', right_on='skill_name', how='left')
          [['job_id','skill_id']]
    )

    fact = df.copy()
    fact = fact.merge(dim_work_type, on='formatted_work_type', how='left')
    fact = fact.merge(dim_application_type, on='application_type', how='left')
    fact = fact.merge(dim_experience_level, on='formatted_experience_level', how='left')
    fact = fact.merge(dim_state, on='state_only', how='left')
    fact = fact.merge(dim_time, on=['original_listed_year','original_listed_month'], how='left')
    fact = fact.merge(dim_company[['company_id','company_dim_id']], on='company_id', how='left')
    fact = fact.merge(dim_industry, on='industry_category', how='left')

    fact_jobs = fact[[
        'job_id',
        'work_type_id','application_type_id','experience_level_id',
        'state_id','time_id','company_dim_id','industry_dim_id',
        'views','applies','normalized_salary','len_description',
        'has_benefits','benefits_count','remote_allowed',
        'company_size','employee_count','follower_count'
    ]]

    for name, df_dim in [
        ('dim_work_type', dim_work_type),
        ('dim_application_type', dim_application_type),
        ('dim_experience_level', dim_experience_level),
        ('dim_state', dim_state),
        ('dim_time', dim_time),
        ('dim_company', dim_company),
        ('dim_industry', dim_industry),
        ('dim_skill', dim_skill),
        ('fact_job_skills', fact_job_skills),
        ('fact_jobs', fact_jobs)
    ]:
        df_dim.to_sql(name, schema=SCHEMA_DEST, con=engine, if_exists='replace', index=False)

    log.info('dimensional_model complete')
