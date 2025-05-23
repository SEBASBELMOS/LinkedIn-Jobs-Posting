import logging
import re
import json
import pandas as pd
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from urllib.request import urlopen
from settings import POSTGRES_CONN_ID

log = logging.getLogger(__name__)

final_columns = [
    'job_id', 'company_id', 'company_name', 'company_size',
    'employee_count', 'follower_count', 'views', 'applies',
    'formatted_work_type', 'remote_allowed', 'application_type',
    'formatted_experience_level', 'normalized_salary', 'len_description',
    'state_only', 'original_listed_month', 'original_listed_year',
    'has_benefits', 'benefits_count', 'industry_category', 'skills_list'
]
defaults = {
    **{c: 0 for c in [
        'company_id', 'company_size', 'employee_count', 'follower_count',
        'views', 'applies', 'len_description', 'has_benefits', 'benefits_count'
    ]},
    **{c: 'unknown' for c in [
        'company_name', 'formatted_work_type', 'application_type', 'formatted_experience_level'
    ]},
    'skills_list': 'unknown'
}

category_patterns = {
    r'\b(manufacturing|production|fabrication)\b':             'manufacturing',
    r'\b(tech|it|information|computer|software|internet|data)\b': 'technology & it',
    r'\b(health|medical|pharma|bio|dental|clinic|veterinary)\b':  'healthcare & life sciences',
    r'\b(finance|bank|insurance|investment|accounting)\b':       'finance & insurance',
    r'\b(retail|e-commerce|fashion|apparel|luxury)\b':           'retail & consumer goods',
    r'\b(education|e-learning|school|training|academic)\b':      'education',
    r'\b(government|public|law|justice|military)\b':             'government & public sector',
    r'\b(media|entertainment|arts|sports|hospitality|travel)\b': 'media, entertainment & hospitality',
    r'\b(energy|oil|gas|mining|utilities|power|solar|wind)\b':    'energy, mining & utilities',
    r'\b(construction|real estate|architecture|engineering)\b':  'construction & real estate',
    r'\b(transportation|logistics|supply chain|automotive|aerospace)\b':'transportation & logistics',
    r'\b(food|beverage|restaurants|catering)\b':                 'food & beverage services',
    r'\b(non-?profit|charity|community)\b':                      'non-profit & social organizations',
    r'\b(agriculture|farming|forestry|horticulture)\b':           'agriculture & forestry',
    r'other':                                                      'other'
}

def map_industry(cat: str) -> str:
    if pd.isna(cat) or cat.lower() == 'other':
        return 'other'
    for pat, label in category_patterns.items():
        if re.search(pat, cat.lower()):
            return label
    return 'other'


def extract_state(loc: str, abbr_map: dict, name_map: dict) -> str:
    if pd.isna(loc) or not isinstance(loc, str):
        return 'other'
    for frag in reversed(loc.split(',')):
        frag = frag.strip()
        if frag.upper() in abbr_map:
            return frag.upper()
        if frag.lower() in name_map:
            return name_map[frag.lower()]
    return 'other'


@task
def transform_api(conn_id: str = POSTGRES_CONN_ID) -> None:
    log.info("Starting task: transform_api")
    hook   = PostgresHook(postgres_conn_id=conn_id)
    engine = hook.get_sqlalchemy_engine()

    hook.run("CREATE SCHEMA IF NOT EXISTS merge;")
    df_api = pd.read_sql_table('api_raw', schema='merge', con=engine)

    df_api.columns = (
        df_api.columns
              .str.strip()
              .str.lower()
              .str.replace(' ', '_', regex=False)
    )

    url = 'https://gist.githubusercontent.com/mshafrir/2646763/raw/states_titlecase.json'
    state_list = json.load(urlopen(url))
    abbr_map = {i['abbreviation']: i['abbreviation'] for i in state_list}
    name_map = {i['name'].lower(): i['abbreviation'] for i in state_list}

    if 'state_only' not in df_api.columns:
        df_api['state_only'] = pd.NA
    df_api['state_only'] = (
        df_api['state_only']
        .fillna('')
        .astype(str)
        .apply(extract_state, args=(abbr_map, name_map))
    )

    df_api.rename(columns={
        'positionid':        'job_id',
        'normalisedsalary':  'normalized_salary',
        'teleworkeligible':  'remote_allowed',
        'jobcategory':       'industry_category',
        'publicationdate':   'original_listed_time'
    }, inplace=True)


    dates = pd.to_datetime(df_api.pop('original_listed_time'), errors='coerce')
    df_api['original_listed_month'] = dates.dt.month_name().str.lower().fillna('other')
    df_api['original_listed_year']  = dates.dt.year.astype('Int64')

    df_api['normalized_salary'] = (
        pd.to_numeric(df_api.get('normalized_salary', 0), errors='coerce')
          .fillna(0)
    )

    df_api['industry_category'] = (
        df_api.get('industry_category', '')
          .fillna('other')
          .apply(map_industry)
          .str.lower()
    )

    text_cols = [c for c in df_api.select_dtypes(include="object").columns
                 if c not in ("state_only",)]
    for col in text_cols:
        df_api[col] = df_api[col].fillna("").str.lower()


    df_api = df_api.reindex(columns=final_columns).fillna(defaults)
    df_api['job_id'] = df_api['job_id'].astype(str)

    hook.run("DROP TABLE IF EXISTS merge.api;")
    df_api.head(0).to_sql(
        name='api', schema='merge', con=engine,
        if_exists='replace', index=False
    )
    hook.insert_rows(
        table='merge.api',
        rows=df_api.replace({pd.NA: None}).values.tolist(),
        target_fields=list(df_api.columns),
        commit_every=5000
    )

    log.info("transform_api completed.")
