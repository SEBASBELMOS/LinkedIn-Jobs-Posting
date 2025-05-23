import os
import logging
import json
import re
import pandas as pd
import numpy as np
from urllib.request import urlopen
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from settings import POSTGRES_CONN_ID
from datetime import datetime

log = logging.getLogger(__name__)

month_name_to_number = {
    "january": 1, "february": 2, "march": 3,     "april": 4,
    "may": 5,     "june": 6,      "july": 7,      "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12
}

def create_timestamp(row):
    try:
        month_str = str(row.original_listed_month).lower()
        year_int  = int(row.original_listed_year)
        month_int = month_name_to_number.get(month_str, None)
        if month_int is None:
            raise KeyError(f"Mes desconocido: {month_str}")
        dt = datetime(year_int, month_int, 1)
        return int(dt.timestamp())
    except Exception as e:
        log.warning(f"Fallo al parsear fecha para job_id={row.get('job_id')}: "
                    f"{row.get('original_listed_month')}, {row.get('original_listed_year')} — {e}. "
                    "Usando timestamp actual.")
        return int(datetime.now().timestamp())

@task
def load_kaggle_data_to_raw(conn_id: str = POSTGRES_CONN_ID) -> None:
    log.info("Starting task: load_raw_data to 'raw' schema")
    hook = PostgresHook(postgres_conn_id=conn_id)
    engine = hook.get_sqlalchemy_engine()

    files = {
        'postings':       '../data/postings.csv',
        'benefits':       '../data/jobs/benefits.csv',
        'job_industries': '../data/jobs/job_industries.csv',
        'job_skills':     '../data/jobs/job_skills.csv',
        'salaries':       '../data/jobs/salaries.csv',
        'companies':      '../data/companies/companies.csv',
        'employee_counts':'../data/companies/employee_counts.csv',
        'industries':     '../data/mappings/industries.csv',
        'skills':         '../data/mappings/skills.csv'
    }

    for table, path in files.items():
        if not os.path.isfile(path):
            log.error(f"CSV not found: {path}")
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


@task
def clean_raw_data(conn_id: str = POSTGRES_CONN_ID) -> None:
    log.info("Starting task: clean_raw_data")
    hook = PostgresHook(postgres_conn_id=conn_id)
    engine = hook.get_sqlalchemy_engine()

    # --- 1) Clean postings ---
    df = pd.read_sql_table('postings', schema='raw', con=engine)
    df['job_id'] = df['job_id'].astype(str)
    df['len_description'] = df['description'].str.len()
    df.drop(columns=[
        'description','job_posting_url','application_url','posting_domain',
        'compensation_type','fips','work_type','sponsored','listed_time',
        'expiry','closed_time','skills_desc','title'
    ], errors='ignore', inplace=True)
    # extract state
    url = 'https://gist.githubusercontent.com/mshafrir/2646763/raw/states_titlecase.json'
    state_list = json.load(urlopen(url))
    abbr_map = {i['abbreviation']: i['abbreviation'] for i in state_list}
    name_map = {i['name'].lower(): i['abbreviation'] for i in state_list}
    def extract_state(loc):
        if pd.isna(loc) or not isinstance(loc, str):
            return 'OTHER'
        for frag in reversed([f.strip() for f in loc.split(',')]):
            if frag.upper() in abbr_map:
                return frag.upper()
            if frag.lower() in name_map:
                return name_map[frag.lower()]
        return 'OTHER'
    df['state_only'] = df['location'].apply(extract_state)
    df.drop(columns=['location'], errors='ignore', inplace=True)
    # fillna & types
    df['applies'] = df['applies'].fillna(0).astype(int)
    df['company_name'] = df['company_name'].fillna('Unknown')
    df['formatted_experience_level'] = df['formatted_experience_level'].fillna('Unknown')
    df['remote_allowed'] = df['remote_allowed'].fillna(0).astype('Int8')
    df['company_id'] = df['company_id'].fillna(-1).astype(int)
    # dates
    ts = df['original_listed_time'].dropna()
    unit = 'ms' if ts.max() > 1e12 else 's'
    df['listed_dt'] = pd.to_datetime(ts, unit=unit, origin='unix', errors='coerce')

    df['original_listed_month'] = df['listed_dt'].dt.month_name()
    df['original_listed_year']  = df['listed_dt'].dt.year


    df.drop(columns=['original_listed_time', 'zip_code', 'listed_dt'], errors='ignore', inplace=True)
     
    # currency conversion
    rates = {'USD':1.0,'EUR':1.10,'CAD':0.75,'BBD':0.50,'AUD':0.65,'GBP':1.25}
    for col in ['min_salary','med_salary','max_salary']:
        if col in df.columns:
            df[col] = df[col] * df['currency'].map(rates).fillna(1.0)
    df.drop(columns=['currency'], errors='ignore', inplace=True)
    # annualize
    factor = {'HOURLY':2080,'MONTHLY':12,'WEEKLY':52,'BIWEEKLY':26,'YEARLY':1}
    for col in ['min_salary','med_salary','max_salary']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df['min_salary_annual'] = df['min_salary'] * df['pay_period'].map(factor)
    df['max_salary_annual'] = df['max_salary'] * df['pay_period'].map(factor)
    df['med_salary_annual'] = df['med_salary'] * df['pay_period'].map(factor)
    df['normalized_salary'] = df[
        ['min_salary_annual','med_salary_annual','max_salary_annual']
    ].mean(axis=1)
    q1, q3 = df['normalized_salary'].quantile([0.25,0.75])
    low = max(q1 - 1.5*(q3-q1), 0)
    high = q3 + 1.5*(q3-q1)
    df['normalized_salary'] = df['normalized_salary'].clip(low, high)
    df.drop(columns=[
        'min_salary','med_salary','max_salary',
        'min_salary_annual','med_salary_annual','max_salary_annual','pay_period'
    ], errors='ignore', inplace=True)

    hook.run("DROP TABLE IF EXISTS cleaned.postings;")
    df.head(0).to_sql('postings', con=engine, schema='cleaned', if_exists='replace', index=False)
    hook.insert_rows(
        table='cleaned.postings',
        rows=df.replace({np.nan: None}).values.tolist(),
        target_fields=list(df.columns),
        commit_every=5000
    )
    log.info("Cleaned postings")

    # --- 2) Clean benefits ---
    df_b = pd.read_sql_table('benefits', schema='raw', con=engine)
    df_b['job_id'] = df_b['job_id'].astype(str)
    df_b = df_b[df_b['job_id'].isin(df['job_id'])]
    df_b['type'] = df_b['type'].str.strip().str.lower()
    df_b = (df_b
            .dropna(subset=['job_id','type'])
            .drop_duplicates(subset=['job_id','type'])
            .groupby('job_id', as_index=False)
            .agg(benefits_count=('type','size'))
           )
    df_b['has_benefits'] = (df_b['benefits_count'] > 0).astype(int)
    hook.run("DROP TABLE IF EXISTS cleaned.benefits;")
    df_b.head(0).to_sql('benefits', con=engine, schema='cleaned', if_exists='replace', index=False)
    hook.insert_rows(
        table='cleaned.benefits',
        rows=df_b.replace({np.nan: None}).values.tolist(),
        target_fields=list(df_b.columns),
        commit_every=5000
    )
    log.info("Cleaned benefits")

    # --- 3) Clean job_industries ---
    df_ji = pd.read_sql_table('job_industries', schema='raw', con=engine)
    df_ji['job_id'] = df_ji['job_id'].astype(str)
    df_ji = df_ji[df_ji['job_id'].isin(df['job_id'])].drop_duplicates().reset_index(drop=True)
    df_ind = pd.read_sql_table('industries', schema='raw', con=engine)[['industry_id']]
    df_ji = df_ji[df_ji['industry_id'].isin(df_ind['industry_id'])]
    hook.run("DROP TABLE IF EXISTS cleaned.job_industries;")
    df_ji.head(0).to_sql('job_industries', con=engine, schema='cleaned', if_exists='replace', index=False)
    hook.insert_rows(
        table='cleaned.job_industries',
        rows=df_ji.replace({np.nan: None}).values.tolist(),
        target_fields=list(df_ji.columns),
        commit_every=5000
    )
    log.info("Cleaned job_industries")

    # --- 4) Clean job_skills ---
    df_js = pd.read_sql_table('job_skills', schema='raw', con=engine)
    df_js['job_id'] = df_js['job_id'].astype(str)
    df_js = df_js[df_js['job_id'].isin(df['job_id'])]
    df_js['skill_abr'] = df_js['skill_abr'].str.strip().str.upper()
    df_js = (df_js
             .dropna(subset=['job_id','skill_abr'])
             .drop_duplicates(subset=['job_id','skill_abr'])
             .reset_index(drop=True)
            )
    hook.run("DROP TABLE IF EXISTS cleaned.job_skills;")
    df_js.head(0).to_sql('job_skills', con=engine, schema='cleaned', if_exists='replace', index=False)
    hook.insert_rows(
        table='cleaned.job_skills',
        rows=df_js.replace({np.nan: None}).values.tolist(),
        target_fields=list(df_js.columns),
        commit_every=5000
    )
    log.info("Cleaned job_skills")

    # --- 5) Clean companies ---
    df_co = pd.read_sql_table('companies', schema='raw', con=engine)
    keep = ['company_id','name','company_size']
    cols = [c for c in keep if c in df_co.columns]
    df_co = df_co[cols]
    if 'name' in df_co:
        df_co['name'] = df_co['name'].fillna('Unknown').str.strip().str.title()
    if 'company_size' in df_co:
        df_co['company_size'] = df_co['company_size'].fillna(0).astype('Int8')
    if 'company_id' in df_co:
        df_co = df_co.dropna(subset=['company_id'])
        df_co['company_id'] = df_co['company_id'].astype(int)
        df_co = df_co.drop_duplicates(subset=['company_id']).reset_index(drop=True)
    hook.run("DROP TABLE IF EXISTS cleaned.companies")
    df_co.head(0).to_sql('companies', con=engine, schema='cleaned', if_exists='replace', index=False)
    hook.insert_rows(
        table='cleaned.companies',
        rows=df_co.replace({np.nan:None}).values.tolist(),
        target_fields=list(df_co.columns),
        commit_every=5000
    )
    log.info("Cleaned companies")

    # --- 6) Clean employee_counts ---
    df_ec = pd.read_sql_table('employee_counts', schema='raw', con=engine)
    if 'time_recorded' in df_ec.columns:
        df_ec = df_ec.drop(columns=['time_recorded'])
    df_ec = df_ec.drop_duplicates(subset=['company_id'], keep='first')
    hook.run("DROP TABLE IF EXISTS cleaned.employee_counts")
    df_ec.head(0).to_sql('employee_counts', con=engine, schema='cleaned', if_exists='replace', index=False)
    hook.insert_rows(
        table='cleaned.employee_counts',
        rows=df_ec.replace({np.nan:None}).values.tolist(),
        target_fields=list(df_ec.columns),
        commit_every=5000
    )
    log.info("Cleaned employee_counts")

    # --- 7) Clean industries ---
    df_in = pd.read_sql_table('industries', schema='raw', con=engine)
    df_in['industry_name'] = df_in['industry_name'].fillna('Unknown').str.strip().str.title()
    df_in = df_in.drop_duplicates(subset=['industry_id']).reset_index(drop=True)
    patterns = [
        (r'\b(manufacturing|production|fabrication)\b',               'Manufacturing'),
        (r'\b(tech|it|information|computer|software|internet|data)\b','Technology & IT'),
        (r'\b(health|medical|pharma|bio|dental|clinic|veterinary)\b', 'Healthcare & Life Sciences'),
        (r'\b(finance|bank|insurance|investment|accounting)\b',       'Finance & Insurance'),
        (r'\b(retail|e-commerce|fashion|apparel|luxury)\b',           'Retail & Consumer Goods'),
        (r'\b(education|e-learning|school|training|academic)\b',      'Education'),
        (r'\b(government|public|law|justice|military)\b',             'Government & Public Sector'),
        (r'\b(media|entertainment|arts|sports|hospitality|travel)\b', 'Media, Entertainment & Hospitality'),
        (r'\b(energy|oil|gas|mining|utilities|power|solar|wind)\b',    'Energy, Mining & Utilities'),
        (r'\b(construction|real estate|architecture|engineering)\b',  'Construction & Real Estate'),
        (r'\b(transportation|logistics|supply chain|automotive|aerospace)\b','Transportation & Logistics'),
        (r'\b(food|beverage|restaurants|catering)\b',                 'Food & Beverage Services'),
        (r'\b(non-?profit|charity|community)\b',                      'Non-Profit & Social Organizations'),
        (r'\b(agriculture|farming|forestry|horticulture)\b',           'Agriculture & Forestry'),
        (r'other',                                                     'Other'),
    ]
    def categorize(name):
        nl = name.lower()
        for pat, cat in patterns:
            if re.search(pat, nl):
                return cat
        return 'Other'
    df_in['industry_category'] = df_in['industry_name'].apply(categorize)
    df_in.drop(columns=['industry_name'], inplace=True)
    hook.run("DROP TABLE IF EXISTS cleaned.industries")
    df_in.head(0).to_sql('industries', con=engine, schema='cleaned', if_exists='replace', index=False)
    hook.insert_rows(
        table='cleaned.industries',
        rows=df_in.replace({np.nan:None}).values.tolist(),
        target_fields=list(df_in.columns),
        commit_every=5000
    )
    log.info("Cleaned industries")

    # --- 8) Clean skills_lookup ---
    df_sk = pd.read_sql_table('skills', schema='raw', con=engine)
    df_sk['skill_abr'] = df_sk['skill_abr'].str.strip().str.upper()
    df_sk['skill_name'] = df_sk['skill_name'].str.strip().str.title()
    df_sk.drop_duplicates(subset=['skill_abr'], inplace=True)
    hook.run("DROP TABLE IF EXISTS cleaned.skills_lookup")
    df_sk.head(0).to_sql('skills_lookup', con=engine, schema='cleaned', if_exists='replace', index=False)
    hook.insert_rows(
        table='cleaned.skills_lookup',
        rows=df_sk.replace({np.nan:None}).values.tolist(),
        target_fields=list(df_sk.columns),
        commit_every=5000
    )
    log.info("Cleaned skills_lookup")

    # --- 9) Clean job_skills_list ---
    df_js = pd.read_sql_table('job_skills', schema='cleaned', con=engine)
    df_map = pd.read_sql_table('skills_lookup', schema='cleaned', con=engine)
    skill_map = dict(zip(df_map['skill_abr'], df_map['skill_name']))
    df_list = (
        df_js.groupby('job_id')['skill_abr'].agg(','.join)
        .reset_index()
        .rename(columns={'skill_abr':'skills_list'})
    )
    df_list['skills_list'] = (
        df_list['skills_list']
        .str.split(',')
        .apply(lambda codes: [skill_map.get(c,c) for c in codes])
        .apply(lambda names: ','.join(names))
    )
    hook.run("DROP TABLE IF EXISTS cleaned.job_skills_list")
    df_list.head(0).to_sql('job_skills_list', con=engine, schema='cleaned', if_exists='replace', index=False)
    hook.insert_rows(
        table='cleaned.job_skills_list',
        rows=df_list.replace({np.nan:None}).values.tolist(),
        target_fields=list(df_list.columns),
        commit_every=5000
    )
    log.info("Cleaned job_skills_list")

    # --- 10) Clean job_industries_category ---
    df_ji = pd.read_sql_table('job_industries', schema='cleaned', con=engine)
    df_ind = pd.read_sql_table('industries', schema='cleaned', con=engine)
    df_cat = (
        df_ji.merge(df_ind[['industry_id','industry_category']], on='industry_id', how='left')
        .drop_duplicates(subset=['job_id'], keep='first')
        .reset_index(drop=True)
    )
    hook.run("DROP TABLE IF EXISTS cleaned.job_industries_category")
    df_cat.head(0).to_sql('job_industries_category', con=engine, schema='cleaned', if_exists='replace', index=False)
    hook.insert_rows(
        table='cleaned.job_industries_category',
        rows=df_cat.replace({np.nan:None}).values.tolist(),
        target_fields=list(df_cat.columns),
        commit_every=5000
    )
    log.info("Cleaned job_industries_category")

    log.info("clean_raw_data completed.")
