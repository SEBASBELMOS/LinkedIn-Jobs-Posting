import logging
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from database.db_connection import creating_engine

# Configure logging for Airflow
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def transform_data():
    """
    Transform raw data from project-etl database into cleaned DataFrames.
    
    Returns:
        dict: Dictionary of table names and transformed DataFrames.
    """

    try:
        existing_engine = creating_engine()
        logger.info("Successfully created existing_engine for project-etl")
    except Exception as e:
        logger.error(f"Failed to create existing_engine: {str(e)}")
        raise


    try:
        jobs_df = pd.read_sql("SELECT * FROM public.jobs", con=existing_engine)
        salaries_df = pd.read_sql("SELECT * FROM public.salaries", con=existing_engine)
        benefits_df = pd.read_sql("SELECT * FROM public.benefits", con=existing_engine)
        employee_counts_df = pd.read_sql("SELECT * FROM public.employee_counts", con=existing_engine)
        industries_df = pd.read_sql("SELECT * FROM public.industries", con=existing_engine)
        skills_industries_df = pd.read_sql("SELECT * FROM public.skills_industries", con=existing_engine)
        companies_df = pd.read_sql("SELECT * FROM public.companies", con=existing_engine)
        logger.info("Successfully loaded raw DataFrames from project-etl")
    except Exception as e:
        logger.error(f"Error loading raw DataFrames: {str(e)}")
        raise


    logger.info("Transforming jobs_df...")

    cols_to_drop = ['med_salary', 'work_type', 'applies', 'closed_time', 'skills_desc', 'max_salary', 'min_salary', 'fips', 'listed_time', 'expiry', 'compensation_type', 'application_url', 'posting_domain']
    jobs_df.drop(columns=cols_to_drop, inplace=True, errors='ignore')


    columns_to_replace_not_specified = ["zip_code", "formatted_experience_level"]
    jobs_df["zip_code"] = jobs_df["zip_code"].astype(str)
    jobs_df[columns_to_replace_not_specified] = jobs_df[columns_to_replace_not_specified].replace(["nan", None], "No specified")
    jobs_df["original_listed_time"] = pd.to_datetime(jobs_df["original_listed_time"], unit="ms")
    jobs_df["company_id"] = jobs_df["company_id"].fillna(-1).astype(int)
    jobs_df["views"] = jobs_df["views"].fillna(0).astype(int)
    jobs_df["remote_allowed"] = jobs_df["remote_allowed"].fillna(0).astype(bool)

    columns_to_replace = ["currency", "pay_period"]
    jobs_df[columns_to_replace] = jobs_df[columns_to_replace].replace([None, pd.NA], "Unknown")


    jobs_df["job_id_modify"] = range(1, len(jobs_df) + 1)
    jobs_df["company_id_modify"] = range(1, len(jobs_df) + 1)


    q1 = jobs_df["normalized_salary"].quantile(0.25)
    q3 = jobs_df["normalized_salary"].quantile(0.75)
    iqr = q3 - q1
    upper_cap = q3 + 1.5 * iqr
    jobs_df["normalized_salary"] = jobs_df["normalized_salary"].clip(upper=upper_cap)
    logger.info(f"jobs_df transformed: {len(jobs_df)} rows")


    logger.info("Transforming salaries_df...")
    def get_unified_salary(row):
        if not pd.isna(row['med_salary']):
            return row['med_salary']
        min_sal = row['min_salary']
        max_sal = row['max_salary']
        if not pd.isna(min_sal) and not pd.isna(max_sal):
            return (min_sal + max_sal) / 2
        if not pd.isna(min_sal):
            return min_sal
        if not pd.isna(max_sal):
            return max_sal
        return np.nan

    salaries_df['raw_salary'] = salaries_df.apply(get_unified_salary, axis=1)


    salary_columns = ['max_salary', 'med_salary', 'min_salary', 'raw_salary']
    for col in salary_columns:
        q1 = salaries_df[col].quantile(0.25)
        q3 = salaries_df[col].quantile(0.75)
        iqr = q3 - q1
        upper_cap = q3 + 1.5 * iqr
        salaries_df[col] = salaries_df[col].clip(upper=upper_cap)
    logger.info(f"salaries_df transformed: {len(salaries_df)} rows")


    logger.info("Transforming benefits_df...")
    if 'inferred' in benefits_df.columns:
        benefits_df = benefits_df.drop(columns=['inferred'])
    benefits_df = benefits_df.groupby('job_id')['type'].apply(list).reset_index()
    logger.info(f"benefits_df transformed: {len(benefits_df)} rows")


    logger.info("Transforming employee_counts_df...")
    employee_counts_df["time_recorded"] = pd.to_datetime(employee_counts_df["time_recorded"], unit="s").dt.date
    logger.info(f"employee_counts_df transformed: {len(employee_counts_df)} rows")


    logger.info("Transforming industries_df...")
    industries_df["industry_name"] = industries_df["industry_name"].replace([None, pd.NA], "Unknown")
    logger.info(f"industries_df transformed: {len(industries_df)} rows")


    logger.info("Transforming skills_industries_df...")
    logger.info(f"skills_industries_df transformed: {len(skills_industries_df)} rows")


    logger.info("Transforming companies_df...")
    companies_df.fillna({
        'zip_code': 'Unknown',
        'state': 'Unknown',
        'company_size': companies_df['company_size'].median(),
        'description': 'No description',
        'address': 'No specific address',
        'city': 'Unknown'
    }, inplace=True)
    companies_df.dropna(subset=['name'], inplace=True)
    companies_df['company_size'] = companies_df['company_size'].astype(int)
    companies_df['zip_code'] = companies_df['zip_code'].replace('0', 'Unknown')
    companies_df['state'] = companies_df['state'].replace('0', 'Unknown')
    logger.info(f"companies_df transformed: {len(companies_df)} rows")


    dataframes_to_load = {
        'jobs': jobs_df,
        'salaries': salaries_df,
        'benefits': benefits_df,
        'employee_counts': employee_counts_df,
        'industries': industries_df,
        'skills_industries': skills_industries_df,
        'companies': companies_df
    }

    existing_engine.dispose()
    logger.info("Closed connection to project-etl database.")

    return dataframes_to_load

if __name__ == "__main__":
    transformed_data = transform_data()
    for table_name, df in transformed_data.items():
        print(f"{table_name}: {len(df)} rows")
        