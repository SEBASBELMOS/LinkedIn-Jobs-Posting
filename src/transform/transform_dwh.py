import sys
import os

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(project_root)

import pandas as pd
import numpy as np
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%d/%m/%Y %I:%M:%S %p"
)

def transform_data(dataframes):
    """
    Transform the extracted DataFrames by applying cleaning and transformations.

    Args:
        dataframes (dict): Dictionary of DataFrames extracted from the raw schema.

    Returns:
        dict: Dictionary of transformed DataFrames.
    """
    logging.info("Starting data transformation.")
    
    transformed_dataframes = dataframes.copy()
    
    # --- Transform jobs_df ---
    if 'jobs' in transformed_dataframes:
        jobs_df = transformed_dataframes['jobs']
        logging.info("Transforming jobs_df...")
        
        cols_to_drop = ['med_salary', 'work_type', 'applies', 'closed_time', 'skills_desc', 
                       'max_salary', 'min_salary', 'fips', 'listed_time', 'expiry', 
                       'compensation_type', 'application_url', 'posting_domain']
        jobs_df.drop(columns=cols_to_drop, inplace=True, errors='ignore')
        
        columns_to_replace_not_specified = ["zip_code", "formatted_experience_level"]
        jobs_df["zip_code"] = jobs_df["zip_code"].astype(str)
        jobs_df[columns_to_replace_not_specified] = jobs_df[columns_to_replace_not_specified].replace(["nan", None], "Not specified")
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
        
        transformed_dataframes['jobs'] = jobs_df
        logging.info(f"jobs_df transformed: {len(jobs_df)} rows")
    
    # --- Transform salaries_df ---
    if 'salaries' in transformed_dataframes:
        salaries_df = transformed_dataframes['salaries']
        logging.info("Transforming salaries_df...")
        
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
        
        transformed_dataframes['salaries'] = salaries_df
        logging.info(f"salaries_df transformed: {len(salaries_df)} rows")
    
    # --- Transform benefits_df ---
    if 'benefits' in transformed_dataframes:
        benefits_df = transformed_dataframes['benefits']
        logging.info("Transforming benefits_df...")
        
        if 'inferred' in benefits_df.columns:
            benefits_df = benefits_df.drop(columns=['inferred'])
        benefits_df = benefits_df.groupby('job_id')['type'].apply(list).reset_index()
        
        transformed_dataframes['benefits'] = benefits_df
        logging.info(f"benefits_df transformed: {len(benefits_df)} rows")
    
    # --- Transform employee_counts_df ---
    if 'employee_counts' in transformed_dataframes:
        employee_counts_df = transformed_dataframes['employee_counts']
        logging.info("Transforming employee_counts_df...")
        
        employee_counts_df["time_recorded"] = pd.to_datetime(employee_counts_df["time_recorded"], unit="s").dt.date
        
        transformed_dataframes['employee_counts'] = employee_counts_df
        logging.info(f"employee_counts_df transformed: {len(employee_counts_df)} rows")
    
    # --- Transform industries_df ---
    if 'industries' in transformed_dataframes:
        industries_df = transformed_dataframes['industries']
        logging.info("Transforming industries_df...")
        
        industries_df["industry_name"] = industries_df["industry_name"].replace([None, pd.NA], "Unknown")
        
        transformed_dataframes['industries'] = industries_df
        logging.info(f"industries_df transformed: {len(industries_df)} rows")
    
    # --- Transform skills_industries_df ---
    if 'skills_industries' in transformed_dataframes:
        skills_industries_df = transformed_dataframes['skills_industries']
        logging.info("Transforming skills_industries_df...")
        
        transformed_dataframes['skills_industries'] = skills_industries_df
        logging.info(f"skills_industries_df transformed: {len(skills_industries_df)} rows")
    
    # --- Transform companies_df ---
    if 'companies' in transformed_dataframes:
        companies_df = transformed_dataframes['companies']
        logging.info("Transforming companies_df...")
        
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
        
        transformed_dataframes['companies'] = companies_df
        logging.info(f"companies_df transformed: {len(companies_df)} rows")
    
    logging.info("Data transformation completed successfully.")
    return transformed_dataframes


if __name__ == "__main__":
    sample_df = pd.DataFrame({
        'job_id': [1, 2],
        'normalized_salary': [50000, 60000],
        'original_listed_time': [1640995200000, 1643673600000],  # Timestamps in milliseconds
        'zip_code': ['12345', '67890'],
        'formatted_experience_level': ['Entry', 'Mid'],
        'company_id': [101, 102],
        'views': [10, 20],
        'remote_allowed': [True, False],
        'currency': ['USD', 'GBP'],
        'pay_period': ['YEARLY', 'MONTHLY']
    })
    dataframes = {'jobs': sample_df}
    transformed_data = transform_data(dataframes)
    for table_name, df in transformed_data.items():
        print(f"{table_name}: {len(df)} rows")