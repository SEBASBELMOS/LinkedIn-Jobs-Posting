import logging
import tempfile
from pathlib import Path
import pandas as pd
import numpy as np
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

@task
def clean_raw_data(conn_id: str, **context) -> dict:
    """
    Task to extract data from raw schema, clean it, and store it in cleaned schema.
    
    Args:
        conn_id: Postgres connection ID
        context: Airflow context variables
        
    Returns:
        Dictionary with paths to stored cleaned data files
    """
    log.info("Starting task: clean_raw_data")
    dag_run_id = context["run_id"]

    hook = PostgresHook(postgres_conn_id=conn_id)
    engine = hook.get_sqlalchemy_engine()

    # Tables to extract from raw schema
    tables_to_extract = [
        'jobs', 'salaries', 'benefits', 'employee_counts',
        'industries', 'companies'
    ]

    raw = {}
    chunk_size = 10000
    
    try:
        # Extract data from raw schema
        for table in tables_to_extract:
            log.info(f"Extracting table: raw.{table}")
            chunks = []
            query = f"SELECT * FROM raw.\"{table}\""
            for chunk in pd.read_sql(query, engine, chunksize=chunk_size):
                # Clean column names
                chunk.columns = [str(col).strip().replace('"', '').lower().replace(' ', '_') for col in chunk.columns]
                chunks.append(chunk)
            if chunks:
                raw[table] = pd.concat(chunks, ignore_index=True)
                log.info(f"Successfully extracted {len(raw[table])} rows for table: {table}")
                log.debug(f"Columns for {table}: {raw[table].columns.tolist()}")
            else:
                log.warning(f"No data extracted for table: {table}")
                raw[table] = pd.DataFrame()
    except Exception as e:
        log.error(f"Extraction error: {str(e)}", exc_info=True)
        raise
    
    currency_to_usd = {
        'USD': 1.0,
        'EUR': 1.09, 
        'GBP': 1.28, 
        'CAD': 0.74, 
        'AUD': 0.67,  
        'BBD': 0.50   
    }
    
    # Define pay period multipliers to convert to YEARLY
    pay_period_to_yearly = {
        'YEARLY': 1,
        'MONTHLY': 12,
        'BIWEEKLY': 26,
        'WEEKLY': 52,
        'HOURLY': 2080  # Assuming 40 hours per week, 52 weeks
    }
        
    if 'jobs' in raw and not raw['jobs'].empty:
        jobs_df = raw['jobs'].copy()
        
        cols_to_drop = ['med_salary', 'work_type', 'applies', 'closed_time', 'skills_desc',
                       'max_salary', 'min_salary', 'fips', 'listed_time', 'expiry',
                       'compensation_type', 'application_url', 'posting_domain']
        
        cols_to_drop_existing = [col for col in cols_to_drop if col in jobs_df.columns]
        jobs_df.drop(columns=cols_to_drop_existing, inplace=True)
        log.info("Dropped unnecessary columns from jobs_df.")
        
        columns_to_replace_not_specified = ["zip_code", "formatted_experience_level"]
        for col in columns_to_replace_not_specified:
            if col in jobs_df.columns:
                if col == "zip_code":
                    jobs_df[col] = jobs_df[col].astype(str)
                jobs_df[col] = jobs_df[col].replace(["nan", None], "No specified")
        
 
        if "original_listed_time" in jobs_df.columns:
            try:
                jobs_df["original_listed_time"] = pd.to_datetime(jobs_df["original_listed_time"], unit="ms", errors='coerce')
            except Exception as e:
                log.warning(f"Error converting original_listed_time: {str(e)}. Trying standard format.")
                jobs_df["original_listed_time"] = pd.to_datetime(jobs_df["original_listed_time"], errors='coerce')
        

        if "company_id" in jobs_df.columns:
            jobs_df["company_id"] = jobs_df["company_id"].fillna(-1).astype(int)
        
        if "views" in jobs_df.columns:
            jobs_df["views"] = jobs_df["views"].fillna(0).astype(int)
        
        if "remote_allowed" in jobs_df.columns:
            jobs_df["remote_allowed"] = jobs_df["remote_allowed"].fillna(0).astype(bool)
        

        jobs_df["job_id_modify"] = range(1, len(jobs_df) + 1)
        jobs_df["company_id_modify"] = range(1, len(jobs_df) + 1)
        log.info("Added modified IDs to jobs_df.")
        
        # Normalize currency to USD if currency column exists
        if "currency" in jobs_df.columns and "normalized_salary" in jobs_df.columns:
            # Fill missing currency with USD
            jobs_df["currency"] = jobs_df["currency"].fillna("USD")
            
            # Convert all salaries to USD
            jobs_df["normalized_salary_usd"] = jobs_df.apply(
                lambda row: row["normalized_salary"] * currency_to_usd.get(row["currency"], 1.0), 
                axis=1
            )

            jobs_df["normalized_salary"] = jobs_df["normalized_salary_usd"]
            jobs_df.drop(columns=["normalized_salary_usd"], inplace=True)

            jobs_df["currency"] = "USD"
            log.info("Normalized all salary values to USD in jobs_df.")
        
        if "pay_period" in jobs_df.columns and "normalized_salary" in jobs_df.columns:

            jobs_df["pay_period"] = jobs_df["pay_period"].fillna("YEARLY")
            
            jobs_df["pay_period"] = jobs_df["pay_period"].str.upper()
            

            jobs_df["normalized_salary_yearly"] = jobs_df.apply(
                lambda row: row["normalized_salary"] * pay_period_to_yearly.get(row["pay_period"], 1.0), 
                axis=1
            )
            
            jobs_df["normalized_salary"] = jobs_df["normalized_salary_yearly"]
            jobs_df.drop(columns=["normalized_salary_yearly"], inplace=True)
            
            jobs_df["pay_period"] = "YEARLY"
            log.info("Normalized all pay periods to YEARLY in jobs_df.")
        
        if "normalized_salary" in jobs_df.columns:
            if not jobs_df["normalized_salary"].isna().all():
                q1 = jobs_df["normalized_salary"].quantile(0.25)
                q3 = jobs_df["normalized_salary"].quantile(0.75)
                iqr = q3 - q1
                upper_cap = q3 + 1.5 * iqr
                jobs_df["normalized_salary"] = jobs_df["normalized_salary"].clip(upper=upper_cap)
                log.info("Capped outliers in normalized_salary for jobs_df.")
        
        raw['jobs'] = jobs_df

    if 'salaries' in raw and not raw['salaries'].empty:
        salaries_df = raw['salaries'].copy()
        
        def get_normalized_salary(row):
            if 'med_salary' in row and not pd.isna(row['med_salary']):
                return row['med_salary']
            
            min_sal = row.get('min_salary', np.nan)
            max_sal = row.get('max_salary', np.nan)
            
            if not pd.isna(min_sal) and not pd.isna(max_sal):
                return (min_sal + max_sal) / 2
            
            if not pd.isna(min_sal):
                return min_sal
            if not pd.isna(max_sal):
                return max_sal
            
            return np.nan
        
        try:
            salaries_df['normalized_salary'] = salaries_df.apply(get_normalized_salary, axis=1)
            log.info("Created normalized_salary column in salaries_df.")
            
            if "pay_period" in salaries_df.columns:
                salaries_df["pay_period"] = salaries_df["pay_period"].fillna("YEARLY")
                
                salaries_df["pay_period"] = salaries_df["pay_period"].str.upper()
                
                salaries_df["normalized_salary"] = salaries_df.apply(
                    lambda row: row["normalized_salary"] * pay_period_to_yearly.get(row["pay_period"], 1.0), 
                    axis=1
                )
                
                salaries_df["pay_period"] = "YEARLY"
                log.info("Normalized all pay periods to YEARLY in salaries_df.")
            
            if not salaries_df["normalized_salary"].isna().all() and len(salaries_df["normalized_salary"].dropna()) > 1:
                q1 = salaries_df["normalized_salary"].quantile(0.25)
                q3 = salaries_df["normalized_salary"].quantile(0.75)
                iqr = q3 - q1
                upper_cap = q3 + 1.5 * iqr
                salaries_df["normalized_salary"] = salaries_df["normalized_salary"].clip(upper=upper_cap)
                log.info("Capped outliers in normalized_salary for salaries_df.")
            
            cols_to_drop = ['min_salary', 'max_salary', 'med_salary']
            cols_to_drop = [col for col in cols_to_drop if col in salaries_df.columns]
            if cols_to_drop:
                salaries_df.drop(columns=cols_to_drop, inplace=True)
                log.info(f"Dropped original salary columns: {cols_to_drop}")
            
        except Exception as e:
            log.error(f"Error processing salaries: {str(e)}", exc_info=True)
            
        raw['salaries'] = salaries_df
        
    if 'benefits' in raw and not raw['benefits'].empty:
        benefits_df = raw['benefits'].copy()
        
        if 'inferred' in benefits_df.columns:
            benefits_df = benefits_df.drop(columns=['inferred'])
        
        if 'job_id' in benefits_df.columns and 'type' in benefits_df.columns:
            try:
                benefits_df = benefits_df.groupby('job_id')['type'].apply(list).reset_index()
                log.info("Grouped benefits by job_id and dropped inferred column in benefits_df.")
            except Exception as e:
                log.warning(f"Could not group benefits: {str(e)}. Keeping original format.")
        
        raw['benefits'] = benefits_df
    if 'employee_counts' in raw and not raw['employee_counts'].empty:
        employee_counts_df = raw['employee_counts'].copy()
        
        # Convert time_recorded to readable date format
        if 'time_recorded' in employee_counts_df.columns:
            try:
                employee_counts_df["time_recorded"] = pd.to_datetime(employee_counts_df["time_recorded"], unit="s", errors='coerce').dt.date
                log.info("Converted time_recorded to readable date format in employee_counts_df.")
            except Exception as e:
                log.warning(f"Error converting time_recorded: {str(e)}. Trying standard format.")
                employee_counts_df["time_recorded"] = pd.to_datetime(employee_counts_df["time_recorded"], errors='coerce').dt.date
        
        raw['employee_counts'] = employee_counts_df
        
    if 'industries' in raw and not raw['industries'].empty:
        industries_df = raw['industries'].copy()
        
        industry_col = 'industry_name' if 'industry_name' in industries_df.columns else 'industry'
        if industry_col in industries_df.columns:
            industries_df[industry_col] = industries_df[industry_col].replace([None, pd.NA], "Unknown")
            log.info(f"Replaced null values in {industry_col} with 'Unknown' in industries_df.")
        
        raw['industries'] = industries_df
    
    if 'companies' in raw and not raw['companies'].empty:
        companies_df = raw['companies'].copy()
        
        fill_values = {
            'zip_code': 'Unknown',
            'state': 'Unknown',
            'city': 'Unknown',
            'description': 'No description',
            'address': 'No specific address'
        }
        
        for col, val in fill_values.items():
            if col in companies_df.columns:
                companies_df[col] = companies_df[col].fillna(val)
        
        if 'company_size' in companies_df.columns:
            if companies_df['company_size'].dtypes in ['int64', 'float64']:
                median_size = companies_df['company_size'].median()
                if pd.isna(median_size):  # If median is also NaN
                    median_size = 0
                companies_df['company_size'] = companies_df['company_size'].fillna(median_size)
                companies_df['company_size'] = companies_df['company_size'].astype(int)
            else:  
                companies_df['company_size'] = companies_df['company_size'].fillna('Unknown')
        
        if 'name' in companies_df.columns:
            companies_df.dropna(subset=['name'], inplace=True)
        
        # Replace '0' with 'Unknown' for location fields
        for field in ['zip_code', 'state']:
            if field in companies_df.columns:
                companies_df[field] = companies_df[field].astype(str).replace('0', 'Unknown')
        
        log.info("Handled missing values and corrected data types in companies_df.")
        
        raw['companies'] = companies_df
    
    base_temp_dir = Path(tempfile.gettempdir()) / "airflow_intermediate_data"
    run_temp_dir = base_temp_dir / dag_run_id
    run_temp_dir.mkdir(parents=True, exist_ok=True)
    log.info(f"Saving cleaned files to: {run_temp_dir}")
    
    output_file_paths = {}
    
    for table_name, df in raw.items():
        if df is not None and not df.empty:
            file_path = run_temp_dir / f"{table_name}_cleaned.parquet"
            try:
                log.info(f"Attempting to save {table_name} with columns: {df.columns.tolist()}")
                df.to_parquet(file_path, index=False, engine='pyarrow')
                output_file_paths[table_name] = str(file_path)
                log.info(f"Successfully saved {table_name} to {file_path}")
            except Exception as e:
                log.error(f"Error saving {table_name} to parquet: {str(e)}", exc_info=True)
        else:
            log.warning(f"DataFrame for {table_name} is None or empty. Skipping save.")
    try:
        log.info("Storing cleaned data in the cleaned schema...")
        for table_name, df in raw.items():
            if df is not None and not df.empty:
                # Drop the table if it exists
                hook.run(f"DROP TABLE IF EXISTS cleaned.{table_name}")
                
                # Create table with proper schema
                df.head(0).to_sql(
                    name=table_name,
                    schema='cleaned',
                    con=engine,
                    if_exists='replace',
                    index=False
                )
                
                log.info(f"Inserting data into cleaned.{table_name}")
                table_columns = ", ".join([f'"{c}"' for c in df.columns])
                
                chunk_size = 10000
                for i in range(0, len(df), chunk_size):
                    chunk = df.iloc[i:i+chunk_size]
                    
                    records = [tuple(x) for x in chunk.replace({np.nan: None}).values]
                    
                    placeholders = ", ".join(["%s"] * len(df.columns))
                    
                    hook.insert_rows(
                        table=f"cleaned.{table_name}",
                        rows=records,
                        target_fields=df.columns.tolist(),
                        commit_every=5000
                    )
                    
                log.info(f"Successfully loaded {len(df)} rows into cleaned.{table_name}")
    except Exception as e:
        log.error(f"Error storing cleaned data in database: {str(e)}", exc_info=True)
    
    log.info(f"Task finished. Output files: {output_file_paths}")
    return output_file_paths