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
                raw[table] = pd.DataFrame()  # Empty DataFrame as placeholder
    except Exception as e:
        log.error(f"Extraction error: {str(e)}", exc_info=True)
        raise
    
    # Define currency conversion rates to USD (as of your reference date)
    currency_to_usd = {
        'USD': 1.0,
        'EUR': 1.09,  # Example rate EUR to USD
        'GBP': 1.28,  # Example rate GBP to USD
        'CAD': 0.74,  # Example rate CAD to USD
        'AUD': 0.67,  # Example rate AUD to USD
        'BBD': 0.50   # Example rate BBD to USD
    }
    
    # Define pay period multipliers to convert to YEARLY
    pay_period_to_yearly = {
        'YEARLY': 1,
        'MONTHLY': 12,
        'BIWEEKLY': 26,
        'WEEKLY': 52,
        'HOURLY': 2080  # Assuming 40 hours per week, 52 weeks
    }
        
    # Clean Jobs data
    if 'jobs' in raw and not raw['jobs'].empty:
        jobs_df = raw['jobs'].copy()
        
        # Drop unnecessary columns
        cols_to_drop = ['med_salary', 'work_type', 'applies', 'closed_time', 'skills_desc',
                       'max_salary', 'min_salary', 'fips', 'listed_time', 'expiry',
                       'compensation_type', 'application_url', 'posting_domain']
        
        # Only drop columns that exist
        cols_to_drop_existing = [col for col in cols_to_drop if col in jobs_df.columns]
        jobs_df.drop(columns=cols_to_drop_existing, inplace=True)
        log.info("Dropped unnecessary columns from jobs_df.")
        
        # Handle missing values and data types
        columns_to_replace_not_specified = ["zip_code", "formatted_experience_level"]
        for col in columns_to_replace_not_specified:
            if col in jobs_df.columns:
                if col == "zip_code":
                    jobs_df[col] = jobs_df[col].astype(str)
                jobs_df[col] = jobs_df[col].replace(["nan", None], "No specified")
        
        # Convert time fields
        if "original_listed_time" in jobs_df.columns:
            try:
                jobs_df["original_listed_time"] = pd.to_datetime(jobs_df["original_listed_time"], unit="ms", errors='coerce')
            except Exception as e:
                log.warning(f"Error converting original_listed_time: {str(e)}. Trying standard format.")
                jobs_df["original_listed_time"] = pd.to_datetime(jobs_df["original_listed_time"], errors='coerce')
        
        # Fill missing values and convert types
        if "company_id" in jobs_df.columns:
            jobs_df["company_id"] = jobs_df["company_id"].fillna(-1).astype(int)
        
        if "views" in jobs_df.columns:
            jobs_df["views"] = jobs_df["views"].fillna(0).astype(int)
        
        if "remote_allowed" in jobs_df.columns:
            jobs_df["remote_allowed"] = jobs_df["remote_allowed"].fillna(0).astype(bool)
        
        # Add modified IDs for dimensional model
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
            
            # Replace original normalized_salary with USD value
            jobs_df["normalized_salary"] = jobs_df["normalized_salary_usd"]
            jobs_df.drop(columns=["normalized_salary_usd"], inplace=True)
            
            # Set all currency to USD since we've normalized
            jobs_df["currency"] = "USD"
            log.info("Normalized all salary values to USD in jobs_df.")
        
        # Normalize pay_period to YEARLY if pay_period column exists
        if "pay_period" in jobs_df.columns and "normalized_salary" in jobs_df.columns:
            # Fill missing pay_period with YEARLY
            jobs_df["pay_period"] = jobs_df["pay_period"].fillna("YEARLY")
            
            # Standardize pay_period capitalization
            jobs_df["pay_period"] = jobs_df["pay_period"].str.upper()
            
            # Convert all salaries to YEARLY
            jobs_df["normalized_salary_yearly"] = jobs_df.apply(
                lambda row: row["normalized_salary"] * pay_period_to_yearly.get(row["pay_period"], 1.0), 
                axis=1
            )
            
            # Replace original normalized_salary with yearly value
            jobs_df["normalized_salary"] = jobs_df["normalized_salary_yearly"]
            jobs_df.drop(columns=["normalized_salary_yearly"], inplace=True)
            
            # Set all pay_period to YEARLY since we've normalized
            jobs_df["pay_period"] = "YEARLY"
            log.info("Normalized all pay periods to YEARLY in jobs_df.")
        
        # Cap salary outliers
        if "normalized_salary" in jobs_df.columns:
            if not jobs_df["normalized_salary"].isna().all():
                q1 = jobs_df["normalized_salary"].quantile(0.25)
                q3 = jobs_df["normalized_salary"].quantile(0.75)
                iqr = q3 - q1
                upper_cap = q3 + 1.5 * iqr
                jobs_df["normalized_salary"] = jobs_df["normalized_salary"].clip(upper=upper_cap)
                log.info("Capped outliers in normalized_salary for jobs_df.")
        
        raw['jobs'] = jobs_df
        
    # Clean Salaries data
    if 'salaries' in raw and not raw['salaries'].empty:
        salaries_df = raw['salaries'].copy()
        
        # Create normalized_salary column (formerly raw_salary)
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
            
            # Normalize pay_period to YEARLY if pay_period column exists
            if "pay_period" in salaries_df.columns:
                # Fill missing pay_period with YEARLY
                salaries_df["pay_period"] = salaries_df["pay_period"].fillna("YEARLY")
                
                # Standardize pay_period capitalization
                salaries_df["pay_period"] = salaries_df["pay_period"].str.upper()
                
                # Convert all salaries to YEARLY
                salaries_df["normalized_salary"] = salaries_df.apply(
                    lambda row: row["normalized_salary"] * pay_period_to_yearly.get(row["pay_period"], 1.0), 
                    axis=1
                )
                
                # Set all pay_period to YEARLY since we've normalized
                salaries_df["pay_period"] = "YEARLY"
                log.info("Normalized all pay periods to YEARLY in salaries_df.")
            
            # Cap outliers in normalized_salary column
            if not salaries_df["normalized_salary"].isna().all() and len(salaries_df["normalized_salary"].dropna()) > 1:
                q1 = salaries_df["normalized_salary"].quantile(0.25)
                q3 = salaries_df["normalized_salary"].quantile(0.75)
                iqr = q3 - q1
                upper_cap = q3 + 1.5 * iqr
                salaries_df["normalized_salary"] = salaries_df["normalized_salary"].clip(upper=upper_cap)
                log.info("Capped outliers in normalized_salary for salaries_df.")
            
            # Remove unnecessary salary columns after normalization
            cols_to_drop = ['min_salary', 'max_salary', 'med_salary']
            cols_to_drop = [col for col in cols_to_drop if col in salaries_df.columns]
            if cols_to_drop:
                salaries_df.drop(columns=cols_to_drop, inplace=True)
                log.info(f"Dropped original salary columns: {cols_to_drop}")
            
        except Exception as e:
            log.error(f"Error processing salaries: {str(e)}", exc_info=True)
            
        raw['salaries'] = salaries_df
        
    # Clean Benefits data
    if 'benefits' in raw and not raw['benefits'].empty:
        benefits_df = raw['benefits'].copy()
        
        # Drop inferred column if it exists
        if 'inferred' in benefits_df.columns:
            benefits_df = benefits_df.drop(columns=['inferred'])
        
        # Group benefits by job_id
        if 'job_id' in benefits_df.columns and 'type' in benefits_df.columns:
            try:
                benefits_df = benefits_df.groupby('job_id')['type'].apply(list).reset_index()
                log.info("Grouped benefits by job_id and dropped inferred column in benefits_df.")
            except Exception as e:
                log.warning(f"Could not group benefits: {str(e)}. Keeping original format.")
        
        raw['benefits'] = benefits_df
    
    # Clean Employee Counts data
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
        
    # Clean Industries data
    if 'industries' in raw and not raw['industries'].empty:
        industries_df = raw['industries'].copy()
        
        # Replace nulls in industry name
        industry_col = 'industry_name' if 'industry_name' in industries_df.columns else 'industry'
        if industry_col in industries_df.columns:
            industries_df[industry_col] = industries_df[industry_col].replace([None, pd.NA], "Unknown")
            log.info(f"Replaced null values in {industry_col} with 'Unknown' in industries_df.")
        
        raw['industries'] = industries_df
    
    # Clean Companies data
    if 'companies' in raw and not raw['companies'].empty:
        companies_df = raw['companies'].copy()
        
        # Fill missing values
        fill_values = {
            'zip_code': 'Unknown',
            'state': 'Unknown',
            'city': 'Unknown',
            'description': 'No description',
            'address': 'No specific address'
        }
        
        # Only fill columns that exist
        for col, val in fill_values.items():
            if col in companies_df.columns:
                companies_df[col] = companies_df[col].fillna(val)
        
        # Handle company_size
        if 'company_size' in companies_df.columns:
            # For company_size, use median or a default value
            if companies_df['company_size'].dtypes in ['int64', 'float64']:
                median_size = companies_df['company_size'].median()
                if pd.isna(median_size):  # If median is also NaN
                    median_size = 0
                companies_df['company_size'] = companies_df['company_size'].fillna(median_size)
                companies_df['company_size'] = companies_df['company_size'].astype(int)
            else:  # If it's not numeric
                companies_df['company_size'] = companies_df['company_size'].fillna('Unknown')
        
        # Drop rows without a company name
        if 'name' in companies_df.columns:
            companies_df.dropna(subset=['name'], inplace=True)
        
        # Replace '0' with 'Unknown' for location fields
        for field in ['zip_code', 'state']:
            if field in companies_df.columns:
                companies_df[field] = companies_df[field].astype(str).replace('0', 'Unknown')
        
        log.info("Handled missing values and corrected data types in companies_df.")
        
        raw['companies'] = companies_df
    
    # Save cleaned data files
    base_temp_dir = Path(tempfile.gettempdir()) / "airflow_intermediate_data"
    run_temp_dir = base_temp_dir / dag_run_id
    run_temp_dir.mkdir(parents=True, exist_ok=True)
    log.info(f"Saving cleaned files to: {run_temp_dir}")
    
    output_file_paths = {}
    
    # Save all dataframes to parquet files
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
                # We could raise here to fail the task, but we'll continue to save others
        else:
            log.warning(f"DataFrame for {table_name} is None or empty. Skipping save.")
    
    # Store cleaned dataframes in the cleaned schema
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
                
                # Use the Postgres hook to insert data in chunks
                log.info(f"Inserting data into cleaned.{table_name}")
                table_columns = ", ".join([f'"{c}"' for c in df.columns])
                
                # Process in chunks to avoid memory issues
                chunk_size = 10000
                for i in range(0, len(df), chunk_size):
                    chunk = df.iloc[i:i+chunk_size]
                    
                    # Create list of tuples for faster insertion
                    records = [tuple(x) for x in chunk.replace({np.nan: None}).values]
                    
                    # Generate placeholders for SQL
                    placeholders = ", ".join(["%s"] * len(df.columns))
                    
                    # Insert data using executemany
                    hook.insert_rows(
                        table=f"cleaned.{table_name}",
                        rows=records,
                        target_fields=df.columns.tolist(),
                        commit_every=5000
                    )
                    
                log.info(f"Successfully loaded {len(df)} rows into cleaned.{table_name}")
    except Exception as e:
        log.error(f"Error storing cleaned data in database: {str(e)}", exc_info=True)
        # We'll still return the file paths even if DB storage fails
        
    log.info(f"Task finished. Output files: {output_file_paths}")
    return output_file_paths