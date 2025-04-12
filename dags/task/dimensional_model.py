import logging
import tempfile
from pathlib import Path
import pandas as pd
import numpy as np
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

@task
def transform_to_dimensional_model(conn_id: str, **context) -> dict:
    log.info("Starting task: transform_to_dimensional_model")
    dag_run_id = context["run_id"]

    hook = PostgresHook(postgres_conn_id=conn_id)
    engine = hook.get_sqlalchemy_engine()

    hook.run("CREATE SCHEMA IF NOT EXISTS dimensional_model")
    log.info("Created dimensional_model schema if it didn't exist")

    tables_to_extract = [
        'jobs', 'salaries', 'benefits', 'employee_counts',
        'industries', 'companies'
    ]

    cleaned_dataframes = {}
    chunk_size = 10000
    try:
        for table in tables_to_extract:
            log.info(f"Extracting table: cleaned.{table}")
            chunks = []
            query = f"SELECT * FROM cleaned.\"{table}\""
            for chunk in pd.read_sql(query, engine, chunksize=chunk_size):
                chunks.append(chunk)
            if chunks:
                cleaned_dataframes[table] = pd.concat(chunks, ignore_index=True)
                log.info(f"Successfully extracted and concatenated {len(cleaned_dataframes[table])} rows for table: {table}")
                log.debug(f"Columns for {table}: {cleaned_dataframes[table].columns.tolist()}")
            else:
                log.warning(f"No data extracted for table: {table}")
    except Exception as e:
        log.error(f"Extraction error: {str(e)}", exc_info=True)
        raise

    transformed_dataframes = {}

    required_tables = ['employee_counts', 'salaries']
    for table in required_tables:
        if table in cleaned_dataframes and not cleaned_dataframes[table].empty:
            transformed_dataframes[table] = cleaned_dataframes[table].copy()
        else:
            log.warning(f"Cleaned dataframe for '{table}' is missing or empty. Skipping copy.")

    if 'jobs' in cleaned_dataframes and not cleaned_dataframes['jobs'].empty:
        jobs_df = cleaned_dataframes['jobs'].copy()
        log.debug(f"Original jobs columns: {jobs_df.columns.tolist()}")
        
        keep_columns = ['job_id', 'company_id', 'title', 'views', 'remote_allowed', 
                        'original_listed_time', 'job_id_modify', 'company_id_modify']

        salary_columns = ['normalized_salary', 'currency', 'pay_period']
        for col in salary_columns:
            if col in jobs_df.columns:
                keep_columns.append(col)
        
        keep_columns = [col for col in keep_columns if col in jobs_df.columns]
        jobs_df = jobs_df[keep_columns].copy()

        for col in ['job_id', 'company_id', 'title']:
             if col not in jobs_df.columns:
                 log.error(f"Essential column '{col}' missing from jobs data after transformations.")
                 raise KeyError(f"Essential column '{col}' missing from jobs data.")

        transformed_dataframes['jobs'] = jobs_df
    else:
        log.error("Jobs data is missing or empty. Cannot proceed.")
        raise ValueError("Jobs data is required but missing or empty.")

    if 'benefits' in cleaned_dataframes and not cleaned_dataframes['benefits'].empty:
        benefits_df = cleaned_dataframes['benefits'].copy()
        if 'job_id' in benefits_df.columns:

            if 'type' in benefits_df.columns and isinstance(benefits_df.iloc[0]['type'], list):
                benefits_df['benefits_count'] = benefits_df['type'].apply(len)
                benefits_count = benefits_df[['job_id', 'benefits_count']]
            else:
    
                benefits_df = benefits_df[['job_id']].drop_duplicates()
                benefits_count = benefits_df.groupby('job_id').size().reset_index(name='benefits_count')
            
            transformed_dataframes['benefits'] = benefits_count
        else:
            log.warning("Column 'job_id' not found in benefits table. Cannot calculate benefits_count.")
            transformed_dataframes['benefits'] = pd.DataFrame(columns=['job_id', 'benefits_count'])
    else:
         log.warning("Benefits data is missing or empty.")
         transformed_dataframes['benefits'] = pd.DataFrame(columns=['job_id', 'benefits_count'])

    if 'companies' in cleaned_dataframes and not cleaned_dataframes['companies'].empty:
        companies_df = cleaned_dataframes['companies'].copy()
        log.debug(f"Original companies columns: {companies_df.columns.tolist()}")
        
        for col in ['company_id', 'name']:
            if col not in companies_df.columns:
                log.error(f"Essential column '{col}' missing from companies data.")
                raise KeyError(f"Essential column '{col}' missing from companies data.")

        transformed_dataframes['companies'] = companies_df
    else:
        log.error("Companies data is missing or empty. Cannot proceed.")
        raise ValueError("Companies data is required but missing or empty.")

    if 'industries' in cleaned_dataframes and not cleaned_dataframes['industries'].empty:
        industries_df = cleaned_dataframes['industries'].copy()
        industry_col = 'industry_name' if 'industry_name' in industries_df.columns else 'industry'
        
        if industry_col in industries_df.columns:
            industries_df['industry'] = industries_df[industry_col] 
        
        if 'company_id' not in industries_df.columns:
            log.error("Column 'company_id' missing from industries data.")
            raise KeyError("Column 'company_id' missing from industries data.")

        transformed_dataframes['industries'] = industries_df
    else:
        log.warning("Industries data is missing or empty.")
        transformed_dataframes['industries'] = pd.DataFrame(columns=['company_id', 'industry'])

    log.info("Building dimensional model")

    dim_company_base = transformed_dataframes['companies'].copy()

    # Merge Employee Counts
    if 'employee_counts' in transformed_dataframes and not transformed_dataframes['employee_counts'].empty:
        emp_counts_df = transformed_dataframes['employee_counts']
        if 'company_id' in emp_counts_df.columns and 'employee_count' in emp_counts_df.columns:
            emp_counts_to_merge = emp_counts_df[['company_id', 'employee_count']].drop_duplicates(subset=['company_id'], keep='last') # Keep most recent if duplicates
            dim_company_base = dim_company_base.merge(emp_counts_to_merge, on='company_id', how='left')
            dim_company_base['employee_count'] = pd.to_numeric(dim_company_base['employee_count'], errors='coerce').fillna(0).astype(int)
        else:
            log.warning("Required columns ('company_id', 'employee_count') not found in employee_counts. Setting default.")
            dim_company_base['employee_count'] = 0
    else:
         log.warning("Employee counts data not available. Setting employee_count to 0.")
         dim_company_base['employee_count'] = 0

    # Merge Industries
    if 'industries' in transformed_dataframes and not transformed_dataframes['industries'].empty:
         industries_df = transformed_dataframes['industries']
         industry_col = 'industry' 
         if 'company_id' in industries_df.columns and industry_col in industries_df.columns:
             industries_to_merge = industries_df[['company_id', industry_col]].drop_duplicates(subset=['company_id'], keep='first') # Keep first industry if duplicates
             dim_company_base = dim_company_base.merge(industries_to_merge, on='company_id', how='left')
             dim_company_base[industry_col] = dim_company_base[industry_col].fillna('Unknown').astype(str)
         else:
            log.warning(f"Required columns ('company_id', '{industry_col}') not found in industries. Setting default.")
            dim_company_base[industry_col] = 'Unknown'
    else:
        log.warning("Industries data not available. Setting industry to 'Unknown'.")
        dim_company_base['industry'] = 'Unknown'

    # Define final columns for dim_company
    dim_company_cols = ['company_id', 'name', 'company_size', 'employee_count', 'industry', 'country', 'city', 'state', 'zip_code']
    dim_company_cols_existing = [col for col in dim_company_cols if col in dim_company_base.columns]
    dim_company = dim_company_base[dim_company_cols_existing].copy()

    # Generate surrogate key
    dim_company['company_key'] = dim_company['company_id'].astype(str) + "_" + dim_company['name'].astype(str).str[:3].fillna('UNK')

    # Ensure final types before saving
    if 'company_size' in dim_company.columns: dim_company['company_size'] = dim_company['company_size'].astype(str)
    if 'employee_count' in dim_company.columns: dim_company['employee_count'] = dim_company['employee_count'].astype(int)
    if 'industry' in dim_company.columns: dim_company['industry'] = dim_company['industry'].astype(str)
    if 'country' in dim_company.columns: dim_company['country'] = dim_company['country'].astype(str)
    if 'name' in dim_company.columns: dim_company['name'] = dim_company['name'].astype(str)

    # --- Dim Location ---
    dim_location_cols = ['city', 'state', 'zip_code', 'country']
    dim_location_cols_existing = [col for col in dim_location_cols if col in dim_company.columns]
    if dim_location_cols_existing:
        dim_location = dim_company[dim_location_cols_existing].drop_duplicates().reset_index(drop=True)
        for col in dim_location_cols_existing:
            dim_location[col] = dim_location[col].astype(str)
        
        # Create location_key by combining country and state
        if 'country' in dim_location.columns and 'state' in dim_location.columns:
            dim_location['location_key'] = dim_location['country'] + dim_location['state']
        elif 'country' in dim_location.columns:
            dim_location['location_key'] = dim_location['country']
        elif 'state' in dim_location.columns:
            dim_location['location_key'] = dim_location['state']
        else:
            dim_location['location_key'] = 'UNK'
    else:
        log.warning("Cannot create Dim Location, required columns missing. Location key will be null in Fact table.")
        dim_location = pd.DataFrame(columns=['location_key'] + dim_location_cols)

    # --- Dim Time ---
    jobs_df_for_time = transformed_dataframes['jobs']
    if 'original_listed_time' in jobs_df_for_time.columns:
        jobs_time = jobs_df_for_time[['original_listed_time']].dropna().copy()
        if not jobs_time.empty:
            jobs_time['time_key_str'] = jobs_time['original_listed_time'].dt.strftime('%Y%m%d')
            dim_time = pd.DataFrame({'time_key': jobs_time['time_key_str'].unique()})
            temp_time = pd.to_datetime(dim_time['time_key'], format='%Y%m%d', errors='coerce')
            valid_time_indices = temp_time.notna()
            dim_time = dim_time[valid_time_indices].copy()
            temp_time = temp_time[valid_time_indices]
            if not dim_time.empty:
                dim_time['year'] = temp_time.dt.year.astype(int)
                dim_time['month'] = temp_time.dt.month.astype(int)
                dim_time['day'] = temp_time.dt.day.astype(int)
                dim_time['time_key'] = dim_time['time_key'].astype(str) # Store key as string
                dim_time = dim_time[['time_key', 'year', 'month', 'day']].drop_duplicates().reset_index(drop=True)
            else:
                log.warning("No valid dates found for Dim Time after processing. Dim Time will be empty.")
                dim_time = pd.DataFrame(columns=['time_key', 'year', 'month', 'day'])
        else:
            log.warning("No valid 'original_listed_time' found in jobs data. Dim Time will be empty.")
            dim_time = pd.DataFrame(columns=['time_key', 'year', 'month', 'day'])
    else:
        log.warning("Column 'original_listed_time' not found in jobs data. Dim Time will be empty.")
        dim_time = pd.DataFrame(columns=['time_key', 'year', 'month', 'day'])

    # --- Fact Jobs ---
    fact_jobs_base = transformed_dataframes['jobs'].copy()

    company_merge_cols = ['company_id', 'company_key'] + dim_location_cols_existing
    company_merge_cols_in_dim = [col for col in company_merge_cols if col in dim_company.columns]
    fact_jobs_base = fact_jobs_base.merge(
        dim_company[company_merge_cols_in_dim],
        on='company_id',
        how='left'
    )

    # 2. Merge Benefits Info
    if 'benefits' in transformed_dataframes and not transformed_dataframes['benefits'].empty:
        fact_jobs_base = fact_jobs_base.merge(
            transformed_dataframes['benefits'],
            on='job_id',
            how='left'
        )
        fact_jobs_base['benefits_count'] = pd.to_numeric(fact_jobs_base['benefits_count'], errors='coerce').fillna(0).astype(int)
    else:
        fact_jobs_base['benefits_count'] = 0

    # 3. Merge Location Key
    location_on_cols = [col for col in dim_location_cols_existing if col in fact_jobs_base.columns]
    if location_on_cols and not dim_location.empty:
        dim_location_merge_cols = ['location_key'] + location_on_cols
        # Ensure merge columns in fact_jobs_base are also string
        for col in location_on_cols:
            fact_jobs_base[col] = fact_jobs_base[col].astype(str)
        fact_jobs_base = fact_jobs_base.merge(
            dim_location[dim_location_merge_cols],
            on=location_on_cols,
            how='left'
        )
        # Drop intermediate location columns from fact table
        fact_jobs_base = fact_jobs_base.drop(columns=[col for col in location_on_cols if col in fact_jobs_base.columns])
    else:
        log.warning("Location dimension merge not possible. Setting location_key to null.")
        fact_jobs_base['location_key'] = None

    # 4. Add Time Key
    if 'original_listed_time' in fact_jobs_base.columns and not dim_time.empty:
        fact_jobs_base['time_key'] = fact_jobs_base['original_listed_time'].dt.strftime('%Y%m%d')
        # Make sure time_key exists in dim_time (enforcing foreign key constraint)
        valid_time_keys = dim_time['time_key'].values
        fact_jobs_base.loc[~fact_jobs_base['time_key'].isin(valid_time_keys), 'time_key'] = None
    else:
        log.warning("Time dimension could not be linked. Setting time_key to null.")
        fact_jobs_base['time_key'] = None

    # 5. Handle salary info
    # Define currency conversion rates if needed
    currency_to_usd = {
        'USD': 1.0,
        'EUR': 1.10,
        'GBP': 1.28,
        'CAD': 0.71,
        'AUD': 0.60,
        'BBD': 0.50
    }
    
    # Define pay period multipliers
    pay_period_to_yearly = {
        'YEARLY': 1,
        'MONTHLY': 12,
        'BIWEEKLY': 26,
        'WEEKLY': 52,
        'HOURLY': 2080
    }
    
    # First check if jobs has salary information
    has_job_salary = all(col in fact_jobs_base.columns for col in ['normalized_salary', 'currency', 'pay_period'])
    
    # If jobs doesn't have complete salary info, try getting from salaries table
    if not has_job_salary and 'salaries' in transformed_dataframes and not transformed_dataframes['salaries'].empty:
        salaries_df = transformed_dataframes['salaries'].copy()
        if 'job_id' in salaries_df.columns and 'normalized_salary' in salaries_df.columns:
            log.info("Getting salary information from salaries table")
            
            # Select relevant columns for merge
            salary_merge_cols = ['job_id', 'normalized_salary']
            if 'currency' in salaries_df.columns:
                salary_merge_cols.append('currency')
            if 'pay_period' in salaries_df.columns:
                salary_merge_cols.append('pay_period')
                
            # Keep only first salary record per job to avoid data multiplication
            salary_merge = salaries_df[salary_merge_cols].drop_duplicates(subset=['job_id'], keep='first')
            
            # Merge with fact_jobs
            fact_jobs_base = fact_jobs_base.merge(salary_merge, on='job_id', how='left')
            
            log.info(f"Merged salary data from salaries table: {len(salary_merge)} records")
    
    # Now normalize the salary data if it exists
    if 'normalized_salary' in fact_jobs_base.columns:
        log.info("Normalizing salary data in fact_jobs")
        
        # Fill missing values
        fact_jobs_base['normalized_salary'] = pd.to_numeric(fact_jobs_base['normalized_salary'], errors='coerce').fillna(0)
        
        # Ensure currency exists and normalize to USD
        if 'currency' in fact_jobs_base.columns:
            fact_jobs_base['currency'] = fact_jobs_base['currency'].fillna('USD')
            
            # Convert all non-USD currencies to USD
            fact_jobs_base['normalized_salary'] = fact_jobs_base.apply(
                lambda row: row['normalized_salary'] * currency_to_usd.get(row['currency'], 1.0) 
                if row['currency'] != 'USD' and row['normalized_salary'] > 0 else row['normalized_salary'], 
                axis=1
            )
            
            # Set all currency to USD since we've normalized
            fact_jobs_base['currency'] = 'USD'
        else:
            # Add currency column if it doesn't exist
            fact_jobs_base['currency'] = 'USD'
        
        # Ensure pay_period exists and normalize to YEARLY
        if 'pay_period' in fact_jobs_base.columns:
            fact_jobs_base['pay_period'] = fact_jobs_base['pay_period'].fillna('YEARLY')
            fact_jobs_base['pay_period'] = fact_jobs_base['pay_period'].str.upper()
            
            # Convert all non-YEARLY pay periods to YEARLY
            fact_jobs_base['normalized_salary'] = fact_jobs_base.apply(
                lambda row: row['normalized_salary'] * pay_period_to_yearly.get(row['pay_period'], 1.0) 
                if row['pay_period'] != 'YEARLY' and row['normalized_salary'] > 0 else row['normalized_salary'], 
                axis=1
            )
            
            # Set all pay_period to YEARLY since we've normalized
            fact_jobs_base['pay_period'] = 'YEARLY'
        else:
            # Add pay_period column if it doesn't exist
            fact_jobs_base['pay_period'] = 'YEARLY'
        
        # Cap outliers to ensure max value doesn't exceed 124,000
        fact_jobs_base['normalized_salary'] = fact_jobs_base['normalized_salary'].clip(upper=124000)
        
        log.info("Salary normalization complete in fact_jobs")
    
    # Final fact table with selected columns
    fact_cols = ['job_id', 'company_key', 'location_key', 'time_key', 'title', 
                'views', 'remote_allowed', 'benefits_count']
    
    # Add salary columns to fact_cols
    salary_cols = ['normalized_salary', 'currency', 'pay_period']
    fact_cols.extend(salary_cols)
    
    # Keep only columns that exist in our dataframe
    fact_cols_existing = [col for col in fact_cols if col in fact_jobs_base.columns]
    fact_jobs = fact_jobs_base[fact_cols_existing].copy()
    
    # Finalize data types
    if 'views' in fact_jobs.columns: fact_jobs['views'] = pd.to_numeric(fact_jobs['views'], errors='coerce').fillna(0).astype(int)
    if 'remote_allowed' in fact_jobs.columns: fact_jobs['remote_allowed'] = fact_jobs['remote_allowed'].astype(bool)
    if 'benefits_count' in fact_jobs.columns: fact_jobs['benefits_count'] = fact_jobs['benefits_count'].astype(int)
    if 'normalized_salary' in fact_jobs.columns: 
        fact_jobs['normalized_salary'] = pd.to_numeric(fact_jobs['normalized_salary'], errors='coerce').fillna(0).astype(float)
        # Final check to ensure no values exceed 124,000
        fact_jobs['normalized_salary'] = fact_jobs['normalized_salary'].clip(upper=124000)

    # Save all dimensional tables to PostgreSQL
    log.info("Saving dimensional model tables to database")
    
    # Dictionary to store table names and row counts
    dim_tables = {
        'dim_company': dim_company,
        'dim_location': dim_location,
        'dim_time': dim_time,
        'fact_jobs': fact_jobs
    }
    
    row_counts = {}
    
    # Function to handle database storage
    def store_df_in_db(df, table_name, schema, hook, engine):
        if df is not None and not df.empty:
            try:
                hook.run(f"DROP TABLE IF EXISTS {schema}.{table_name}")

                df.head(0).to_sql(
                    name=table_name,
                    schema=schema,
                    con=engine,
                    if_exists='replace',
                    index=False
                )
                
                log.info(f"Created {schema}.{table_name} table structure")
                
                # Insert data in chunks to avoid memory issues
                chunk_size = 10000
                total_rows = 0
                
                for i in range(0, len(df), chunk_size):
                    chunk = df.iloc[i:i+chunk_size]
                    
                    # Create list of tuples for faster insertion
                    records = [tuple(x) for x in chunk.replace({pd.NA: None, np.nan: None}).values]
                    
                    # Insert data using executemany
                    hook.insert_rows(
                        table=f"{schema}.{table_name}",
                        rows=records,
                        target_fields=df.columns.tolist(),
                        commit_every=5000
                    )
                    
                    total_rows += len(chunk)
                    log.info(f"Inserted chunk of {len(chunk)} rows into {schema}.{table_name}, total: {total_rows}/{len(df)}")
                
                return len(df)
            except Exception as e:
                log.error(f"Error storing {table_name} in database: {str(e)}", exc_info=True)
                return 0
        else:
            log.warning(f"DataFrame {table_name} is None or empty. Skipping database save.")
            return 0
    
    for table_name, df in dim_tables.items():
        rows_stored = store_df_in_db(df, table_name, 'dimensional_model', hook, engine)
        row_counts[table_name] = rows_stored
    
    temp_dir = Path(tempfile.mkdtemp())
    saved_paths = {}
    
    for table_name, df in dim_tables.items():
        if not df.empty:
            output_path = temp_dir / f"{table_name}.csv"
            df.to_csv(output_path, index=False)
            log.info(f"Saved backup of {table_name} with {len(df)} rows to {output_path}")
            saved_paths[table_name] = str(output_path)
    
    log.info("Dimensional model transformation and database loading complete")
    return {
        'dim_model_directory': str(temp_dir),
        'table_paths': saved_paths,
        'row_counts': row_counts,
        'dag_run_id': dag_run_id
    }