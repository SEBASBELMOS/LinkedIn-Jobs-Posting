import sys
import os
import pandas as pd
import logging
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from sqlalchemy.exc import SQLAlchemyError

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)

from src.database.db_connection import create_docker_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%d/%m/%Y %I:%M:%S %p"
)

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type(SQLAlchemyError),
    before_sleep=lambda retry_state: logging.info(f"Retrying due to database error: attempt {retry_state.attempt_number}")
)

def extracting_db_data():
    """
    Extract data from the raw schema of the project-etl database and return it as a dictionary of DataFrames.

    Returns:
        dict: A dictionary where keys are table names and values are the corresponding DataFrames.
    """
    engine = create_docker_engine()
    
    tables = [
        'jobs',
        'salaries',
        'benefits',
        'employee_counts',
        'industries',
        'skills_industries',
        'companies'
    ]
    
    dataframes = {}
    
    try:
        logging.info("Starting to extract data from the raw schema of the project-etl database.")
        
        for table in tables:
            try:
                logging.info(f"Extracting data from raw.{table} table.")
                df = pd.read_sql(f"SELECT * FROM raw.{table}", con=engine)
                if df.empty:
                    raise ValueError(f"No data found in raw.{table} table.")
                dataframes[table] = df
                logging.info(f"Successfully extracted {len(df)} rows from raw.{table} table.")
            except Exception as e:
                logging.error(f"Error extracting data from raw.{table} table: {str(e)}")
                raise
        
        logging.info("Data extraction from raw schema completed successfully.")
        return dataframes
    
    except Exception as e:
        logging.error(f"Error during data extraction from raw schema: {str(e)}")
        raise

if __name__ == "__main__":
    dataframes = extracting_db_data()
    print("Extracted tables:", list(dataframes.keys()))