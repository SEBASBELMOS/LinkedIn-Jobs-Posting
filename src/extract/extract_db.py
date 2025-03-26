import sys
import os

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(project_root)

from src.database.db_connection import create_gcp_engine
import pandas as pd
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%d/%m/%Y %I:%M:%S %p"
)

## ----- DB Extract ----- ##

def extracting_db_data():
    """
    Extract data from the raw schema of the project-etl database and return it as a dictionary of DataFrames.

    Returns:
        dict: A dictionary where keys are table names and values are the corresponding DataFrames.
    """
    engine = create_gcp_engine()
    
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
    
    finally:
        engine.dispose()
        logging.info("Database engine disposed and connections closed.")
        
if __name__ == "__main__":
    dataframes = extracting_db_data()
    print("Extracted tables:", list(dataframes.keys()))
    