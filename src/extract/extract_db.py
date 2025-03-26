from database.db_connection import creating_engine, disposing_engine

import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%d/%m/%Y %I:%M:%S %p")

## ----- DB Extract ----- ##

def extracting_db_data():
    """
    Extracting data from the LJP table and return it as a DataFrame.   

    """
    engine = creating_engine()
    
    try:
        logging.info("Starting to extract the data from the Linkedin Job Postings table.")
        df = pd.read_sql_table("linkedin-postings-clean", engine)
        logging.info("Data extracted from the Linkedin Job Postings table.")
        
        return df
    except Exception as e:
        logging.error(f"Error extracting data from the Linkedin Job Postings table: {e}.")
    finally:
        disposing_engine(engine)
        