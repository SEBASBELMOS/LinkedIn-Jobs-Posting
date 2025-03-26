import sys
import os

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
sys.path.append(project_root)

import json
import pandas as pd
import logging

from src.extract.extract_db import extracting_db_data
from src.transform.transform_dwh import transform_data
from src.load.load_dwh import load_to_dwh

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%d/%m/%Y %I:%M:%S %p"
)


def extract_task():
    """
    Extract data from the raw schema of the project-etl database.

    Returns:
        str: JSON string containing the dictionary of DataFrames.
    """
    try:
        logging.info("Starting extract_task: Extracting data from the raw schema.")
        dataframes = extracting_db_data()
        # Serialise the dictionary of DataFrames to JSON
        serialised_dataframes = {
            table: df.to_json(orient="records")
            for table, df in dataframes.items()
        }
        return json.dumps(serialised_dataframes)
    except Exception as e:
        logging.error(f"Error in extract_task: {str(e)}")
        raise

def transform_task(df_json):
    """
    Transform the extracted data.

    Args:
        df_json (str): JSON string containing the dictionary of DataFrames.

    Returns:
        str: JSON string containing the dictionary of transformed DataFrames.
    """
    try:
        logging.info("Starting transform_task: Transforming the extracted data.")
        # Deserialise the JSON string back to a dictionary of DataFrames
        serialised_dataframes = json.loads(df_json)
        dataframes = {
            table: pd.DataFrame(json.loads(df_json_str))
            for table, df_json_str in serialised_dataframes.items()
        }

        transformed_dataframes = transform_data(dataframes)
        # Serialise the transformed DataFrames back to JSON
        serialised_transformed = {
            table: df.to_json(orient="records")
            for table, df in transformed_dataframes.items()
        }
        return json.dumps(serialised_transformed)
    except Exception as e:
        logging.error(f"Error in transform_task: {str(e)}")
        raise

def load_task(df_json):
    """
    Load the transformed data into the cleaned schema of the project-etl database.

    Args:
        df_json (str): JSON string containing the dictionary of transformed DataFrames.

    Returns:
        str: JSON string (passed through for the next task).
    """
    try:
        logging.info("Starting load_task: Loading data into the cleaned schema.")
        # Deserialise the JSON string back to a dictionary of DataFrames
        serialised_dataframes = json.loads(df_json)
        dataframes = {
            table: pd.DataFrame(json.loads(df_json_str))
            for table, df_json_str in serialised_dataframes.items()
        }

        load_to_dwh(dataframes)
        logging.info("load_task completed successfully.")
        return df_json 
    except Exception as e:
        logging.error(f"Error in load_task: {str(e)}")
        raise

def validate_task(df_json):
    """
    Validate the data distribution to diagnose issues like the April spike.

    Args:
        df_json (str): JSON string containing the dictionary of transformed DataFrames.

    Returns:
        None
    """
    try:
        logging.info("Starting validate_task: Validating data distribution.")
        # Deserialise the JSON string back to a dictionary of DataFrames
        serialised_dataframes = json.loads(df_json)
        dataframes = {
            table: pd.DataFrame(json.loads(df_json_str))
            for table, df_json_str in serialised_dataframes.items()
        }
        if 'jobs' in dataframes:
            jobs_df = dataframes['jobs']
            logging.info("Jobs Data Distribution:")
            logging.info(jobs_df[['normalised_salary', 'original_listed_time']].describe().to_string())
            logging.info("\nDate Distribution:")
            logging.info(jobs_df['original_listed_time'].value_counts().sort_index().to_string())
            logging.info(f"\nTotal unique jobs: {jobs_df['job_id'].nunique()}")
            logging.info(f"Total rows: {len(jobs_df)}")
        logging.info("validate_task completed successfully.")
    except Exception as e:
        logging.error(f"Error in validate_task: {str(e)}")
        raise