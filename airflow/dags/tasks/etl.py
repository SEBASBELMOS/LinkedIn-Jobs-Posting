import sys
import os
import json
import pandas as pd
import logging
import pickle

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
sys.path.append(project_root)

from src.extract.extract_db import extracting_db_data
from src.transform.transform_dwh import transform_data
from src.load.load_dwh import load_to_dwh

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%d/%m/%Y %I:%M:%S %p"
)


def extract_task(**kwargs):
    """
    Extract data from the raw schema of the project-etl database.

    Returns:
        str: JSON string containing the dictionary of DataFrames.
    """
    try:
        logging.info("Starting extract_task: Extracting data from the raw schema.")
        dataframes = extracting_db_data()
        # Serialise the dictionary of DataFrames to JSON
        temp_dir = os.path.join(project_root, "temp")
        os.makedirs(temp_dir, exist_ok=True)
        temp_file = os.path.join(temp_dir, f"extracted_data_{kwargs['run_id']}.pkl")
        with open(temp_file, 'wb') as f:
            pickle.dump(dataframes, f)
        
        logging.info(f"Extracted data saved to {temp_file}")
        return temp_file
    except Exception as e:
        logging.error(f"Error in extract_task: {str(e)}")
        raise

def transform_task(**kwargs):
    """
    Transform the extracted data.

    Args:
        df_json (str): JSON string containing the dictionary of DataFrames.

    Returns:
        str: JSON string containing the dictionary of transformed DataFrames.
    """
    try:
        logging.info("Starting transform_task: Transforming the extracted data.")
        ti = kwargs['ti']
        # Get the file path from XCom
        temp_file = ti.xcom_pull(task_ids='extract')
        
        # Load the data from the file
        with open(temp_file, 'rb') as f:
            dataframes = pickle.load(f)
        
        # Transform the data
        transformed_dataframes = transform_data(dataframes)
        
        # Save the transformed data to a new temporary file
        temp_dir = os.path.join(project_root, "temp")
        os.makedirs(temp_dir, exist_ok=True)
        transformed_file = os.path.join(temp_dir, f"transformed_data_{kwargs['run_id']}.pkl")
        with open(transformed_file, 'wb') as f:
            pickle.dump(transformed_dataframes, f)
        
        logging.info(f"Transformed data saved to {transformed_file}")
        return transformed_file
    except Exception as e:
        logging.error(f"Error in transform_task: {str(e)}")
        raise

def load_task(**kwargs):
    """
    Load the transformed data into the cleaned schema of the project-etl database.

    Args:
        ti: Task instance to pull the file path from XCom.

    Returns:
        str: Path to the temporary file (passed through for the next task).
    """
    try:
        logging.info("Starting load_task: Loading data into the cleaned schema.")
        ti = kwargs['ti']
        # Get the file path from XCom
        temp_file = ti.xcom_pull(task_ids='transform')
        
        # Load the data from the file
        with open(temp_file, 'rb') as f:
            dataframes = pickle.load(f)
        
        # Load the data to the DWH
        load_to_dwh(dataframes)
        logging.info("load_task completed successfully.")
        return temp_file
    except Exception as e:
        logging.error(f"Error in load_task: {str(e)}")
        raise

def validate_task(**kwargs):
    """
    Validate the data distribution to diagnose issues like the April spike.

    Args:
        ti: Task instance to pull the file path from XCom.

    Returns:
        None
    """
    try:
        logging.info("Starting validate_task: Validating data distribution.")
        ti = kwargs['ti']
        # Get the file path from XCom
        temp_file = ti.xcom_pull(task_ids='load')
        
        # Load the data from the file
        with open(temp_file, 'rb') as f:
            dataframes = pickle.load(f)
        
        # Validate the data
        if 'jobs' in dataframes:
            jobs_df = dataframes['jobs']
            logging.info("Jobs Data Distribution:")
            logging.info(jobs_df[['normalized_salary', 'original_listed_time']].describe().to_string())
            logging.info("\nDate Distribution:")
            logging.info(jobs_df['original_listed_time'].value_counts().sort_index().to_string())
            logging.info(f"\nTotal unique jobs: {jobs_df['job_id'].nunique()}")
            logging.info(f"Total rows: {len(jobs_df)}")
        logging.info("validate_task completed successfully.")
    except Exception as e:
        logging.error(f"Error in validate_task: {str(e)}")
        raise

def cleanup_task(**kwargs):
    """
    Clean up temporary files created during the pipeline.

    Args:
        ti: Task instance to pull the file paths from XCom.

    Returns:
        None
    """
    try:
        logging.info("Starting cleanup_task: Removing temporary files.")
        ti = kwargs['ti']
        # Get the file paths from XCom
        extract_file = ti.xcom_pull(task_ids='extract')
        transform_file = ti.xcom_pull(task_ids='transform')
        
        # Remove the files if they exist
        for temp_file in [extract_file, transform_file]:
            if temp_file and os.path.exists(temp_file):
                os.remove(temp_file)
                logging.info(f"Removed temporary file: {temp_file}")
        logging.info("cleanup_task completed successfully.")
    except Exception as e:
        logging.error(f"Error in cleanup_task: {str(e)}")
        raise