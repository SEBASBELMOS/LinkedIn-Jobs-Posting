import sys
import os

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(project_root)

import json
import pandas as pd
import logging
from src.extract.extract_db import extracting_db_data
from src.transform.transform_dwh import transforming_db_data
from src.load.load_dwh import loading_db_data

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
        # Serialize the dictionary of DataFrames to JSON
        serialized_dataframes = {
            table: df.to_json(orient="records")
            for table, df in dataframes.items()
        }
        return json.dumps(serialized_dataframes)
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
        # Deserialize the JSON string back to a dictionary of DataFrames
        serialized_dataframes = json.loads(df_json)
        dataframes = {
            table: pd.DataFrame(json.loads(df_json_str))
            for table, df_json_str in serialized_dataframes.items()
        }
        transformed_dataframes = transforming_db_data(dataframes)
        serialized_transformed = {
            table: df.to_json(orient="records")
            for table, df in transformed_dataframes.items()
        }
        return json.dumps(serialized_transformed)
    except Exception as e:
        logging.error(f"Error in transform_task: {str(e)}")
        raise

def load_task(df_json):
    """
    Load the transformed data into the cleaned schema of the project-etl database.

    Args:
        df_json (str): JSON string containing the dictionary of transformed DataFrames.

    Returns:
        None
    """
    try:
        logging.info("Starting load_task: Loading data into the cleaned schema.")
        # Deserialize the JSON string back to a dictionary of DataFrames
        serialized_dataframes = json.loads(df_json)
        dataframes = {
            table: pd.DataFrame(json.loads(df_json_str))
            for table, df_json_str in serialized_dataframes.items()
        }
        
        loading_db_data(dataframes)
        logging.info("load_task completed successfully.")
    except Exception as e:
        logging.error(f"Error in load_task: {str(e)}")
        raise