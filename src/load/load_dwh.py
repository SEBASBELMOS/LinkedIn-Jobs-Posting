import logging
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from database.db_connection import creating_engine


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_to_dwh(dataframes_to_load, schema='public'):
    """
    Load transformed DataFrames into the linkedin-postings-clean database.
    
    Args:
        dataframes_to_load (dict): Dictionary of table names and DataFrames to load.
        schema (str): Database schema to load into (default: 'public').
    """
    # Create database engine
    try:
        clean_engine = creating_engine()
        logger.info("Successfully created clean_engine for linkedin-postings-clean")
    except Exception as e:
        logger.error(f"Failed to create clean_engine: {str(e)}")
        raise


    for table_name, df in dataframes_to_load.items():
        try:
            with clean_engine.begin() as connection:
                logger.info(f"Loading {table_name} with {len(df)} rows and columns: {list(df.columns)}")
                

                df.to_sql(table_name, connection, schema=schema, if_exists='replace', index=False)
                logger.info(f"Successfully loaded {table_name} into linkedin-postings-clean")


                result = connection.execute(text(f"SELECT * FROM {schema}.{table_name} LIMIT 1"))
                sample_row = result.fetchone()
                if sample_row:
                    logger.info(f"Validation: Found data in {table_name} - Sample: {sample_row}")
                else:
                    logger.warning(f"Validation: No data found in {table_name} after upload")

        except SQLAlchemyError as e:
            logger.error(f"Error loading {table_name} to linkedin-postings-clean: {str(e)}")
            raise


    with clean_engine.connect() as connection:
        for table_name in dataframes_to_load.keys():
            try:
                result = connection.execute(text(f"SELECT COUNT(*) FROM {schema}.{table_name}"))
                row_count = result.fetchone()[0]
                logger.info(f"Table {table_name} in linkedin-postings-clean has {row_count} rows")
                

                existence_check = connection.execute(
                    text(f"SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = :schema AND tablename = :name)"),
                    {"schema": schema, "name": table_name}
                )
                exists = existence_check.fetchone()[0]
                if exists:
                    logger.info(f"Table {table_name} exists in {schema} schema")
                else:
                    logger.warning(f"Table {table_name} not found in {schema} schema")

            except SQLAlchemyError as e:
                logger.error(f"Error verifying {table_name}: {str(e)}")
                raise

    
    clean_engine.dispose()
    logger.info("Closed connection to linkedin-postings-clean database.")

if __name__ == "__main__":

    import pandas as pd
    sample_df = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})
    dataframes_to_load = {'test_table': sample_df}
    load_to_dwh(dataframes_to_load)
    