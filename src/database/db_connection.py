import os
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

driver = os.getenv("POSTGRES_DRIVER", "postgresql+psycopg2")
user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
host = os.getenv("POSTGRES_HOST")
port = os.getenv("POSTGRES_PORT")
database = os.getenv("POSTGRES_DB")

_engine = None

def create_docker_engine():
    """Creates or returns a SQLAlchemy engine using environment variables."""
    global _engine
    if _engine is not None:
        logger.info("Reusing existing database engine.")
        return _engine

    missing_vars = []
    if not user: missing_vars.append("POSTGRES_USER")
    if not password: missing_vars.append("POSTGRES_PASSWORD")
    if not host: missing_vars.append("POSTGRES_HOST")
    if not port: missing_vars.append("POSTGRES_PORT")
    if not database: missing_vars.append("POSTGRES_DB")

    if missing_vars:
        error_msg = f"Missing required database connection environment variables: {', '.join(missing_vars)}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    url = f"{driver}://{user}:{password}@{host}:{port}/{database}"
    logger.info(f"Attempting to connect to database: {driver}://{user}:***@{host}:{port}/{database}")

    try:
        _engine = create_engine(url, connect_args={'connect_timeout': 10}, echo=False, isolation_level="AUTOCOMMIT")
        with _engine.connect() as connection:
            logger.info("Successfully connected to the project-etl database and created engine.")
        return _engine
    except SQLAlchemyError as e:
        logger.error(f"Error creating database engine or connecting: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred during engine creation: {str(e)}")
        raise

def dispose_engine():
    """Dispose of the engine at the end of the DAG run."""
    global _engine
    if _engine is not None:
        _engine.dispose()
        logger.info("Database engine disposed.")
        _engine = None

if __name__ == "__main__":
    logger.info("Running database connection test...")
    try:
        engine = create_docker_engine()
        if engine:
            with engine.connect() as conn:
                result = conn.execute(text("SELECT version();"))
                db_version = result.fetchone()
                logger.info(f"Database version: {db_version[0] if db_version else 'N/A'}")
            dispose_engine()
            logger.info("Database connection test successful.")
        else:
            logger.error("Failed to create database engine.")
    except Exception as e:
        logger.error(f"Database connection test failed: {e}")