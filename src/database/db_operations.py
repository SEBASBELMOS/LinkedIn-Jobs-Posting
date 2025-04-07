import os
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv("./env/.env")

driver = os.getenv("PG_DRIVER")
user = os.getenv("PG_USER")
password = os.getenv("PG_PASSWORD")
host = os.getenv("PG_HOST")
port = os.getenv("PG_PORT")
database = os.getenv("PG_DATABASE")

def create_gcp_engine():
    url = f"{driver}://{user}:{password}@{host}:{port}/{database}"
    try:
        engine = create_engine(url)
        logger.info("Successfully created GCP database engine")
        return engine
    except Exception as e:
        logger.error(f"Failed to create GCP database engine: {str(e)}")
        raise

if __name__ == "__main__":
    engine = create_gcp_engine()
    with engine.connect() as conn:
        result = conn.execute(text("SELECT version();"))
        logger.info(f"Database version: {result.fetchone()}")
    engine.dispose()