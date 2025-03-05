import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv("./env/linkedin_postings_clean.env")

driver = os.getenv("PG_DRIVER", "postgresql+psycopg2")
user = os.getenv("PG_USER")
password = os.getenv("PG_PASSWORD")
host = os.getenv("PG_HOST")
port = os.getenv("PG_PORT")
database = os.getenv("PG_DATABASE", "linkedin-postings-clean")

def create_clean_engine():
    url = f"{driver}://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(url, echo=False)
    return engine