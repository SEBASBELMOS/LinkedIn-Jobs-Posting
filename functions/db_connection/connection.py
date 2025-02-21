import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from pathlib import Path

# Load .env correctly
env_path = Path(__file__).resolve().parent.parent / "env" / ".env"
load_dotenv(env_path)

# Fetch environment variables
driver = os.getenv("PG_DRIVER")
user = os.getenv("PG_USER")
password = os.getenv("PG_PASSWORD")
host = os.getenv("PG_HOST")
port = os.getenv("PG_PORT")
database = os.getenv("PG_DATABASE")

# Debug print
print(f"Driver: {driver}")
print(f"User: {user}")
print(f"Password: {password}")
print(f"Host: {host}")
print(f"Port: {port}")
print(f"Database: {database}")

# Database connection function
def creating_engine():
    url = f"{driver}://{user}:{password}@{host}:{port}/{database}"
    print(f"Connection URL: {url}")  # Debugging connection URL
    engine = create_engine(url)
    return engine
