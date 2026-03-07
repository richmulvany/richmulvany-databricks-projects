from sqlalchemy import create_engine
from langchain_community.utilities import SQLDatabase
import os

def get_database() -> SQLDatabase:
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    http_path = os.getenv("DATABRICKS_WAREHOUSE_ID")

    if not host or not token or not http_path:
        raise ValueError("DATABRICKS_HOST, DATABRICKS_TOKEN, and DATABRICKS_WAREHOUSE_ID must be set")

    engine = create_engine(
        f"databricks+connector://token:{token}@{host}:443/{http_path}&catalog=03_gold&schema=hr"
    )

    return SQLDatabase(engine)