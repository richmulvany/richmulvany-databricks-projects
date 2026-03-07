from sqlalchemy import create_engine
from langchain_community.utilities import SQLDatabase
import os

def get_database():
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")  # new env variable

    engine = create_engine(
        f"databricks+connector://token:{token}@{host}:443/{http_path}"
    )

    return SQLDatabase(engine)