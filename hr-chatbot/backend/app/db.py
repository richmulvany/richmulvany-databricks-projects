from sqlalchemy import create_engine
from langchain_community.utilities import SQLDatabase
import os

def get_database():
    engine = create_engine(
        f"databricks+connector://token:{os.getenv('DATABRICKS_TOKEN')}@{os.getenv('DATABRICKS_HOST')}"
    )

    return SQLDatabase(engine)