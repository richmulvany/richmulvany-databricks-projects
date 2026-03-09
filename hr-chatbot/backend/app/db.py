from sqlalchemy import create_engine
from langchain_community.utilities import SQLDatabase
import os

def get_database() -> SQLDatabase:
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    catalog = os.getenv("DATABRICKS_CATALOG", "main")
    schema = os.getenv("DATABRICKS_SCHEMA", "default")

    if not host or not token or not http_path:
        raise ValueError(
            "DATABRICKS_HOST, DATABRICKS_TOKEN, and DATABRICKS_HTTP_PATH must be set"
        )

    engine = create_engine(
        f"databricks://token:{token}@{host}"
        f"?http_path={http_path}&catalog={catalog}&schema={schema}"
    )

    # sample_rows_in_table_info helps LLM generate better queries
    return SQLDatabase(engine, sample_rows_in_table_info=2)