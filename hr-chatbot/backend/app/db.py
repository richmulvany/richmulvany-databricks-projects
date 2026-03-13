import os
import logging
from sqlalchemy import create_engine

logging.getLogger("databricks.sql").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)

def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Environment variable {name} is not set")
    return value

def get_engine():

    server_hostname = _require_env("DATABRICKS_HOST")
    http_path = _require_env("DATABRICKS_HTTP_PATH")
    access_token = _require_env("DATABRICKS_TOKEN")
    catalog = _require_env("DATABRICKS_CATALOG",)
    schema = _require_env("DATABRICKS_SCHEMA")

    connection_string = (
        f"databricks://token:{access_token}@{server_hostname}"
        f"?http_path={http_path}"
        f"&catalog={catalog}"
        f"&schema={schema}"
    )

    return create_engine(
        connection_string,
        pool_pre_ping=True,
        pool_recycle=3600
    )