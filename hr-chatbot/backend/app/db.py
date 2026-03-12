import os
from sqlalchemy import create_engine


def get_engine():

    server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    access_token = os.getenv("DATABRICKS_TOKEN")

    connection_string = (
        f"databricks://token:{access_token}@{server_hostname}"
        f"?http_path={http_path}"
    )

    engine = create_engine(connection_string)

    return engine