# Databricks notebook source
# DBTITLE 1,Import Dependencies
import requests
import base64

# COMMAND ----------

# DBTITLE 1,Configure Notebook / Assign Variables
github_user = dbutils.secrets.get(scope = "github", key = "github_user")
github_repo = dbutils.secrets.get(scope = "github", key = "github_repo")
github_token = dbutils.secrets.get(scope = "github", key = "github_token")

dbutils.widgets.text("branch", "main")
branch = dbutils.widgets.get("branch")

catalog = "03_gold"

dbutils.widgets.text("schema", "warcraftlogs")
schema = dbutils.widgets.get("schema")

export_dir = "data-exports"
commit_message_prefix = "Auto-export from Databricks:"

# COMMAND ----------

# Switch context
spark.sql(f"USE {catalog}.{schema}")
tables_df = spark.sql(f"SHOW TABLES IN {catalog}.{schema}")
table_names = [row.tableName for row in tables_df.collect()]

for table in table_names:
    print(f"Processing table: {table}")
    
    try:
        # Load table as pandas DataFrame
        df = spark.table(f"{catalog}.{schema}.{table}").toPandas()
        csv_bytes = df.to_csv(index=False).encode("utf-8")
        b64_csv = base64.b64encode(csv_bytes).decode("utf-8")
        
        # File path in GitHub
        file_path = f"{export_dir}/{table}.csv"
        api_url = f"https://api.github.com/repos/{github_user}/{github_repo}/contents/{file_path}"
        # Check if file already exists (for updating)
        check = requests.get(api_url + f"?ref={branch}", headers={"Authorization": f"token {github_token}"})
        sha = check.json().get("sha") if check.status_code == 200 else None

        payload = {
            "message": f"{commit_message_prefix} {table}",
            "content": b64_csv,
            "branch": branch
        }
        if sha:
            payload["sha"] = sha

        r = requests.put(api_url, json=payload, headers={"Authorization": f"token {github_token}"})
        if r.status_code in (200, 201):
            print(f"✅ Uploaded: {file_path}")
        else:
            print(f"❌ Failed to upload {table}: {r.status_code} {r.text}")

    except Exception as e:
        print(f"⚠️ Error processing {table}: {e}")
