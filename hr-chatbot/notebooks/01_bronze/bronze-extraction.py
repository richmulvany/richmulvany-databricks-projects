# Databricks notebook source
# DBTITLE 1,Install Dependencies
from pyspark.sql import functions as F
from pyspark.sql.types import *
import yaml
import uuid

# COMMAND ----------

# DBTITLE 1,Configure Notebook
DATASET_NAME = "hr_employees"
CONTRACT_VERSION = "1.0.0"
RAW_PATH = "/Volumes/01_bronze/hr/ibm_analytics/WA_Fn-UseC_-HR-Employee-Attrition.csv"
BRONZE_TABLE = "01_bronze.hr.ibm_analytics_hr_employees"

load_id = str(uuid.uuid4())

# COMMAND ----------

# DBTITLE 1,Load Contract
contract_row = (
    spark.table("00_governance.contracts.contract_registry")
    .filter(
        (F.col("dataset_name") == DATASET_NAME) &
        (F.col("version") == CONTRACT_VERSION) &
        (F.col("is_active") == True)
    )
    .limit(1)
    .collect()
)

if not contract_row:
    raise Exception(f"No active contract found for {DATASET_NAME} v{CONTRACT_VERSION}")

contract_yaml = yaml.safe_load(contract_row[0]["contract_yaml"])
expected_columns = contract_yaml["columns"]

# COMMAND ----------

# DBTITLE 1,Read Raw Data
df_raw = (
    spark.read
    .option("header", True)
    .csv(RAW_PATH)
)

# COMMAND ----------

expected_column_names = {col["name"] for col in expected_columns}
actual_column_names = set(df_raw.columns)

# Missing columns
missing = expected_column_names - actual_column_names
if missing:
    raise Exception(f"Missing columns in raw data: {missing}")

# Unexpected columns
extra = actual_column_names - expected_column_names
if extra:
    raise Exception(f"Unexpected columns in raw data: {extra}")

# COMMAND ----------

df_bronze = (
    df_raw
    .withColumn("_ingestion_timestamp", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
    .withColumn("_contract_version", F.lit(CONTRACT_VERSION))
    .withColumn("_load_id", F.lit(load_id))
)

# COMMAND ----------

# DBTITLE 1,Write to Bronze
(
    df_bronze
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(BRONZE_TABLE)
)

print(f"✅ Successfully ingested {df_bronze.count()} records with load_id={load_id}")
