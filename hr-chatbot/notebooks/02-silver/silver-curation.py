# Databricks notebook source
# DBTITLE 1,Dependencies
import yaml
import hashlib
from pyspark.sql import functions as F
from pyspark.sql import Row
from datetime import datetime

# COMMAND ----------

# DBTITLE 1,Configure Paths and Tables
DATASET_NAME = "hr_employees"
BRONZE_TABLE = "01_bronze.hr.ibm_analytics_hr_employees"
SILVER_TABLE = "02_silver.hr.ibm_analytics_hr_employees"

CONTRACT_REGISTRY = "00_governance.contracts.contract_registry"
SILVER_SCHEMA_HASH = "00_governance.contracts.silver_schema_hash"

# COMMAND ----------

# DBTITLE 1,Load Latest Contract from Delta
contract_row = (
    spark.table(CONTRACT_REGISTRY)
    .filter((F.col("dataset_name") == DATASET_NAME) & (F.col("is_active") == True))
    .orderBy(F.col("deployed_at").desc())
    .limit(1)
    .collect()
)

if not contract_row:
    raise Exception(f"No active contract found for {DATASET_NAME}")

contract_yaml = yaml.safe_load(contract_row[0]["contract_yaml"])
CONTRACT_VERSION = contract_yaml["version"]

# Compute schema hash
def compute_schema_hash(contract_yaml):
    schema_str = yaml.dump(contract_yaml["columns"], sort_keys=True)
    return hashlib.md5(schema_str.encode()).hexdigest()

schema_hash = compute_schema_hash(contract_yaml)

# COMMAND ----------

# DBTITLE 1,Read Bronze Data
df_bronze = spark.table(BRONZE_TABLE)

# COMMAND ----------

# DBTITLE 1,Check if Silver Exists
if not spark.catalog.tableExists(SILVER_TABLE):
    # First run: create Silver table from Bronze
    print(f"Silver table {SILVER_TABLE} does not exist. Creating from Bronze...")
    df_silver = df_bronze.select([c["name"] for c in contract_yaml["columns"]])
    df_silver.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLE)
    
    silver_columns = {f.name: f.dataType.simpleString() for f in df_silver.schema}
    last_hash = None
else:
    # Silver exists → get schema & last applied hash
    silver_schema = spark.table(SILVER_TABLE).schema
    silver_columns = {f.name: f.dataType.simpleString() for f in silver_schema}

    last_hash_row = (
        spark.table(SILVER_SCHEMA_HASH)
        .filter(F.col("table_name") == SILVER_TABLE)
        .orderBy(F.col("migrated_at").desc())
        .select("schema_hash")
        .first()
    )
    last_hash = last_hash_row["schema_hash"] if last_hash_row else None

# COMMAND ----------

# DBTITLE 1,Generate ALTER TABLE Statements for Schema Drift
type_mapping = {"string": "STRING", "integer": "INT", "boolean": "BOOLEAN"}
alter_statements = []

if last_hash != schema_hash and silver_columns:
    print("Schema change detected! Generating ALTER TABLE statements...")
    for col in contract_yaml["columns"]:
        col_name = col["name"]
        col_type = type_mapping[col["type"].lower()]
        if col_name not in silver_columns:
            stmt = f"ALTER TABLE {SILVER_TABLE} ADD COLUMN {col_name} {col_type}"
            alter_statements.append(stmt)

# COMMAND ----------

# DBTITLE 1,Apply ALTER TABLE Statements
for stmt in alter_statements:
    print(f"Executing: {stmt}")
    spark.sql(stmt)

# COMMAND ----------

# DBTITLE 1,Update Silver Schema Hash Table
migration_row = Row(
    table_name=SILVER_TABLE,
    schema_hash=schema_hash,
    migrated_at=datetime.utcnow()
)

spark.createDataFrame([migration_row]) \
    .write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(SILVER_SCHEMA_HASH)

# COMMAND ----------

# DBTITLE 1,Update Unity Catalog Column Comments
for col in contract_yaml["columns"]:
    col_name = col["name"]
    description = col.get("description", "")
    spark.sql(f"""
        COMMENT ON COLUMN {SILVER_TABLE}.{col_name} IS '{description}'
    """)

print(f"✅ Silver table {SILVER_TABLE} is up-to-date with contract v{CONTRACT_VERSION}")
