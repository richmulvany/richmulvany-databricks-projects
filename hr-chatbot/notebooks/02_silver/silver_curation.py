# Databricks notebook source
# DBTITLE 1,Dependencies
import yaml
import hashlib
from pyspark.sql import functions as F
from pyspark.sql import Row
from datetime import datetime

# COMMAND ----------

# DBTITLE 1,Configuration
DATASET_NAME = "hr_employees"
BRONZE_TABLE = "01_bronze.hr.ibm_analytics_hr_employees"
SILVER_TABLE = "02_silver.hr.ibm_analytics_hr_employees"
CONTRACT_REGISTRY = "00_governance.contracts.contract_registry"
SILVER_SCHEMA_HASH = "00_governance.contracts.silver_schema_hash"

FAIL_ON_VIOLATION = True  # raise exceptions if migration fails

# COMMAND ----------

# DBTITLE 1,Load Latest Contract from Registry
contract_row = (
    spark.table(CONTRACT_REGISTRY)
    .filter((F.col("dataset_name") == DATASET_NAME) & (F.col("is_active") == True))
    .orderBy(F.col("deployed_at").desc())
    .limit(1)
    .collect()
)

if not contract_row:
    raise Exception(f"No active contract found for {DATASET_NAME}")

contract_raw = contract_row[0]["contract_yaml"]
CONTRACT_VERSION = contract_row[0]["version"]
contract = yaml.safe_load(contract_raw)

# Compute schema hash
def compute_schema_hash(contract_yaml):
    schema_str = yaml.dump(contract_yaml["columns"], sort_keys=True)
    return hashlib.md5(schema_str.encode()).hexdigest()

schema_hash = compute_schema_hash(contract)

# COMMAND ----------

# DBTITLE 1,Read Bronze Data
df_bronze = spark.table(BRONZE_TABLE)

# COMMAND ----------

# DBTITLE 1,Check Silver Table Schema & Detect Drift
alter_statements = []
type_mapping = {"string": "STRING", "integer": "INT", "boolean": "BOOLEAN"}

if spark.catalog.tableExists(SILVER_TABLE):
    # Existing Silver schema
    silver_schema = spark.table(SILVER_TABLE).schema
    silver_columns = {f.name: f.dataType.simpleString() for f in silver_schema}

    # Last applied hash
    last_hash_row = (
        spark.table(SILVER_SCHEMA_HASH)
        .filter(F.col("table_name") == SILVER_TABLE)
        .orderBy(F.col("migrated_at").desc())
        .select("schema_hash")
        .first()
    )
    last_hash = last_hash_row["schema_hash"] if last_hash_row else None
else:
    silver_columns = {}
    last_hash = None

# COMMAND ----------

# DBTITLE 1,Generate ALTER TABLE Statements
for col in contract["columns"]:
    col_name = col["name"]
    col_type = type_mapping[col["type"].lower()]

    if col_name not in silver_columns:
        alter_statements.append(f"ALTER TABLE {SILVER_TABLE} ADD COLUMN {col_name} {col_type}")

# COMMAND ----------

# DBTITLE 1,Apply ALTER TABLE Statements
for stmt in alter_statements:
    print(f"Executing: {stmt}")
    spark.sql(stmt)

# COMMAND ----------

# DBTITLE 1,Write Silver Data
df_silver = df_bronze.select([c["name"] for c in contract["columns"]])
df_silver.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLE)

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
for col in contract["columns"]:
    col_name = col["name"]
    description = col.get("description", "")
    spark.sql(f"""
        COMMENT ON COLUMN {SILVER_TABLE}.{col_name} IS '{description}'
    """)

print(f"✅ Silver table '{SILVER_TABLE}' updated successfully with contract v{CONTRACT_VERSION}")
