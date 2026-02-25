# Databricks notebook source
# DBTITLE 1,Dependencies
import yaml
import hashlib
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType
from functools import reduce
from datetime import datetime

# COMMAND ----------
# DBTITLE 1,Configure Paths and Tables
DATASET_NAME = "hr_employees"
BRONZE_TABLE = "01_bronze.hr.ibm_analytics_hr_employees"
SILVER_TABLE = "02_silver.hr.ibm_analytics_hr_employees"
CONTRACT_REGISTRY = "00_governance.contracts.contract_registry"
SILVER_SCHEMA_HASH = "00_governance.contracts.silver_schema_hash"

# COMMAND ----------
# DBTITLE 1,Load Active Contract From Registry
contract_row = (
    spark.table(CONTRACT_REGISTRY)
    .filter((F.col("dataset_name") == DATASET_NAME) & (F.col("is_active") == True))
    .orderBy(F.col("deployed_at").desc())
    .limit(1)
    .collect()
)

if not contract_row:
    raise Exception("No active contract found.")

contract = yaml.safe_load(contract_row[0]["contract_yaml"])
schema_hash = contract_row[0]["schema_hash"]

# Extract business and derived columns
business_cols = contract["schema"]["business_columns"]
derived_cols = contract["schema"]["derived_columns"]
all_cols = business_cols + derived_cols

# COMMAND ----------
# DBTITLE 1,Load Bronze
df_bronze = spark.table(BRONZE_TABLE)

# COMMAND ----------
# DBTITLE 1,Type Enforcement
type_mapping = {
    "string": "string",
    "integer": "int",
    "double": "double",
    "boolean": "boolean"
}

df_silver = df_bronze

for col in all_cols:
    col_name = col["name"]
    col_type = type_mapping[col["type"].lower()]

    if col_name not in df_silver.columns:
        continue

    if col_type == "boolean":
        df_silver = df_silver.withColumn(
            col_name,
            F.when(F.col(col_name).isin("Yes", "yes", "1", 1, "Y"), True)
             .when(F.col(col_name).isin("No", "no", "0", 0), False)
             .otherwise(None)
             .cast(BooleanType())
        )
    else:
        df_silver = df_silver.withColumn(
            col_name,
            F.col(col_name).cast(col_type)
        )

# COMMAND ----------
# DBTITLE 1,Deduplicate
df_silver = df_silver.dropDuplicates(contract["schema"]["primary_key"])

# COMMAND ----------
# DBTITLE 1,Derived Columns
if "YearsAtCompany" in df_silver.columns:
    df_silver = df_silver.withColumn(
        "YearsAtCompanyBucket",
        F.when(F.col("YearsAtCompany") < 3, "0-2")
         .when(F.col("YearsAtCompany") < 6, "3-5")
         .otherwise("6+")
    )

if {"YearsInCurrentRole", "YearsAtCompany"}.issubset(df_silver.columns):
    df_silver = df_silver.withColumn(
        "TenureRatio",
        F.when(F.col("YearsAtCompany") > 0,
               F.col("YearsInCurrentRole") / F.col("YearsAtCompany"))
         .otherwise(None)
    )

if {"MonthlyIncome", "YearsAtCompany"}.issubset(df_silver.columns):
    df_silver = df_silver.withColumn(
        "IncomePerYearAtCompany",
        F.when(F.col("YearsAtCompany") > 0,
               F.col("MonthlyIncome") / F.col("YearsAtCompany"))
         .otherwise(None)
    )

# COMMAND ----------
# DBTITLE 1,Reorder Columns
business_col_names = [c["name"] for c in business_cols]
derived_col_names = [c["name"] for c in derived_cols]

metadata_cols = [
    "_ingestion_timestamp",
    "_source_file",
    "_contract_version",
    "_load_id"
]

final_cols = business_col_names + derived_col_names + metadata_cols
df_silver = df_silver.select(*[c for c in final_cols if c in df_silver.columns])

# COMMAND ----------
# DBTITLE 1,Write Silver
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable(SILVER_TABLE)

# COMMAND ----------
# DBTITLE 1,Update Schema Hash Table
spark.createDataFrame([{
    "table_name": SILVER_TABLE,
    "schema_hash": schema_hash,
    "migrated_at": datetime.utcnow()
}]).write.format("delta").mode("append").saveAsTable(SILVER_SCHEMA_HASH)

# COMMAND ----------
# DBTITLE 1,Update Column Comments
for col in all_cols:
    description = col.get("description", "").replace("'", "''")
    spark.sql(f"""
        COMMENT ON COLUMN {SILVER_TABLE}.{col['name']}
        IS '{description}'
    """)

# Metadata comments
spark.sql(f"""
COMMENT ON COLUMN {SILVER_TABLE}._ingestion_timestamp
IS 'Timestamp when record was ingested into Bronze layer.'
""")

spark.sql(f"""
COMMENT ON COLUMN {SILVER_TABLE}._source_file
IS 'Path of the source file ingested.'
""")

spark.sql(f"""
COMMENT ON COLUMN {SILVER_TABLE}._contract_version
IS 'Version of the contract used for ingestion.'
""")

spark.sql(f"""
COMMENT ON COLUMN {SILVER_TABLE}._load_id
IS 'Unique identifier for the ingestion load.'
""")
