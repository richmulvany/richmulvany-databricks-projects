# Databricks notebook source
# DBTITLE 1,Dependencies
import yaml
import hashlib
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.types import BooleanType
from datetime import datetime
from functools import reduce

# COMMAND ----------
# DBTITLE 1,Configure Paths and Tables

DATASET_NAME = "hr_employees"
CONTRACT_PATH = "/Workspace/Users/ricard.mulvany@gmail.com/richmulvany-databricks-projects/hr-chatbot/contracts/hr_employees_v0.yml"

BRONZE_TABLE = "01_bronze.hr.ibm_analytics_hr_employees"
SILVER_TABLE = "02_silver.hr.ibm_analytics_hr_employees"

CONTRACT_REGISTRY = "00_governance.contracts.contract_registry"
SILVER_SCHEMA_HASH = "00_governance.contracts.silver_schema_hash"

# COMMAND ----------
# DBTITLE 1,Load Latest Contract

with open(CONTRACT_PATH, "r") as f:
    contract_raw = f.read()

contract = yaml.safe_load(contract_raw)
CONTRACT_VERSION = contract["version"]

def compute_schema_hash(contract_yaml):
    schema_str = yaml.dump(contract_yaml["columns"], sort_keys=True)
    return hashlib.md5(schema_str.encode()).hexdigest()

schema_hash = compute_schema_hash(contract)

# COMMAND ----------
# DBTITLE 1,Load Bronze Table

if not spark.catalog.tableExists(BRONZE_TABLE):
    raise Exception(f"Bronze table {BRONZE_TABLE} does not exist")

df_bronze = spark.table(BRONZE_TABLE)

# COMMAND ----------
# DBTITLE 1,Schema Enforcement & Type Casting

type_mapping = {
    "string": "string",
    "integer": "int",
    "boolean": "boolean"
}

df_silver = df_bronze

for col in contract["columns"]:
    col_name = col["name"]
    col_type = type_mapping[col["type"].lower()]
    
    if col_name not in df_silver.columns:
        continue

    if col_type == "int":
        df_silver = df_silver.withColumn(
            col_name,
            F.col(col_name).cast("int")
        )

    elif col_type == "boolean":
        # Proper Yes/No -> Boolean handling
        df_silver = df_silver.withColumn(
            col_name,
            F.when(F.col(col_name).isin("Yes", "yes", "1", 1), True)
             .when(F.col(col_name).isin("No", "no", "0", 0), False)
             .otherwise(None)
             .cast(BooleanType())
        )

    else:
        df_silver = df_silver.withColumn(
            col_name,
            F.trim(F.col(col_name)).cast("string")
        )

# COMMAND ----------
# DBTITLE 1,Normalize String Columns (Safe Standardization)

string_cols = [
    c["name"]
    for c in contract["columns"]
    if c["type"].lower() == "string"
]

for col_name in string_cols:
    if col_name in df_silver.columns:
        df_silver = df_silver.withColumn(
            col_name,
            F.initcap(F.trim(F.col(col_name)))
        )

# COMMAND ----------
# DBTITLE 1,Deduplicate on Primary Key

primary_keys = contract["primary_key"]
df_silver = df_silver.dropDuplicates(primary_keys)

# COMMAND ----------
# DBTITLE 1,Filter Critical Nulls (Contract-Driven)

critical_cols = [
    c["name"]
    for c in contract["columns"]
    if not c.get("nullable", True)
]

if critical_cols:
    df_silver = df_silver.filter(
        reduce(lambda a, b: a & b,
               [F.col(c).isNotNull() for c in critical_cols])
    )

# COMMAND ----------
# DBTITLE 1,Filter Critical Nulls (Contract-Driven)

critical_cols = [
    c["name"]
    for c in contract["columns"]
    if not c.get("nullable", True)
]

if critical_cols:
    df_silver = df_silver.filter(
        reduce(lambda a, b: a & b,
               [F.col(c).isNotNull() for c in critical_cols])
    )

# COMMAND ----------
# DBTITLE 1,Derived / Structural Columns (Silver-Appropriate)

# Years at Company Bucket
if "YearsAtCompany" in df_silver.columns:
    df_silver = df_silver.withColumn(
        "YearsAtCompanyBucket",
        F.when(F.col("YearsAtCompany") < 3, "0-2")
         .when(F.col("YearsAtCompany") < 6, "3-5")
         .otherwise("6+")
    )

# Tenure Ratio (Safe division)
if {"YearsInCurrentRole", "YearsAtCompany"}.issubset(df_silver.columns):
    df_silver = df_silver.withColumn(
        "TenureRatio",
        F.when(F.col("YearsAtCompany") > 0,
               F.col("YearsInCurrentRole") / F.col("YearsAtCompany"))
         .otherwise(None)
    )

# Income Per Year at Company
if {"MonthlyIncome", "YearsAtCompany"}.issubset(df_silver.columns):
    df_silver = df_silver.withColumn(
        "IncomePerYearAtCompany",
        F.when(F.col("YearsAtCompany") > 0,
               F.col("MonthlyIncome") / F.col("YearsAtCompany"))
         .otherwise(None)
    )

# COMMAND ----------
# DBTITLE 1,Detect Silver Table Drift

if spark.catalog.tableExists(SILVER_TABLE):

    silver_schema = spark.table(SILVER_TABLE).schema
    silver_columns = {f.name: f.dataType.simpleString() for f in silver_schema}

    last_hash_row = spark.table(SILVER_SCHEMA_HASH) \
        .filter(F.col("table_name") == SILVER_TABLE) \
        .orderBy(F.col("migrated_at").desc()) \
        .select("schema_hash") \
        .first()

    last_hash = last_hash_row["schema_hash"] if last_hash_row else None

else:
    silver_columns = {}
    last_hash = None

alter_statements = []

for col in contract["columns"]:
    col_name = col["name"]
    col_type = type_mapping[col["type"].lower()].upper()

    if col_name not in silver_columns:
        alter_statements.append(
            f"ALTER TABLE {SILVER_TABLE} ADD COLUMN {col_name} {col_type}"
        )

for stmt in alter_statements:
    print(f"Executing: {stmt}")
    spark.sql(stmt)

# COMMAND ----------
# DBTITLE 1,Write Silver Table

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable(SILVER_TABLE)

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
    description = col.get("description", "").replace("'", "''")

    spark.sql(f"""
        COMMENT ON COLUMN {SILVER_TABLE}.{col_name} 
        IS '{description}'
    """)
