# Databricks notebook source
# DBTITLE 1,Install Dependencies
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql import Row
from datetime import datetime
import yaml
import uuid

# COMMAND ----------

# DBTITLE 1,Configure Notebook
DATASET_NAME = "hr_employees"
RAW_PATH = "/Volumes/01_bronze/hr/ibm_analytics/WA_Fn-UseC_-HR-Employee-Attrition.csv"
BRONZE_TABLE = "01_bronze.hr.ibm_analytics_hr_employees"
VIOLATIONS_TABLE = "00_governance.contracts.contract_violations"

FAIL_ON_VIOLATION = True
load_id = str(uuid.uuid4())

# COMMAND ----------

# DBTITLE 1,Establish Violation Logger
def log_violation(
    violation_type,
    column_name,
    violation_details,
    record_count
):
    violation_row = Row(
        dataset_name=DATASET_NAME,
        contract_version=CONTRACT_VERSION,
        violation_type=violation_type,
        column_name=column_name,
        violation_details=violation_details,
        record_count=record_count,
        detected_at=datetime.utcnow()
    )

    spark.createDataFrame([violation_row]) \
         .write.mode("append") \
         .saveAsTable(VIOLATIONS_TABLE)

    if FAIL_ON_VIOLATION:
        raise Exception(
            f"{violation_type} detected on {column_name}: {violation_details}"
        )

# COMMAND ----------

# DBTITLE 1,Load Contract
contract_df = (
    spark.table("00_governance.contracts.contract_registry")
    .filter(
        (F.col("dataset_name") == DATASET_NAME) &
        (F.col("is_active") == True)
    )
    .orderBy(F.col("deployed_at").desc())
)

contract_row = contract_df.limit(1).collect()

if not contract_row:
    raise Exception(f"No active contract found for {DATASET_NAME}")

contract_yaml = yaml.safe_load(contract_row[0]["contract_yaml"])

CONTRACT_VERSION = contract_yaml["version"]
expected_columns = contract_yaml["columns"]
primary_keys = contract_yaml["primary_key"]

# COMMAND ----------

# DBTITLE 1,Read Raw Data
df_raw = (
    spark.read
    .option("header", True)
    .csv(RAW_PATH)
)

# COMMAND ----------

# DBTITLE 1,Validate Column Names
actual_cols = set(df_raw.columns)
expected_cols = {c["name"] for c in expected_columns}

missing = expected_cols - actual_cols
extra = actual_cols - expected_cols

if missing:
    log_violation(
        "MISSING_COLUMNS",
        "N/A",
        f"Missing columns: {missing}",
        0
    )

if extra:
    log_violation(
        "UNEXPECTED_COLUMNS",
        "N/A",
        f"Unexpected columns: {extra}",
        0
    )

# COMMAND ----------

# DBTITLE 1,Apply Contract Schema

type_mapping = {
    "string": StringType(),
    "integer": IntegerType(),
    "boolean": BooleanType()
}

for col in expected_columns:
    col_name = col["name"]
    col_type = col["type"].lower()

    spark_type = type_mapping.get(col_type)

    if not spark_type:
        raise Exception(f"Unsupported type in contract: {col_type}")

    df_raw = df_raw.withColumn(
        col_name,
        F.col(col_name).cast(spark_type)
    )

# COMMAND ----------

# DBTITLE 1,Check Non-Nullable Columns
for col in expected_columns:
    if not col["nullable"]:
        null_count = df_raw.filter(F.col(col["name"]).isNull()).count()

        if null_count > 0:
            log_violation(
                "NULLABILITY_VIOLATION",
                col["name"],
                "Non-nullable column contains NULL values",
                null_count
            )

# COMMAND ----------

# DBTITLE 1,Check Allowed Values
for col in expected_columns:
    if "allowed_values" in col:
        col_name = col["name"]
        allowed = col["allowed_values"]

        invalid_count = (
            df_raw
            .filter(~F.col(col_name).isin(allowed))
            .count()
        )

        if invalid_count > 0:
            log_violation(
                "DOMAIN_VIOLATION",
                col_name,
                f"Values outside allowed set {allowed}",
                invalid_count
            )

# COMMAND ----------

# DBTITLE 1,Check Primary Key Uniqueness
duplicate_count = (
    df_raw
    .groupBy(primary_keys)
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if duplicate_count > 0:
    log_violation(
        "PRIMARY_KEY_VIOLATION",
        ",".join(primary_keys),
        "Duplicate primary keys detected",
        duplicate_count
    )

# COMMAND ----------

# DBTITLE 1,Check Schema Evolution
if spark.catalog.tableExists(BRONZE_TABLE):

    existing_schema = spark.table(BRONZE_TABLE).schema
    existing_fields = {f.name: f.dataType.simpleString() for f in existing_schema}

    expected_fields = {
        col["name"]: type_mapping[col["type"].lower()].simpleString()
        for col in expected_columns
    }

    for field_name, field_type in expected_fields.items():
        if field_name in existing_fields:
            if existing_fields[field_name] != field_type:
                log_violation(
                    "SCHEMA_EVOLUTION",
                    field_name,
                    f"Expected {field_type}, found {existing_fields[field_name]}",
                    0
                )

# COMMAND ----------

# DBTITLE 1,Add Metadata
df_bronze = (
    df_raw
    .withColumn("_ingestion_timestamp", F.current_timestamp())
    .withColumn("_source_file", F.col('_metadata.file_path'))
    .withColumn("_contract_version", F.lit(CONTRACT_VERSION))
    .withColumn("_load_id", F.lit(load_id))
)

# COMMAND ----------

df_bronze.printSchema()

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
