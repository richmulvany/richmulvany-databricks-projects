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

SILVER_TABLE = "02_silver.hr.ibm_analytics_hr_employees"
GOLD_EMP_TABLE = "03_gold.hr.employee_level"
GOLD_DEPT_TABLE = "03_gold.hr.department_summary"
GOLD_JOB_TABLE = "03_gold.hr.jobrole_summary"
GOLD_KPI_TABLE = "03_gold.hr.kpi_metrics"

GOLD_SCHEMA_HASH = "00_governance.contracts.gold_schema_hash"
CONTRACT_REGISTRY = "00_governance.contracts.contract_registry"

# COMMAND ----------
# DBTITLE 1,Load Active Contract
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

business_cols = [c["name"] for c in contract["schema"]["business_columns"]]
derived_cols = [c["name"] for c in contract["schema"]["derived_columns"]]

metadata_cols = [
    "_ingestion_timestamp",
    "_source_file",
    "_contract_version",
    "_load_id"
]

final_cols = business_cols + derived_cols + metadata_cols

# COMMAND ----------
# DBTITLE 1,Load Silver Table
df_silver = spark.table(SILVER_TABLE)

# COMMAND ----------
# DBTITLE 1,Employee-Level Gold Table
df_employee = df_silver.select(*[c for c in final_cols if c in df_silver.columns])

df_employee.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable(GOLD_EMP_TABLE)

# Update schema hash
spark.createDataFrame([{
    "table_name": GOLD_EMP_TABLE,
    "schema_hash": schema_hash,
    "migrated_at": datetime.utcnow()
}]).write.format("delta").mode("append").saveAsTable(GOLD_SCHEMA_HASH)

# COMMAND ----------
# DBTITLE 1,Department-Level Summary Table
df_dept = (
    df_employee.groupBy("Department")
    .agg(
        F.count("EmployeeNumber").alias("num_employees"),
        F.avg("Age").alias("avg_age"),
        F.avg("YearsAtCompany").alias("avg_years_at_company"),
        F.avg("MonthlyIncome").alias("avg_monthly_income"),
        F.avg("PerformanceRating").alias("avg_performance_rating"),
        F.sum(F.when(F.col("Attrition") == "Yes", 1).otherwise(0)).alias("num_attritions")
    )
)

df_dept.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable(GOLD_DEPT_TABLE)

spark.createDataFrame([{
    "table_name": GOLD_DEPT_TABLE,
    "schema_hash": schema_hash,
    "migrated_at": datetime.utcnow()
}]).write.format("delta").mode("append").saveAsTable(GOLD_SCHEMA_HASH)

# COMMAND ----------
# DBTITLE 1,JobRole-Level Summary Table
df_job = (
    df_employee.groupBy("JobRole")
    .agg(
        F.count("EmployeeNumber").alias("num_employees"),
        F.avg("Age").alias("avg_age"),
        F.avg("YearsAtCompany").alias("avg_years_at_company"),
        F.avg("MonthlyIncome").alias("avg_monthly_income"),
        F.avg("PerformanceRating").alias("avg_performance_rating"),
        F.sum(F.when(F.col("Attrition") == "Yes", 1).otherwise(0)).alias("num_attritions")
    )
)

df_job.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable(GOLD_JOB_TABLE)

spark.createDataFrame([{
    "table_name": GOLD_JOB_TABLE,
    "schema_hash": schema_hash,
    "migrated_at": datetime.utcnow()
}]).write.format("delta").mode("append").saveAsTable(GOLD_SCHEMA_HASH)

# COMMAND ----------
# DBTITLE 1,KPI / Metrics Table
# Example KPIs for chatbot / analytics
df_kpi = df_employee.agg(
    F.count("EmployeeNumber").alias("total_employees"),
    F.sum(F.when(F.col("Attrition") == "Yes", 1).otherwise(0)).alias("total_attritions"),
    F.avg("MonthlyIncome").alias("avg_monthly_income"),
    F.avg("YearsAtCompany").alias("avg_tenure"),
    F.avg("PerformanceRating").alias("avg_performance_rating"),
    F.avg("JobSatisfaction").alias("avg_job_satisfaction"),
    F.avg("WorkLifeBalance").alias("avg_worklife_balance")
)

df_kpi = df_kpi.withColumn("_load_id", F.lit(str(uuid.uuid4()))) \
               .withColumn("_generated_at", F.current_timestamp())

df_kpi.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable(GOLD_KPI_TABLE)

spark.createDataFrame([{
    "table_name": GOLD_KPI_TABLE,
    "schema_hash": schema_hash,
    "migrated_at": datetime.utcnow()
}]).write.format("delta").mode("append").saveAsTable(GOLD_SCHEMA_HASH)

# COMMAND ----------
# DBTITLE 1,Add Column Comments for Gold Tables
# Mapping: table_name -> {column_name -> description}
gold_comments = {
    GOLD_EMP_TABLE: {
        col["name"]: col.get("description", "").replace("'", "''")
        for col in contract["schema"]["business_columns"] + contract["schema"]["derived_columns"]
    },
    GOLD_DEPT_TABLE: {
        "Department": "Department name",
        "num_employees": "Number of employees in department",
        "avg_age": "Average age in department",
        "avg_years_at_company": "Average tenure in department",
        "avg_monthly_income": "Average monthly income in department",
        "avg_performance_rating": "Average performance rating in department",
        "num_attritions": "Number of employees who left in department"
    },
    GOLD_JOB_TABLE: {
        "JobRole": "Job role name",
        "num_employees": "Number of employees in job role",
        "avg_age": "Average age in job role",
        "avg_years_at_company": "Average tenure in job role",
        "avg_monthly_income": "Average monthly income in job role",
        "avg_performance_rating": "Average performance rating in job role",
        "num_attritions": "Number of employees who left in job role"
    },
    GOLD_KPI_TABLE: {
        "total_employees": "Total number of employees",
        "total_attritions": "Total number of employees who left",
        "avg_monthly_income": "Average monthly income across employees",
        "avg_tenure": "Average tenure across employees",
        "avg_performance_rating": "Average performance rating",
        "avg_job_satisfaction": "Average job satisfaction",
        "avg_worklife_balance": "Average work-life balance",
        "_load_id": "Load identifier for this aggregation",
        "_generated_at": "Timestamp when this KPI row was generated"
    }
}

# Apply comments
for table, cols in gold_comments.items():
    for col_name, desc in cols.items():
        spark.sql(f"COMMENT ON COLUMN {table}.{col_name} IS '{desc}'")
