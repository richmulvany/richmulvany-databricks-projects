# Databricks notebook source
# DBTITLE 1,Dependencies
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
import yaml
import uuid

# COMMAND ----------

# DBTITLE 1,Configure Paths & Tables
DATASET_NAME = "hr_employees"
SILVER_TABLE = "02_silver.hr.ibm_analytics_hr_employees"

# Gold tables with new prefixes
GOLD_EMP_TABLE = "03_gold.hr.ibm_analytics_employees"
GOLD_DEPT_TABLE = "03_gold.hr.ibm_analytics_department_summary"
GOLD_JOB_TABLE = "03_gold.hr.ibm_analytics_job_summary"
GOLD_KPI_TABLE = "03_gold.hr.ibm_analytics_kpi_metrics"

# COMMAND ----------

# DBTITLE 1,Load Silver Table
df_silver = spark.table(SILVER_TABLE)
load_id = str(uuid.uuid4())
generated_at = datetime.utcnow()

# COMMAND ----------

# DBTITLE 1,Create Employee-Level Gold Table
df_emp_gold = df_silver.withColumn("_load_id", F.lit(load_id)) \
                       .withColumn("_generated_at", F.lit(generated_at))

df_emp_gold.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable(GOLD_EMP_TABLE)

# COMMAND ----------

# DBTITLE 1,Create Department Summary
dept_agg_exprs = [
    F.count("*").alias("num_employees"),
    F.round(F.avg("Age"), 1).alias("avg_age"),
    F.round(F.avg("YearsAtCompany"), 1).alias("avg_years_at_company"),
    F.round(F.avg("MonthlyIncome"), 1).alias("avg_monthly_income"),
    F.round(F.avg("PerformanceRating"), 1).alias("avg_performance_rating"),
    F.sum(F.when(F.col("Attrition") == "Yes", 1).otherwise(0)).alias("num_attritions")
]

df_dept_gold = df_silver.groupBy("Department") \
                        .agg(*dept_agg_exprs) \
                        .withColumn("_load_id", F.lit(load_id)) \
                        .withColumn("_generated_at", F.lit(generated_at))

df_dept_gold.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable(GOLD_DEPT_TABLE)

# COMMAND ----------

# DBTITLE 1,Create Job Role Summary
job_agg_exprs = [
    F.count("*").alias("num_employees"),
    F.round(F.avg("Age"), 1).alias("avg_age"),
    F.round(F.avg("YearsAtCompany"), 1).alias("avg_years_at_company"),
    F.round(F.avg("MonthlyIncome"), 1).alias("avg_monthly_income"),
    F.round(F.avg("PerformanceRating"), 1).alias("avg_performance_rating"),
    F.sum(F.when(F.col("Attrition") == "Yes", 1).otherwise(0)).alias("num_attritions")
]

df_job_gold = df_silver.groupBy("JobRole") \
                       .agg(*job_agg_exprs) \
                       .withColumn("_load_id", F.lit(load_id)) \
                       .withColumn("_generated_at", F.lit(generated_at))

df_job_gold.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable(GOLD_JOB_TABLE)

# COMMAND ----------

# DBTITLE 1,Create KPI / Metrics Table
df_kpi_gold = df_silver.agg(
    F.count("*").alias("total_employees"),
    F.sum(F.when(F.col("Attrition") == "Yes", 1).otherwise(0)).alias("total_attritions"),
    F.round(F.avg("MonthlyIncome"), 1).alias("avg_monthly_income"),
    F.round(F.avg("YearsAtCompany"), 1).alias("avg_tenure"),
    F.round(F.avg("PerformanceRating"), 1).alias("avg_performance_rating"),
    F.round(F.avg("JobSatisfaction"), 1).alias("avg_job_satisfaction"),
    F.round(F.avg("WorkLifeBalance"), 1).alias("avg_worklife_balance")
).withColumn("_load_id", F.lit(load_id)) \
 .withColumn("_generated_at", F.lit(generated_at))

df_kpi_gold.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable(GOLD_KPI_TABLE)

# COMMAND ----------

# DBTITLE 1,Automatically Generate Column Comments
contract_path = "/Workspace/Users/ricard.mulvany@gmail.com/richmulvany-databricks-projects/hr-chatbot/contracts/hr_employees_v0.yml"
with open(contract_path, "r") as f:
    contract_yaml = yaml.safe_load(f)

def generate_comments(table_name, columns):
    for col_name, desc in columns.items():
        desc_clean = desc.replace("'", "''")
        spark.sql(f"COMMENT ON COLUMN {table_name}.{col_name} IS '{desc_clean}'")

# Employee table comments from contract
emp_columns = {
    c["name"]: c.get("description", "")
    for c in contract_yaml["schema"]["business_columns"] + contract_yaml["schema"]["derived_columns"]
}
for meta_col in ["_load_id", "_generated_at"]:
    emp_columns[meta_col] = f"Metadata column {meta_col}"

generate_comments(GOLD_EMP_TABLE, emp_columns)

# Department summary comments
dept_columns = {c: c.replace("_", " ").capitalize() for c in df_dept_gold.columns}
generate_comments(GOLD_DEPT_TABLE, dept_columns)

# Job summary comments
job_columns = {c: c.replace("_", " ").capitalize() for c in df_job_gold.columns}
generate_comments(GOLD_JOB_TABLE, job_columns)

# KPI/metrics comments
kpi_columns = {c: c.replace("_", " ").capitalize() for c in df_kpi_gold.columns}
generate_comments(GOLD_KPI_TABLE, kpi_columns)

print("✅ Gold layer tables created.")
