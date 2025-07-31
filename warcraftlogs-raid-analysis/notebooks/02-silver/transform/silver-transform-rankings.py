# Databricks notebook source
# DBTITLE 1,Import Dependencies
from pyspark.sql.functions import col, lower, regexp_replace, date_format

# COMMAND ----------

# DBTITLE 1,Read Bronze Rankings Tables
# Read the bronze tables for DPS and HPS rankings.  These tables contain a
# single column named `rankings` which holds the JSON output of the
# GraphQL rankings call, along with metadata columns such as report_start,
# report_id and bronze_ingestion_time.  We'll standardize column names and
# prepare them for downstream consumption by renaming the JSON field.
rankings_dps_raw = spark.read.table("01_bronze.warcraftlogs.rankings_dps")
rankings_hps_raw = spark.read.table("01_bronze.warcraftlogs.rankings_hps")

# COMMAND ----------

# DBTITLE 1,Transform DPS Rankings
rankings_dps = (
    rankings_dps_raw
    .withColumnRenamed("rankings", "rankings_json")
    .withColumn("report_date", date_format(col("report_start"), "yyyy-MM-dd E"))
    .select(
        "report_id",
        "rankings_json",
        "report_date",
        "bronze_ingestion_time"
    )
)

# Lowercase and sanitize string columns where appropriate (report_id and
# report_date are already standardized; rankings_json is left untouched)
for c, dtype in rankings_dps.dtypes:
    if dtype == "string" and c != "rankings_json":
        rankings_dps = rankings_dps.withColumn(c, lower(col(c)))
        rankings_dps = rankings_dps.withColumn(c, regexp_replace(col(c), r"[ ,\-]", "_"))

# COMMAND ----------

# DBTITLE 1,Transform HPS Rankings
rankings_hps = (
    rankings_hps_raw
    .withColumnRenamed("rankings", "rankings_json")
    .withColumn("report_date", date_format(col("report_start"), "yyyy-MM-dd E"))
    .select(
        "report_id",
        "rankings_json",
        "report_date",
        "bronze_ingestion_time"
    )
)
for c, dtype in rankings_hps.dtypes:
    if dtype == "string" and c != "rankings_json":
        rankings_hps = rankings_hps.withColumn(c, lower(col(c)))
        rankings_hps = rankings_hps.withColumn(c, regexp_replace(col(c), r"[ ,\-]", "_"))

# COMMAND ----------

# DBTITLE 1,Write to Staging Area
# Write each ranking DataFrame to its own staging table.  Downstream jobs can
# parse the JSON contained in `rankings_json` as needed.
rankings_dps.write.mode("overwrite").saveAsTable("02_silver.staging.warcraftlogs_rankings_dps")
print("✅ Table 02_silver.staging.warcraftlogs_rankings_dps written.")
rankings_hps.write.mode("overwrite").saveAsTable("02_silver.staging.warcraftlogs_rankings_hps")
print("✅ Table 02_silver.staging.warcraftlogs_rankings_hps written.")
