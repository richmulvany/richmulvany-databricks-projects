# Databricks notebook source
# DBTITLE 1,Import Dependencies
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# DBTITLE 1,Retrieve Variable
# Exit early if report_id is not available as data probably already ingested
try:
    report_id = dbutils.jobs.taskValues.get(key="report_id", taskKey="bronze_ingestion_events-task")
except Exception as e:
    print(f"⚠️ Could not retrieve 'report_id' from dbutils.variables: {e}")
    dbutils.notebook.exit("Exiting: report_id not available via dbutils.variables.")

# COMMAND ----------

# DBTITLE 1,Log Extraction
log_df = spark.createDataFrame([(report_id,)], ["report_id"]) \
    .withColumn("extracted_at", current_timestamp())
 
log_df.write.mode("append").saveAsTable("01_bronze.logs.warcraftlogs_extraction_log")
print(f"📌 Logged report {report_id} as extracted.")
