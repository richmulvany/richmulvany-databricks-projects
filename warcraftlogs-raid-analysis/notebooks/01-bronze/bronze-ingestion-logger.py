# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# DBTITLE 1,Retrieve Variable
report_id = dbutils.jobs.taskValues.get(key="report_id", taskKey="bronze_ingestion_events-task")

# COMMAND ----------

# DBTITLE 1,Log Ingestion
log_df = spark.createDataFrame([(report_id,)], ["report_id"]) \
    .withColumn("ingested_at", current_timestamp())
 
log_df.write.mode("append").saveAsTable("raid_report_tracking")
print(f"📌 Logged report {report_id} as ingested.")
