# Databricks notebook source
# DBTITLE 1,Import Dependencies
from pyspark.sql.functions import col, expr, split, lower, lit, when, size, date_format
import re

# COMMAND ----------

# DBTITLE 1,Configure Notebook / Assign Variables
lower_columns = ["name"]
useful_columns = ["difficulty","id", "name", "startTime", "endTime", "report_start_date", "report_id"]

# COMMAND ----------

# DBTITLE 1,Read Table
df = spark.read.table("01_bronze.warcraftlogs.fights")
display(df)

# COMMAND ----------

# DBTITLE 1,Transform
df = df.select(useful_columns)

def camel_to_snake(name):
    return re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()

df = df.toDF(*[camel_to_snake(col) for col in df.columns])

for column in lower_columns:
  df = df.withColumn(column, lower(col(column)))

df = df.withColumnRenamed("id", "pull_number") \
  .withColumnRenamed("report_start_date", "report_date")

df = df.withColumn("report_date", date_format(col("report_date"), "yyyy-MM-dd EE"))

df = df.where(col("difficulty").isNotNull())

df = df.withColumn(
    "difficulty",
    when(col("difficulty") == 1, "story_mode")
    .when(col("difficulty") == 2, "lfr")
    .when(col("difficulty") == 3, "normal")
    .when(col("difficulty") == 4, "heroic")
    .when(col("difficulty") == 5, "mythic")
    .otherwise("unknown")
)

# COMMAND ----------

display(df)

# COMMAND ----------

# DBTITLE 1,Write to Silver
df.write.mode("append").saveAsTable("02_silver.warcraftlogs.fights_boss_pulls")
