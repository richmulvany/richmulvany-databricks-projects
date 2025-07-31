# Databricks notebook source
# DBTITLE 1,Import Dependencies
from pyspark.sql.functions import col, expr, split, lower, lit, when, size, date_format, round
import re

# COMMAND ----------

# DBTITLE 1,Configure Notebook / Assign Variables
lower_columns = ["raid_name", "boss_name"]

# COMMAND ----------

# DBTITLE 1,Read Table
df = spark.read.table("01_bronze.warcraftlogs.fights")

# COMMAND ----------

# DBTITLE 1,Transform
df = df.select(
    "report_id",
    col("report_start").alias("report_date"),
    col("gameZone.name").alias("raid_name"),
    col("name").alias("boss_name"),
    col("difficulty").alias("raid_difficulty"),
    col("id").alias("pull_number"),
    col("averageItemLevel").alias("average_item_level"),
    col("startTime").alias("pull_start_time"),
    col("endTime").alias("pull_end_time"),
    col("lastPhase").alias("last_phase"),
    col("lastPhaseIsIntermission").alias("last_phase_is_intermission"),
    col("fightPercentage").alias("boss_percentage"),
    "kill"
)

for column in lower_columns:
  df = df.withColumn(column, lower(col(column)))

df = df.withColumn("report_date", date_format(col("report_date"), "yyyy-MM-dd EE"))

df = df.withColumn("average_item_level", round(col("average_item_level"), 2))

# Remove invis resets
df = df.where(col("raid_difficulty").isNotNull())

df = df.withColumn(
    "raid_difficulty",
    when(col("raid_difficulty") == 1, "story_mode")
    .when(col("raid_difficulty") == 2, "lfr")
    .when(col("raid_difficulty") == 3, "normal")
    .when(col("raid_difficulty") == 4, "heroic")
    .when(col("raid_difficulty") == 5, "mythic")
    .otherwise("unknown")
)

# COMMAND ----------

# DBTITLE 1,Write TempView
df.write.mode("overwrite").saveAsTable("02_silver.staging.warcraftlogs_fights_boss_pulls")
