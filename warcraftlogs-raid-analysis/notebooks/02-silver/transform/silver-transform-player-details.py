# Databricks notebook source
# DBTITLE 1,Import Dependencies
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, expr, split, lower, lit, when, size, date_format, regexp_replace
import re

# COMMAND ----------

# DBTITLE 1,Read Tables
player_details = spark.read.table("01_bronze.warcraftlogs.player_details")

# COMMAND ----------

# DBTITLE 1,Transform
player_details = player_details.withColumn("icon", lower(col("icon"))) \
    .withColumn("cls_spec", split(col("icon"), "-")) \
    .withColumn("player_class", col("cls_spec")[0]) \
    .withColumn("player_spec", when(size(col("cls_spec")) > 1, col("cls_spec")[1]).otherwise("multiple")) \
    .withColumn("report_start", date_format(col("report_start"), "yyyy-MM-dd E")) \
    .select(
        "report_id",
        col("report_start").alias("report_date"),
        col("name").alias("player_name"),
        col("id").alias("player_id"),
        col("guid").alias("player_guid"),
        col("role").alias("player_role"),
        "player_class",
        "player_spec",
        col("maxItemLevel").alias("player_item_level")
)

lower_cols = ["player_name", "player_class", "player_spec", "player_role"]

for c in lower_cols:
    player_details = player_details.withColumn(c, lower(c))
    player_details = player_details.withColumn(c, regexp_replace(c, r"[ ,-]", "_"))
    
player_details = player_details.dropDuplicates()

# COMMAND ----------

# DBTITLE 1,Write to Staging Area
player_details.write.mode("overwrite").saveAsTable(f"02_silver.staging.warcraftlogs_player_details")
print(f"✅ Table 02_silver.staging.warcraftlogs_player_details written.")
