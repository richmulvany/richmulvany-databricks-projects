# Databricks notebook source
# DBTITLE 1,Import Dependencies
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, expr, split, lower, lit, when, size, date_format, explode_outer, regexp_replace
import re

# COMMAND ----------

# DBTITLE 1,Read Tables
guild_roster = spark.read.table("01_bronze.warcraftlogs.guild_roster")

# COMMAND ----------

# DBTITLE 1,Select Data
guild_roster = guild_roster.where(col("guild_name") == "Student Council")
guild_roster = guild_roster.select(
    col("guildRank").alias("guild_rank"),
    col("name").alias("player_name"),
    col("id").alias("player_id"),
    col("classID").alias("player_class_id"),
    col("level").alias("player_level"),
    col("faction.name").alias("player_faction")
)

# COMMAND ----------

# DBTITLE 1,Transform
for column, dtype in guild_roster.dtypes:
    if dtype == "string":
        guild_roster = guild_roster.withColumn(column, lower(column))
        guild_roster = guild_roster.withColumn(column, regexp_replace(column, r"[ ,-]", "_"))

# COMMAND ----------

# DBTITLE 1,Write to Staging Area
guild_roster.write.mode("overwrite").saveAsTable(f"02_silver.staging.warcraftlogs_guild_roster")
print(f"✅ Table 02_silver.staging.warcraftlogs_guild_roster written.")
