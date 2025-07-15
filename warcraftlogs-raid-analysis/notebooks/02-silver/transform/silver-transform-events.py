# Databricks notebook source
# DBTITLE 1,Import Dependencies
from pyspark.sql.functions import col, expr, split, lower, lit, when, size, date_format, count
import re

# COMMAND ----------

# DBTITLE 1,Configure Notebook / Assign Variables
cols_to_drop = ["bronze_ingestion_time", "source_file"]

# COMMAND ----------

# DBTITLE 1,Read Table
df = spark.read.table("01_bronze.warcraftlogs.events")

# COMMAND ----------

# DBTITLE 1,Transform
def camel_to_snake(name):
    # normal transitions (e.g. GameID → Game_ID)
    name = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', name) 
    # handle acronyms at end (e.g. GameID → Game_ID)
    name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name)       
    return name.lower()

df = df.toDF(*[camel_to_snake(column_name) for column_name in df.columns])

df = df.withColumnRenamed("fight", "pull_number") \
  .withColumnRenamed("report_start", "report_date")

df = df.withColumn("report_date", date_format(col("report_date"), "yyyy-MM-dd EE"))

# COMMAND ----------

# DBTITLE 1,Parse
types_list = [row["type"] for row in df.select("type").distinct().collect()]
print(types_list)

buff_types = [t for t in types_list if "empower" in t]
debuff_types = [t for t in types_list if "debuff" in t]
cast_types = [t for t in types_list if "cast" in t]

df_casts = df.where(col("type").isin(cast_types))
df_buffs = df.where(col("type").isin(buff_types))
df_debuffs = df.where(col("type").isin(debuff_types))
df_deaths = df.where(col("type") == "death")

# COMMAND ----------

# DBTITLE 1,Remove Redundant Columns
def drop_empty_columns(df):
    # Get non-null counts for all columns
    non_null_counts = df.select([count(col(c)).alias(c) for c in df.columns]).collect()[0]
    # Identify columns that are fully null
    empty_cols = [c for c in df.columns if non_null_counts[c] == 0]
    # Log which columns are being dropped
    if empty_cols:
        print(f"Dropping empty columns: {', '.join(empty_cols)}")
    else:
        print("No empty columns to drop.")
    # Select only non-empty columns
    non_empty_cols = [c for c in df.columns if non_null_counts[c] > 0]
    return df.select(*non_empty_cols)

df_casts = drop_empty_columns(df_casts)
df_deaths = drop_empty_columns(df_deaths)
df_buffs = drop_empty_columns(df_buffs)
df_debuffs = drop_empty_columns(df_debuffs)

df_casts = df_casts.drop(*cols_to_drop)
df_deaths = df_deaths.drop(*cols_to_drop)
df_buffs = df_buffs.drop(*cols_to_drop)
df_debuffs = df_debuffs.drop(*cols_to_drop)

# COMMAND ----------

# DBTITLE 1,Write TempView
df_casts.write.mode("overwrite").saveAsTable("02_silver.staging.warcraftlogs_events_casts")
df_buffs.write.mode("overwrite").saveAsTable("02_silver.staging.warcraftlogs_events_buffs")
df_debuffs.write.mode("overwrite").saveAsTable("02_silver.staging.warcraftlogs_events_debuffs")
df_deaths.write.mode("overwrite").saveAsTable("02_silver.staging.warcraftlogs_events_deaths")
