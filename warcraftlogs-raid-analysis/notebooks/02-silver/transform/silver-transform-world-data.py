# Databricks notebook source
# DBTITLE 1,Import Dependencies
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, expr, split, lower, lit, when, size, date_format, explode_outer, regexp_replace
import re

# COMMAND ----------

# DBTITLE 1,Read Tables
world_data = spark.read.table("01_bronze.warcraftlogs.world_data")

# COMMAND ----------

# DBTITLE 1,Unpivot Dataframes
expansions = world_data.select(
    explode_outer("expansions").alias("expansion")
).select(
    col("expansion.id").alias("expansion_id"),
    col("expansion.name").alias("expansion_name"),
)

zones = world_data.select(
    explode_outer("zones").alias("zone")
).select(
    col("zone.id").alias("zone_id"),
    col("zone.name").alias("zone_name"),
    col("zone.frozen").alias("zone_frozen")
)

# COMMAND ----------

# DBTITLE 1,Transform
for name, df in [("expansions", expansions), ("zones", zones)]:
    for column, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(column, lower(column))
            df = df.withColumn(column, regexp_replace(column, r"[ ,-]", "_"))
    globals()[name] = df

# COMMAND ----------

# DBTITLE 1,Write to Staging Area
for name, df in [("expansions", expansions), ("zones", zones)]:
    df.write.mode("overwrite").saveAsTable(f"02_silver.staging.warcraftlogs_world_data_{name}")
    print(f"✅ Table 02_silver.staging.warcraftlogs_world_data_{name} written.")
