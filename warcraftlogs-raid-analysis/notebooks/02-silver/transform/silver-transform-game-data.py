# Databricks notebook source
# DBTITLE 1,Import Dependencies
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, expr, split, lower, lit, when, size, date_format, explode_outer, regexp_replace
import re

# COMMAND ----------

# DBTITLE 1,Read Tables
game_data = spark.read.table("01_bronze.warcraftlogs.game_data")

# COMMAND ----------

# DBTITLE 1,Unpivot Dataframes
abilities = game_data.select(
    explode_outer("abilities").alias("ability")
).select(
    col("ability.id").alias("ability_id"),
    col("ability.name").alias("ability_name"),
    col("ability.icon").alias("ability_icon"),
)

classes = game_data.select(
    explode_outer("classes").alias("class")
).select(
    col("class.id").alias("class_id"),
    col("class.slug").alias("class_name"),
    explode_outer("class.specs").alias("spec")
).select(
    "class_id",
    "class_name",
    col("spec.id").alias("spec_id"),
    col("spec.slug").alias("spec_name")
)

items = game_data.select(
    explode_outer("items").alias("item")
).select(
    col("item.id").alias("item_id"),
    col("item.name").alias("item_name"),
    col("item.icon").alias("item_icon")
)

zones = game_data.select(
    explode_outer("zones").alias("zone")
).select(
    col("zone.id").alias("zone_id"),
    col("zone.name").alias("zone_name")
)

# COMMAND ----------

# DBTITLE 1,Transform
for name, df in [("abilities", abilities), ("classes", classes), ("items", items), ("zones", zones)]:
    for column, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(column, lower(column))
            df = df.withColumn(column, regexp_replace(column, r"[ ,-]", "_"))
    globals()[name] = df

# COMMAND ----------

# DBTITLE 1,Write to Staging Area
for name, df in [("abilities", abilities), ("classes", classes), ("items", items), ("zones", zones)]:
    df.write.mode("overwrite").saveAsTable(f"02_silver.staging.warcraftlogs_game_data_{name}")
    print(f"✅ Table 02_silver.staging.warcraftlogs_game_data_{name} written.")
