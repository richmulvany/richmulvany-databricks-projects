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
# Explode each collection from the nested world_data structure.  This includes
# expansions, zones and their encounters (bosses).  Encounters are nested inside
# each zone in the bronze world_data payload.
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
    col("zone.frozen").alias("zone_frozen"),
)

# Encounters (bosses) per zone.  Each encounter record retains the parent zone
# identifiers so that relationships are preserved downstream.  If a zone has no
# encounters, explode_outer yields null rows which will be dropped by
# subsequent transformations.
encounters = (
    world_data
    .select(explode_outer("zones").alias("zone"))
    .select(
        col("zone.id").alias("zone_id"),
        col("zone.name").alias("zone_name"),
        explode_outer("zone.encounters").alias("encounter")
    )
    .select(
        "zone_id", "zone_name",
        col("encounter.id").alias("boss_id"),
        col("encounter.name").alias("boss_name")
    )
)

# COMMAND ----------

# DBTITLE 1,Transform
def normalize_columns(df):
    """Lowercase and sanitize string columns by replacing spaces and punctuation."""
    for column, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(column, lower(col(column)))
            df = df.withColumn(column, regexp_replace(col(column), r"[ ,\-]", "_"))
    return df

expansions = normalize_columns(expansions)
zones = normalize_columns(zones)
encounters = normalize_columns(encounters)

# COMMAND ----------

# DBTITLE 1,Write to Staging Area
for name, df in [
    ("expansions", expansions),
    ("zones", zones),
    ("encounters", encounters),
]:
    df.write.mode("overwrite").saveAsTable(f"02_silver.staging.warcraftlogs_world_data_{name}")
    print(f"✅ Table 02_silver.staging.warcraftlogs_world_data_{name} written.")
