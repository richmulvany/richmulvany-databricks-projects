# Databricks notebook source
# DBTITLE 1,Import Dependencies
import dlt
from pyspark.sql.functions import col, explode, from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType, IntegerType, DoubleType

# COMMAND ----------

# --- bronze: raw API responses as JSON strings in a mount or volume
@dlt.table(name="bronze_raw_reports", comment="Raw GraphQL responses from WarcraftLogs API")
def bronze_raw_reports():
    # If you use Auto Loader, you can use spark.readStream with cloudFiles; in DLT use spark.read
    bronze_path = "/Volumes/01_bronze/warcraftlogs/raw_api_calls/"
    df = spark.read.format("json").load(bronze_path)
    # Keep original json payload for lineage and reprocessing
    return df

# --- silver: normalized reports table
@dlt.table(name="silver_reports", comment="One row per report metadata")
def silver_reports():
    df = dlt.read("bronze_raw_reports")
    # adapt field names based on the GraphQL response; this is an example mapping
    reports = (df.select(
        col("report.id").alias("report_id"),
        col("report.owner.name").alias("owner_name"),
        col("report.title").alias("title"),
        col("report.zoneName").alias("zone_name"),
        col("report.startTime").alias("start_time"),
        col("report.endTime").alias("end_time"),
        col("report.jsonPayload").alias("payload")
    ))
    return reports

# --- silver: fights
@dlt.table(name="silver_fights", comment="Fight-level metadata extracted from each report")
def silver_fights():
    reports = dlt.read("silver_reports")
    # assume payload contains an array 'fights'
    fights = reports.selectExpr("report_id", "explode(payload.fights) as fight")
    fights_flat = fights.select(
        col("report_id"),
        col("fight.id").alias("fight_id"),
        col("fight.name").alias("name"),
        col("fight.startTime").alias("start_time"),
        col("fight.endTime").alias("end_time"),
        col("fight.bossPercentage").alias("boss_percentage")
    )
    return fights_flat

# --- silver: events (example extracting first-death events)
@dlt.table(name="silver_events_first_death", comment="First death events per actor per pull")
def silver_events_first_death():
    reports = dlt.read("silver_reports")
    # payload.events should be present if you requested a table with events
    events = reports.selectExpr("report_id", "explode(payload.events) as event")
    first_deaths = events.filter("event.type = 'death'")
    fd = first_deaths.select(
        col("report_id"),
        col("event.timestamp").alias("timestamp"),
        col("event.targetID").alias("actor_id"),
        col("event.sourceName").alias("source_name")
    )
    return fd

# --- gold: simple player performance rollup
@dlt.table(name="gold_player_summary", comment="Aggregated per-player DPS/HPS and first death stats")
def gold_player_summary():
    fights = dlt.read("silver_fights")
    events = dlt.read("silver_events_first_death")
    # placeholder: join and aggregate using actual fields your payload exposes
    # This is intentionally simple — adapt to real fields from your flattened schema
    player_summary = events.groupBy("actor_id").count().withColumnRenamed("count","first_deaths")
    return player_summary
