# Databricks notebook source
# DBTITLE 1,Import Dependencies
from pyspark.sql.functions import col, expr, split, lower, lit, when, size, date_format, count, explode_outer, explode, sum, regexp_extract
from pyspark.sql.types import StringType
import re

# COMMAND ----------

# DBTITLE 1,Configure Notebook / Assign Variables
exclude_cols = ["report_id", "report_start_date", "report_date"]

# COMMAND ----------

# DBTITLE 1,Read Table
df = spark.read.table("01_bronze.warcraftlogs.tables")

# COMMAND ----------

# DBTITLE 1,Extract Metadata from Filename
def _prepare_entries_df(df):
    # Derive pull_number and data_type from source_file
    df = df.withColumn("pull_number", expr("try_cast(regexp_extract(source_file, '_fight(\\\d+)', 1) AS INT)"))
    df = df.withColumn("data_type", regexp_extract("source_file", "_table_([^_]+)", 1))
    df = df.withColumn("report_start_date", date_format(col("report_start_date"), "yyyy-MM-dd EE"))

    return df.select(
        "report_id",
        "pull_number",
        col("report_start_date").alias("report_date"),
        "data_type",
        explode_outer("table_json.data.entries").alias("entry")
    ).where(col("entry").isNotNull())

# COMMAND ----------

# DBTITLE 1,Establish Parsers
def _parse_damage_abilities(entry_df):
    return (entry_df
        .where((col("data_type") == "DamageDone") & col("entry.damage.abilities").isNotNull() & (size(col("entry.damage.abilities")) > 0))
        .select("report_id", "pull_number", "report_date", explode_outer("entry.damage.abilities").alias("damage_ability"))
        .select(
            "report_id", "pull_number", "report_date",
            col("damage_ability.name").alias("damage_ability_name"),
            col("damage_ability.total").alias("damage_ability_total"),
            col("damage_ability.totalReduced").alias("damage_ability_total_reduced"),
            col("damage_ability.type").alias("damage_ability_type")
        )
    )

def _parse_healing_abilities(entry_df):
    return (entry_df
        .where((col("data_type") == "Healing") & col("entry.healing.abilities").isNotNull() & (size(col("entry.healing.abilities")) > 0))
        .select("report_id", "pull_number", "report_date", explode_outer("entry.healing.abilities").alias("healing_ability"))
        .select(
            "report_id", "pull_number", "report_date",
            col("healing_ability.name").alias("healing_ability_name"),
            col("healing_ability.total").alias("healing_ability_total"),
            col("healing_ability.totalReduced").alias("healing_ability_total_reduced"),
            col("healing_ability.type").alias("healing_ability_type")
        )
    )

def _parse_events(entry_df):
    return (entry_df
        .where((col("data_type") == "Deaths") & col("entry.events").isNotNull() & (size(col("entry.events")) > 0))
        .select("report_id", "pull_number", "report_date", explode_outer("entry.events").alias("event"))
        .select(
            "report_id", "pull_number", "report_date",
            col("event.timestamp").alias("event_timestamp"),
            col("event.amount").alias("event_amount"),
            col("event.type").alias("event_type"),
            col("event.ability.name").alias("event_ability_name"),
            col("event.source.name").alias("event_source_name"),
            col("event.targetID").alias("event_target_id")
        )
    )

def _parse_gear(entry_df):
    return (entry_df
        .where(col("entry.gear").isNotNull())
        .select("report_id", "pull_number", "report_date", explode_outer("entry.gear").alias("gear"))
        .select(
            "report_id", "pull_number", "report_date",
            col("gear.id").alias("gear_id"),
            col("gear.name").alias("gear_name"),
            col("gear.itemLevel").alias("gear_item_level"),
            col("gear.slot").alias("gear_slot"),
            col("gear.icon").alias("gear_icon")
        )
    )

def _parse_pets(entry_df):
    return (entry_df
        .where(col("entry.pets").isNotNull())
        .select("report_id", "pull_number", "report_date", explode_outer("entry.pets").alias("pet"))
        .select(
            "report_id", "pull_number", "report_date",
            col("pet.name").alias("pet_name"),
            col("pet.total").alias("pet_total"),
            col("pet.type").alias("pet_type"),
            col("pet.guid").alias("pet_guid")
        )
    )


# COMMAND ----------

# DBTITLE 1,Split and Unpivot Dataframes
def parse_by_data_type(df):
    entry_df = _prepare_entries_df(df)
    
    results = {
        "damage_abilities": _parse_damage_abilities(entry_df),
        "healing_abilities": _parse_healing_abilities(entry_df),
        "events": _parse_events(entry_df),
    }

    # Gear and pets parsed for all data types
    for dtype in ["DamageDone", "Healing", "Deaths"]:
        df_filtered = entry_df.where(col("data_type") == dtype)
        results[f"gear_{dtype}"] = _parse_gear(df_filtered)
        results[f"pets_{dtype}"] = _parse_pets(df_filtered)

    return results

parsed = parse_by_data_type(df)

# COMMAND ----------

# DBTITLE 1,Transform and Export
def _lowercase_string_columns(df, exclude_cols=None):
    if exclude_cols is None:
        exclude_cols = []
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType) and f.name not in exclude_cols]
    for col_name in string_cols:
        df = df.withColumn(col_name, lower(col(col_name)))
    return df

def clean_and_export(parsed_dfs, df_summary=None):
    if df_summary is not None:
        df_summary = df_summary.where(col("player_class").isNotNull()).where(col("player_class") != "npc")
        names = [row["player_name"] for row in df_summary.select("player_name").distinct().collect()]
    else:
        names = []

    for name, df in parsed_dfs.items():
        df = _lowercase_string_columns(df)
        if "player_name" in df.columns and names:
            df = df.where(col("player_name").isin(names))
        table_suffix = name.removeprefix("df_") if name.startswith("df_") else name
        df.write.mode("overwrite").saveAsTable(f"02_silver.staging.warcraftlogs_tables_{table_suffix}")

clean_and_export(parsed)
