# Databricks notebook source
# DBTITLE 1,Import Dependencies
from pyspark.sql.functions import col, expr, split, lower, lit, when, size, date_format, count, explode_outer, explode, sum, regexp_extract
from pyspark.sql.types import StringType
import re
from pyspark.sql import DataFrame

# COMMAND ----------
# DBTITLE 1,Configure Notebook / Assign Variables
exclude_cols = ["report_id", "report_start_date", "report_date"]

# COMMAND ----------
# DBTITLE 1,Read Table
df = spark.read.table("01_bronze.warcraftlogs.tables")

# COMMAND ----------
# DBTITLE 1,Extract Metadata from Filename
def prepare_entries_df(df):
    df = df.withColumn("pull_number", expr("try_cast(regexp_extract(source_file, '_fight(\\d+)', 1) AS INT)"))
    df = df.withColumn("data_type", regexp_extract("source_file", "_table_([^_]+)", 1))
    df = df.withColumn("report_start_date", date_format(col("report_start_date"), "yyyy-MM-dd EE"))

    return df.select(
        "report_id",
        "pull_number",
        col("report_start_date").alias("report_date"),
        "data_type",
        explode_outer("table_json.data.entries").alias("entry")
    ).where(col("entry").isNotNull())

entry_df = prepare_entries_df(df)

# COMMAND ----------
# DBTITLE 1,Establish Parsers
def _extract_healing_summary(df):
    return df.where(col("data_type") == "Healing").select(
        "report_id", "pull_number", "report_date",
        col("entry.name").alias("player_name"),
        col("entry.total").alias("healing_total"),
        col("entry.overheal").alias("overhealing"),
        col("entry.activeTime").alias("active_time"),
        col("entry.type").alias("player_class"),
        col("entry.itemLevel").alias("item_level")
    )

def _extract_deaths_summary(df):
    return df.where(col("data_type") == "Deaths").select(
        "report_id", "pull_number", "report_date",
        col("entry.name").alias("player_name"),
        col("entry.timestamp").alias("death_timestamp"),
        col("entry.deathWindow").alias("death_window"),
        col("entry.type").alias("player_class")
    )

def _extract_damage_summary(df):
    return df.where(col("data_type") == "DamageDone").select(
        "report_id", "pull_number", "report_date",
        col("entry.name").alias("player_name"),
        col("entry.total").alias("damage_total"),
        col("entry.activeTime").alias("active_time"),
        col("entry.type").alias("player_class"),
        col("entry.itemLevel").alias("item_level")
    )

def _extract_abilities(df):
    return df.select(
        "report_id", "pull_number", "report_date",
        col("entry.name").alias("player_name"),
        explode_outer("entry.abilities").alias("ability")
    ).select(
        "report_id", "pull_number", "report_date", "player_name",
        col("ability.name").alias("ability_name"),
        col("ability.total").alias("ability_total"),
        col("ability.totalReduced").alias("ability_total_reduced"),
        col("ability.type").alias("ability_type")
    )

def _extract_targets(df):
    return df.select(
        "report_id", "pull_number", "report_date",
        col("entry.name").alias("player_name"),
        explode_outer("entry.targets").alias("target")
    ).select(
        "report_id", "pull_number", "report_date", "player_name",
        col("target.name").alias("target_name"),
        col("target.total").alias("target_total"),
        col("target.totalReduced").alias("target_total_reduced"),
        col("target.type").alias("target_type")
    )

def _extract_pets(df):
    return df.where(col("entry.pets").isNotNull()).select(
        "report_id", "pull_number", "report_date",
        col("entry.name").alias("player_name"),
        explode_outer("entry.pets").alias("pet")
    ).select(
        "report_id", "pull_number", "report_date", "player_name",
        col("pet.name").alias("pet_name"),
        col("pet.total").alias("pet_total"),
        col("pet.type").alias("pet_type"),
        col("pet.guid").alias("pet_guid")
    )

def _extract_gear(df):
    return df.where(col("entry.gear").isNotNull()).select(
        "report_id", "pull_number", "report_date",
        col("entry.name").alias("player_name"),
        explode_outer("entry.gear").alias("gear")
    ).select(
        "report_id", "pull_number", "report_date", "player_name",
        col("gear.id").alias("gear_id"),
        col("gear.name").alias("gear_name"),
        col("gear.itemLevel").alias("gear_item_level"),
        col("gear.slot").alias("gear_slot"),
        col("gear.icon").alias("gear_icon")
    )

# COMMAND ----------
# DBTITLE 1,Transform and Export
def _lowercase_string_columns(df, exclude_cols=None):
    if exclude_cols is None:
        exclude_cols = []

    for field in df.schema.fields:
        col_name = field.name
        if field.dataType.simpleString() == "string" and col_name not in exclude_cols:
            snake_case = re.sub(r'(?<!^)(?=[A-Z])', '_', col_name).lower()
            df = df.withColumnRenamed(col_name, snake_case)
            df = df.withColumn(snake_case, lower(col(snake_case)))

    return df

def clean_and_export(dfs, exclude_cols=None):
    summary = dfs.get("df_player_summary")
    print("✅ Summary schema:", summary.columns)

    expected_cols = ["player_name", "player_class"]
    missing_cols = [c for c in expected_cols if c not in summary.columns]
    if missing_cols:
        summary = summary.withColumns({
            "player_name": col("entry.name"),
            "player_class": col("entry.type")
        })

    if "player_name" in summary.columns and "player_class" in summary.columns:
        summary.select("player_name", "player_class").limit(1).show()
    else:
        print("❌ Required columns missing in summary. Available columns:", summary.columns)

    summary = summary.where(col("player_class").isNotNull()).where(col("player_class") != "npc")
    player_names = [row["player_name"] for row in summary.select("player_name").distinct().collect()]

    for name, df in dfs.items():
        if not hasattr(df, "schema"):
            print(f"⚠️ Skipping {name}: not a DataFrame")
            continue
        df = _lowercase_string_columns(df, exclude_cols or None)
        if name != "df_player_summary" and "player_name" in df.columns and "damage_total" in df.columns:
            df = df.where(col("player_name").isin(player_names))
        table_suffix = name.removeprefix("df_") if name.startswith("df_") else name
        df.write.mode("overwrite").saveAsTable(f"02_silver.staging.warcraftlogs_tables_{table_suffix}")

summary_df = _extract_damage_summary(entry_df)
print(f"✅ Extracted summary rows: {summary_df.count()}")
summary_df.select("player_name", "damage_total", "player_class").show(10, truncate=False)

output = {
    "df_player_summary": summary_df,
    "df_player_abilities": _extract_abilities(entry_df),
    "df_player_targets": _extract_targets(entry_df),
    "df_player_pets": _extract_pets(entry_df),
    "df_player_gear": _extract_gear(entry_df),
    "df_healing_summary": _extract_healing_summary(entry_df),
    "df_deaths_summary": _extract_deaths_summary(entry_df),
}

clean_and_export(output, exclude_cols)
