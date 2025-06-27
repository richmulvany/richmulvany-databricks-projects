# Databricks notebook source
# DBTITLE 1,Import Dependencies
from pyspark.sql.functions import col, expr, split, lower, lit, when, size, date_format, count, explode_outer, explode
from pyspark.sql.types import StringType
import re

# COMMAND ----------

# DBTITLE 1,Configure Notebook / Assign Variables
exclude_cols = ["report_id", "report_start_date", "report_date"]

# COMMAND ----------

# DBTITLE 1,Read Table
df = spark.read.table("01_bronze.warcraftlogs.tables")

# COMMAND ----------

# DBTITLE 1,Explode Dataframe
# Explode the already-parsed entries field
df_exploded = df.withColumn("entry", explode(col("table_json.data.entries")))

# Flatten and select
df_flat = df_exploded.select(
    "report_id",
    "report_start_date",
    col("entry.*")
)

df_flat.printSchema()

# COMMAND ----------

# DBTITLE 1,Read Fights Table
fights_df = spark.read.table("02_silver.warcraftlogs.fights_boss_pulls")

# COMMAND ----------

# DBTITLE 1,Extract Individual Tables
# Summary
df_summary = df_flat.select(
    "report_id",
    "report_start_date",
    col("fight").alias("pull_number"),
    "total",
    "overkill",
    "overheal",
    "guid",
    col("name").alias("player_name"),
    col("type").alias("player_class"),
    col("activeTime").alias("active_time"),
    col("deathWindow").alias("death_window"),
    col("itemLevel").alias("item_level"),
).join(fights_df, on=["report_id", "pull_number"], how="inner")

# Abilities
df_abilities = df_flat.select(
    "report_id",
    "report_start_date",
    col("fight").alias("pull_number"),
    col("name").alias("actor_name"),
    explode_outer("abilities").alias("ability")
).select(
    "report_id", "report_start_date", "pull_number", "actor_name",
    col("ability.name").alias("ability_name"),
    col("ability.petName").alias("pet_name"),
    col("ability.total").alias("total"),
    col("ability.totalReduced").alias("total_reduced"),
    col("ability.type").alias("ability_type")
).join(fights_df, on=["report_id", "pull_number"], how="inner")

# Damage abilities
df_damage_abilities = df_flat.select(
    "report_id", "report_start_date", col("fight").alias("pull_number"),
    col("name").alias("actor_name"),
    explode_outer("damageAbilities").alias("damage_ability")
).select(
    "report_id", "report_start_date", "pull_number", "actor_name",
    col("damage_ability.name").alias("ability_name"),
    col("damage_ability.total").alias("total"),
    col("damage_ability.totalReduced").alias("total_reduced"),
    col("damage_ability.type").alias("ability_type")
).join(fights_df, on=["report_id", "pull_number"], how="inner")

# Events
df_events = df_flat.select(
    "report_id", "report_start_date", col("fight").alias("pull_number"),
    col("name").alias("actor_name"),
    explode_outer("events").alias("event")
).select(
    "report_id", "report_start_date", "pull_number", "actor_name",
    col("event.timestamp"),
    col("event.type").alias("event_type"),
    col("event.amount"),
    col("event.absorbed"),
    col("event.overkill"),
    col("event.unmitigatedAmount").alias("unmitigated_amount"),
    col("event.ability.name").alias("ability_name"),
    col("event.ability.guid").alias("ability_guid"),
    col("event.sourceID").alias("source_id"),
    col("event.targetID").alias("target_id"),
    col("event.sourceIsFriendly").alias("source_is_friendly"),
    col("event.targetIsFriendly").alias("target_is_friendly"),
    col("event.tick")
).join(fights_df, on=["report_id", "pull_number"], how="inner")

# Gear
df_gear = df_flat.select(
    "report_id", "report_start_date", col("fight").alias("pull_number"),
    col("name").alias("actor_name"),
    explode_outer("gear").alias("gear_item")
).select(
    "report_id", "report_start_date", "pull_number", "actor_name",
    col("gear_item.name").alias("item_name"),
    col("gear_item.id").alias("item_id"),
    col("gear_item.slot").alias("slot"),
    col("gear_item.itemLevel").alias("item_level"),
    col("gear_item.quality").alias("quality"),
    col("gear_item.setID").alias("set_id")
).join(fights_df, on=["report_id", "pull_number"], how="inner")

# Pets
df_pets = df_flat.select(
    "report_id", "report_start_date", col("fight").alias("pull_number"),
    col("name").alias("actor_name"),
    explode_outer("pets").alias("pet")
).select(
    "report_id", "report_start_date", "pull_number", "actor_name",
    col("pet.name").alias("pet_name"),
    col("pet.guid").alias("pet_guid"),
    col("pet.total"),
    col("pet.totalReduced").alias("total_reduced"),
    col("pet.type").alias("pet_type")
).join(fights_df, on=["report_id", "pull_number"], how="inner")

# Talents
df_talents = df_flat.select(
    "report_id", "report_start_date", col("fight").alias("pull_number"),
    col("name").alias("actor_name"),
    explode_outer("talents").alias("talent_id")
).select(
    "report_id", "report_start_date", "pull_number", "actor_name", "talent_id"
).join(fights_df, on=["report_id", "pull_number"], how="inner")

# Targets
df_targets = df_flat.select(
    "report_id", "report_start_date", col("fight").alias("pull_number"),
    col("name").alias("actor_name"),
    explode_outer("targets").alias("target")
).select(
    "report_id", "report_start_date", "pull_number", "actor_name",
    col("target.name").alias("target_name"),
    col("target.total").alias("total"),
    col("target.totalReduced").alias("total_reduced"),
    col("target.type").alias("target_type")
).join(fights_df, on=["report_id", "pull_number"], how="inner")

# Nested: damage.abilities
df_damage_abilities_nested = df_flat.select(
    "report_id", "report_start_date", col("fight").alias("pull_number"),
    col("name").alias("actor_name"),
    explode_outer("damage.abilities").alias("damage_ability")
).select(
    "report_id", "report_start_date", "pull_number", "actor_name",
    col("damage_ability.name").alias("ability_name"),
    col("damage_ability.total").alias("total"),
    col("damage_ability.totalReduced").alias("total_reduced"),
    col("damage_ability.type").alias("ability_type")
).join(fights_df, on=["report_id", "pull_number"], how="inner")

# Nested: damage.sources
df_damage_sources = df_flat.select(
    "report_id", "report_start_date", col("fight").alias("pull_number"),
    col("name").alias("actor_name"),
    explode_outer("damage.sources").alias("damage_source")
).select(
    "report_id", "report_start_date", "pull_number", "actor_name",
    col("damage_source.name").alias("source_name"),
    col("damage_source.total").alias("total"),
    col("damage_source.totalReduced").alias("total_reduced"),
    col("damage_source.type").alias("source_type")
).join(fights_df, on=["report_id", "pull_number"], how="inner")

# Nested: healing.abilities
df_healing_abilities = df_flat.select(
    "report_id", "report_start_date", col("fight").alias("pull_number"),
    col("name").alias("actor_name"),
    explode_outer("healing.abilities").alias("healing_ability")
).select(
    "report_id", "report_start_date", "pull_number", "actor_name",
    col("healing_ability.name").alias("ability_name"),
    col("healing_ability.total").alias("total"),
    col("healing_ability.totalReduced").alias("total_reduced"),
    col("healing_ability.type").alias("ability_type")
).join(fights_df, on=["report_id", "pull_number"], how="inner")

# Nested: healing.sources
df_healing_sources = df_flat.select(
    "report_id", "report_start_date", col("fight").alias("pull_number"),
    col("name").alias("actor_name"),
    explode_outer("healing.sources").alias("healing_source")
).select(
    "report_id", "report_start_date", "pull_number", "actor_name",
    col("healing_source.name").alias("source_name"),
    col("healing_source.total").alias("total"),
    col("healing_source.totalReduced").alias("total_reduced"),
    col("healing_source.type").alias("source_type")
).join(fights_df, on=["report_id", "pull_number"], how="inner")

# COMMAND ----------

# DBTITLE 1,Transform
# Lowercase string columns in all tables
def lowercase_string_columns(df, exclude_cols=None):
    if exclude_cols is None:
        exclude_cols = []

    string_cols = [f.name for f in df.schema.fields 
                   if isinstance(f.dataType, StringType) and f.name not in exclude_cols]

    for col_name in string_cols:
        df = df.withColumn(col_name, lower(col(col_name)))
    
    return df

exploded_tables = {
    "df_summary": df_summary,
    "df_abilities": df_abilities,
    "df_damage_abilities": df_damage_abilities,
    "df_events": df_events,
    "df_gear": df_gear,
    "df_pets": df_pets,
    "df_talents": df_talents,
    "df_targets": df_targets,
    "df_damage_abilities_nested": df_damage_abilities_nested,
    "df_damage_sources": df_damage_sources,
    "df_healing_abilities": df_healing_abilities,
    "df_healing_sources": df_healing_sources
}

for name in exploded_tables:
    exploded_tables[name] = lowercase_string_columns(exploded_tables[name], exclude_cols)

# Remove NPC's from summary
exploded_tables["df_summary"] = exploded_tables["df_summary"].where(col("player_class") != "npc")

# Drop redundant report_start_date columns
for name in exploded_tables:
    exploded_tables[name] = exploded_tables[name].drop("report_start_date")

# COMMAND ----------

# DBTITLE 1,Write to Silver
for name, df in exploded_tables.items():
    table_suffix = name.removeprefix("df_")  # For Python 3.9+
    df.write.mode("append").saveAsTable(f"02_silver.warcraftlogs.tables_{table_suffix}")
