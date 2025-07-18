# Databricks notebook source
# DBTITLE 1,Import Dependencies
from pyspark.sql.functions import (
    col, expr, split, lower, lit, when, size, date_format, count,
    explode_outer, explode, sum, regexp_extract, regexp_replace,
    to_json, max_by, first, length, element_at, row_number,
    posexplode, round, try_element_at, udf
)
from pyspark.sql.types import StringType
import re
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Configure Notebook / Assign Variables
spark.sql("USE CATALOG 02_silver")
spark.sql("USE SCHEMA staging")

class_definitions = spark.table("02_silver.warcraftlogs.d_game_data_classes")
classes = [row["class_name"] for row in class_definitions.select("class_name").distinct().collect()]

# COMMAND ----------

# DBTITLE 1,Establish Functions
def camel_to_snake(camel_case_str):
    # Adjust regex to identify camel case transitions and acronyms
    snake_case_str = re.sub(
        r'(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])',
        '_',
        camel_case_str
    )
    # Convert to lowercase
    snake_case_str = snake_case_str.lower()
    # Handle acronyms by checking for sequences of uppercase letters
    return re.sub(r'(_[A-Z]+)', lambda x: x.group(0).lower(), snake_case_str)

# Create a UDF from the camel_to_snake function
camel_to_snake_udf = udf(camel_to_snake, StringType())

# COMMAND ----------

# DBTITLE 1,Read Table
raw_tables_df = spark.read.table("01_bronze.warcraftlogs.tables")

# COMMAND ----------

# DBTITLE 1,Parse Dataframes
tables_df = (
    raw_tables_df
    .withColumn("pull_number", regexp_extract("source_file", r"fight(\d+)_", 1))
    .withColumn("data_type", regexp_extract("source_file", r"table_(\w+)_", 1))
    .withColumn("report_date", date_format(col("report_start"), "yyyy-MM-dd EE"))
)

tables_df = tables_df.select(
    "report_id",
    "report_date",
    camel_to_snake_udf(col("data_type")).alias("data_type"),
    col("pull_number").cast("int"),
    "table_json"
)

data_types = [row["data_type"] for row in tables_df.select("data_type").distinct().collect()]

tables_dict = {}
for data_type in data_types:
    df_name = f"{data_type}_df"
    globals()[df_name] = tables_df.filter(col("data_type") == data_type)
    tables_dict[data_type] = globals()[df_name]
    print(f"Created DataFrame {df_name}")
print(f"Created {len(data_types)} DataFrames")

# COMMAND ----------

# DBTITLE 1,Transform
# --- Dispels ---
dispels_df = dispels_df.filter(
    (col("table_json.data.entries").isNotNull()) &
    (size(col("table_json.data.entries")) > 0)
)

dispels_df = (
    dispels_df
    .select("*", explode("table_json.data.entries").alias("entry"))
    .select("*", explode("entry.entries").alias("ability_entry"))
    .select("*", explode("ability_entry.details").alias("detail"))
    .select(
        "report_id", "report_date", "pull_number",
        explode("detail.abilities").alias("dispel_ability"),
        col("detail.name").alias("name"),
        col("detail.type").alias("type")
    )
    .select(
        "report_id", "report_date", "pull_number",
        lower(col("name")).alias("player_name"),
        lower(col("type")).alias("player_class"),
        regexp_replace(lower(col("dispel_ability.name")), r' ', '_').alias("ability_name"),
        col("dispel_ability.total").alias("ability_casts")
    )
    .filter(col("player_class").isin(classes))
)

# --- Buffs ---
buffs_df = buffs_df.filter(
    (col("table_json.data.auras").isNotNull()) &
    (size(col("table_json.data.auras")) > 0)
)

buffs_df = (
    buffs_df
    .select("*", explode("table_json.data.auras").alias("aura"))
    .select("*", explode("aura.bands").alias("band"))
    .select(
        "report_id", "report_date", "pull_number",
        col("aura.guid").alias("player_guid"),
        regexp_replace(lower(col("aura.name")), r' ', '_').alias("buff_name"),
        col("aura.type").alias("buff_type"),
        col("aura.totalUses").alias("buff_casts"),
        col("aura.totalUptime").alias("buff_uptime"),
        col("band.startTime").alias("band_start_time"),
        col("band.endTime").alias("band_end_time")
    )
)

# --- Summary Healing ---
summary_df_healing = summary_df.filter(
    (col("table_json.data.healingDone").isNotNull()) &
    (size(col("table_json.data.healingDone")) > 0)
)

summary_df_healing = (
    summary_df_healing
    .select("*", explode("table_json.data.healingDone").alias("healing_done"))
    .select(
        "report_id", "report_date", "pull_number",
        col("healing_done.id").alias("player_id"),
        col("healing_done.guid").alias("player_guid"),
        lower(col("healing_done.name")).alias("player_name"),
        lower(col("healing_done.type")).alias("player_class"),
        col("healing_done.total").alias("healing_done_total")
    )
    .filter(col("player_class").isin(classes))
)

# --- Summary Damage ---
summary_df_damage = summary_df.filter(
    (col("table_json.data.damageDone").isNotNull()) &
    (size(col("table_json.data.damageDone")) > 0)
)

summary_df_damage = (
    summary_df_damage
    .select("*", explode("table_json.data.damageDone").alias("damage_done"))
    .select(
        "report_id", "report_date", "pull_number",
        col("damage_done.id").alias("player_id"),
        col("damage_done.guid").alias("player_guid"),
        lower(col("damage_done.name")).alias("player_name"),
        lower(col("damage_done.type")).alias("player_class"),
        col("damage_done.total").alias("damage_done_total")
    )
    .filter(col("player_class").isin(classes))
)

# --- Summary Damage Taken ---
summary_df_damage_taken = summary_df.filter(
    (col("table_json.data.damageTaken").isNotNull()) &
    (size(col("table_json.data.damageTaken")) > 0)
)

summary_df_damage_taken = (
    summary_df_damage_taken
    .select("*", explode("table_json.data.damageTaken").alias("damage_taken"))
    .select(
        "report_id", "report_date", "pull_number",
        col("damage_taken.guid").alias("player_guid"),
        regexp_replace(lower(col("damage_taken.name")), r'[- ]', '_').alias("damaging_ability_name"),
        lower(col("damage_taken.type")).alias("damaging_ability_type"),
        col("damage_taken.total").alias("damage_taken_total")
    )
)

# --- Summary Metadata ---
summary_df_metadata = (
    summary_df
    .select(
        "report_id", "report_date",
        col("table_json.data.gameVersion").alias("game_version"),
        col("table_json.data.logVersion").alias("log_version")
    )
    .dropDuplicates()
)

# --- Composition ---
composition_df = (
    tables_df
    .select(
        "report_id", "report_date",
        explode("table_json.data.composition").alias("c")
    )
    .select(
        "report_id", "report_date",
        col("c.id").alias("player_id"),
        col("c.guid").alias("player_guid"),
        lower(col("c.name")).alias("player_name"),
        lower(col("c.type")).alias("player_class"),
        explode(col("c.specs")).alias("spec")
    )
    .select(
        "report_id", "report_date", "player_id", "player_guid",
        "player_name", "player_class",
        lower(col("spec.spec")).alias("player_spec"),
        lower(col("spec.role")).alias("player_role")
    )
)

# --- Casts ---
casts_df = casts_df.filter(
    (col("table_json.data.entries").isNotNull()) &
    (size(col("table_json.data.entries")) > 0)
)

casts_df = (
    casts_df
    .select("*", explode("table_json.data.entries").alias("entry"))
    .select(
        "report_id", "report_date", "pull_number",
        lower(col("entry.name")).alias("player_name"),
        camel_to_snake_udf(split(col("entry.icon"), "-")[0]).alias("player_class"),
        camel_to_snake_udf(split(col("entry.icon"), "-")[1]).alias("player_spec"),
        explode("entry.abilities").alias("ability")
    )
    .select(
        "report_id", "report_date", "pull_number",
        "player_name", "player_class", "player_spec",
        regexp_replace(lower(col("ability.name")), r'[ -]', '_').alias("ability_name"),
        col("ability.total").alias("ability_casts")
    )
    .filter(col("player_class").isin(classes))
)

# COMMAND ----------

# DBTITLE 1,Export
tables: dict[str, DataFrame] = {
    "dispels": dispels_df,
    "buffs": buffs_df,
    "summary_healing": summary_df_healing,
    "summary_damage": summary_df_damage,
    "summary_damage_taken": summary_df_damage_taken,
    "summary_metadata": summary_df_metadata,
    "composition": composition_df,
    "casts": casts_df,
    "healing": healing_df,
    "damage_done": damage_done_df
}

for name, df in tables.items():
    table_name = f"warcraftlogs_tables_{name}"
    df.write \
      .format("delta") \
      .mode("overwrite") \
      .saveAsTable(table_name)
    print(f"Written table: {table_name}")

print("✅ All tables written.")
