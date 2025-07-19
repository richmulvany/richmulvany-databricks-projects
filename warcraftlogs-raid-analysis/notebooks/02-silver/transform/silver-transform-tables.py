# Databricks notebook source
# DBTITLE 1,Import Dependencies
from pyspark.sql.functions import (
    col, expr, split, lower, lit, when, size, date_format,
    explode_outer, explode, sum, regexp_extract, regexp_replace,
    udf
)
from pyspark.sql.types import StringType
import re
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

# COMMAND ----------
# DBTITLE 1,Configure Notebook / Assign Variables
spark.sql("USE CATALOG 02_silver")
spark.sql("USE SCHEMA staging")

class_defs = spark.table("02_silver.warcraftlogs.d_game_data_classes")
classes = [r["class_name"] for r in class_defs.select("class_name").distinct().collect()]

# COMMAND ----------
# DBTITLE 1,Establish Functions
def camel_to_snake(s: str) -> str:
    tmp = re.sub(r'(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])', '_', s)
    tmp = tmp.lower()
    return re.sub(r'(_[A-Z]+)', lambda m: m.group(0).lower(), tmp)

camel_to_snake_udf = udf(camel_to_snake, StringType())

# COMMAND ----------
# DBTITLE 1,Read Bronze “tables”
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
data_types = [r["data_type"] for r in tables_df.select("data_type").distinct().collect()]
tables_dict = {}
for dt in data_types:
    df_name = f"{dt}_df"
    globals()[df_name] = tables_df.filter(col("data_type") == dt)
    tables_dict[dt] = globals()[df_name]
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
    .select(
        "report_id", "report_date", "pull_number",
        explode("table_json.data.entries").alias("entry")
    )
    .select(
        "report_id", "report_date", "pull_number",
        lower(col("entry.name")).alias("player_name"),
        lower(col("entry.type")).alias("player_class"),
        explode("entry.abilities").alias("dispel_ability")
    )
    .select(
        "report_id", "report_date", "pull_number", "player_name", "player_class",
        regexp_replace(lower(col("dispel_ability.name")), r" ", "_").alias("ability_name"),
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
    .select(
        "report_id", "report_date", "pull_number",
        explode("table_json.data.auras").alias("aura")
    )
    .select(
        "report_id", "report_date", "pull_number",
        col("aura.guid").alias("player_guid"),
        regexp_replace(lower(col("aura.name")), r" ", "_").alias("buff_name"),
        col("aura.type").alias("buff_type"),
        col("aura.totalUses").alias("buff_casts"),
        col("aura.totalUptime").alias("buff_uptime"),
        explode("aura.bands").alias("band")
    )
    .select(
        "report_id", "report_date", "pull_number", "player_guid",
        "buff_name", "buff_type", "buff_casts", "buff_uptime",
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
    .select(
        "report_id", "report_date", "pull_number",
        explode("table_json.data.healingDone").alias("healing_done")
    )
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
    .select(
        "report_id", "report_date", "pull_number",
        explode("table_json.data.damageDone").alias("damage_done")
    )
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
    .select(
        "report_id", "report_date", "pull_number",
        explode("table_json.data.damageTaken").alias("damage_taken")
    )
    .select(
        "report_id", "report_date", "pull_number",
        col("damage_taken.guid").alias("player_guid"),
        regexp_replace(lower(col("damage_taken.name")), r"[- ]", "_").alias("damaging_ability_name"),
        lower(col("damage_taken.type")).alias("damaging_ability_type"),
        col("damage_taken.total").alias("damage_taken_total")
    )
)

# --- Summary Metadata ---
summary_df_metadata = summary_df.select(
    "report_id", "report_date",
    col("table_json.data.gameVersion").alias("game_version"),
    col("table_json.data.logVersion").alias("log_version")
).dropDuplicates()

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
        explode("c.specs").alias("spec")
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
    .select(
        "report_id", "report_date", "pull_number",
        explode("table_json.data.entries").alias("entry")
    )
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
        regexp_replace(lower(col("ability.name")), r"[ \-]", "_").alias("ability_name"),
        col("ability.total").alias("ability_casts")
    )
    .filter(col("player_class").isin(classes))
)

# --- Healing ---
healing_df = healing_df.filter(
    (col("table_json.data.entries").isNotNull()) &
    (size(col("table_json.data.entries")) > 0)
)
healing_df = (
    healing_df
    .select(
        "report_id", "report_date", "pull_number",
        explode("table_json.data.entries").alias("entry")
    )
    .select(
        "report_id", "report_date", "pull_number",
        lower(col("entry.name")).alias("player_name"),
        lower(col("entry.icon")).alias("icon"),
        explode("entry.abilities").alias("ability"),
        explode("entry.damageAbilities").alias("incoming_ability")
    )
    .withColumn("cls_spec", split(col("icon"), "-"))
    .withColumn("player_class", col("cls_spec")[0])
    .withColumn("player_spec", when(size(col("cls_spec")) > 1, col("cls_spec")[1]).otherwise("unknown"))
    .select(
        "report_id", "report_date", "pull_number",
        "player_name", "player_class", "player_spec",
        regexp_replace(lower(col("ability.name")), r"[ \-]", "_").alias("healing_ability_name"),
        col("ability.total").alias("healing_ability_healing"),
        col("ability.type").alias("healing_ability_type"),
        regexp_replace(lower(col("incoming_ability.name")), r"[ \-]", "_").alias("incoming_ability_name"),
        col("incoming_ability.type").alias("incoming_ability_type"),
        col("incoming_ability.totalReduced").alias("incoming_damage_reduced")
    )
    .filter(col("player_class").isin(classes))
)

# --- Damage Done ---
damage_done_df = damage_done_df.filter(
    (col("table_json.data.entries").isNotNull()) &
    (size(col("table_json.data.entries")) > 0)
)
damage_done_df = (
    damage_done_df
    .select(
        "report_id", "report_date", "pull_number",
        explode("table_json.data.entries").alias("entry")
    )
    .select(
        "report_id", "report_date", "pull_number",
        lower(col("entry.name")).alias("player_name"),
        lower(col("entry.icon")).alias("icon"),
        explode("entry.abilities").alias("ability")
    )
    .withColumn("cls_spec", split(col("icon"), "-")),
    .withColumn("player_class", col("cls_spec")[0]),
    .withColumn("player_spec", when(size(col("cls_spec")) > 1, col("cls_spec")[1]).otherwise("unknown"))
    .select(
        "report_id", "report_date", "pull_number",
        "player_name", "player_class", "player_spec",
        regexp_replace(lower(col("ability.name")), r"[ \-]", "_").alias("ability_name"),
        col("ability.total").alias("ability_damage")
    )
    .filter(col("player_class").isin(classes))
)

# COMMAND ----------
# DBTITLE 1,Export
tbls = {
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
for name, df in tbls.items():
    tname = f"warcraftlogs_tables_{name}"
    df.write.format("delta").mode("overwrite").saveAsTable(tname)
    print(f"Written table: {tname}")

print("✅ All tables written.")
