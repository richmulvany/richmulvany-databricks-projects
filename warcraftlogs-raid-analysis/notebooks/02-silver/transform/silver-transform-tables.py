# Databricks notebook source
# DBTITLE 1,Import Dependencies
from pyspark.sql.functions import col, expr, split, lower, lit, when, size, date_format, count, explode_outer, explode, sum, regexp_extract, regexp_replace, to_json, max_by, first, length, element_at, row_number, posexplode
from pyspark.sql.types import StringType
import re
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Configure Notebook / Assign Variables
spark.sql("USE CATALOG 02_silver")
spark.sql("USE SCHEMA staging")

# COMMAND ----------

# DBTITLE 1,Read Table
tables_df = spark.read.table("01_bronze.warcraftlogs.tables")

# COMMAND ----------

tables_df.printSchema()

# COMMAND ----------

composition_df = (
  tables_df
    .select("report_id", explode("table_json.data.composition").alias("c"))
    .select(
      col("report_id"),
      col("c.id").alias("player_id"),
      col("c.guid").alias("player_guid"),
      col("c.name").alias("player_name"),
      col("c.type").alias("player_class"),
      explode(col("c.specs")).alias("spec")
    )
    .select(
      "report_id","player_id","player_guid","player_name","player_class",
      col("spec.spec").alias("player_spec"),
      col("spec.role").alias("player_role")
    )
)
composition_df.show()

# COMMAND ----------

# DBTITLE 1,Extract Metadata from Filename
tables_df = tables_df.withColumn("report_date", date_format(col("report_start"), "yyyy-MM-dd EE"))

ability_totals_df = (
    tables_df
      .select("report_id", "report_date", explode("table_json.data.abilitytotals").alias("a"))
      .select(
        "report_id",
        "report_date",
        col("a.guid").alias("guid"),
        col("a.name").alias("name"),
        col("a.type").alias("type"),
        col("a.abilityIcon").alias("ability_icon"),
        col("a.total").alias("total")
      )
)

auras_df = (
  tables_df
    .select("report_id", "report_date", explode("table_json.data.auras").alias("a"))
    .select(
      col("report_id"),
      "report_date",
      col("a.guid").alias("guid"),
      col("a.name").alias("name"),
      col("a.abilityIcon").alias("ability_icon"),
      col("a.type").alias("type"),
      col("a.totalUptime").alias("total_uptime"),
      col("a.totalUses").alias("total_uses"),
      explode(col("a.bands")).alias("band")
    )
    .select(
      "report_id","guid","name","ability_icon","type","total_uptime","total_uses",  
      col("band.startTime").alias("band_start_time"),
      col("band.endTime").alias("band_end_time")
    )
)

composition_df = (
  tables_df
    .select("report_id", "report_date", explode("table_json.data.composition").alias("c"))
    .select(
      col("report_id"),
      "report_date",
      col("c.id").alias("id"),
      col("c.guid").alias("player_guid"),
      col("c.name").alias("player_name"),
      col("c.type").alias("player_class"),
      explode(col("c.specs")).alias("player_spec")
    )
    .select(
      "report_id","id","guid","name","type",
      col("spec.spec").alias("spec_name"),
      col("spec.role").alias("spec_role")
    )
)

damage_done_df = (
  tables_df
    .select("report_id", "report_date", explode("table_json.data.damageDone").alias("d"))
    .select(
      col("report_id"),
      "report_date",
      col("d.id").alias("id"),
      col("d.guid").alias("guid"),
      col("d.name").alias("name"),
      col("d.icon").alias("icon"),
      col("d.type").alias("type"),
      col("d.total").alias("total")
    )
)

damage_taken_df = (
  tables_df
    .select("report_id", "report_date", explode("table_json.data.damageTaken").alias("d"))
    .select(
      col("report_id"),
      "report_date",
      col("d.guid").alias("guid"),
      col("d.name").alias("name"),
      col("d.abilityIcon").alias("ability_icon"),
      col("d.type").alias("type"),
      col("d.total").alias("total"),
      col("d.composite").alias("composite")
    )
)

death_events_df = (
  tables_df
    .select("report_id", "report_date", explode("table_json.data.deathEvents").alias("d"))
    .select(
      col("report_id"),
      "report_date",
      col("d.id").alias("id"),
      col("d.guid").alias("guid"),
      col("d.name").alias("name"),
      col("d.icon").alias("icon"),
      col("d.type").alias("type"),
      col("d.deathTime").alias("death_time"),
      col("d.ability.guid").alias("ability_guid"),
      col("d.ability.name").alias("ability_name"),
      col("d.ability.type").alias("ability_type"),
      col("d.ability.abilityIcon").alias("ability_icon")
    )
)

healing_pulls_df = (
    tables_df
      .withColumn("fight", explode("table_json.data.fights"))
      .withColumn("h",     explode("table_json.data.healingDone"))
      .select(
          col("report_id"),
          col("report_date"),
          col("fight.id").alias("fight_id"),
          col("fight.start").alias("fight_start"),
          col("h.id").alias("id"),
          col("h.guid").alias("guid"),
          col("h.name").alias("name"),
          col("h.icon").alias("icon"),
          col("h.type").alias("type"),
          col("h.total").alias("total")
      )
)

entries_df = (
  tables_df
    .select("report_id", "report_date", explode("table_json.data.entries").alias("e"))
    .select(
      col("report_id"),
      "report_date",
      col("e.guid").alias("entry_guid"),
      col("e.name").alias("entry_name"),
      col("e.type").alias("entry_type"),
      col("e.timestamp").alias("entry_timestamp")
    )
)

meta_df = (
  tables_df
    .select(
      col("report_id"),
      col("report_date"),
      col("table_json.data.gameVersion").alias("game_version"),
      col("table_json.data.logVersion").alias("log_version")
    ).dropna().dropDuplicates()
)

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Transform
def camel_to_snake(name):
    # normal transitions (e.g. GameID → Game_ID)
    name = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', name) 
    # handle acronyms at end (e.g. GameID → Game_ID)
    name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name)       
    return name.lower()

# Create a UDF from the camel_to_snake function
camel_to_snake_udf = udf(camel_to_snake, StringType())

healing_done_df = healing_done_df.groupBy(lower("name").alias("player_name")) \
  .agg(
    sum("total").alias("healing_done_total"),
    first("id").alias("player_id"),
    first("guid").alias("player_guid"),
    camel_to_snake_udf(first("type")).alias("player_class"),
    camel_to_snake_udf(split(max_by("icon", length("icon")), "-")[1]).alias("player_spec"),
    first("report_id").alias("report_id"),
    first("report_date").alias("report_date")
)

death_events_df = death_events_df.select(
    "report_id",
    "report_date",
    col("id").alias("player_id"),
    col("guid").alias("player_guid"),
    lower(col("name")).alias("player_name"),
    camel_to_snake_udf(col("type")).alias("player_class"),
    camel_to_snake_udf(element_at(split(col("icon"), "-"), 1)).alias("player_spec"),
    regexp_replace(r':', '', regexp_replace(lower(col("ability_name")), r'[- ]', '_')).alias("ability_name"),
    "ability_type",
    "ability_icon"
)

damage_taken_df = damage_taken_df.select(
    "report_id",
    "report_date",
    col("guid").alias("player_guid"),
    regexp_replace(regexp_replace(lower(col("name")), r'[- ]', '_'), r':', '').alias("ability_name"),
    "ability_icon",
    col("type").alias("ability_type"),
    col("total").alias("damage_taken_total")
)



# COMMAND ----------

# 2) normalize name and define your window per (report, player) ordered by fight_start
with_names = healing_pulls_df.withColumn("player_name", lower(col("name")))
player_window = (
    Window
      .partitionBy("report_id", "player_name")
      .orderBy(col("fight_start"))
)

# 3) add pull_number = row index within each (report,player) partition
indexed = with_names.withColumn("pull_number", row_number().over(player_window))

# 4) derive all your “first” columns _per row_ instead of via agg()
result = (
    indexed
      .withColumn("player_id",   col("id"))
      .withColumn("player_guid", col("guid"))
      .withColumn("player_class", camel_to_snake_udf(col("type")))
      .withColumn("player_spec",  camel_to_snake_udf(split(col("icon"), "-")[1]))
      .select(
         "report_id",
         "report_date",
         "fight_id",       # ← now you know exactly which pull it came from
         "player_name",
         "player_id",
         "player_guid",
         "player_class",
         "player_spec",
         "pull_number",    # ← 1,2,3… per player, per report
         col("total").alias("healing_done")
      )
)

# COMMAND ----------

# 1) explode fights with its index
fights_with_idx = (
  tables_df
    .select(
      "report_id",
      "report_date",
      posexplode("table_json.data.fights").alias("pull_index","fight")
    )
    .select(
      "report_id",
      "report_date",
      (col("pull_index") + 1).alias("pull_number"),  # zero→one based
      col("fight.id").alias("fight_id"),
      col("fight.start").alias("fight_start"),
      col("fight.end").alias("fight_end")
    )
)

# 2) join that back to healingDone
healing_pulls_df = (
  fights_with_idx
    .join(
      tables_df.select("report_id", explode("table_json.data.healingDone").alias("h")),
      on="report_id"
    )
    .select(
        "report_id",
        "report_date",
        "pull_number",
        col("h.id").alias("player_id"),
        col("h.guid").alias("player_guid"),
        lower(col("h.name")).alias("player_name"),
        camel_to_snake_udf(col("h.type")).alias("player_class"),
        camel_to_snake_udf(element_at(split(col("h.icon"), "-"), 1)).alias("player_spec"),
        col("h.total").alias("healing_done")
    )
)

per_pull_totals = (
    healing_pulls_df
      .groupBy(
          "report_id",
          "report_date",
          "pull_number",
          "player_name",
          "player_id",
          "player_guid",
          "player_class",
          "player_spec"
      )
      .agg(
          sum("healing_done").alias("healing_done_total")
      )
      .orderBy(
          "report_id",
          "pull_number",
          "player_name"
      )
)

# COMMAND ----------

display(healing_pulls_df.where(col("pull_number") == 2))
display(per_pull_totals)

# COMMAND ----------

test = damage_done_df.groupBy(lower("name").alias("player_name")).agg(
    "total".alias("damage_done_total")
)
display(test)
display(damage_done_df)

# COMMAND ----------

display(damage_taken_df)

# COMMAND ----------



# COMMAND ----------

display(death_events_df)

# COMMAND ----------

# DBTITLE 1,Transform



# COMMAND ----------

tables_dict = {
    "tables_meta" : meta_df,
    "tables_entries" : entries_df,
    "tables_healing_done" : healing_done_df,
    "tables_death_events" : death_events_df,
    "tables_damage_taken" : damage_taken_df,
    "tables_damage_done" : damage_done_df,
    "tables_composition" : composition_df,
    "tables_auras" : auras_df,
    "tables_ability_totals" : ability_totals_df
}

for name, df in tables_dict.items():
    print(name)
    df.show()

# COMMAND ----------

tables_dict = {
    "tables_meta" : meta_df,
    "tables_players" : players_df,
    "tables_entries" : entries_df,
    "tables_healing_done" : healing_done_df,
    "tables_fights" : fights_df,
    "tables_death_events" : death_events_df,
    "tables_damage_taken" : damage_taken_df,
    "tables_damage_done" : damage_done_df,
    "tables_composition" : composition_df,
    "tables_auras" : auras_df,
    "tables_ability_totals" : ability_totals_df
}

for name, df in tables_dict.items():
    df.format("delta").mode("overwrite").partitionBy("report_id")\
    .saveAsTable(name)

# COMMAND ----------

display(entry_df)

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
