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

display(df)

# COMMAND ----------

# DBTITLE 1,Extract Metadata from Filename
df = df.withColumn("pull_number", regexp_extract("source_file", r"_fight(\d+)", 1).cast("int"))
df = df.withColumn("data_type", regexp_extract("source_file", r"_table_([^_]+)", 1))

# COMMAND ----------

# DBTITLE 1,Split and Unpivot Dataframes
# Format `report_date`
df = df.withColumn("report_start_date", date_format(col("report_start_date"), "yyyy-MM-dd EE"))

# Split dataframes
df_damage_done = df.where(col("data_type") == "DamageDone").select(
    "report_id",
    "pull_number",
    col("report_start_date").alias("report_date"),
    explode_outer("table_json.data.entries").alias("entry")
)

df_healing = df.where(col("data_type") == "Healing").select(
    "report_id",
    "pull_number",
    col("report_start_date").alias("report_date"),
    explode_outer("table_json.data.entries").alias("entry")
)

df_deaths = df.where(col("data_type") == "Deaths").select(
    "report_id",
    "pull_number",
    col("report_start_date").alias("report_date"),
    explode_outer("table_json.data.entries").alias("entry")
)

# Remove null entries
df_damage_done = df_damage_done.where(col("entry").isNotNull())
df_healing = df_healing.where(col("entry").isNotNull())
df_deaths = df_deaths.where(col("entry").isNotNull())


# COMMAND ----------

def dfParser(df):
    result = {}
    df_explode = df.select(
        "report_id",
        "pull_number",
        "report_date",
        explode_outer("entry.abilities").alias("ability"),
        col("entry.activeTime").alias("active_time"),
        explode_outer("entry.damage.abilities").alias("damage_ability"),
        col("entry.damage.ActiveTime").alias("damage_active_time"),
        col("entry.damage.activeTimeReduced").alias("damage_active_time_reduced"),
        col("entry.damage.blocked").alias("damage_blocked"),
        explode_outer("entry.damage.damageAbilities").alias("damage_damage_ability"),
        col("entry.damage.overheal").alias("damage_overheal"),
        explode_outer("entry.damage.sources").alias("damage_sources"),
        col("entry.damage.total").alias("damage_total"),
        col("entry.damage.totalReduced").alias("damage_total_reduced"),
        explode_outer("entry.damageAbilities").alias("damage_abilities"),
        col("entry.deathWindow").alias("death_window"),
        explode_outer("entry.events").alias("event"),
        "entry.fight",
        explode_outer("entry.gear").alias("gear"),
        explode_outer("entry.healing.abilities").alias("healing_abilities"),
        col("entry.healing.activeTime").alias("healing_active_time"),
        col("entry.healing.activeTimeReduced").alias("healing_active_time_reduced"),
        explode_outer("entry.healing.damageAbilities").alias("healing_damage_ability"),
        explode_outer("entry.healing.sources").alias("healing_sources"),
        col("entry.healing.total").alias("healing_total"),
        col("entry.healing.totalReduced").alias("healing_total_reduced"),
        col("entry.itemLevel").alias("item_level"),
        col("entry.killingBlow.abilityIcon").alias("killing_blow_ability_icon"),
        col("entry.killingBlow.guid").alias("killing_blow_guid"),
        col("entry.killingBlow.name").alias("killing_blow_name"),
        col("entry.killingBlow.type").alias("killing_blow_type"),
        col("entry.name").alias("player_name"),
        "entry.overheal",
        "entry.overkill",
        explode_outer("entry.pets").alias("pets"),
        explode_outer("entry.talents").alias("talents"),
        explode_outer("entry.targets").alias("targets"),
        col("entry.timestamp").alias("timestamp"),
        "entry.total",
        col("entry.totalReduced").alias("total_reduced"),
        col("entry.type").alias("player_class")
    )
    result[f"{df}_explode"] = df_explode

    # --- Abilities
    result[f"{df}_abilities"] = df_explode.select(
        "report_id",
        "pull_number",
        "report_date",
        col("ability.name").alias("ability_name"),
        col("ability.petName").alias("ability_pet_name"),
        col("ability.total").alias("ability_total"),
        col("ability.totalReduced").alias("ability_total_reduced"),
        col("ability.type").alias("ability_type")
    )

    # --- Damaging Ability
    result[f"{df}_ability"] = df_explode.select(
        "report_id",
        "pull_number",
        "report_date",
        col("damage_ability.name").alias("damage_ability_name"),
        col("damage_ability.total").alias("damage_ability_total"),
        col("damage_ability.totalReduced").alias("damage_ability_total_reduced")
        )

    # --- Damage Abilities
    result[f"{df}_abilities"] = df_explode.select(
        "report_id",
        "pull_number",
        "report_date",
        col("damage_abilities.name").alias("damage_abilities_name"),
        col("damage_abilities.total").alias("damage_abilities_total"),
        col("damage_abilities.totalReduced").alias("damage_abilities_total_reduced"),
        col("damage_abilities.type").alias("damage_abilities_type")
    )

    # --- Damage Events
    result[f"{df}_events"] = df_explode.select(
        "report_id",
        "pull_number",
        "report_date",
        col("event.ability.abilityIcon").alias("ability_icon"),
        col("event.ability.guid").alias("ability_guid"),
        col("event.ability.name").alias("ability_name"),
        col("event.ability.type").alias("ability_type"),
        col("event.absorbed").alias("absorbed"),
        col("event.amount").alias("amount"),
        col("event.blocked").alias("blocked"),
        col("event.fight").alias("fight"),
        col("event.hitType").alias("hit_type"),
        col("event.isAoE").alias("is_aoe"),
        col("event.mitigated").alias("mitigated"),
        col("event.overkill").alias("overkill"),
        col("event.source.guid").alias("source_guid"),
        col("event.source.icon").alias("source_icon"),
        col("event.source.id").alias("source_id"),
        col("event.source.name").alias("source_name"),
        col("event.source.type").alias("source_type"),
        col("event.sourceID").alias("source_id"),
        col("event.sourceInstance").alias("source_instance"),
        col("event.sourceIsFriendly").alias("source_is_friendly"),
        col("event.targetID").alias("target_id"),
        col("event.targetIsFriendly").alias("target_is_friendly"),
        col("event.targetMarker").alias("target_marker"),
        col("event.tick").alias("tick"),
        col("event.timestamp").alias("timestamp"),
        col("event.type").alias("event_type"),
        col("event.unmitigatedAmount").alias("unmitigated_amount")
    )

    # --- Gear
    result[f"{df}_gear"] = df_explode.select(
        "report_id",
        "pull_number",
        "report_date",
        explode_outer("gear.bonusIDs").alias("bonus_id"),
        explode_outer("gear.gems").alias("gem"),
        col("gear.icon").alias("gear_icon"),
        col("gear.id").alias("gear_id"),
        col("gear.itemLevel").alias("gear_item_level"),
        col("gear.name").alias("gear_name"),
        col("gear.permanentEnchant").alias("gear_permanent_enchant"),
        col("gear.permanentEnchantName").alias("gear_permanent_enchant_name"),
        col("gear.quality").alias("gear_quality"),
        col("gear.setID").alias("gear_set_id"),
        col("gear.slot").alias("gear_slot"),
        col("gear.temporaryEnchant").alias("gear_temporary_enchant"),
        col("gear.temporaryEnchantName").alias("gear_temporary_enchant_name")
    )

    # Healing
    result[f"{df}_healing"] = df_explode.select(
        "report_id",
        "pull_number",
        "report_date",
        col("healing_abilities.name").alias("healing_abilities_name"),
        col("healing_abilities.petName").alias("healing_abilities_pet_name"),
        col("healing_abilities.total").alias("healing_abilities_total"),
        col("healing_abilities.totalReduced").alias("healing_abilities_total_reduced"),
        col("healing_abilities.type").alias("healing_abilities_type"),
        "healing_active_time",
        "healing_active_time_reduced",
        col("healing_damage_ability.name").alias("healing_damage_ability_name"),
        col("healing_damage_ability.total").alias("healing_damage_ability_total"),
        col("healing_damage_ability.totalReduced").alias("healing_damage_ability_total_reduced"),
        col("healing_damage_ability.type").alias("healing_damage_ability_type"),
        col("healing_sources.name").alias("healing_sources_name"),
        col("healing_sources.total").alias("healing_sources_total"),
        col("healing_sources.totalReduced").alias("healing_sources_total_reduced"),
        col("healing_sources.type").alias("healing_sources_type"),
        "healing_total",
        "healing_total_reduced"
    )

    # --- Pets
    result[f"{df}_pets"] = df_explode.select(
        "report_id",
        "pull_number",
        "report_date",
        col("pets.activeTime").alias("pet_active_time"),
        col("pets.guid").alias("pet_guid"),
        col("pets.icon").alias("pet_icon"),
        col("pets.id").alias("pet_id"),
        col("pets.name").alias("pet_name"),
        col("pets.total").alias("pet_total"),
        col("pets.totalReduced").alias("pet_total_reduced"),
        col("pets.type").alias("pet_type")
    )

    # --- Talents
    result[f"{df}_talents"] = df_explode.select(
        "report_id",
        "pull_number",
        "report_date",
        "talents"
    )

    # --- Targets
    result[f"{df}_targets"] = df_explode.select(
        "report_id",
        "pull_number",
        "report_date",
        col("targets.name").alias("target_name"),
        col("targets.total").alias("target_total"),
        col("targets.totalReduced").alias("target_total_reduced"),
        col("targets.type").alias("target_type")
    )

    return result

# COMMAND ----------

damage_done_dict = dfParser(df_damage_done)

# COMMAND ----------

print(damage_done_dict.keys())

# COMMAND ----------

df_damage_done_explode.printSchema()

# COMMAND ----------

print("damage_done_abilities:")
display(df_damage_done_abilities)
print("damage_ability:")
display(df_damage_ability)
print("damage_abilities:")
display(df_damage_abilities)
print("damage_events:")
display(df_damage_events)
print("gear:")
display(df_gear)
print("healing:")
display(df_healing)
print("pets:")
display(df_pets)
print("talents:")
display(df_talents)
print("targets:")
display(df_targets)

# COMMAND ----------

df_deaths.printSchema()

# COMMAND ----------

df_healing_explode = df_healing.select(
    "report_id",
    "pull_number",
    "report_date",

)

# COMMAND ----------

display(df_damage_done_explode)

# COMMAND ----------

df_damage_done_abilities = df_damage_done.select(
    "report_id",
    "pull_number",
    "report_date",
    explode_outer("entry.abilities").alias("ability")
)

# COMMAND ----------

display(df_damage_done_abilities)

# COMMAND ----------

df_damage_done_abilities_2 = df_damage_done_abilities.select(
    "report_id",
    "pull_number",
    "report_date",
    "player_name",
    col("ability.name").alias("ability_name"),
    col("ability.petName").alias("ability_pet_name"),
    col("ability.type").alias("ability_type"),
    col("ability.total").alias("ability_total"),
    col("ability.totalReduced").alias("ability_total_reduced")
)

display(df_damage_done_abilities_2)

# COMMAND ----------

df_damage_done.printSchema()
df_healing.printSchema()
df_deaths.printSchema()

# COMMAND ----------

display(df_damage_done)

# COMMAND ----------

df_damage_done_explode = df_damage_done.select(
    "report_id",
    "pull_number",
    "report_date",
    explode_outer("entry.abilities").alias("ability"),
    col("entry.activeTime").alias("active_time"),
    explode_outer("entry.damage.abilities").alias("damage_ability"),
    explode_outer("entry.damageAbilities").alias("damage_abilities"),
    col("entry.deathWindow").alias("death_window"),
    "entry.events",
    "entry.events",
    "entry.gear",
    explode_outer("entry.healing.abilities").alias("healing_abilities"),
    col("entry.itemLevel").alias("item_level"),
    # explode_outer("entry.killingBlow").alias("killing_blow"),
    col("entry.name").alias("player_name"),
    "entry.overheal",
    "entry.overkill",
    explode_outer("entry.pets"),
    explode_outer("entry.talents"),
    explode_outer("entry.targets"),
    col("entry.timestamp").alias("timestamp"),
    "entry.total",
    col("entry.totalReduced").alias("total_reduced"),
    col("entry.type").alias("player_class")
)

# COMMAND ----------

display(df_damage_done_explode)

# COMMAND ----------

# Databricks notebook: Silver layer extraction from "tables" bronze files
# Focused on DamageDone, Healing, Deaths tables

from pyspark.sql.functions import input_file_name, regexp_extract, explode_outer, col

# Load all bronze files for tables
bronze_path = "/Volumes/01_bronze/warcraftlogs/raw_api_calls/tables/"
df_raw = spark.read.json(bronze_path)

# Extract metadata from filename
df = df_raw.withColumn("source_file", input_file_name())
df = df.withColumn("report_id", regexp_extract("source_file", r"([^/]+?)_fight", 1))
df = df.withColumn("pull_number", regexp_extract("source_file", r"_fight(\d+)", 1).cast("int"))
df = df.withColumn("data_type", regexp_extract("source_file", r"_table_([^_]+)", 1))

# Explode entries array (1 per actor)
df_entries = df.select(
    "report_id", "pull_number", "data_type", "source_file",
    col("report_start_date"),  # propagate if present
    explode_outer("table_json.data.entries").alias("entry")
)

# --- Table 1: player_summary (1 row per player per fight)
df_player_summary = df_entries.select(
    "report_id", "pull_number", "report_start_date",
    col("entry.name").alias("player_name"),
    col("entry.guid").alias("player_guid"),
    col("entry.type").alias("player_class"),
    col("entry.activeTime").alias("active_time"),
    col("entry.deathWindow").alias("death_window"),
    col("entry.total").alias("total_damage_or_healing"),
    col("entry.overkill"),
    col("entry.overheal"),
    col("entry.itemLevel").alias("item_level"),
    col("data_type")
)

# --- Table 2: abilities_used (1 row per ability used by player per fight)
df_abilities = df_entries.select(
    "report_id", "pull_number", "report_start_date",
    col("entry.name").alias("player_name"),
    explode_outer("entry.abilities").alias("ability")
).select(
    "report_id", "pull_number", "report_start_date", "player_name",
    col("ability.name").alias("ability_name"),
    col("ability.petName").alias("pet_name"),
    col("ability.total"),
    col("ability.totalReduced"),
    col("ability.type").alias("ability_type")
)

# --- Table 3: damage_sources (sources that damaged the player)
df_damage_sources = df_entries.select(
    "report_id", "pull_number", "report_start_date",
    col("entry.name").alias("player_name"),
    explode_outer("entry.damage.sources").alias("source")
).select(
    "report_id", "pull_number", "report_start_date", "player_name",
    col("source.name").alias("source_name"),
    col("source.total"),
    col("source.totalReduced"),
    col("source.type").alias("source_type")
)

# --- Table 4: talents

df_talents = df_entries.select(
    "report_id", "pull_number", "report_start_date",
    col("entry.name").alias("player_name"),
    explode_outer("entry.talents").alias("talent_id")
)

# --- Save silver tables
(df_player_summary.write.mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable("silver.tables_player_summary"))

(df_abilities.write.mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable("silver.tables_abilities_used"))

(df_damage_sources.write.mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable("silver.tables_damage_sources"))

(df_talents.write.mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable("silver.tables_talents"))
4

# COMMAND ----------

# DBTITLE 1,Explode Dataframe
# Explode entries array (1 per actor)
df_entries = df.select(
    "report_id", "pull_number", "data_type", "source_file",
    col("report_date"),  # propagate if present
    explode_outer("table_json.data.entries").alias("entry")
)

df_entries.printSchema()

# COMMAND ----------

display(df_entries)

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Read Fights Table
fights_df = spark.read.table("02_silver.warcraftlogs.fights_boss_pulls")

# COMMAND ----------

# DBTITLE 1,Extract Tables
# --- Table 1: player_summary (1 row per player per fight)
df_player_summary = df_entries.select(
    "report_id", "pull_number", 
    col("report_start_date").alias("report_date"),
    col("entry.name").alias("player_name"),
    col("entry.guid").alias("player_guid"),
    col("entry.type").alias("player_class"),
    col("entry.activeTime").alias("active_time"),
    col("entry.deathWindow").alias("death_window"),
    col("entry.total").alias("total_damage_or_healing"),
    col("entry.overkill"),
    col("entry.overheal"),
    col("entry.itemLevel").alias("item_level"),
    col("data_type")
)

# --- Table 2: abilities_used (1 row per ability used by player per fight)
df_abilities = df_entries.select(
    "report_id", "pull_number",
    col("report_start_date").alias("report_date"),
    col("entry.name").alias("player_name"),
    explode_outer("entry.abilities").alias("ability")
).select(
    "report_id", "pull_number", "player_name", "report_date",
    col("ability.name").alias("ability_name"),
    col("ability.petName").alias("pet_name"),
    col("ability.total"),
    col("ability.totalReduced"),
    col("ability.type").alias("ability_type")
)

# --- Table 3: damage_sources (sources that damaged the player)
df_damage_sources = df_entries.select(
    "report_id", "pull_number", 
    col("report_start_date").alias("report_date"),
    col("entry.name").alias("player_name"),
    explode_outer("entry.damage.sources").alias("source")
).select(
    "report_id", "pull_number", "player_name",
    col("source.name").alias("source_name"),
    col("source.total"),
    col("source.totalReduced"),
    col("source.type").alias("source_type")
)

# --- Table 4: talents

df_talents = df_entries.select(
    "report_id", "pull_number", 
    col("report_start_date").alias("report_date"),
    col("entry.name").alias("player_name"),
    explode_outer("entry.talents").alias("talent_id")
)

# COMMAND ----------

display(df_player_summary)
display(df_abilities)
display(df_damage_sources)
display(df_talents)

# COMMAND ----------

# DBTITLE 1,Extract Tables
# --- Player Summary ---
df_player_summary = df_entries.select(
    "report_id",
    col("report_start_date").alias("report_date"),
    "bronze_ingestion_time",
    col("name").alias("player_name"),
    col("guid").alias("player_guid"),
    col("type").alias("player_class"),
    col("total").alias("total"),
    col("overkill"), col("overheal"),
    col("activeTime"), col("deathWindow"), col("itemLevel"),
    col("fight").alias("pull_number")
)
 
# --- Abilities ---
df_player_abilities = df_entries.filter(col("abilities").isNotNull()) \
    .withColumn("ability", explode_outer("abilities")) \
    .select(
        "report_id",
        col("report_start_date").alias("report_date"),
        "bronze_ingestion_time",
        col("name").alias("player_name"),
        col("fight").alias("pull_number"),
        col("ability.name").alias("ability_name"),
        col("ability.petName").alias("pet_name"),
        col("ability.total").alias("ability_total"), 
        col("ability.totalReduced").alias("ability_total_reduced"),
        col("ability.type").alias("ability_type")
    )
 
# --- Damage Abilities (merged) ---
df_player_damage_abilities = df_entries.filter(col("damage.abilities").isNotNull()) \
    .withColumn("damage_ability", explode_outer("damage.abilities")) \
    .select(
        "report_id",
        col("report_start_date").alias("report_date"),
        "bronze_ingestion_time",
        col("name").alias("player_name"),
        col("fight").alias("pull_number"),
        col("damage_ability.name").alias("damage_ability_name"),
        col("damage_ability.total").alias("damage_ability_total"),
        col("damage_ability.totalReduced").alias("damage_ability_total_reduced"),
        col("damage_ability.type").alias("damage_ability_type")
    )
 
# --- Healing Abilities ---
df_player_healing_abilities = df_entries.filter(col("healing.abilities").isNotNull()) \
    .withColumn("healing_ability", explode_outer("healing.abilities")) \
    .select(
        "report_id",
        col("report_start_date").alias("report_date"),
        "bronze_ingestion_time",
        col("name").alias("player_name"),
        col("fight").alias("pull_number"),
        col("healing_ability.name").alias("healing_ability_name"),
        col("healing_ability.total").alias("healing_ability_total"),
        col("healing_ability.totalReduced").alias("healing_ability_total_reduced"),
        col("healing_ability.type").alias("healing_ability_type")
    )
 
# --- Events ---
df_player_events = df_entries.filter(col("events").isNotNull()) \
    .withColumn("event", explode_outer("events")) \
    .select(
        "report_id",
        col("report_start_date").alias("report_date"),
        "bronze_ingestion_time",
        col("name").alias("player_name"),
        col("fight").alias("pull_number"),
        col("event.timestamp").alias("event_timestamp"),
        col("event.type").alias("event_type"),
        col("event.amount").alias("event_amount"),
        col("event.absorbed").alias("event_absorbed"),
        col("event.overkill").alias("event_overkill"),
        col("event.ability.name").alias("event_ability_name"),
        col("event.ability.guid").alias("event_ability_guid"),
        col("event.sourceID").alias("event_source_id"),
        col("event.targetID").alias("event_target_id")
    )
 
# --- Gear ---
df_player_gear = df_entries.filter(col("gear").isNotNull()) \
    .withColumn("gear_item", explode_outer("gear")) \
    .select(
        "report_id",
        col("report_start_date").alias("report_date"),
        "bronze_ingestion_time",
        col("name").alias("player_name"),
        col("fight").alias("pull_number"),
        col("gear_item.name").alias("item_name"),
        col("gear_item.id").alias("item_id"),
        col("gear_item.slot").alias("item_slot"), 
        col("gear_item.itemLevel").alias("item_level"),
        col("gear_item.quality").alias("item_quality"),
        col("gear_item.setID").alias("set_id")
    )
 
# --- Pets ---
df_player_pets = df_entries.filter(col("pets").isNotNull()) \
    .withColumn("pet", explode_outer("pets")) \
    .select(
        "report_id",
        col("report_start_date").alias("report_date"),
        "bronze_ingestion_time",
        col("name").alias("player_name"), 
        col("fight").alias("pull_number"),
        col("pet.name").alias("pet_name"),
        col("pet.total").alias("pet_total"),
        col("pet.totalReduced").alias("pet_total_reduced"),
        col("pet.type").alias("pet_type")
    )
 
# --- Talents ---
df_player_talents = df_entries.filter(col("talents").isNotNull()) \
    .withColumn("talent_id", explode_outer("talents")) \
    .select(
        "report_id",
        col("report_start_date").alias("report_date"),
        "bronze_ingestion_time",
        col("name").alias("player_name"),
        col("fight").alias("pull_number"),
        "talent_id"
    )
 
# --- Targets ---
df_player_targets = df_entries.filter(col("targets").isNotNull()) \
    .withColumn("target", explode_outer("targets")) \
    .select(
        "report_id",
        col("report_start_date").alias("report_date"),
        "bronze_ingestion_time",
        col("name").alias("player_name"),
        col("fight").alias("pull_number"),
        col("target.name").alias("target_name"),
        col("target.total").alias("target_total"), 
        col("target.totalReduced").alias("target_total_reduced"), 
        col("target.type").alias("target_type")
    )

# COMMAND ----------

display(df_player_abilities)

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
    "df_player_summary": df_player_summary,
    "df_player_abilities": df_player_abilities,
    "df_player_damage_abilities": df_player_damage_abilities,
    "df_player_healing_abilities": df_player_healing_abilities,
    "df_player_events": df_player_events,
    "df_player_gear": df_player_gear,
    "df_player_pets": df_player_pets,
    "df_player_talents": df_player_talents,
    "df_player_targets": df_player_targets,
}

for name in exploded_tables:
    exploded_tables[name] = lowercase_string_columns(exploded_tables[name], exclude_cols)

# Remove nulls and NPC's from summary
exploded_tables["df_player_summary"] = exploded_tables["df_player_summary"].where(col("player_class").isNotNull())
exploded_tables["df_player_summary"] = exploded_tables["df_player_summary"].where(col("player_class") != "npc")

# Remove NPC's from all tables using summary
collected_summary = exploded_tables["df_player_summary"].collect()
names = []
for row in collected_summary:
    name = row["player_name"]
    if name not in names:
        names.append(name)
for name in exploded_tables:
    exploded_tables[name] = exploded_tables[name].where(col("player_name").isin(names))

# Remove non-boss pulls using df_fights
# collected_fights = fights_df.collect()
# pulls = []
# for row in collected_fights:
#     pull = row["pull_number"]
#     if pull not in pulls:
#         pulls.append(pull)
# for name in exploded_tables:
#     exploded_tables[name] = exploded_tables[name].where(col("pull_number").isin(pulls))

# COMMAND ----------

# DBTITLE 1,Write to Staging Area
for name, df in exploded_tables.items():
    table_suffix = name.removeprefix("df_") 
    df.write.mode("overwrite").saveAsTable(f"02_silver.staging.warcraftlogs_tables_{table_suffix}")
