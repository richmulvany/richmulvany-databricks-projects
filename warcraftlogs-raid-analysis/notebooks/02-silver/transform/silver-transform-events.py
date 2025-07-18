# Databricks notebook source
# DBTITLE 1,Import Dependencies
from pyspark.sql.functions import col, expr, split, lower, lit, when, size, date_format, count
import re

# COMMAND ----------

# DBTITLE 1,Configure Notebook / Assign Variables
path = "02_silver.staging"

columns_to_keep = [
    "report_id",
    col("fight").alias("pull_number"), 
    "timestamp", "type",
    "sourceID", "sourceInstance", "targetID", "attackerID",
    "abilityGameID", "extraAbilityGameID", "hitType", "amount", 
    "unmitigatedAmount", "mitigated", "absorbed", 
    col("isAoE").alias("is_aoe"), 
    col("report_start").alias("report_date")
]

table_map = {
    "events_damage": ["damage", "absorbed", "healabsorbed"],
    "events_heal": ["heal"],
    "events_aura": [
        "applybuff","refreshbuff","removebuff","applybuffstack","removebuffstack",
        "applydebuff","refreshdebuff","removedebuff","applydebuffstack","removedebuffstack",
        "aurabroken","empowerstart","empowerend"
    ],
    "events_casts": ["begincast", "cast", "interrupt", "extraattacks"],
    "events_resource": ["resourcechange"],
    "events_combatant": ["create", "combatantinfo", "summon", "resurrect"],
    "events_lifecycle": ["encounterstart", "encounterend", "death"],
    "events_dispel": ["dispel"],
}

# COMMAND ----------

# DBTITLE 1,Read Table
events_df = spark.read.table("01_bronze.warcraftlogs.events")

base_df = events_df.withColumn("report_start", date_format(col("report_start"), "yyyy-MM-dd EE")).select(*columns_to_keep)

# COMMAND ----------

# DBTITLE 1,Convert Field Names to Snakecase
def camel_to_snake(name):
    # Add underscore between a lowercase char and an uppercase char (handles acronyms followed by lowercase)
    name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name)
    
    # Add underscore between acronym and next capitalized word (e.g., IDParser → ID_Parser)
    name = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', name)

    return name.lower()

base_df = base_df.toDF(*[camel_to_snake(column_name) for column_name in base_df.columns])

# COMMAND ----------

# DBTITLE 1,Write Type Tables
for table_name, types in table_map.items():
    (base_df
        .filter(base_df.type.isin(types))
        .write
        .mode("overwrite")
        .option("mergeSchema", "true")
        .partitionBy("pull_number")
        .saveAsTable(f"{path}.warcraftlogs_{table_name}")
    )
    print(f"{table_name} written to {path}.warcraftlogs_{table_name}.")
print(f"✅ All individual tables written to {path}.warcraftlogs_*.")

# COMMAND ----------

# DBTITLE 1,Write Full Event Table
(base_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .partitionBy("pull_number", "type")
    .saveAsTable(f"{path}.warcraftlogs_events_all")
)
print(f"✅ events_all written to {path}.warcraftlogs_events_all.")
