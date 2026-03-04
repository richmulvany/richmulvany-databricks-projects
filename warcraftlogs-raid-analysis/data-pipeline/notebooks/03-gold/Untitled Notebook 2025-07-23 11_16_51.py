# Databricks notebook source
import pandas as pd

# COMMAND ----------

guild_progression = spark.table("`03_gold`.warcraftlogs.guild_progression")
boss = "chrome king gallywix"

# COMMAND ----------

panel_df = (
    guild_progression_filtered.groupby("report_date")["encounter_order"]
    .agg(["min", "max"])
    .reset_index()
    .rename(columns={"min": "first_pull", "max": "last_pull"})
    .assign(panel_index=lambda df: df.index)
)

# COMMAND ----------

guild_progression_boss = guild_progression[
    (guild_progression["boss_name"]  == boss) &
    (guild_progression["raid_difficulty"] == "mythic")
]

# COMMAND ----------

progression_time = guild_progression_boss.groupby("report_id") \
    .agg({
        "pull_start_time": "min",
          "pull_end_time": "max",
          "fight_duration_sec": "sum"
          }) \
    .withColumnsRenamed({
        "min(pull_start_time)": "raid_start",
        "max(pull_end_time)": "raid_end",
        "sum(fight_duration_sec)": "pull_time"
        }) 

# COMMAND ----------

progression_time.show()

# COMMAND ----------

# Filter pulls
guild_progression_boss = guild_progression[
    (guild_progression["boss_name"]  == boss) &
    (guild_progression["raid_difficulty"] == "mythic")
] 

# Calculate time spent on boss
raid_start = min(guild_progression_boss[guild_progression_boss["pull_start_time"]])
raid_end = max(guild_progression_boss[guild_progression_boss["pull_end_time"]])
pull_time = sum(guild_progression_boss["fight_duration_sec"])
yap_time = (raid_end - raid_start) / pull_time
