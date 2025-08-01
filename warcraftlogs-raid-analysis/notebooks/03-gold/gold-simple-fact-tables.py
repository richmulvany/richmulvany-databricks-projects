# Databricks notebook source
# DBTITLE 1,Import Dependencies


# COMMAND ----------

# DBTITLE 1,Configure Notebook / Assign Variables


# COMMAND ----------

# DBTITLE 1,Load Data
composition = spark.table("`02_silver`.warcraftlogs.f_tables_composition").alias("c")
player_deaths = spark.table("`02_silver`.warcraftlogs.f_tables_summary_deaths").alias("d")
boss_pulls = spark.table("`02_silver`.warcraftlogs.f_fights_boss_pulls").alias("p")
actors = spark.table("`02_silver`.warcraftlogs.f_actors_players").alias("a")
player_details = spark.table("`02_silver`.warcraftlogs.f_player_details").alias("pd")
ranks_dps = spark.table("`02_silver`.warcraftlogs.f_rankings_dps").alias("rd")
ranks_hps = spark.table("`02_silver`.warcraftlogs.f_rankings_hps").alias("rh")

# COMMAND ----------

# DBTITLE 1,Transform
player_deaths_joined = player_deaths.join(
    boss_pulls, ["report_id", "report_date", "pull_number"]
) \
.select(
    "d.report_id",
    "d.report_date",
    "p.raid_name",
    "p.boss_name",
    "p.raid_difficulty",
    "d.pull_number",
    "d.player_id",
    "d.player_guid",
    "d.player_name",
    "d.player_class",
    "d.player_spec",
    "d.death_time",
    "d.death_ability_guid",
    "d.death_ability_name"
)

player_details_clean = player_details.dropDuplicates()

# COMMAND ----------

# DBTITLE 1,Write to Gold
composition.write.mode("overwrite").saveAsTable("`03_gold`.warcraftlogs.composition")
player_deaths_joined.write.mode("overwrite").saveAsTable("`03_gold`.warcraftlogs.player_deaths")
player_details_clean.write.mode("overwrite").saveAsTable("`03_gold`.warcraftlogs.player_details")
ranks_dps.write.mode("overwrite").saveAsTable("`03_gold`.warcraftlogs.ranks_dps")
ranks_hps.write.mode("overwrite").saveAsTable("`03_gold`.warcraftlogs.ranks_hps")
