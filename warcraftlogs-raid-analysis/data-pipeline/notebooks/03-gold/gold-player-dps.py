# Databricks notebook source
# DBTITLE 1,Import Dependencies
from pyspark.sql.functions import col, round, sum, max

# COMMAND ----------

# DBTITLE 1,Load Required Silver Tables
fights_df = spark.read.table("`02_silver`.warcraftlogs.f_fights_boss_pulls").alias("f")
damage_df = spark.read.table("`02_silver`.warcraftlogs.f_tables_summary_damage").alias("d")

# COMMAND ----------

# DBTITLE 1,Transform
# Calculate fight length
fights_df = fights_df.withColumn("pull_length_seconds", round((col("pull_end_time") - col("pull_start_time")) / 1000, 0))

# Join dataframes
joined_df = damage_df.join(fights_df, ["report_id", "pull_number"])

# Calculate DPS
dps_df = joined_df.withColumn("damage_per_second", round((col("d.damage_done_total") / col("pull_length_seconds")), 0)) 

dps_df = dps_df.select(
    "f.report_id",
    "f.report_date",
    "f.raid_name",
    "f.boss_name",
    "f.raid_difficulty",
    "f.pull_number",
    "d.player_id",
    "d.player_guid",
    "d.player_name",
    "d.player_class",
    col("d.damage_done_total").alias("damage_done"),
    "pull_length_seconds",
    "damage_per_second"
)


# COMMAND ----------

output_table = "03_gold.warcraftlogs.player_dps"
dps_df.write.mode("overwrite").saveAsTable(output_table)

print(f"✅ Player DPS table written to {output_table}")
