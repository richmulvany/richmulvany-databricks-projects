# Databricks notebook source
# DBTITLE 1,Import Dependencies
from pyspark.sql.functions import col, round, sum, max

# COMMAND ----------

# DBTITLE 1,Load Required Silver Tables
fights_df = spark.read.table("`02_silver`.warcraftlogs.f_fights_boss_pulls").alias("f")
healing_df = spark.read.table("`02_silver`.warcraftlogs.f_tables_summary_healing").alias("h")

# COMMAND ----------

# DBTITLE 1,Transform
# Calculate fight length
fights_df = fights_df.withColumn("pull_length_seconds", round((col("pull_end_time") - col("pull_start_time")) / 1000, 0))

# Join dataframes
joined_df = healing_df.join(fights_df, ["report_id", "pull_number"])

# Calculate DPS
hps_df = joined_df.withColumn("healing_per_second", round((col("h.healing_done_total") / col("pull_length_seconds")), 0)) 

hps_df = hps_df.select(
    "f.report_id",
    "f.report_date",
    "f.raid_name",
    "f.boss_name",
    "f.raid_difficulty",
    "f.pull_number",
    "h.player_id",
    "h.player_guid",
    "h.player_name",
    "h.player_class",
    col("h.healing_done_total").alias("healing_done"),
    "pull_length_seconds",
    "healing_per_second"
)


# COMMAND ----------

output_table = "03_gold.warcraftlogs.player_hps"
hps_df.write.mode("overwrite").saveAsTable(output_table)

print(f"✅ Player HPS table written to {output_table}")
