# Databricks notebook source
# DBTITLE 1,Import Dependencies
from pyspark.sql import Window
from pyspark.sql.functions import col, lead, count, when, min, row_number

# COMMAND ----------

deaths_df = spark.table("02_silver.warcraftlogs.f_tables_summary_deaths").alias("d")
fights_df = spark.table("02_silver.warcraftlogs.f_fights_boss_pulls").alias("f")

# COMMAND ----------

# Join deaths to fight metadata
deaths_with_meta = deaths_df.join(
    fights_df,
    on=["report_id", "report_date", "pull_number"],
    how="inner"
).alias("df")

# COMMAND ----------

# Rank deaths by timestamp per pull
death_window = Window.partitionBy("df.report_id", "df.report_date", "df.pull_number").orderBy("df.death_time")

ranked_deaths = deaths_with_meta.withColumn("death_rank", row_number().over(death_window))
first_deaths = ranked_deaths.filter(col("death_rank") == 1).alias("fd")

first_deaths = first_deaths.select(
    "fd.report_id",
    "fd.report_date",
    "fd.pull_number",
    "fd.raid_name",
    "fd.boss_name",
    "fd.raid_difficulty",
    "fd.player_id",
    "fd.player_guid",
    "fd.player_name",
    "fd.player_class",
    "fd.player_spec",
    "fd.death_time",
    "fd.death_ability_guid",
    "fd.death_ability_name",
)

# COMMAND ----------

# Write to gold
output_table = "03_gold.warcraftlogs.player_first_deaths"
first_deaths.write.mode("overwrite").saveAsTable(output_table)

print(f"✅ Player first deaths written to {output_table}")
