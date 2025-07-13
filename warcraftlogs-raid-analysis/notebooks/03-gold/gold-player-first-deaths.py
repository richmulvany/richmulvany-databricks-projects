# Databricks notebook source
# DBTITLE 1,Import Dependencies
from pyspark.sql import Window
from pyspark.sql.functions import col, lead, count, when, min, row_number

# COMMAND ----------

deaths_df = spark.table("02_silver.warcraftlogs.events_deaths")
fights_df = spark.table("02_silver.warcraftlogs.fights_boss_pulls")
death_summary_df = spark.table("02_silver.warcraftlogs.tables_deaths_summary").drop("pull_number")

# COMMAND ----------

# Step 1: Get deaths with fight info and kill status
deaths_with_meta = deaths_df.join(fights_df, on=["report_id", "report_date", "pull_number"], how="inner")

deaths_with_meta = deaths_with_meta.join(death_summary_df, 
                                         (deaths_with_meta["report_id"] == death_summary_df["report_id"]) & 
                                         (deaths_with_meta["report_date"] == death_summary_df["report_date"]) &
                                         (deaths_with_meta["timestamp"] == death_summary_df["death_timestamp"]), 
                                         how="inner") \
    .drop(death_summary_df["report_id"]) \
    .drop(death_summary_df["report_date"]) \
    .drop("timestamp")

display(deaths_with_meta)

# COMMAND ----------

death_window = Window.partitionBy("pull_number").orderBy("death_timestamp")
 
# Add death rank
player_deaths_ranked = deaths_with_meta.withColumn(
    "death_rank", row_number().over(death_window)
)

# Take first deaths
first_deaths = player_deaths_ranked.filter(col("death_rank") == 1)

# COMMAND ----------

first_deaths = first_deaths.select(
    "report_id",
    "report_date",
    "pull_number",
    "killer_id",
    "killing_ability_game_id",
    "source_id",
    col("difficulty").alias("boss_difficulty"),
    col("name").alias("boss_name"),
    "start_time",
    "end_time",
    "player_name",
    "death_timestamp",
    "death_window",
    "player_class",
    )

# COMMAND ----------

first_deaths.show()

# COMMAND ----------

output_table = "03_gold.warcraftlogs.player_first_deaths"
first_deaths.write.mode("overwrite").saveAsTable(output_table)

print(f"✅ Player DPS table written to {output_table}")
