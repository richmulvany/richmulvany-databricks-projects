# Databricks notebook source
# DBTITLE 1,Import Dependencies
from pyspark.sql import Window
from pyspark.sql.functions import col, lead, count, when, min, row_number

# COMMAND ----------

deaths_df = spark.table("02_silver.warcraftlogs.events_deaths").alias("d")
fights_df = spark.table("02_silver.warcraftlogs.fights_boss_pulls").alias("f")
actors_df = spark.table("02_silver.warcraftlogs.actors_players").alias("a")

# COMMAND ----------

# Join deaths to fight metadata
deaths_with_meta = deaths_df.join(
    fights_df,
    on=["report_id", "report_date", "pull_number"],
    how="inner"
).alias("df")

# COMMAND ----------

# Rank deaths by timestamp per pull
death_window = Window.partitionBy("df.report_id", "df.report_date", "df.pull_number").orderBy("df.timestamp")

ranked_deaths = deaths_with_meta.withColumn("death_rank", row_number().over(death_window))
first_deaths = ranked_deaths.filter(col("death_rank") == 1).alias("fd")

# COMMAND ----------

# Join to get player_name and class info
first_deaths = first_deaths.join(
    actors_df,
    (col("fd.report_id") == col("a.report_id")) &
    (col("fd.report_date") == col("a.report_date")) &
    (col("fd.target_id") == col("a.player_id")),
    how="left"
)

# Select desired columns
first_deaths = first_deaths.select(
    col("fd.report_id").alias("report_id"),
    col("fd.report_date").alias("report_date"),
    col("fd.pull_number").alias("pull_number"),
    col("fd.killer_id").alias("killer_id"),
    col("fd.killing_ability_game_id").alias("killing_ability_game_id"),
    col("fd.source_id").alias("source_id"),
    col("fd.difficulty").alias("boss_difficulty"),
    col("fd.name").alias("boss_name"),
    col("fd.start_time").alias("start_time"),
    col("fd.end_time").alias("end_time"),
    col("a.player_name").alias("player_name"),
    col("a.player_class").alias("player_class"),
    col("fd.timestamp").alias("death_timestamp")
)


# COMMAND ----------

first_deaths.show()

# COMMAND ----------

# Write to gold
output_table = "03_gold.warcraftlogs.player_first_deaths"
first_deaths.write.mode("overwrite").saveAsTable(output_table)

print(f"✅ Player first deaths written to {output_table}")
