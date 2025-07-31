# Databricks notebook source
# DBTITLE 1,Import Dependencies
from pyspark.sql.window import Window
from pyspark.sql.functions import col, when, row_number, min, lit

# COMMAND ----------

# DBTITLE 1,Load Silver Table
fights_df = spark.read.table("02_silver.warcraftlogs.f_fights_boss_pulls")

# COMMAND ----------

# DBTITLE 1,Transform
fights_enriched = fights_df.withColumn(
    "fight_duration_sec", (col("pull_end_time") - col("pull_start_time")) / 1000
).withColumn(
    "fight_outcome", when(col("kill"), "kill").otherwise("wipe")
).withColumn(
    "boss_hp_remaining_pct", when(col("kill"), lit(0.0)).otherwise(col("boss_percentage"))
)

# Add lowest HP so far by boss + difficulty
lowest_hp_window = Window.partitionBy("boss_name", "raid_difficulty") \
    .orderBy("report_date", "pull_number") \
    .rowsBetween(Window.unboundedPreceding, 0)

# Add sequential encounter order by boss + difficulty
encounter_order_window = Window.partitionBy("boss_name", "raid_difficulty") \
    .orderBy("report_date", "pull_number")

fights_enriched = fights_enriched.withColumn(
    "boss_hp_lowest_pull", min("boss_hp_remaining_pct").over(lowest_hp_window)
)

# Add sequential encounter order by boss + difficulty
encounter_order_window = Window.partitionBy("boss_name", "raid_difficulty") \
    .orderBy("report_date", "pull_number")

fights_enriched = fights_enriched.withColumn(
    "encounter_order", row_number().over(encounter_order_window)
)

# COMMAND ----------

# DBTITLE 1,Select Final Columns
final_cols = [
    "report_id", "report_date", "pull_number", 
    "boss_name", "raid_difficulty", "pull_start_time", "pull_end_time",
    "fight_duration_sec", "last_phase", "last_phase_is_intermission", "fight_outcome", 
    "boss_hp_remaining_pct", "boss_hp_lowest_pull", "encounter_order"
]

guild_progression_df = fights_enriched.select(*final_cols)

# COMMAND ----------

# DBTITLE 1,Write Gold Table
guild_progression_df.write.mode("overwrite").saveAsTable("03_gold.warcraftlogs.guild_progression")
print(f'{guild_progression_df.count()} rows written to table')
