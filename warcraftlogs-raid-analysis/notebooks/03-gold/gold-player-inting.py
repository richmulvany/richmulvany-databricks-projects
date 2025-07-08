# Databricks notebook source
# DBTITLE 1,Import Dependencies
from pyspark.sql import Window
from pyspark.sql.functions import col, lead, count, when, min

# COMMAND ----------

deaths_df = spark.table("02_silver.warcraftlogs.events_deaths")
fights_df = spark.table("02_silver.warcraftlogs.fights_boss_pulls")
death_summary_df = spark.table("02_silver.warcraftlogs.tables_deaths_summary").drop("pull_number")

# COMMAND ----------

display(fights_df)

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

# Step 3: Identify death cascade
# Window ordered by time within fight
death_window = Window.partitionBy("pull_number").orderBy("death_timestamp")
 
# Lead the next 3 deaths
deaths_with_leads = deaths_with_meta.withColumn("next_death_ts_1", lead("death_timestamp", 1).over(death_window)) \
    .withColumn("next_death_ts_2", lead("death_timestamp", 2).over(death_window)) \
    .withColumn("next_death_ts_3", lead("death_timestamp", 3).over(death_window))
 
# Step 4: Define wipe cascade trigger — next 3 deaths within 15s of this one
deaths_with_trigger_flag = deaths_with_leads.withColumn(
    "is_trigger",
    when(
        (col("next_death_ts_3") - col("death_timestamp") <= 20000),
        True
    ).otherwise(False)
)
 
# Step 5: Keep only the first triggering death per fight
triggering_deaths = deaths_with_trigger_flag \
    .filter(col("is_trigger")) \
    .withColumn("min_ts", min("death_timestamp").over(Window.partitionBy("pull_number"))) \
    .filter(col("death_timestamp") == col("min_ts")) \
    .drop("min_ts")

# COMMAND ----------

triggering_deaths = triggering_deaths.select(
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
    "next_death_ts_1",
    "next_death_ts_2",
    "next_death_ts_3"
    )

# COMMAND ----------

output_table = "03_gold.warcraftlogs.player_inting"
triggering_deaths.write.mode("overwrite").saveAsTable(output_table)

print(f"✅ Player DPS table written to {output_table}")
