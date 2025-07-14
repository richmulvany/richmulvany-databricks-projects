# Databricks notebook source
# DBTITLE 1,Import Dependencies
from pyspark.sql import Window
from pyspark.sql.functions import col, lead, count, when, min

# COMMAND ----------

actors_df = spark.table("02_silver.warcraftlogs.actors_players")
fights_df = spark.table("02_silver.warcraftlogs.fights_boss_pulls")

# COMMAND ----------

# Step 1: Join fights_df and actors_df on report_id
players_df = fights_df.join(actors_df, on="report_id")

# Step 2: Group by player_name and boss_name and count pulls
pull_counts = players_df.groupBy("player_name", col("name").alias("boss_name")).agg(
    count("pull_number").alias("total_pulls")
    )

# Step 3: Show the result
pull_counts.show()

# COMMAND ----------

output_table = "03_gold.warcraftlogs.player_pull_counts"
pull_counts.write.mode("overwrite").option("mergeSchema", True).saveAsTable(output_table)

print(f"✅ Player Pull Count table written to {output_table}")
