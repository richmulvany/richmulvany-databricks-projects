# Databricks notebook source
# DBTITLE 1,Import Dependencies
from pyspark.sql.functions import col, round, sum, max

# COMMAND ----------

# DBTITLE 1,Load Required Silver Tables
silver_table = "02_silver.warcraftlogs.tables_player_summary"
df = spark.read.table(silver_table)

# COMMAND ----------

df = df.where(col("active_time").isNotNull() & (col("active_time") > 0))

df = df.groupBy("report_id", "report_date", "player_class", "player_name") \
       .agg(
           sum("damage_total").alias("damage_total"),
           sum("active_time").alias("active_time"),
           max("item_level").alias("item_level")
       )

df = df.withColumn("dps", round(col("damage_total") / col("active_time")*1000, 0))

# COMMAND ----------

output_table = "03_gold.warcraftlogs.player_dps"
df.write.mode("overwrite").saveAsTable(output_table)

print(f"✅ Player DPS table written to {output_table}")
