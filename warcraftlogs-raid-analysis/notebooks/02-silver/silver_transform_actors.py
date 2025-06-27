# Databricks notebook source
# DBTITLE 1,Import Dependencies
from pyspark.sql.functions import col, expr, split, lower, lit, when, size, date_format

# COMMAND ----------

# DBTITLE 1,Configure Notebook / Assign Variables
lower_columns = ["icon", "name", "type"]
useful_columns = ["icon","id", "name", "type", "report_start_date", "report_id"]

# COMMAND ----------

# DBTITLE 1,Read Table
df = spark.read.table("01_bronze.warcraftlogs.actors")

# COMMAND ----------

# DBTITLE 1,Transform
df = df.select(useful_columns)

for column in lower_columns:
  df = df.withColumn(column, lower(col(column)))

player_df = df.where((col("type") == "player") & (col("icon") != "unknown"))

player_df = player_df.drop("type")

player_df = player_df.withColumn("class_spec", split(col("icon"), "-")) \
        .withColumn("player_class", col("class_spec")[0]) \
        .withColumn("player_spec", when(size(col("class_spec")) > 1, col("class_spec")[1]).otherwise(None)) \
        .drop("class_spec","icon")

player_df = player_df.withColumnRenamed("id", "player_id") \
        .withColumnRenamed("report_start_date", "report_date") \
        .withColumnRenamed("name", "player_name")

player_df = player_df.withColumn("report_date", date_format(col("report_date"), "yyyy-MM-dd EE"))

# COMMAND ----------

display(player_df)

# COMMAND ----------

# DBTITLE 1,Write to Silver
player_df.write.mode("append").saveAsTable("02_silver.warcraftlogs.actors_players")
