# Databricks notebook source
# DBTITLE 1,Import Dependencies
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, expr, split, lower, lit, when, size, date_format
import re

# COMMAND ----------

# DBTITLE 1,Read Tables
actors_raw = spark.table("01_bronze.warcraftlogs.actors")
pd_raw = spark.table("01_bronze.warcraftlogs.player_details")

# COMMAND ----------

# DBTITLE 1,Define Functions
def camel_to_snake(name):
    # normal transitions (e.g. GameID → Game_ID)
    name = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', name) 
    # handle acronyms at end (e.g. GameID → Game_ID)
    name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name)       
    return name.lower()

# Create a UDF from the camel_to_snake function
camel_to_snake_udf = udf(camel_to_snake, StringType())

# COMMAND ----------

# DBTITLE 1,Transform
# Flatten player_details: one row per combatant with their role & spec slug
player_details = (
    pd_raw
    .select(
        col("id").alias("actor_id"),
        col("specs"),    # spec slug
        col("role")         # tank/healer/melee/ranged
    )
    .dropDuplicates(["actor_id"])
)

# Filter to players, join row & specName
players = (
    actors_raw
    .filter(col("type") == "Player")
    .join(player_details, actors_raw.id == player_details.actor_id, how="left")
)

# Drop null for server - avoids udf errors
players = players.filter(col("server").isNotNull())

# Parse class & spec from `icon` (e.g. "warrior-fury")
df = (
    players
    .withColumn("icon", lower(col("icon")))
    .withColumn("cls_spec", split(col("icon"), "-"))
    .withColumn("player_class", col("cls_spec")[0])
    .withColumn("player_spec", when(size(col("cls_spec")) > 1, col("cls_spec")[1]).otherwise("multiple"))
    .drop("cls_spec", "icon", "type", "gameID", "subType", "petOwner", "actor_id")
    .withColumnRenamed("id", "player_id")
    .withColumnRenamed("name", "player_name")
    .withColumn("player_name", lower(col("player_name")))
    .withColumnRenamed("server", "player_server")
    .withColumn("player_server", camel_to_snake_udf(col("player_server")))
    .withColumnRenamed("report_start", "report_date")
    .withColumn("report_date", date_format(col("report_date"), "yyyy-MM-dd E"))
    .withColumnRenamed("role", "player_role")
    .select(
        "player_id",
        "player_name",
        "player_server",
        "player_class",
        "player_spec",
        "player_role",
        "report_date",
        "report_id",
        "bronze_ingestion_time"
    )
)

# COMMAND ----------

# DBTITLE 1,Write to Staging Area
df.write.mode("overwrite").saveAsTable("02_silver.staging.warcraftlogs_actors_players")
