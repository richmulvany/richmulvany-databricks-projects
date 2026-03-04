# Databricks notebook source
# DBTITLE 1,Import Dependencies
from pyspark.sql.functions import (
    col, from_json, explode, size, lit, regexp_replace, trim, lower
)
from pyspark.sql.types import (
    StructType, StructField, ArrayType,
    IntegerType, StringType, DoubleType, StructType
)

# COMMAND ----------

# DBTITLE 1,Define Schemas
# --- Define the schemas (simplified to the relevant parts) ---
character_schema = StructType([
    StructField("amount", DoubleType(), True),
    StructField("best", StringType(), True),
    StructField("bracket", IntegerType(), True),
    StructField("bracketData", DoubleType(), True),
    StructField("bracketPercent", IntegerType(), True),
    StructField("class", StringType(), True),
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("rank", StringType(), True),  # e.g. "~1839"
    StructField("rankPercent", IntegerType(), True),
    StructField("server", StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("region", StringType(), True),
    ]), True),
    StructField("spec", StringType(), True),
    StructField("totalParses", IntegerType(), True)
])

roles_schema = StructType([
    StructField("dps", StructType([
        StructField("characters", ArrayType(character_schema), True),
        StructField("name", StringType(), True),
    ]), True),
    StructField("healers", StructType([
        StructField("characters", ArrayType(character_schema), True),
        StructField("name", StringType(), True),
    ]), True),
    StructField("tanks", StructType([
        StructField("characters", ArrayType(character_schema), True),
        StructField("name", StringType(), True),
    ]), True),
])

fight_schema = StructType([
    StructField("fightID", IntegerType(), True),
    StructField("encounter", StructType([
        StructField("name", StringType(), True)
    ]), True),
    StructField("roles", roles_schema, True),
    # include other fight-level fields if needed 
])

report_schema = StructType([
    StructField("data", ArrayType(fight_schema), True)
])

# helper to extract and tag each role
def explode_role(df, role_name, role_struct_path):
    return (
        df
        .select(
            col("report_id"),
            col("fight.fightID").alias("fight_id"),
            col("fight.encounter.name").alias("boss_name"),
            explode(col(f"fight.roles.{role_name}.characters")).alias("r")
        )
        .withColumn("role", lit(role_name))
    )

# COMMAND ----------

# DBTITLE 1,Read Bronze Rankings Tables
# Read the bronze tables for DPS and HPS rankings.  These tables contain a
# single column named `rankings` which holds the JSON output of the
# GraphQL rankings call, along with metadata columns such as report_start,
# report_id and bronze_ingestion_time.  We'll standardize column names and
# prepare them for downstream consumption by renaming the JSON field.
rankings_dps_raw = spark.read.table("01_bronze.warcraftlogs.rankings_dps")
rankings_hps_raw = spark.read.table("01_bronze.warcraftlogs.rankings_hps")

# COMMAND ----------

# DBTITLE 1,Parse DPS Rankings
dps_parsed = rankings_dps_raw.withColumn("parsed", from_json(col("rankings"), report_schema))

# drop entries where data is null/empty
dps_nonempty = dps_parsed.filter(size(col("parsed.data")) > 0)

# explode fights
dps_fights = dps_nonempty.select(
    col("report_id"),
    explode(col("parsed.data")).alias("fight")
)

dps_df_dps = explode_role(dps_fights, "dps", "fight.roles.dps.characters")
healers_df_dps = explode_role(dps_fights, "healers", "fight.roles.healers.characters")
tanks_df_dps = explode_role(dps_fights, "tanks", "fight.roles.tanks.characters")

# union all roles
dps_combined = dps_df_dps.unionByName(healers_df_dps).unionByName(tanks_df_dps)

# select / normalize fields, e.g. strip "~" from rank and cast
dps_final = (
    dps_combined
    .select(
        col("report_id"),
        lower(col("boss_name")).alias("boss_name"),
        col("fight_id").alias("pull_number"),
        col("role").alias("player_role"),
        col("r.id").alias("player_id"),
        lower(col("r.name")).alias("player_name"),
        lower(col("r.class")).alias("player_class"),
        lower(col("r.spec")).alias("player_spec"),
        # clean "~1234" -> 1234
        regexp_replace(col("r.rank"), "~", "").alias("rank_raw"),
        col("r.amount").alias("dps_amount"),
        col("r.totalParses").alias("total_parses"),
        col("r.rankPercent").alias("parse_percent"),
        col("r.bracketPercent").alias("bracket_percent")
    )
    .withColumn("parse_rank", trim(col("rank_raw")))  # optionally cast to integer later: .cast("int")
    .drop("rank_raw")
)

# COMMAND ----------

# DBTITLE 1,Parse HPS Rankings
hps_parsed = rankings_hps_raw.withColumn("parsed", from_json(col("rankings"), report_schema))

# drop entries where data is null/empty
hps_nonempty = hps_parsed.filter(size(col("parsed.data")) > 0)

# explode fights
hps_fights = hps_nonempty.select(
    col("report_id"),
    explode(col("parsed.data")).alias("fight")
)

dps_df_hps = explode_role(hps_fights, "dps", "fight.roles.dps.characters")
healers_df_hps = explode_role(hps_fights, "healers", "fight.roles.healers.characters")
tanks_df_hps = explode_role(hps_fights, "tanks", "fight.roles.tanks.characters")

# union all roles
hps_combined = dps_df_hps.unionByName(healers_df_hps).unionByName(tanks_df_hps)

# select / normalize fields, e.g. strip "~" from rank and cast
hps_final = (
    hps_combined
    .select(
        col("report_id"),
        lower(col("boss_name")).alias("boss_name"),
        col("fight_id").alias("pull_number"),
        col("role").alias("player_role"),
        col("r.id").alias("player_id"),
        lower(col("r.name")).alias("player_name"),
        lower(col("r.class")).alias("player_class"),
        lower(col("r.spec")).alias("player_spec"),
        # clean "~1234" -> 1234
        regexp_replace(col("r.rank"), "~", "").alias("rank_raw"),
        col("r.amount").alias("hps_amount"),
        col("r.totalParses").alias("total_parses"),
        col("r.rankPercent").alias("parse_percent"),
        col("r.bracketPercent").alias("bracket_percent")
    )
    .withColumn("parse_rank", trim(col("rank_raw")))  # optionally cast to integer later: .cast("int")
    .drop("rank_raw")
)

# COMMAND ----------

# DBTITLE 1,Write to Staging Area
# Write each ranking DataFrame to its own staging table. 
dps_final.write.mode("overwrite").saveAsTable("02_silver.staging.warcraftlogs_rankings_dps")
print("✅ Table 02_silver.staging.warcraftlogs_rankings_dps written.")
hps_final.write.mode("overwrite").saveAsTable("02_silver.staging.warcraftlogs_rankings_hps")
print("✅ Table 02_silver.staging.warcraftlogs_rankings_hps written.")
