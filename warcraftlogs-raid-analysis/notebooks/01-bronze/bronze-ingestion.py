# Databricks notebook source
# DBTITLE 1,Import Dependencies
from pyspark.sql.functions import col, explode, lit, regexp_extract, current_timestamp, input_file_name
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType, StructField,
    ArrayType, StringType, IntegerType, LongType, DoubleType, BooleanType
)

# COMMAND ----------
# DBTITLE 1,Configure Notebook / Assign Variables
try:
    report_id = dbutils.jobs.taskValues.get(key="report_id", taskKey="bronze_ingestion_events-task")
except Exception as e:
    print(f"⚠️ Could not retrieve 'report_id': {e}")
    dbutils.notebook.exit("Exiting: report_id not available")

# Widget for fight_id (not used for tables)
dbutils.widgets.text("fight_id", "")
_ = dbutils.widgets.get("fight_id")

# Dropdown for data source
dbutils.widgets.dropdown(
    "data_source", "tables",
    ["fights", "events", "actors", "tables", "game_data",
     "world_data", "guild_roster", "player_details"]
)
data_source = dbutils.widgets.get("data_source")

# Path to raw JSON
raw_json_path = f"/Volumes/01_bronze/warcraftlogs/raw_api_calls/{report_id}/{data_source}/*.json"

# COMMAND ----------
# DBTITLE 1,Define Extract & Load
def extract_json_to_bronze_table(json_path: str, data_source: str) -> DataFrame:
    """
    Extracts WarcraftLogs raw JSON into Delta format based on data_source,
    enforcing a stable schema for the 'tables' data_source to avoid merge conflicts.
    """
    if data_source == "tables":
        # Define nested schema for `table.data`
        data_fields = [
            StructField("auras", ArrayType(
                StructType([
                    StructField("name", StringType(), True),
                    StructField("guid", LongType(), True),
                    StructField("type", IntegerType(), True),
                    StructField("abilityIcon", StringType(), True),
                    StructField("totalUptime", LongType(), True),
                    StructField("totalUses", LongType(), True),
                    StructField("bands", ArrayType(
                        StructType([
                            StructField("startTime", LongType(), True),
                            StructField("endTime", LongType(), True)
                        ])
                    ), True)
                ])
            ), True),
            StructField("entries", ArrayType(
                StructType([
                    StructField("name", StringType(), True),
                    StructField("id", IntegerType(), True),
                    StructField("guid", LongType(), True),
                    StructField("type", StringType(), True),
                    StructField("icon", StringType(), True),
                    StructField("total", LongType(), True),
                    StructField("activeTime", LongType(), True),
                    StructField("activeTimeReduced", LongType(), True),
                    StructField("abilities", ArrayType(
                        StructType([
                            StructField("name", StringType(), True),
                            StructField("total", LongType(), True),
                            StructField("type", IntegerType(), True)
                        ])
                    ), True),
                    StructField("damageAbilities", ArrayType(
                        StructType([
                            StructField("name", StringType(), True),
                            StructField("totalReduced", LongType(), True),
                            StructField("type", StringType(), True)
                        ])
                    ), True)
                ])
            ), True),
            StructField("composition", ArrayType(
                StructType([
                    StructField("name", StringType(), True),
                    StructField("id", IntegerType(), True),
                    StructField("guid", LongType(), True),
                    StructField("type", StringType(), True),
                    StructField("specs", ArrayType(
                        StructType([
                            StructField("spec", StringType(), True),
                            StructField("role", StringType(), True)
                        ])
                    ), True)
                ])
            ), True),
            StructField("healingDone", ArrayType(
                StructType([
                    StructField("name", StringType(), True),
                    StructField("id", IntegerType(), True),
                    StructField("guid", LongType(), True),
                    StructField("type", StringType(), True),
                    StructField("icon", StringType(), True),
                    StructField("total", LongType(), True)
                ])
            ), True),
            StructField("damageDone", ArrayType(
                StructType([
                    StructField("name", StringType(), True),
                    StructField("id", IntegerType(), True),
                    StructField("guid", LongType(), True),
                    StructField("type", StringType(), True),
                    StructField("icon", StringType(), True),
                    StructField("total", LongType(), True)
                ])
            ), True),
            StructField("damageTaken", ArrayType(
                StructType([
                    StructField("name", StringType(), True),
                    StructField("guid", LongType(), True),
                    StructField("type", IntegerType(), True),
                    StructField("abilityIcon", StringType(), True),
                    StructField("total", LongType(), True)
                ])
            ), True),
            StructField("deathEvents", ArrayType(
                StructType([
                    StructField("ability", StructType([
                        StructField("name", StringType(), True),
                        StructField("guid", LongType(), True),
                        StructField("abilityIcon", StringType(), True),
                        StructField("type", IntegerType(), True)
                    ]), True),
                    StructField("deathTime", LongType(), True),
                    StructField("guid", LongType(), True),
                    StructField("icon", StringType(), True),
                    StructField("id", IntegerType(), True),
                    StructField("name", StringType(), True),
                    StructField("type", StringType(), True)
                ])
            ), True),
            StructField("totalTime", LongType(), True),
            StructField("itemLevel", DoubleType(), True),
            StructField("useTargets", BooleanType(), True),
            StructField("startTime", LongType(), True),
            StructField("endTime", LongType(), True),
            StructField("logVersion", IntegerType(), True),
            StructField("gameVersion", IntegerType(), True),
            StructField("logFileDetails", StringType(), True),
            StructField("playerDetails", StructType([
                StructField("healers", ArrayType(StringType()), True),
                StructField("tanks", ArrayType(StringType()), True),
                StructField("dps", ArrayType(StringType()), True)
            ]), True)
        ]
        table_struct = StructType([StructField("data", StructType(data_fields), True)])

        # Full schema including ingestion metadata
        full_schema = StructType([
            StructField("__metadata__", StructType([
                StructField("report_id", StringType(), True),
                StructField("report_start", StringType(), True)
            ]), True),
            StructField("reportData", StructType([
                StructField("report", StructType([
                    StructField("table", table_struct, True)
                ]), True)
            ]), True)
        ])

        # Read with explicit schema
        raw_json_df = (
            spark.read
                .schema(full_schema)
                .option("multiline", True)
                .json(json_path)
                .withColumn("source_file", input_file_name())
                .withColumn("report_start", col("__metadata__.report_start"))
        )
    else:
        # Generic reader for other data_sources
        raw_json_df = (
            spark.read
                .option("multiline", True)
                .option("includeMetadata", True)
                .json(json_path)
                .withColumn("source_file", col("_metadata.file_path"))
                .withColumn("report_start", col("__metadata__.report_start"))
        )

    # Branch on data_source to explode out records...
    if data_source == "player_details":
        pd_struct = raw_json_df.selectExpr(
            "reportData.report.playerDetails.data.playerDetails AS pd",
            "source_file", "report_start"
        )
        # (unchanged tanks/healers/dps logic here)
        exploded_df = tanks_df.unionByName(healers_df).unionByName(dps_df)

    elif data_source == "events":
        extracted = raw_json_df.selectExpr(
            "reportData.report.events.data AS records",
            "source_file", "report_start"
        )
        exploded_df = extracted.withColumn("record", explode(col("records"))).select("record.*", "source_file", "report_start")

    elif data_source == "fights":
        extracted = raw_json_df.selectExpr(
            "reportData.report.fights AS records",
            "source_file", "report_start"
        )
        exploded_df = extracted.withColumn("record", explode(col("records"))).select("record.*", "source_file", "report_start")

    elif data_source == "actors":
        extracted = raw_json_df.selectExpr(
            "reportData.report.masterData.actors AS records",
            "source_file", "report_start"
        )
        exploded_df = extracted.withColumn("record", explode(col("records"))).select("record.*", "source_file", "report_start")

    elif data_source == "tables":
        exploded_df = raw_json_df.selectExpr(
            "reportData.report.table AS table_json",
            "source_file", "report_start"
        )

    elif data_source == "game_data":
        exploded_df = raw_json_df.selectExpr(
            "gameData.abilities.data   AS abilities",
            "gameData.classes          AS classes",
            "gameData.items.data       AS items",
            "gameData.zones.data       AS zones",
            "source_file", "report_start"
        )

    elif data_source == "world_data":
        extracted = raw_json_df.selectExpr(
            "worldData.expansions   AS expansions",
            "worldData.regions      AS regions",
            "worldData.zones        AS zones",
            "source_file", "report_start"
        )
        exploded_df = extracted

    elif data_source == "guild_roster":
        extracted = raw_json_df.selectExpr(
            "guildData.guild.id           AS guild_id",
            "guildData.guild.name         AS guild_name",
            "guildData.guild.members.data AS records",
            "source_file", "report_start"
        )
        exploded_df = extracted \
            .withColumn("member", explode(col("records"))) \
            .select(
                "guild_id", "guild_name",
                col("member.*"),
                "source_file", "report_start"
            )

    else:
        raise ValueError(f"Unsupported data_source: {data_source}")

    # Add ingestion metadata and write
    report_id_expr = r"/([^/]+)/" + data_source + "/"
    final_df = (
        exploded_df
        .withColumn("bronze_ingestion_time", current_timestamp())
        .withColumn("report_id", regexp_extract(col("source_file"), report_id_expr, 1))
    )

    append = ["fights", "events", "actors", "tables", "player_details"]
    overwrite = ["game_data", "world_data", "guild_roster"]
    table_name = f"01_bronze.warcraftlogs.{data_source}"

    if data_source in append:
        final_df.write.mode("append").format("delta").option("mergeSchema", True).saveAsTable(table_name)
    else:
        final_df.write.mode("overwrite").format("delta").option("mergeSchema", True).saveAsTable(table_name)

    print(f"✅ Extracted and saved: {data_source} → {table_name}")
    return final_df

# COMMAND ----------
# DBTITLE 1,Write to Unity Catalog
extract_json_to_bronze_table(
    json_path=raw_json_path,
    data_source=data_source
)
