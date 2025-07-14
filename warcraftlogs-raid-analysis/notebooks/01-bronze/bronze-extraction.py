# Databricks notebook source
# DBTITLE 1,Import Dependencies
from pyspark.sql.functions import col, explode, from_json, lit, regexp_extract, current_timestamp, input_file_name, regexp_extract
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, MapType

# COMMAND ----------

# DBTITLE 1,Configure Notebook / Assign Variables
# Exit early if report_id is not available as data probably already ingested
try:
    report_id = dbutils.jobs.taskValues.get(key="report_id", taskKey="bronze_ingestion_events-task")
except Exception as e:
    print(f"⚠️ Could not retrieve 'report_id' from dbutils.variables: {e}")
    dbutils.notebook.exit("Exiting: report_id not available via dbutils.variables.")

dbutils.widgets.text("fight_id", "")
fight_id = dbutils.widgets.get("fight_id") or None

dbutils.widgets.dropdown("data_source", "fights", ["fights", "events", "actors", "tables", "game_data", "world_data", "guild_roster", "player_details"])
data_source = dbutils.widgets.get("data_source")

raw_json_path = f"/Volumes/01_bronze/warcraftlogs/raw_api_calls/{report_id}/{data_source}/*.json"

# COMMAND ----------

# DBTITLE 1,Define Extract & Load
def extract_json_to_bronze_table(json_path: str, data_source: str) -> DataFrame:
    """
    Extracts WarcraftLogs raw JSON into Delta format based on data_source.

    Args:
        json_path (str): Path to raw JSON files (e.g., /Volumes/.../<data_source>/*.json)
        data_source (str): One of 'events', 'fights', 'actors', 'tables',
                           'game_data', 'world_data', 'guild_roster', 'player_details'

    Returns:
        DataFrame: The extracted and flattened DataFrame, written to Delta
    """
    raw_json_df = (
        spark.read
            .option("multiline", True)
            .option("includeMetadata", True)
            .json(json_path)
            .withColumn("source_file", col("_metadata.file_path"))
            .withColumn("report_start", col("__metadata__.report_start"))
    )

    if data_source == "player_details":
        # drill into the nested payload: reportData.report.playerDetails.data.playerDetails
        pd_struct = raw_json_df.selectExpr(
            "reportData.report.playerDetails.data.playerDetails AS pd",
            "source_file", "report_start"
        )
        tanks_df = pd_struct.selectExpr("pd.tanks AS recs", "source_file", "report_start") \
                             .withColumn("record", explode(col("recs"))) \
                             .select(col("record.*"), lit("tank").alias("role"), "source_file", "report_start")
        healers_df = pd_struct.selectExpr("pd.healers AS recs", "source_file", "report_start") \
                               .withColumn("record", explode(col("recs"))) \
                               .select(col("record.*"), lit("healer").alias("role"), "source_file", "report_start")
        dps_df = pd_struct.selectExpr("pd.dps AS recs", "source_file", "report_start") \
                           .withColumn("record", explode(col("recs"))) \
                           .select(col("record.*"), lit("dps").alias("role"), "source_file", "report_start")
        exploded_df = tanks_df.unionByName(healers_df).unionByName(dps_df)

    elif data_source == "events":
        extracted = raw_json_df.selectExpr(
            "reportData.report.events.data AS records",
            "source_file", "report_start"
        )
        exploded_df = extracted \
            .withColumn("record", explode(col("records"))) \
            .select("record.*", "source_file", "report_start")

    elif data_source == "fights":
        extracted = raw_json_df.selectExpr(
            "reportData.report.fights AS records",
            "source_file", "report_start"
        )
        exploded_df = extracted \
            .withColumn("record", explode(col("records"))) \
            .select("record.*", "source_file", "report_start")

    elif data_source == "actors":
        extracted = raw_json_df.selectExpr(
            "reportData.report.masterData.actors AS records",
            "source_file", "report_start"
        )
        exploded_df = extracted \
            .withColumn("record", explode(col("records"))) \
            .select("record.*", "source_file", "report_start")

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
            "gameData.encounters.data  AS encounters",
            "source_file", "report_start"
        )

    elif data_source == "world_data":
        exploded_df = raw_json_df.selectExpr(
            "worldData.expansions   AS expansions",
            "worldData.regions      AS regions",
            "worldData.zones        AS zones",
            "source_file", "report_start"
        )

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
        raise ValueError(f"❌ Unsupported data_source: {data_source}")

    # Add ingestion metadata
    report_id_expr = r"/([^/]+)/" + data_source + "/"
    final_df = exploded_df \
        .withColumn("bronze_ingestion_time", current_timestamp()) \
        .withColumn("report_id", regexp_extract(col("source_file"), report_id_expr, 1))

    # Write to Delta
    table_name = f"01_bronze.warcraftlogs.{data_source}"
    final_df.write.mode("append").format("delta").option("mergeSchema", True).saveAsTable(table_name)

    print(f"✅ Extracted and saved: {data_source} → {table_name}")
    return final_df


# COMMAND ----------

# DBTITLE 1,Write to Unity Catalog
extract_json_to_bronze_table(
    json_path=raw_json_path,
    data_source=data_source
)
