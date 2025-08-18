# Databricks notebook source
# DBTITLE 1,Import Dependencies
from pyspark.sql.functions import col, explode, lit, regexp_extract, current_timestamp, from_json, to_json
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType, StructField,
    ArrayType, StringType, IntegerType, LongType, DoubleType, BooleanType
)

# COMMAND ----------

# DBTITLE 1,Configure Notebook / Assign Variables
try:
    # Pull the report_id propagated from the events ingestion task.  If this
    # notebook is run outside of a Databricks job, a ValueError will be thrown
    # and the notebook will exit gracefully.
    report_id = dbutils.jobs.taskValues.get(key="report_id", taskKey="bronze_ingestion_events-task")
except Exception as e:
    print(f"⚠️ Could not retrieve 'report_id': {e}")
    dbutils.notebook.exit("Exiting: report_id not available")

# Temp
# dbutils.widgets.text("report_id", "")
# report_id = dbutils.widgets.get("report_id")

# Widget for fight_id (not used for tables)
dbutils.widgets.text("fight_id", "")
_ = dbutils.widgets.get("fight_id")

# Dropdown for data source
# Include the new ranking data sources so that the extraction layer can
# process raw ranking JSON.  The default remains "tables".
dbutils.widgets.dropdown(
    "data_source", "tables",
    [
        "fights", "events", "actors", "tables", "game_data",
        "world_data", "guild_roster", "player_details",
        # ranking data sources
        "rankings_dps", "rankings_hps"
    ]
)
data_source = dbutils.widgets.get("data_source")

# Path to raw JSON files
raw_json_path = f"/Volumes/01_bronze/warcraftlogs/raw_api_calls/{report_id}/{data_source}/*.json"

# COMMAND ----------

# DBTITLE 1,Define Extract & Load
def extract_json_to_bronze_table(json_path: str, data_source: str) -> DataFrame:
    """
    Extracts WarcraftLogs raw JSON into Delta format based on data_source,
    enforcing a stable schema for the 'tables' data_source to avoid merge conflicts.
    New data sources for rankings (DPS/HPS) simply pass through the JSON
    structure, storing the full rankings payload for downstream processing.
    """
    # --------------------------------------------------
    # 1) Read raw JSON with explicit schema for tables
    # --------------------------------------------------
    raw_json_df = None
    if data_source == "tables":
        # Build nested StructType for `reportData.report.table.data`
        data_fields = [
            # Auras
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
            # Generic entries
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
            # Composition
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
            # Summary arrays
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
            StructField("logFileDetails", StringType(), True)
        ]
        table_struct = StructType([StructField("data", StructType(data_fields), True)])
        # Combine with only __metadata__ (no user-defined _metadata)
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
        raw_json_df = (
            spark.read
                 .schema(full_schema)
                 .json(json_path)
                 .withColumn("source_file", lit(json_path))
        )
    else:
        # For non-table data sources, read the JSON generically.  The schema will
        # be inferred and can evolve as fields change.  We still attach a
        # `source_file` column for downstream traceability.
        raw_json_df = spark.read.json(json_path).withColumn("source_file", lit(json_path))

    # --------------------------------------------------
    # 2) Explode or select fields based on data_source
    # --------------------------------------------------
    if data_source == "fights":
        # Flatten fights
        extracted = raw_json_df.selectExpr(
            "reportData.report.fights AS fights",
            "source_file", 
            "__metadata__.report_start AS report_start"
        )
        exploded_df = extracted.withColumn("fight", explode("fights")).select(
            "fight.*", "source_file", "report_start"
        )
    elif data_source == "tables":
    # Bring the table.data struct out and keep report_start/source_file
    extracted = raw_json_df.selectExpr(
        "reportData.report.table.data AS tables",
        "source_file", "report_start"
    )
    exploded_df = extracted.withColumn("table", explode("tables")).select(
        "table.*", "source_file", "report_start"
    )
    elif data_source == "events":
        # Events are already saved in separate files per page, so we can just
        # explode the events list.  Each row represents an individual event.
        extracted = raw_json_df.selectExpr(
            "reportData.report.events.data AS events",
            "source_file", 
            "__metadata__.report_start AS report_start"
        )
        exploded_df = extracted.withColumn("event", explode("events")).select(
            "event.*", "source_file", "report_start"
        )
    elif data_source == "actors":
        extracted = raw_json_df.selectExpr(
            "reportData.report.masterData.actors AS actors",
            "source_file",
            "__metadata__.report_start AS report_start"
        )
        exploded_df = extracted.withColumn("actor", explode("actors")).select(
            "actor.*", "source_file", "report_start"
        )
    elif data_source == "game_data":
        extracted = raw_json_df.selectExpr(
            "gameData.abilities.data   AS abilities",
            "gameData.classes          AS classes",
            "gameData.items.data       AS items",
            "gameData.zones.data       AS zones",
            "source_file", 
            "__metadata__.report_start AS report_start"
        )
        exploded_df = extracted
    elif data_source == "world_data":
        extracted = raw_json_df.selectExpr(
            "worldData.expansions   AS expansions",
            "worldData.regions      AS regions",
            "worldData.zones        AS zones",
            "source_file",
            "__metadata__.report_start AS report_start"
        )
        exploded_df = extracted
    elif data_source == "guild_roster":
        extracted = raw_json_df.selectExpr(
            "guildData.guild.id           AS guild_id",
            "guildData.guild.name         AS guild_name",
            "guildData.guild.members.data AS records",
            "source_file", 
            "__metadata__.report_start AS report_start"
        )
        exploded_df = extracted \
            .withColumn("member", explode(col("records"))) \
            .select(
                "guild_id", "guild_name",
                col("member.*"),
                "source_file", "report_start"
            )
    elif data_source == "player_details":
        extracted = raw_json_df.selectExpr(
            "reportData.report.playerDetails AS player_details",
            "source_file", 
            "__metadata__.report_start AS report_start"
        )
        exploded_df = extracted
    elif data_source.startswith("rankings_"):
        # Rankings return a nested struct that may evolve over time (e.g. new
        # pagination fields).  To avoid Delta merge conflicts when the schema
        # changes between runs, we serialise the entire rankings struct into a
        # single JSON string.  This preserves the raw payload for later
        # parsing in the silver layer and guarantees a consistent column type.
        extracted = raw_json_df.selectExpr(
            "reportData.report.rankings AS rankings",
            "source_file", "__metadata__.report_start As report_start"
        )
        # Convert the rankings struct into a JSON string to ensure a stable
        # schema across ingestions.
        extracted = extracted.select(
            to_json(col("rankings")).alias("rankings"),
            "source_file", "report_start"
        )
        exploded_df = extracted
    else:
        raise ValueError(f"Unsupported data_source: {data_source}")

    # --------------------------------------------------
    # 3) Append ingestion metadata & write to Delta
    # --------------------------------------------------
    report_id_expr = r"/([^/]+)/" + data_source + "/"
    final_df = exploded_df \
        .withColumn("bronze_ingestion_time", current_timestamp()) \
        .withColumn("report_id", regexp_extract(col("source_file"), report_id_expr, 1))
    # Determine whether to append or overwrite based on data_source.  Appended
    # data sources can accumulate multiple JSONs (e.g. fights, events).  Overwrite
    # sources only store the most recent version (e.g. metadata tables).
    append = ["fights", "events", "actors", "tables", "player_details",
              "rankings_dps", "rankings_hps"]
    overwrite = ["game_data", "world_data", "guild_roster"]
    table_name = f"01_bronze.warcraftlogs.{data_source}"
    if data_source in append:
        final_df.write.mode("append").format("delta").option("mergeSchema", True).saveAsTable(table_name)
    else:
        final_df.write.mode("overwrite").format("delta").option("mergeSchema", True).saveAsTable(table_name)
    print(f"âœ… Extracted and saved: {data_source} â†’ {table_name}")
    return final_df

# COMMAND ----------

# DBTITLE 1,Write to Unity Catalog
extract_json_to_bronze_table(
    json_path=raw_json_path,
    data_source=data_source
)
