# Databricks notebook source
# DBTITLE 1,Import Dependencies
from pyspark.sql.functions import col, explode, from_json, lit, regexp_extract, current_timestamp, input_file_name, regexp_extract
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, MapType

# COMMAND ----------

# DBTITLE 1,Configure Notebook / Assign Variables
dbutils.widgets.text("report_id", "")
report_id = dbutils.widgets.get("report_id")

dbutils.widgets.text("fight_id", "")
fight_id = dbutils.widgets.get("fight_id") or None

dbutils.widgets.dropdown("data_source", "fights", ["fights", "events", "actors", "tables"])
data_source = dbutils.widgets.get("data_source")

# COMMAND ----------

# DBTITLE 1,Extract Data from Volume
raw_json_path = f"/Volumes/01_bronze/warcraftlogs/raw_api_calls/{report_id}/{data_source}/*.json"

# COMMAND ----------

# DBTITLE 1,Extract & Load
def extract_json_to_bronze_table(json_path: str, data_source: str) -> DataFrame:
    """
    Extracts WarcraftLogs raw JSON into Delta format based on data_source.

    Args:
        json_path (str): Path to raw JSON files (e.g., /Volumes/.../events/*.json)
        data_source (str): One of 'events', 'fights', 'actors', 'tables'

    Returns:
        DataFrame: The extracted and flattened DataFrame, written to Delta
    """

    # Load JSON and attach metadata
    raw_json_df = (
        spark.read
        .option("multiline", True)
        .option("includeMetadata", True)
        .json(json_path)
        .withColumn("source_file", col("_metadata.file_path"))
    )

    # Initialize exploded_df per data_source
    if data_source == "events":
        extracted = raw_json_df.selectExpr("data.reportData.report.events.data as records", "source_file")
        exploded_df = extracted.withColumn("record", explode(col("records"))).select("record.*", "source_file")

    elif data_source == "fights":
        extracted = raw_json_df.selectExpr("data.reportData.report.fights as records", "source_file")
        exploded_df = extracted.withColumn("record", explode(col("records"))).select("record.*", "source_file")

    elif data_source == "actors":
        extracted = raw_json_df.selectExpr("data.reportData.report.masterData.actors as records", "source_file")
        exploded_df = extracted.withColumn("record", explode(col("records"))).select("record.*", "source_file")

    elif data_source == "tables":
        # tables return JSON dicts — keep raw
        exploded_df = raw_json_df.selectExpr("data.reportData.report.table as table_json", "source_file")

    else:
        raise ValueError(f"❌ Unsupported data_source: {data_source}")

    # Add metadata columns
    if data_source != "tables":
        final_df = (
            exploded_df
            .withColumn("bronze_ingestion_time", current_timestamp())
            .withColumn("report_id", regexp_extract(col("source_file"), r"/([^/]+)/" + data_source + "/", 1))
        )
    else:
        final_df = (
            exploded_df
            .withColumn("bronze_ingestion_time", current_timestamp())
            .withColumn("report_id", regexp_extract(col("source_file"), r"/([^/]+)/tables/", 1))
        )

    # Write to Delta
    table_name = f"01_bronze.warcraftlogs.{data_source}"
    final_df.write.mode("append").format("delta").saveAsTable(table_name)
    print(f"✅ Extracted and saved: {data_source} → {table_name}")

    return final_df

# COMMAND ----------

# DBTITLE 1,Write to Unity Catalog
extract_json_to_bronze_table(
    json_path=f"/Volumes/01_bronze/warcraftlogs/raw_api_calls/{report_id}/{data_source}/*.json",
    data_source=data_source
)
