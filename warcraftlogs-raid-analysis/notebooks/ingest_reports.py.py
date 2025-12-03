# Databricks notebook source
# DBTITLE 1,Import dependencies
from warcraftlogs import WarcraftLogsClient
from warcraftlogs.ingestion import (
    fetch_report_summary,
    fetch_fight_table,
    write_bronze_json,
)
from warcraftlogs.utils import load_json_file, save_json_file
from warcraftlogs.schemas import ReportSummary
import os
import json
from datetime import datetime

# COMMAND ----------

# DBTITLE 1,Configure notebook / assign variables
dbutils.widgets.text("guild_slug", "student-council")
dbutils.widgets.text("server_slug", "twisting-nether")
dbutils.widgets.text("server_region", "eu")
dbutils.widgets.text("max_reports", "5")   # how many recent reports to look at
dbutils.widgets.text("force_report_id", "")   # optional forced ingestion

guild_slug = dbutils.widgets.get("guild_slug")
server_slug = dbutils.widgets.get("server_slug")
server_region = dbutils.widgets.get("server_region")
max_reports = int(dbutils.widgets.get("max_reports"))
force_report_id = dbutils.widgets.get("force_report_id")

# Bronze output location
BRONZE_BASE = "/Volumes/01_bronze/warcraftlogs/raw_api_calls/"

# For incremental ingestion
CHECKPOINT_FILE = "/Volumes/01_bronze/warcraftlogs/checkpoints/latest_report.json"

# COMMAND ----------

# DBTITLE 1,Create API client
api_token = dbutils.secrets.get("warcraftlogs", "api_token")
client = WarcraftLogsClient(api_token=api_token)

# COMMAND ----------

# DBTITLE 1,Load checkpoint
checkpoint = load_json_file(CHECKPOINT_FILE)
last_ingested_start = checkpoint.get("latest_start_time") if checkpoint else None

# COMMAND ----------

# DBTITLE 1,Fetch recent reports
if force_report_id:
    # user is forcing ingestion for a specific report
    report_ids = [force_report_id]
else:
    recent_reports = client.get_latest_reports_for_guild(
        guild_slug,
        server_slug,
        server_region,
        limit=max_reports
    )
    # Filter: only reports newer than checkpoint
    if last_ingested_start:
        recent_reports = [
            r for r in recent_reports
            if r["startTime"] > last_ingested_start
        ]
    report_ids = [r["id"] for r in recent_reports]

if not report_ids:
    print("No new reports to ingest.")
    dbutils.notebook.exit("SUCCESS")

print(f"Reports to ingest: {report_ids}")

# COMMAND ----------

# DBTITLE 1,Process reports
max_start_time_seen = last_ingested_start or 0

for report_id in report_ids:
    # Get metadata
    metadata: ReportSummary = fetch_report_summary(client, report_id)
    print(f"Ingesting report {report_id}: {metadata.title}")

    report_start = metadata.startTime
    if report_start > max_start_time_seen:
        max_start_time_seen = report_start

    # Write metadata (Bronze)
    bronze_meta_path = os.path.join(BRONZE_BASE, f"{report_id}_metadata.json")
    write_bronze_json(bronze_meta_path, metadata.dict())

    # For each fight, fetch multiple table types
    for fight in metadata.fights:
        fight_id = fight.id

        # Choose table types you want in Bronze
        data_types = [
            "DamageDone",
            "DamageTaken",
            "Healing",
            "Casts",
            "Buffs",
            "Deaths",
        ]

        for dt in data_types:
            table_json = fetch_fight_table(
                client=client,
                report_id=report_id,
                fight_id=fight_id,
                data_type=dt
            )

            out_path = os.path.join(
                BRONZE_BASE,
                f"{report_id}_fight{fight_id}_{dt}.json"
            )
            write_bronze_json(out_path, table_json)

print("Ingestion complete.")

# COMMAND ----------

# DBTITLE 1,Update checkpoint
save_json_file(CHECKPOINT_FILE, {"latest_start_time": max_start_time_seen})

print(f"Checkpoint updated: {max_start_time_seen}")
