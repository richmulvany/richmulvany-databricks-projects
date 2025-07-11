# Databricks notebook source
# DBTITLE 1,Import Libraries
import requests, json, os
from datetime import datetime
from pyspark.sql.functions import current_timestamp

# COMMAND ----------
# DBTITLE 1,Widget Setup
dbutils.widgets.text("report_id", "")
report_id = dbutils.widgets.get("report_id") or None

dbutils.widgets.dropdown("data_source", "fights", ["fights", "events", "actors", "tables"])
data_source = dbutils.widgets.get("data_source")

# COMMAND ----------
# DBTITLE 1,Setup and Auth
def get_access_token():
    client_id = dbutils.secrets.get(scope="warcraftlogs", key="client_id")
    client_secret = dbutils.secrets.get(scope="warcraftlogs", key="client_secret")
    response = requests.post(
        "https://www.warcraftlogs.com/oauth/token",
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret
        }
    )
    return response.json()["access_token"]

token = get_access_token()
headers = {"Authorization": f"Bearer {token}"}
base_url = "https://www.warcraftlogs.com/api/v2/client"

# COMMAND ----------
# DBTITLE 1,Determine Report ID
if not report_id:
    # Default to most recent report from guild
    query = """
    {
      reportData {
        reports(
          guildName: \"Student Council\",
          guildServerSlug: \"twisting-nether\",
          guildServerRegion: \"EU\",
          limit: 1
        ) {
          data {
            code
            startTime
          }
        }
      }
    }
    """
    response = requests.post(base_url, json={"query": query}, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch latest report: {response.status_code}")
    report_data = response.json()["data"]["reportData"]["reports"]["data"]
    if not report_data:
        raise Exception("No reports found")

    report_meta = report_data[0]
    report_id = report_meta["code"]
    report_start_ts = int(report_meta["startTime"]) // 1000
    report_start_date = datetime.utcfromtimestamp(report_start_ts).isoformat()
else:
    # If user supplied report_id, fetch startTime to get report_start_date
    query = f"""
    {{
      reportData {{
        report(code: \"{report_id}\") {{
          startTime
        }}
      }}
    }}
    """
    response = requests.post(base_url, json={"query": query}, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch report {report_id}: {response.status_code}")
    data = response.json()["data"]["reportData"]["report"]
    if not data:
        raise Exception(f"No data found for report {report_id}")
    report_start_ts = int(data["startTime"]) // 1000
    report_start_date = datetime.utcfromtimestamp(report_start_ts).isoformat()

# COMMAND ----------
# DBTITLE 1,Check for Previous Ingestion
def is_already_ingested(report_id: str) -> bool:
    try:
        df = spark.table("01_bronze.logs.warcraftlogs_ingestion_log")
        return df.filter(df.report_id == report_id).count() > 0
    except:
        return False  # Tracking table might not exist yet

if is_already_ingested(report_id):
    print(f"⏭ Report {report_id} already ingested. Skipping.")
    dbutils.notebook.exit("Skipped - already ingested")

# COMMAND ----------
# DBTITLE 1,Ingest Data Based on Source
def save_output(subfolder: str, filename: str, data: dict):
    path = f"/Volumes/01_bronze/warcraftlogs/raw_api_calls/{report_id}/{subfolder}/{filename}"
    dbutils.fs.mkdirs(os.path.dirname(path))
    dbutils.fs.put(path, json.dumps(data), overwrite=True)
    print(f"✅ Saved to: {path}")

ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")

# -- FIGHTS --
if data_source == "fights":
    query = f"""
    {{
      reportData {{
        report(code: \"{report_id}\") {{
          fights {{ id name startTime endTime kill difficulty }}
        }}
      }}
    }}
    """
    response = requests.post(base_url, json={"query": query}, headers=headers)
    output = response.json()
    if "errors" in output:
        raise Exception(f"GraphQL Error: {output['errors']}")
    output["__metadata__"] = {"report_id": report_id, "report_start_date": report_start_date}
    save_output("fights", f"{report_id}_fights_{ts}.json", output)

# -- ACTORS --
elif data_source == "actors":
    query = f"""
    {{
      reportData {{
        report(code: \"{report_id}\") {{
          masterData {{ actors {{ id name type icon }} }}
        }}
      }}
    }}
    """
    response = requests.post(base_url, json={"query": query}, headers=headers)
    output = response.json()
    if "errors" in output:
        raise Exception(f"GraphQL Error: {output['errors']}")
    output["__metadata__"] = {"report_id": report_id, "report_start_date": report_start_date}
    save_output("actors", f"{report_id}_actors_{ts}.json", output)

# -- EVENTS --
elif data_source == "events":
    fight_query = f"""
    {{
      reportData {{
        report(code: \"{report_id}\") {{
          fights {{ id startTime endTime name kill }}
        }}
      }}
    }}
    """
    fight_response = requests.post(base_url, json={"query": fight_query}, headers=headers)
    fights = fight_response.json()["data"]["reportData"]["report"]["fights"]

    essential_data_types = ["Casts", "Deaths", "Debuffs"]
    for data_type in essential_data_types:
        for fight in fights:
            fid = fight["id"]
            start_time = fight["startTime"]
            end_time = fight["endTime"]
            page = 1
            current_start = start_time
            print(f"▶️ {data_type} for fight {fid} ({fight['name']})")
            while True:
                args = [
                    f"dataType: {data_type}",
                    f"fightIDs: [{fid}]",
                    f"startTime: {current_start}",
                    f"endTime: {end_time}"
                ]
                arg_block = ", ".join(args)
                query = f"""
                {{
                  reportData {{
                    report(code: \"{report_id}\") {{
                      events({arg_block}) {{ data nextPageTimestamp }}
                    }}
                  }}
                }}
                """
                response = requests.post(base_url, json={"query": query}, headers=headers)
                output = response.json()
                if "errors" in output:
                    raise Exception(f"GraphQL Error ({data_type}, fight {fid}, page {page}): {output['errors']}")
                events = output["data"]["reportData"]["report"]["events"]["data"]
                if not events:
                    break
                filename = f"{report_id}_fight{fid}_{data_type}_page{page}_{ts}.json"
                output["__metadata__"] = {"report_id": report_id, "report_start_date": report_start_date}
                save_output("events", filename, output)
                next_page_ts = output["data"]["reportData"]["report"]["events"].get("nextPageTimestamp")
                if not next_page_ts or next_page_ts >= end_time:
                    break
                current_start = next_page_ts
                page += 1

# -- TABLES --
elif data_source == "tables":
    fight_query = f"""
    {{
      reportData {{
        report(code: \"{report_id}\") {{
          fights {{ id startTime endTime name kill }}
        }}
      }}
    }}
    """
    fight_response = requests.post(base_url, json={"query": fight_query}, headers=headers)
    fights = fight_response.json()["data"]["reportData"]["report"]["fights"]

    table_data_types = ["DamageDone", "Healing", "Deaths"]

    for data_type in table_data_types:
        for fight in fights:
            fid = fight["id"]
            query = f"""
            {{
              reportData {{
                report(code: \"{report_id}\") {{
                  table(dataType: {data_type}, fightIDs: [{fid}])
                }}
              }}
            }}
            """
            response = requests.post(base_url, json={"query": query}, headers=headers)
            output = response.json()
            if "errors" in output:
                raise Exception(f"GraphQL Error (table {data_type}, fight {fid}): {output['errors']}")
            filename = f"{report_id}_fight{fid}_table_{data_type}_{ts}.json"
            output["__metadata__"] = {"report_id": report_id, "report_start_date": report_start_date}
            save_output("tables", filename, output)

# COMMAND ----------
# DBTITLE 1,Post Report ID Variable for Logging
if data_source == "events":
    dbutils.jobs.taskValues.set(key="report_id", value=report_id)
