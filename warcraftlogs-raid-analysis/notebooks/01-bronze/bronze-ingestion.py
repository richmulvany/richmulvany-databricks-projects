# Databricks notebook source
# DBTITLE 1,Import Libraries
import os
import json
import requests
import time
import random
import logging
from datetime import datetime, timezone
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# COMMAND ----------

# DBTITLE 1,Set Variables
GUILD_NAME = "Student Council"
GUILD_SERVER_SLUG = "twisting-nether"
GUILD_SERVER_REGION = "EU"
BASE_URL = "https://www.warcraftlogs.com/api/v2/client"
INGEST_LOG_TABLE = "01_bronze.logs.warcraftlogs_ingestion_log"
RAW_BASE_PATH = "/Volumes/01_bronze/warcraftlogs/raw_api_calls"

# COMMAND ----------

# DBTITLE 1,Configure Notebook
widgets = dbutils.widgets
widgets.text("report_id", "")
widgets.dropdown(
    "data_source", "fights",
    [
        "fights", "actors", "events", "tables", "player_details",
        "game_data", "world_data", "guild_roster"
    ]
)
 
# Guild overrides
widgets.text("guild_name", GUILD_NAME)
widgets.text("guild_server_slug", GUILD_SERVER_SLUG)
widgets.text("guild_server_region", GUILD_SERVER_REGION)
 
# Read widget values
report_id = widgets.get("report_id") or None
data_source = widgets.get("data_source")
GUILD_NAME = widgets.get("guild_name")
GUILD_SERVER_SLUG = widgets.get("guild_server_slug")
GUILD_SERVER_REGION = widgets.get("guild_server_region")

# COMMAND ----------

# DBTITLE 1,Authenticate and GraphQL Helper with Retries
def get_access_token():
    client_id = dbutils.secrets.get(scope="warcraftlogs", key="client_id")
    client_secret = dbutils.secrets.get(scope="warcraftlogs", key="client_secret")
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret
    }
    resp = requests.post("https://www.warcraftlogs.com/oauth/token", data=payload)
    resp.raise_for_status()
    return resp.json()["access_token"]
 
_token = get_access_token()
HEADERS = {"Authorization": f"Bearer {_token}"}

def gql_query(query: str, variables: dict = None, max_retries: int = 5) -> dict:
    """
    Execute a GraphQL query with retries and exponential backoff on failures.
    """
    last_error = None
    for attempt in range(1, max_retries + 1):
        body = {"query": query}
        if variables:
            body["variables"] = variables
        try:
            resp = requests.post(BASE_URL, json=body, headers=HEADERS)
            resp.raise_for_status()
            data = resp.json()
            if "errors" not in data:
                return data.get("data")
            # GraphQL-level errors
            error_list = data["errors"]
            logging.warning(f"GraphQL errors on attempt {attempt}: {error_list}")
            last_error = RuntimeError(f"GraphQL errors: {error_list}")
        except Exception as e:
            logging.warning(f"Exception on attempt {attempt}: {e}")
            last_error = e

        if attempt == max_retries:
            # All retries exhausted
            raise last_error

        # Exponential backoff with jitter
        backoff_seconds = (2 ** attempt) + random.random()
        time.sleep(backoff_seconds)

# COMMAND ----------

# DBTITLE 1,Retrieve Report Metadata
def get_report_metadata(report_code: str = None) -> dict:
    if report_code:
        q = '''
        query($code: String!) {
          reportData { report(code: $code) { code startTime } }
        }'''
        variables = {"code": report_code}
        result = gql_query(q, variables)
        rep = result["reportData"]["report"]
    else:
        q = '''
        query($name: String!, $slug: String!, $region: String!) {
          reportData {
            reports(
              guildName: $name,
              guildServerSlug: $slug,
              guildServerRegion: $region,
              limit: 1
            ) { data { code startTime } }
          }
        }'''
        vars = {"name": GUILD_NAME, "slug": GUILD_SERVER_SLUG, "region": GUILD_SERVER_REGION}
        result = gql_query(q, vars)
        data = result["reportData"]["reports"]["data"]
        if not data:
            raise ValueError("No reports found for guild")
        rep = data[0]
 
    code = rep["code"]
    ts_ms = int(rep["startTime"])
    ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    return {"code": code, "start_ts": ts, "start_iso": ts.isoformat()}

meta = get_report_metadata(report_id)
report_id = meta["code"]

# COMMAND ----------

# DBTITLE 1,Check Ingestion
def was_ingested(code: str) -> bool:
    try:
        df = spark.table(INGEST_LOG_TABLE)
        return df.filter(col("report_id") == code).limit(1).count() > 0
    except Exception:
        return False
    
if was_ingested(report_id):
    print(f"⏭ Report {report_id} already ingested. Skipping.")
    dbutils.notebook.exit("Skipped - already ingested")

# COMMAND ----------

# DBTITLE 1,Establish Functions
def save_json(subfolder: str, filename: str, payload: dict):
    target = Path(RAW_BASE_PATH) / report_id / subfolder
    target.mkdir(parents=True, exist_ok=True)
    path = target / filename
    with open(path, "w") as f:
        json.dump(
            {
                "__metadata__": {
                    "report_id": report_id,
                    "report_start": meta["start_iso"],
                },
                **payload,
            },
            f,
        )
    print(f"✅ {subfolder}: {path}")

def fetch_fights():
    q = """query($code: String!) { reportData { report(code: $code) {
      fights {
        id name startTime endTime kill difficulty
        averageItemLevel bossPercentage fightPercentage
        gameZone { id name }
      }
    } } }"""
    data = gql_query(q, {"code": report_id})
    save_json(
        "fights", f"{report_id}_fights_{datetime.utcnow():%Y%m%dT%H%M%S}.json", data
    )


def fetch_actors():
    q = """query($code: String!) { reportData { report(code: $code) {
      masterData(translate:false) {
        logVersion gameVersion lang
        actors { id gameID name type subType server petOwner icon }
      }
    } } }"""
    data = gql_query(q, {"code": report_id})
    save_json(
        "actors", f"{report_id}_actors_{datetime.utcnow():%Y%m%dT%H%M%S}.json", data
    )


def fetch_events():
    fights = gql_query(
        """query($code: String!) { reportData { report(code: $code) { fights { id startTime endTime name } } }}""",
        {"code": report_id},
    )["reportData"]["report"]["fights"]
    for fight in fights:
        fid, start_ms, end_ms = fight["id"], fight["startTime"], fight["endTime"]
        page, cursor = 1, start_ms
        while True:
            q = f"""query {{ reportData {{ report(code: "{report_id}") {{ events(
                fightIDs: [{fid}], startTime: {cursor}, endTime: {end_ms},
                dataType: All, includeResources: true, limit: 10000
            ) {{ data nextPageTimestamp }} }} }} }}"""
            result = gql_query(q)
            evts = result["reportData"]["report"]["events"]["data"]
            if not evts:
                break
            fname = f"{report_id}_fight{fid}_events_page{page}_{datetime.utcnow():%Y%m%dT%H%M%S}.json"
            save_json("events", fname, result)
            next_ts = result["reportData"]["report"]["events"].get("nextPageTimestamp")
            if not next_ts or next_ts >= end_ms:
                break
            cursor, page = next_ts, page + 1


def fetch_tables():
    fights = gql_query(
        """query($c:String!){ reportData{ report(code:$c){ fights{id} }}}""",
        {"c": report_id},
    )["reportData"]["report"]["fights"]
    for fight in fights:
        fid = fight["id"]
        for dt in ["Summary", "DamageDone", "Dispels", "Healing", "Buffs", "Casts", "Deaths"]:
            q = f"""query {{ reportData {{ report(code: "{report_id}") {{ table(dataType: {dt}, fightIDs: [{fid}]) }} }} }}"""
            res = gql_query(q)
            fname = f"{report_id}_fight{fid}_table_{dt}_{datetime.utcnow():%Y%m%dT%H%M%S}.json"
            save_json("tables", fname, res)


def fetch_game_data():
    # Fetch static game metadata with corrected fields
    q = """query {
      gameData {
        abilities(limit: 100) { data { id name icon } }
        classes            { id name slug specs { id name slug } }
        items(limit: 100)  { data { id name icon } }
        zones(limit: 100)  { data { id name } }
      }
    }"""
    data = gql_query(q)
    save_json("game_data", f"game_data_{datetime.utcnow():%Y%m%dT%H%M%S}.json", data)


def fetch_world_data():
    # Fetch expansions, regions, and zones in one call
    q = """query {
      worldData {
        expansions {
          id
          name
        }
        regions {
          id
          name
          slug
        }
        zones {
          id
          name
          frozen
        }
      }
    }"""
    data = gql_query(q)
    save_json("world_data", f"world_data_{datetime.utcnow():%Y%m%dT%H%M%S}.json", data)


def fetch_guild_roster(limit: int = 100):
    q = """query($name:String!,$slug:String!,$region:String!,$limit:Int!) {
      guildData {
        guild(name:$name, serverSlug:$slug, serverRegion:$region) {
          id
          name
          description
          faction { name }
          server  { name slug }
          members(limit: $limit) {
            data {
              id
              name
              classID
              level
              faction { name }
              server  { name slug }
              guildRank
              hidden
            }
          }
        }
      }
    }"""
    variables = {
        "name": GUILD_NAME,
        "slug": GUILD_SERVER_SLUG,
        "region": GUILD_SERVER_REGION,
        "limit": limit,
    }
    data = gql_query(q, variables)
    save_json(
        "guild_roster",
        f"guild_{GUILD_NAME}_{datetime.utcnow():%Y%m%dT%H%M%S}.json",
        data,
    )


def fetch_player_details():
    # Get all fight IDs for the report
    fights_q = """query($code:String!){
      reportData {
        report(code:$code) {
          fights { id }
        }
      }
    }"""
    fights_data = gql_query(fights_q, {"code": report_id})
    fight_ids = [f["id"] for f in fights_data["reportData"]["report"]["fights"]]

    # Fetch playerDetails using those fight IDs
    pd_q = """query($code:String!,$fids:[Int!]!) {
      reportData {
        report(code:$code) {
          playerDetails(fightIDs: $fids, includeCombatantInfo: false)
        }
      }
    }"""
    data = gql_query(pd_q, {"code": report_id, "fids": fight_ids})
    save_json(
        "player_details",
        f"{report_id}_playerDetails_{datetime.utcnow():%Y%m%dT%H%M%S}.json",
        data,
    )

# COMMAND ----------

# DBTITLE 1,Fetch Data
if data_source == "fights":
    fetch_fights()
elif data_source == "actors":
    fetch_actors()
elif data_source == "events":
    fetch_events()
elif data_source == "tables":
    fetch_tables()
elif data_source == "game_data":
    fetch_game_data()
elif data_source == "world_data":
    fetch_world_data()
elif data_source == "guild_roster":
    fetch_guild_roster()
elif data_source == "player_details":
    fetch_player_details()
else:
    raise ValueError(f"Unknown data_source: {data_source}")

# COMMAND ----------

# DBTITLE 1,Propogate Report ID for Downstream
if data_source == "events":
    dbutils.jobs.taskValues.set(key="report_id", value=report_id)
