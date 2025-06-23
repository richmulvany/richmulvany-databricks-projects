# Databricks notebook source
# DBTITLE 1,Install Libraries
# MAGIC %pip install requests_toolbelt
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Import Dependencies
import requests, json, os
from datetime import datetime
import uuid

# COMMAND ----------

# DBTITLE 1,Configure Notebook / Assign Variables
dbutils.widgets.text("report_id", "")
report_id = dbutils.widgets.get("report_id")

dbutils.widgets.text("fight_id", "")
fight_id = dbutils.widgets.get("fight_id") or None

dbutils.widgets.dropdown("data_source", "fights", ["fights", "events", "actors", "tables"])
data_source = dbutils.widgets.get("data_source")

def get_access_token():
    """
    Retrieves an OAuth2 access token from WarcraftLogs using client credentials stored in Databricks secrets.
    """
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

# DBTITLE 1,Call API and Write to Volume
# Fights Call
if data_source == "fights":
    query = f"""
    {{
      reportData {{
        report(code: "{report_id}") {{
          fights {{
            id
            name
            startTime
            endTime
            kill
            difficulty
          }}
        }}
      }}
    }}
    """
    response = requests.post(base_url, json={"query": query}, headers=headers)
    
    if response.status_code != 200:
        raise Exception(f"Request failed with status code {response.status_code}")
    
    output = response.json()

    if "errors" in output:
        raise Exception(f"GraphQL Error: {output['errors']}")

    # Save fights to JSON
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    filename = f"{report_id}_fights_{ts}.json"
    path = f"/Volumes/01_bronze/warcraftlogs/raw_api_calls/{report_id}/fights/{filename}"
    dbutils.fs.mkdirs(os.path.dirname(path))
    dbutils.fs.put(path, json.dumps(output), overwrite=True)
    print(f"✅ Saved fights data to: {path}")

# Actors Call
elif data_source == "actors":
    query = f"""
    {{
      reportData {{
        report(code: "{report_id}") {{
          masterData {{
            actors {{
              id
              name
              type
              icon
            }}
          }}
        }}
      }}
    }}
    """
    response = requests.post(base_url, json={"query": query}, headers=headers)
    
    if response.status_code != 200:
        raise Exception(f"Request failed with status code {response.status_code}")
    
    output = response.json()

    if "errors" in output:
        raise Exception(f"GraphQL Error: {output['errors']}")

    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    filename = f"{report_id}_actors_{ts}.json"
    path = f"/Volumes/01_bronze/warcraftlogs/raw_api_calls/{report_id}/actors/{filename}"
    dbutils.fs.mkdirs(os.path.dirname(path))
    dbutils.fs.put(path, json.dumps(output), overwrite=True)
    print(f"✅ Saved actor data to: {path}")

# Events call
elif data_source == "events":
    # Step 1: Pull fights
    fight_query = f"""
    {{
      reportData {{
        report(code: "{report_id}") {{
          fights {{
            id
            startTime
            endTime
            name
            kill
          }}
        }}
      }}
    }}
    """
    fight_response = requests.post(base_url, json={"query": fight_query}, headers=headers)
    
    if fight_response.status_code != 200:
        raise Exception(f"Request failed with status code {fight_response.status_code}")
    
    fight_data = fight_response.json()

    if "errors" in fight_data:
        raise Exception(f"GraphQL Error (fights): {fight_data['errors']}")

    fights = fight_data["data"]["reportData"]["report"]["fights"]

    # Step 2: Loop over dataTypes and fights
    essential_data_types = [
        "Casts", "DamageDone", "DamageTaken",
        "Healing", "Deaths", "Buffs", "Debuffs"
    ]

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
                    report(code: "{report_id}") {{
                      events({arg_block}) {{
                        data
                        nextPageTimestamp
                      }}
                    }}
                  }}
                }}
                """

                response = requests.post(base_url, json={"query": query}, headers=headers)
                
                if response.status_code != 200:
                    raise Exception(f"Request failed with status code {response.status_code}")
                
                json_data = response.json()

                if "errors" in json_data:
                    raise Exception(f"GraphQL Error ({data_type}, fight {fid}, page {page}): {json_data['errors']}")

                try:
                    events = json_data["data"]["reportData"]["report"]["events"]["data"]
                except KeyError:
                    print(f"❌ Malformed response for {data_type}, fight {fid}, page {page}")
                    break

                if not events:
                    print(f"⚠️ No {data_type} events for fight {fid}, page {page}")
                    break

                ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
                filename = f"{report_id}_fight{fid}_{data_type}_page{page}_{ts}.json"
                path = f"/Volumes/01_bronze/warcraftlogs/raw_api_calls/{report_id}/events/{filename}"
                dbutils.fs.mkdirs(os.path.dirname(path))
                dbutils.fs.put(path, json.dumps(json_data), overwrite=True)
                print(f"✅ Saved {data_type} fight {fid}, page {page} to: {path}")

                try:
                    next_page_ts = json_data["data"]["reportData"]["report"]["events"]["nextPageTimestamp"]
                    if not next_page_ts or next_page_ts >= end_time:
                        break
                    current_start = next_page_ts
                except KeyError:
                    break

                page += 1

# Fight tables call
elif data_source == "tables":
    # Step 1: Pull fights
    fight_query = f"""
    {{
      reportData {{
        report(code: "{report_id}") {{
          fights {{
            id
            startTime
            endTime
            name
            kill
          }}
        }}
      }}
    }}
    """
    fight_response = requests.post(base_url, json={"query": fight_query}, headers=headers)
    
    if fight_response.status_code != 200:
        raise Exception(f"Request failed with status code {fight_response.status_code}")
    
    fight_data = fight_response.json()

    if "errors" in fight_data:
        raise Exception(f"GraphQL Error (fights): {fight_data['errors']}")

    fights = fight_data["data"]["reportData"]["report"]["fights"]

    # Step 2: Loop over table dataTypes
    table_data_types = ["DamageDone", "Healing", "Deaths"]  # feel free to expand

    for data_type in table_data_types:
        for fight in fights:
            fid = fight["id"]
            query = f"""
              {{
              reportData {{
                report(code: "{report_id}") {{
                  table(dataType: {data_type}, fightIDs: [{fid}])
                }}
              }}
            }}
            """

            response = requests.post(base_url, json={"query": query}, headers=headers)
            
            if response.status_code != 200:
                raise Exception(f"Request failed with status code {response.status_code}")
            
            json_data = response.json()

            if "errors" in json_data:
                raise Exception(f"GraphQL Error (table {data_type}, fight {fid}): {json_data['errors']}")

            # Save to file
            ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
            filename = f"{report_id}_fight{fid}_table_{data_type}_{ts}.json"
            path = f"/Volumes/01_bronze/warcraftlogs/raw_api_calls/{report_id}/tables/{filename}"
            dbutils.fs.mkdirs(os.path.dirname(path))
            dbutils.fs.put(path, json.dumps(json_data), overwrite=True)
            print(f"✅ Saved table {data_type} for fight {fid} to: {path}")
