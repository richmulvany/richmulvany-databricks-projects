# Databricks notebook source
# DBTITLE 1,Import dependencies
import os
import json
import logging
from datetime import datetime
from typing import Dict, Any
from .client import WarcraftLogsClient

# COMMAND ----------

# DBTITLE 1,Utility: ensure directory exists
def _ensure_dir(path: str):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

# COMMAND ----------

# DBTITLE 1,Utility: checkpoint to remember last ingested report startTime
def _load_checkpoint(path: str) -> int:
    if not os.path.exists(path):
        return 0
    with open(path, "r") as f:
        return int(f.read().strip())


def _save_checkpoint(path: str, ts: int):
    with open(path, "w") as f:
        f.write(str(ts))

# COMMAND ----------

# DBTITLE 1,Fetchers — minimal logic, let the client do GraphQL
def fetch_report(client: WarcraftLogsClient, report_code: str) -> Dict[str, Any]:
    """
    Fetch the high-level metadata + fights index.
    The fights themselves are actually stored inside this GraphQL tree.
    """
    q = """
    query($code:String!) {
      reportData {
        report(code:$code) {
          code
          title
          owner { name }
          startTime
          endTime
          zone { name }
          fights { id name startTime endTime bossPercentage }
        }
      }
    }
    """

    data = client.graphql(q, {"code": report_code})
    report = data["reportData"]["report"]

    if report is None:
        raise ValueError(f"Report '{report_code}' not found.")

    return report


def fetch_fight_events(
    client: WarcraftLogsClient,
    report_code: str,
    fight_id: int,
    view: str = "Events",
    limit: int = 5000,
) -> Dict[str, Any]:
    """
    Fetch events for one fight.
    This uses the table(dataType: ...) GraphQL endpoint internally.
    """
    q = """
    query($code:String!, $fight:Int!, $view:String!, $limit:Int!) {
      reportData {
        report(code:$code) {
          table(fightIDs: [$fight], dataType:$view, limit:$limit)
        }
      }
    }
    """

    data = client.graphql(
        q,
        {
            "code": report_code,
            "fight": fight_id,
            "view": view,
            "limit": limit,
        },
    )

    return data["reportData"]["report"]["table"]

# COMMAND ----------

# DBTITLE 1,Orchestrator — fetch report → write JSON → fetch fights → write JSON
def ingest_single_report(
    client: WarcraftLogsClient,
    report: Dict[str, Any],
    output_dir: str,
):
    """
    Given `{ code, startTime, ... }`, fetch:
      1) Report metadata + fights index → write to JSON
      2) For each fight → fetch events → write to JSON
    """
    code = report["code"]
    start = report["startTime"]
    start_str = datetime.utcfromtimestamp(start / 1000).strftime("%Y-%m-%d_%H-%M-%S")

    report_dir = os.path.join(output_dir, code)
    _ensure_dir(report_dir)

    logging.info(f"Fetching full report: {code}")

    # --------------------------
    # 1) Fetch high-level report
    # --------------------------
    full_report = fetch_report(client, code)

    report_path = os.path.join(report_dir, f"report_{start_str}.json")
    with open(report_path, "w") as f:
        json.dump(full_report, f, indent=2)

    # --------------------------
    # 2) Fetch each fight's events
    # --------------------------
    for fight in full_report["fights"]:
        fight_id = fight["id"]
        logging.info(f"Fetching events for report {code}, fight {fight_id}")

        table_json = fetch_fight_events(client, code, fight_id, view="Events")

        fight_path = os.path.join(report_dir, f"fight_{fight_id}.json")
        with open(fight_path, "w") as f:
            json.dump(
                {
                    "report_code": code,
                    "fight_id": fight_id,
                    "table": table_json,
                },
                f,
                indent=2,
            )

    logging.info(f"Finished ingestion for report {code}")

# COMMAND ----------

# DBTITLE 1,Main incremental orchestration
def ingest_latest_reports(
    client: WarcraftLogsClient,
    guild_name: str,
    server_slug: str,
    region: str,
    output_dir: str,
    checkpoint_path: str,
):
    """
    Incremental ingestion pipeline:
      1) Load last ingested `startTime` checkpoint
      2) Fetch newest report metadata
      3) If startTime > checkpoint → ingest
      4) Save checkpoint
    """
    _ensure_dir(output_dir)
    _ensure_dir(os.path.dirname(checkpoint_path))

    last_ts = _load_checkpoint(checkpoint_path)

    latest = client.get_latest_report_metadata(
        guild_name=guild_name,
        server_slug=server_slug,
        region=region,
    )

    latest_ts = latest["startTime"]

    logging.info(
        f"Last checkpoint = {last_ts}, latest report = {latest['code']} @ {latest_ts}"
    )

    # No new report
    if latest_ts <= last_ts:
        logging.info("No new reports to ingest.")
        return

    # Ingest
    ingest_single_report(client, latest, output_dir)

    # Update checkpoint
    _save_checkpoint(checkpoint_path, latest_ts)

    logging.info("Ingestion completed.")

# COMMAND ----------

# DBTITLE 1,Fetch latest guild reports
def fetch_latest_guild_reports(
    guild: str,
    server: str,
    region: str,
    limit: int = 10,
):
    q = """
    query($guild:String!, $server:String!, $region:String!, $limit:Int!) {
      guildData {
        guild(name:$guild, serverSlug:$server, serverRegion:$region) {
          reports(limit:$limit) {
            data {
              code
              title
              startTime
              endTime
              zone { id name }
              owner { name }
            }
          }
        }
      }
    }
    """
    return gql_query(q, {
        "guild": guild,
        "server": server,
        "region": region,
        "limit": limit,
    })


# COMMAND ----------

# DBTITLE 1,Fetch fight metadata
def fetch_fights(report_id: str):
    q = """
    query($code:String!) {
      reportData {
        report(code:$code) {
          fights {
            id name
            startTime endTime
            kill
            bossPercentage fightPercentage
            lastPhase lastPhaseIsIntermission
            difficulty size
          }
        }
      }
    }
    """
    return gql_query(q, {"code": report_id})

# COMMAND ----------

# DBTITLE 1,Fetch ability dictionary
def fetch_ability_lookup(report_id: str):
    q = """
    query($code:String!) {
      reportData {
        report(code:$code) {
          masterData(translate:true) {
            abilities {
              gameID name subtext
              icon
            }
          }
        }
      }
    }
    """
    return gql_query(q, {"code": report_id})

# COMMAND ----------

# DBTITLE 1,Fetch tables
def fetch_table(report_id: str, fight_id: int, table_type: str):
    """
    table_type: 'DamageDone', 'DamageTaken', 'Healing', 'Summaries', etc.
    """
    q = """
    query($code:String!, $fight:Int!, $type:TableType!) {
      reportData {
        report(code:$code) {
          table(fightIDs:[$fight], dataType:$type)
        }
      }
    }
    """
    return gql_query(q, {
        "code": report_id,
        "fight": fight_id,
        "type": table_type,
    })

# COMMAND ----------

# DBTITLE 1,Fetch events
def fetch_all_events(report_id: str, fight_id: int, start: int, end: int):
    """
    Returns a dict: { "events": [...], "count": N }
    """
    q = """
    query($code:String!, $start:Int!, $end:Int!, $fight:Int!, $page:Int!) {
      reportData {
        report(code:$code) {
          events(
            startTime:$start,
            endTime:$end,
            fightIDs:[$fight],
            page:$page
          ) {
            data
            nextPageTimestamp
          }
        }
      }
    }
    """

    events = []
    page_start = start

    while True:
        payload = gql_query(
            q,
            {
                "code": report_id,
                "fight": fight_id,
                "start": page_start,
                "end": end,
                "page": 1,
            }
        )

        ev = payload["reportData"]["report"]["events"]
        events.extend(ev["data"])

        nxt = ev["nextPageTimestamp"]
        if nxt is None:
            break

        page_start = nxt

    return {"events": events, "count": len(events)}

# COMMAND ----------

# DBTITLE 1,Fetch player details
def fetch_player_details(report_id: str):
    q = """
    query($code:String!) {
      reportData {
        report(code:$code) {
          playerDetails {
            data {
              id name type server
              gear { id name icon itemLevel }
              talents { id name icon }
            }
          }
        }
      }
    }
    """
    return gql_query(q, {"code": report_id})
