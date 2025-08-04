Student Council WarcraftLogs Analysis
Overview

This repository contains an end‑to‑end analytics project for Student Council’s
World of Warcraft guild. The project ingests raw combat logs from
Warcraft Logs via their API, stages the data on
the Databricks Lakehouse using a classic bronze/silver/gold medallion
architecture, and serves interactive dashboards through a Streamlit
application. The dashboards allow guild members to explore their raid
performance, track progression and gear, and identify areas for improvement.

The project is organised into two main parts:

    warcraftlogs‑raid‑analysis/ – Databricks notebooks for ingesting
    Warcraft Logs API data, transforming it into structured tables, and
    computing aggregated metrics (DPS, HPS, deaths, attendance, etc.).

    external‑dashboards/warcraftlogs‑streamlit‑app/ – a Streamlit
    application that uses the exported gold tables to build interactive
    dashboards for raid performance analysis.

Databricks medallion pipeline
Bronze layer

Raw data from the Warcraft Logs API is loaded into the bronze layer.
These tables mirror the JSON returned by the API. For example, the
events table contains one row per event (damage, heal, ability cast,
debuff apply/remove, etc.), with fields such as timestamp, sourceID,
targetID, abilityGameID and so on. The bronze layer retains these
events exactly as received, ensuring that no information is lost.
Silver layer

The silver layer normalises the bronze data, splitting it into
logical tables for easier querying and joining. The notebook
silver‑transform‑events reads the raw events table and writes out a
separate Delta table for each event type. A table_map defines which
event types belong to each table; for example, events_damage stores
damage and absorbed events, events_heal stores heal events,
and events_combatant stores create, combatantinfo, summon and
resurrect events
raw.githubusercontent.com
. The notebook partitions the
output tables by pull number and writes them back to the silver
staging area
raw.githubusercontent.com
.

Another silver notebook, silver‑transform‑actors, flattens player
metadata from the bronze actors and player_details tables. It
extracts each player’s class and spec from the icon field, converts
the nested id and server fields to a clean schema, and produces a
table with columns such as player_id, player_name, player_class,
player_spec, player_role, report_date and report_id
raw.githubusercontent.com
.
Gold layer

The gold layer derives aggregated statistics that underpin the
dashboards. Each notebook in notebooks/03‑gold reads one or more
silver tables, performs metric calculations, and writes the results as
Delta tables under the 03_gold.warcraftlogs schema. Notable gold
notebooks include:

    gold‑player‑dps.py – joins the fights table with summary damage
    data, computes the length of each pull and calculates each player’s
    damage per second (DPS). The output includes columns for
    report_id, raid_name, boss_name, raid_difficulty, pull
    number, player identifiers, total damage and DPS
    raw.githubusercontent.com
    .

    gold‑player‑hps.py – mirrors the DPS notebook but for
    healing per second (HPS), joining healing summary tables with
    fights and deriving healing_per_second for each player
    raw.githubusercontent.com
    .

    gold‑player‑inting.py – analyses death events to identify
    "death cascades" (sometimes called inting). It joins the deaths
    table with fight metadata, uses window functions to look ahead to the
    next three deaths, flags deaths where three subsequent deaths occur
    within 15 seconds
    raw.githubusercontent.com
    , keeps only the first such
    trigger per fight and writes a table of inting events.

    gold‑player‑item‑level.py – computes each player’s average item
    level per pull by exploding the gear information contained in
    combatant‑info events and summarising it across the pull. This file
    is provided in this repository and should be executed to produce the
    03_gold.warcraftlogs.player_item_level table.

After the gold tables have been created, selected metrics are exported
to CSV via the data‑exports folder. These CSVs are small enough to
be consumed by the Streamlit app directly via GitHub’s raw URLs.
Data exports

The data‑exports directory contains CSVs derived from the gold tables.
Examples include:

    player_dps.csv – per‑pull DPS for every player. Fields include
    report_id, report_date, raid_name, boss_name,
    raid_difficulty, pull_number, player_id, player_name,
    player_class, total damage and damage_per_second
    raw.githubusercontent.com
    .

    ranks_dps.csv – parse ranking data from Warcraft Logs. It
    contains per‑pull DPS alongside ranking statistics such as
    total_parses, parse_percent and bracket_percent (parse
    percentile normalised by item level)
    raw.githubusercontent.com
    .

    player_details.csv – metadata for each player on each raid
    report. It holds fields such as player_name, player_role,
    player_class, player_spec and a player_item_level column
    which records the player’s gear level
    raw.githubusercontent.com
    .

Many other exports exist (healing, deaths, inting, pull counts,
guild roster, game data), but the above illustrate the typical schema.
Streamlit dashboard

The external‑dashboards/warcraftlogs‑streamlit‑app folder contains a
multi‑page Streamlit application. It reads the exported CSVs directly
from GitHub and renders charts with Altair and Plotly. The pages are
configured through .streamlit/pages.toml to appear in the sidebar in
the following order: home, damage, healing, deaths and
players. Each page shares a consistent header with the guild logo
and links to Raider.IO, Discord, X (Twitter) and Warcraft Logs.
Home page

The landing page shows your guild’s latest boss kill and presents
progression charts for each boss. A kill summary component displays the
boss name and kill date alongside an image
raw.githubusercontent.com
.
Progression charts layer a band showing the range of pull numbers on
each raid night, a line for boss health remaining over successive pulls
and another line for the lowest boss health achieved
raw.githubusercontent.com
.
There is also a kill composition dot chart which groups players by
role (tank, healer, melee DPS, ranged DPS) and colours them by class
using a predefined palette
raw.githubusercontent.com
.
Damage page

This page provides selectors for raid, boss, difficulty, player roles and
whether to include wipes. It loads player_dps.csv and ranks_dps.csv
to show each player’s average parse percentile (clipped at 100),
average bracket percentile and their difference. Bar charts display
these comparisons by class, and a separate chart plots DPS against
parse percentile. A table summarises each player’s average and best
parses, average DPS and log count. The data is filtered to kill pulls
by default.
Healing page

Mirroring the damage page, the healing page reads player_hps.csv and
ranks_healing.csv (not shown here) to present HPS metrics and parse
percentiles for healers. It uses the same filters and colour palette.
Deaths page

The deaths page analyses player survivability. It loads player_deaths.csv,
player_first_deaths.csv, player_pull_counts.csv and player_inting.csv to
compute per‑player death counts, first‑death rates and inting
incidents (death cascades). Charts display death rates by class and
a table reports counts, rates and the percentage of pulls where a
player was the first to die. A kill/wipe toggle lets you include only
successful pulls when calculating these metrics.
Players page

The players page brings together attendance, parse vs bracket and gear
progression metrics. Attendance is calculated from player_pull_counts.csv
by normalising each player’s total pulls against the raid night with the
highest pulls; results are shown in a bar chart. The parse vs
bracket view compares each player’s average parse percentile against
the bracket percentile from Warcraft Logs and highlights the
difference. The item level view reads player_details.csv,
converts the report_date to a real date and plots each selected
player’s player_item_level over time. A table lists the most
recent item level for the selected players.
Getting started
Requirements

    Databricks: to run the notebooks. The pipeline uses
    Databricks Runtime with PySpark. You will need a cluster with
    adequate permissions and the Databricks CLI configured to access
    your workspace.

    Python 3.9+ with streamlit, pandas, altair, requests and
    plotly to run the dashboard locally.

Running the Databricks pipeline

    Clone this repository to your Databricks workspace or mount it via
    Databricks Repos.

    Configure a Warcraft Logs API key as a secret in your
    workspace. The bronze ingestion notebooks (not included here) expect
    this secret to fetch data.

    Run the notebooks in order:

        Ingest raw events and metadata into the bronze schema.

        Execute the silver transformation notebooks (e.g.
        silver‑transform‑events.py, silver‑transform‑actors.py).

        Execute the gold notebooks (gold‑player‑dps.py, gold‑player‑hps.py,
        gold‑player‑inting.py, gold‑player‑item‑level.py) to compute
        aggregated metrics.

    Optionally run any export notebooks to save the gold tables as
    CSVs into the data‑exports folder.

Running the Streamlit app locally

    Install the required Python packages:

pip install streamlit pandas altair plotly requests

Navigate to the dashboard directory and start the app:

    cd external‑dashboards/warcraftlogs‑streamlit‑app
    streamlit run home.py

    Streamlit will read the CSV exports directly from GitHub via the
    functions defined in each page (for example, load_csv uses
    https://raw.githubusercontent.com/.../data‑exports/player_dps.csv to
    download the data).

    Use the sidebar to navigate between pages and adjust filters such as
    raid, boss, difficulty and player roles.

Deploying on Databricks Lakehouse Apps

Databricks Lakehouse Apps allow you to deploy Streamlit apps within
Databricks. To do this:

    Create a Lakehouse Apps workspace and upload or link this
    repository.

    Set the Entry point file to external‑dashboards/warcraftlogs‑streamlit‑app/home.py.

    Ensure that your gold tables are accessible in the workspace and
    update the REPO_URL constants in each page if the CSVs are stored
    somewhere other than GitHub.

Contributing

Contributions are welcome! If you wish to add new metrics, improve
visualisations or support other content types (e.g. Mythic+), please
open an issue or a pull request. When adding new gold metrics, use
the existing notebooks as templates. Remember to add an export to
data‑exports and update the Streamlit app accordingly.
