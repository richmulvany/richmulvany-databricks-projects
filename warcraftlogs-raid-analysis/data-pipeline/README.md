# Student Council WarcraftLogs Analysis

## Overview

This repository contains an end‑to‑end analytics project for *Student Council’s* World of Warcraft guild.  
The project ingests raw combat logs from [Warcraft Logs](https://www.warcraftlogs.com/) via their API, stages the data on the Databricks Lakehouse using a classic **bronze/silver/gold medallion architecture**, and serves interactive dashboards through a **Streamlit application**.

The dashboards allow guild members to:
- Explore their raid performance
- Track progression and gear
- Identify areas for improvement

### Project Structure

The project is organised into two main parts:

- `warcraftlogs-raid-analysis/` – Databricks notebooks for ingesting Warcraft Logs API data, transforming it into structured tables, and computing aggregated metrics (DPS, HPS, deaths, attendance, etc.).
- `external-dashboards/warcraftlogs-streamlit-app/` – A Streamlit app that uses the exported gold tables to build interactive dashboards for raid performance analysis.

---

## Databricks Medallion Pipeline

### Bronze Layer

- Raw data from the Warcraft Logs API is loaded here.
- These tables **mirror the JSON** returned by the API.
- Example: `events` table includes `timestamp`, `sourceID`, `targetID`, `abilityGameID`, etc.
- **No transformations** – raw ingestion only.

### Silver Layer

- Normalises bronze data into **logical tables** for easier analysis.
- Example: `silver-transform-events.py` creates:
  - `events_damage` for damage and absorbed events
  - `events_heal` for heal events
  - `events_combatant` for combat metadata like create/summon/resurrect

- `silver-transform-actors.py` flattens player metadata:
  - Extracts player class/spec from icons
  - Normalises nested fields
  - Output includes `player_name`, `player_class`, `player_role`, `report_date`, `report_id`, etc.

### Gold Layer

Aggregated statistics that power the dashboards:

- **`gold-player-dps.py`**
  - Joins `fights` with damage summaries
  - Calculates DPS and outputs: `report_id`, `boss_name`, `player_name`, `damage`, `dps`, etc.

- **`gold-player-hps.py`**
  - Like DPS, but for healing

- **`gold-player-inting.py`**
  - Identifies death cascades ("inting") using window functions
  - Flags if 3+ deaths occur within 15s after one death

- **`gold-player-item-level.py`**
  - Explodes gear data from combatant-info events
  - Computes average item level per pull

---

## Data Exports

CSV files derived from the gold layer, stored in `data-exports/`:

- **`player_dps.csv`** – per-pull DPS with player/raid metadata
- **`ranks_dps.csv`** – parse percentile and ranking data
- **`player_details.csv`** – player metadata including class, role, spec, and gear level

> Also includes healing, deaths, inting, pull counts, guild roster, and other files.

---

## Streamlit Dashboard

Located in `external-dashboards/warcraftlogs-streamlit-app/`.  
Reads CSVs from GitHub using raw URLs and visualises data using Altair and Plotly.

### Pages (sidebar order in `.streamlit/pages.toml`)

1. **Home**
2. **Damage**
3. **Healing**
4. **Deaths**
5. **Players**

Each page shares a common header with the guild logo and links to:
- Raider.IO  
- Discord  
- Twitter/X  
- Warcraft Logs

---

## Page Details

### Home Page

- Shows the latest boss kill and progression charts
- Kill summary component (boss + kill date + image)
- Charts show:
  - Range of pull numbers per night
  - Boss health over time
  - Kill composition by class and role

### Damage Page

- Filters: raid, boss, difficulty, roles, wipe toggle
- Reads: `player_dps.csv`, `ranks_dps.csv`
- Visualisations:
  - Bar charts for parse vs bracket percentile
  - DPS vs parse percentile scatter plot
- Table includes avg/best parse %, avg DPS, log count

### Healing Page

- Mirrors the Damage page but for healers
- Reads: `player_hps.csv`, `ranks_healing.csv`

### Deaths Page

- Reads: `player_deaths.csv`, `player_first_deaths.csv`, `player_pull_counts.csv`, `player_inting.csv`
- Shows:
  - Per-player death counts
  - First death rate
  - Inting incidents
- Charts by class; tables for pull/death stats

### Players Page

- Combines attendance, parse vs bracket, and item level
- Attendance = normalised pull count
- Item level plotted over time from `player_details.csv`
- Table shows most recent item level per player

---

## Getting Started

### Requirements

- **Databricks** (PySpark, Databricks Runtime)
- **Python 3.9+** with:
  - `streamlit`
  - `pandas`
  - `altair`
  - `plotly`
  - `requests`

---

## Running the Databricks Pipeline

1. Clone this repository into your Databricks workspace or mount with Repos.
2. Store your Warcraft Logs API key as a **Databricks secret**.
3. Run notebooks in order:

```text
→ Bronze: Ingest raw events and metadata  
→ Silver: silver-transform-events.py, silver-transform-actors.py  
→ Gold: gold-player-dps.py, gold-player-hps.py, gold-player-inting.py, gold-player-item-level.py  
```

4. Export gold tables to `data-exports/` if needed.

---

## Running the Streamlit App Locally

Install dependencies:

```bash
pip install streamlit pandas altair plotly requests
```

Run the app:

```bash
cd external-dashboards/warcraftlogs-streamlit-app
streamlit run home.py
```

> App will fetch CSV data directly from GitHub raw URLs.  
> Use the sidebar to filter by boss, difficulty, roles, etc.

---

## Deploying on Databricks Lakehouse Apps

1. Create a **Lakehouse Apps workspace**
2. Upload or link this repository
3. Set Entry point file to:

```
external-dashboards/warcraftlogs-streamlit-app/home.py
```

4. Ensure gold tables are available and update `REPO_URL` if needed.

---

## Contributing

Contributions welcome!

- Add new metrics using gold notebook templates
- Add corresponding CSV export to `data-exports/`
- Update the Streamlit app to reflect new data

Please open an issue or pull request to propose improvements – especially support for other formats like **Mythic+**.

---
