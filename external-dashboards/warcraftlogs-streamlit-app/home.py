# home.py
import streamlit as st
import pandas as pd
import requests
import altair as alt
from io import StringIO

# --- GitHub raw base URL ---
REPO_URL = "https://raw.githubusercontent.com/richmulvany/richmulvany-databricks-projects/main/data-exports"

# --- Helper to load CSVs directly from GitHub ---
def load_csv(file_name: str) -> pd.DataFrame:
    url = f"{REPO_URL}/{file_name}"
    response = requests.get(url)
    response.raise_for_status()
    return pd.read_csv(StringIO(response.text))

# --- Streamlit UI ---
logo_path = "https://pbs.twimg.com/profile_images/1490380290962952192/qZk9xi5l_200x200.jpg"

st.logo(
    logo_path,
    link="https://www.warcraftlogs.com/guild/id/586885"
)

st.image(
    "https://pbs.twimg.com/profile_banners/1485002107849789443/1716388990/1500x500"
)

st.markdown(f"""
<div style="text-align: center;">
    <div style="display: flex; align-items: center; justify-content: center;">
        <a href="/" style="text-decoration: none;">
            <img src="{logo_path}" width="64" style="border-radius: 100%; border: 2px solid #FFFFFF; margin-right: 12px;">
        </a>
        <a href="/" style="text-decoration: none; color: inherit;">
            <h1 style="margin: 0;">sc warcraftlogs</h1>
        </a>
    </div>
</div>
""", unsafe_allow_html=True)

<div style="display: flex; justify-content: center; align-items: center; flex-wrap: wrap; gap: 20px; margin-top: 10px;">
 
https://cdn.raiderio.net/images/brand/Mark_White.png" width="32" style="position: relative; top: 2px;" alt="Raider.IO"/>
  </a>
 
https://cdn.prod.website-files.com/6257adef93867e50d84d30e2/66e3d7f4ef6498ac018f2c55_Symbol.svg" width="32" style="position: relative; top: 4px;" alt="Discord"/>
  </a>
 
https://upload.wikimedia.org/wikipedia/commons/thumb/b/b7/X_logo.jpg/1200px-X_logo.jpg" width="36" style="position: relative; top: 0px;" alt="Twitter/X"/>
  </a>
 
https://assets.rpglogs.com/cms/WCL_White_Icon_01_9b25d38cba.png" width="32" style="position: relative; top: 3px; border-radius: 50%;" alt="Warcraft Logs"/>
  </a>
 
</div>
""", unsafe_allow_html=True)

st.divider()
# --- Import progression data --- #
with st.spinner("Loading data..."):
    guild_progression = load_csv("guild_progression.csv")


# --- Get most recent boss kill --- #
kills_only = guild_progression[guild_progression["fight_outcome"] == "kill"]
first_kills = (
    kills_only
    .groupby(["boss_name", "raid_difficulty"])
    .agg({"report_date": "min"})
    .reset_index()
    .rename(columns={"report_date": "first_kill_date"})
)
most_recent_first_kill = first_kills.sort_values("first_kill_date", ascending=False).iloc[0]
boss = most_recent_first_kill["boss_name"]
date = most_recent_first_kill["first_kill_date"].split()[0]

# --- Most recent boss kill announcement --- #
st.markdown(f"## last boss kill: **{boss}**")
st.image("https://pbs.twimg.com/media/GwAb3VQWEAEou3T?format=jpg&name=medium", use_container_width=True)
st.caption(f"killed {date}")

# Filter for boss + difficulty
filtered_df = guild_progression[
    (guild_progression["boss_name"] == boss) &
    (guild_progression["raid_difficulty"] == "mythic")
]

# Get min/max encounter_order per report
panel_df = (
    filtered_df.groupby("report_date")["encounter_order"]
    .agg(["min", "max"])
    .reset_index()
    .rename(columns={"min": "first_pull", "max": "last_pull"})
    .assign(panel_index=lambda df: df.index)
)

# Plot background shading
panel_chart = alt.Chart(panel_df).mark_rect(opacity=0.05).encode(
    x="first_pull:Q",
    x2="last_pull:Q",
    color=alt.condition(
        "datum.panel_index % 2 === 0",
        alt.value("grey"),
        alt.value("white")
    ),
    tooltip=["report_date", "first_pull", "last_pull"]
)

# Base chart setup
x_axis = alt.X("encounter_order:Q", title="pull number")
y_axis = alt.Y("boss_hp_remaining_pct:Q", title="boss hp remaining (%)", scale=alt.Scale(reverse=False))

# Line for actual boss HP at each pull
actual_line = alt.layer(
    alt.Chart(filtered_df).mark_line(color="lightgrey").encode(
    x=x_axis,
    y=y_axis,
    tooltip=["report_date", "fight_outcome", "boss_hp_remaining_pct"]
    ),
    alt.Chart(filtered_df).mark_point(color="lightgrey", filled=True).encode(
    x=x_axis,
    y=y_axis,
    tooltip=["report_date", "fight_outcome", "boss_hp_remaining_pct"]
    )
)

# Line for lowest HP so far (running minimum)
lowest_line = alt.Chart(filtered_df).mark_line(color="#BB86FC").encode(
    x="encounter_order:Q",
    y="boss_hp_lowest_pull:Q",
    tooltip=["encounter_order", "boss_hp_lowest_pull"]
)

# Combine chart
combined_chart = alt.layer(panel_chart, actual_line, lowest_line).properties(
    title=f"{boss} progression"
)

# Display chart
st.altair_chart(combined_chart, use_container_width=True)

# --- Import DPS data --- #

# --- Import HPS data --- #
