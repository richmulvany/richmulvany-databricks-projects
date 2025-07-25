# home.py
import streamlit as st
import pandas as pd
import json
import requests
import altair as alt
import plotly.graph_objects as go
from io import StringIO

# --- GitHub raw base URL ---
REPO_URL = "https://raw.githubusercontent.com/richmulvany/richmulvany-databricks-projects/main"

# --- Helpers to load files directly from GitHub ---
def load_csv(file_name: str) -> pd.DataFrame:
    url = f"{REPO_URL}/data-exports/{file_name}"
    response = requests.get(url)
    response.raise_for_status()
    return pd.read_csv(StringIO(response.text))

def load_json(file_name: str) -> dict:
    url = f"{REPO_URL}/external-dashboards/warcraftlogs-streamlit-app/{file_name}"
    response = requests.get(url)
    response.raise_for_status()
    return json.loads(response.text)

# --- Streamlit UI ---
logo_path = "https://pbs.twimg.com/profile_images/1490380290962952192/qZk9xi5l_200x200.jpg"

# Set tab config
st.set_page_config(page_title="players · sc-warcraftlogs", page_icon=logo_path, layout="wide")


# Function to workaround container size
def st_normal():
    _, col, _ = st.columns([1, 8.5, 1])
    return col

st.logo(
    logo_path,
    link="https://www.warcraftlogs.com/guild/id/586885"
)

st.image(
    "https://pbs.twimg.com/profile_banners/1485002107849789443/1716388990/1500x500"
)

# --- Title with logo --- #
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

# --- Social icons --- #
st.markdown("""
<div style="display: flex; justify-content: center; align-items: center; flex-wrap: wrap; gap: 20px; margin-top: 10px;">
  <a href="https://raider.io/guilds/eu/twisting-nether/Student%20Council" target="_blank">
    <img src="https://cdn.raiderio.net/images/brand/Mark_White.png" width="32" style="position: relative; top: 0px;" alt="Raider.IO"/>
  </a>
  <a href="https://discord.gg/studentcouncil" target="_blank">
    <img src="https://cdn.prod.website-files.com/6257adef93867e50d84d30e2/66e3d7f4ef6498ac018f2c55_Symbol.svg" width="32" style="position: relative; top: 0px;" alt="Discord"/>
  </a>
  <a href="https://x.com/SCTNGuild" target="_blank">
    <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/b/b7/X_logo.jpg/1200px-X_logo.jpg" width="36" style="position: relative; top: 0px;" alt="Twitter/X"/>
  </a>
  <a href="https://www.warcraftlogs.com/guild/id/586885" target="_blank">
    <img src="https://assets.rpglogs.com/cms/WCL_White_Icon_01_9b25d38cba.png" width="32" style="position: relative; top: 0px; border-radius: 50%;" alt="Warcraft Logs"/>
  </a>
</div>
""", unsafe_allow_html=True)

st.divider()

# --- Load data --- #
with st.spinner("Loading data..."):
    guild_progression = load_csv("guild_progression.csv")
    player_dps = load_csv("player_dps.csv")
    player_hps = load_csv("player_hps.csv")
    player_deaths = load_csv("player_deaths.csv")

# --- Determine most recent first kill --- #
kills_only = guild_progression[guild_progression["fight_outcome"] == "kill"]
first_kill_rows = (
    kills_only
    .sort_values("report_date")
    .drop_duplicates(subset=["boss_name", "raid_difficulty"], keep="first")
)
most_recent_first_kill_row = first_kill_rows.sort_values("report_date", ascending=False).iloc[0]

boss = most_recent_first_kill_row["boss_name"]
date = most_recent_first_kill_row["report_date"].split()[0]
record_kill_report = most_recent_first_kill_row["report_id"]
record_kill_pull_number = most_recent_first_kill_row["pull_number"]

# --- Show boss kill summary --- #
with st_normal():
    st.markdown(f"## last boss kill: **{boss}**")
    st.image("https://pbs.twimg.com/media/GwAb3VQWEAEou3T?format=jpg&name=medium", use_container_width=True)
    st.caption(f"killed {date}")

# --- Filter progression data for this boss --- #
guild_progression_filtered = guild_progression[
    (guild_progression["boss_name"] == boss) &
    (guild_progression["raid_difficulty"] == "mythic")
]

# --- Generate background shading per report date --- #
panel_df = (
    guild_progression_filtered.groupby("report_date")["encounter_order"]
    .agg(["min", "max"])
    .reset_index()
    .rename(columns={"min": "first_pull", "max": "last_pull"})
    .assign(panel_index=lambda df: df.index)
)

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

# --- Line chart of progression --- #
x_axis = alt.X("encounter_order:Q", title="pull number")
y_axis = alt.Y("boss_hp_remaining_pct:Q", title="boss hp remaining (%)", scale=alt.Scale(reverse=False))

actual_line = alt.layer(
    alt.Chart(guild_progression_filtered).mark_line(color="lightgrey").encode(
        x=x_axis,
        y=y_axis,
        tooltip=["report_date", "fight_outcome", "boss_hp_remaining_pct"]
    ),
    alt.Chart(guild_progression_filtered).mark_point(color="lightgrey", filled=True).encode(
        x=x_axis,
        y=y_axis,
        tooltip=["report_date", "fight_outcome", "boss_hp_remaining_pct"]
    )
)

lowest_line = alt.Chart(guild_progression_filtered).mark_line(color="#BB86FC").encode(
    x="encounter_order:Q",
    y="boss_hp_lowest_pull:Q",
    tooltip=["encounter_order", "boss_hp_lowest_pull"]
)

combined_chart = alt.layer(panel_chart, actual_line, lowest_line).properties(
    title=f"{boss} progression"
)

with st_normal():
    st.altair_chart(combined_chart, use_container_width=True)

# --- Class colours for bar charts --- #
CLASS_COLOURS = load_json("class_colours.json")

# --- Filter DPS data for the kill --- #
player_dps_filtered = player_dps[
    (player_dps["report_id"] == record_kill_report) &
    (player_dps["pull_number"] == record_kill_pull_number)
]
player_dps_filtered = player_dps_filtered.sort_values("damage_per_second", ascending=False)

# --- DPS Bar Chart --- #
bar_chart = (
    alt.Chart(player_dps_filtered)
    .mark_bar()
    .encode(
        y=alt.Y("player_name:N",
                sort=player_dps_filtered["player_name"].tolist(),
                title="",
                axis=alt.Axis(labelOverlap=False)),
        x=alt.X("damage_per_second:Q", title="dps"),
        color=alt.Color("player_class:N", title="class",
                        scale=alt.Scale(domain=list(CLASS_COLOURS.keys()),
                                        range=list(CLASS_COLOURS.values()))).legend(None),
        tooltip=[
            alt.Tooltip("player_name", title="player"),
            alt.Tooltip("player_class", title="class"),
            alt.Tooltip("damage_per_second", title="dps"),
            alt.Tooltip("damage_done", title="damage done"),
        ],
    )
    .properties(
        width=800,
        height=400,
        title=f"dps per player on first {boss} kill"
    )
)

with st_normal():
    st.altair_chart(bar_chart, use_container_width=True)

# --- Filter HPS for the kill --- #
player_hps_filtered = player_hps[
    (player_hps["report_id"] == record_kill_report) &
    (player_hps["pull_number"] == record_kill_pull_number)
]
player_hps_filtered = player_hps_filtered.sort_values("healing_per_second", ascending=False)

# --- HPS Bar Chart --- #
bar_chart = (
    alt.Chart(player_hps_filtered)
    .mark_bar()
    .encode(
        y=alt.Y("player_name:N",
                sort=player_hps_filtered["player_name"].tolist(),
                title="",
                axis=alt.Axis(labelOverlap=False)),
        x=alt.X("healing_per_second:Q", title="hps"),
        color=alt.Color("player_class:N", title="class",
                        scale=alt.Scale(domain=list(CLASS_COLOURS.keys()),
                                        range=list(CLASS_COLOURS.values()))).legend(None),
        tooltip=[
            alt.Tooltip("player_name", title="player"),
            alt.Tooltip("player_class", title="class"),
            alt.Tooltip("healing_per_second", title="hps"),
            alt.Tooltip("healing_done", title="healing done"),
        ],
    )
    .properties(
        width=800,
        height=400,
        title=f"hps per player on first {boss} kill"
    )
)

with st_normal():
    st.altair_chart(bar_chart, use_container_width=True)

# --- Show donut stats --- #

# --- Deaths --- #
# Filter deaths
player_deaths_filtered = player_deaths[
    (player_deaths["boss_name"] == boss) & 
    (player_deaths["raid_difficulty"] == "mythic")
]

# Count top 3 death abilities
top_3_deaths = (
    player_deaths_filtered["death_ability_name"]
    .value_counts()
    .nlargest(3)
    .reset_index()
)

top_3_deaths.columns = ["category", "value"]

# Define color palette
color_palette = ["#BB86FC", "#3700B3", "#03DAC6"]

# Sample chart data
charts_data = [
    {
        "data": top_3_deaths,
        "label": f"{player_deaths_filtered.shape[0]}<br>deaths"
    },
    {
        "data": pd.DataFrame({"category": ["X", "Y", "Z"], "value": [20, 50, 30]}),
        "label": "Chart 2"
    },
    {
        "data": pd.DataFrame({"category": ["Dog", "Cat", "Fish"], "value": [25, 25, 50]}),
        "label": "Chart 3"
    }
]

# Use columns for horizontal layout
cols = st.columns(3)

for i, col in enumerate(cols):
    with col:
        data = charts_data[i]["data"].copy()
        label = charts_data[i]["label"]

        # Replace underscores with spaces
        data["category"] = (
            data["category"]
            .str.replace("_", " ")
            .str.replace(r"\s+", "<br>", n=1, regex=True)
        )

        fig = go.Figure(
            data=[
                go.Pie(
                    labels=data["category"],
                    values=data["value"],
                    hole=0.5,
                    textinfo="label+percent",
                    textposition="outside",
                    marker=dict(colors=color_palette[:len(data)]),
                    showlegend=False,
                    sort=False,
                    direction="clockwise",
                    automargin=True,
                    rotation=0  # rotates slices to center labels vertically
                )
            ]
        )

        fig.update_traces(
            textfont=dict(size=13),
            insidetextorientation="radial"  # helps center any inner labels if shown
        )

        fig.update_layout(
            margin=dict(t=20, b=20, l=20, r=20),
            height=300,
            width=300,
            annotations=[
                dict(
                    text=label,
                    x=0.5,
                    y=0.5,
                    font_size=16,
                    showarrow=False,
                    font_color="white"
                )
            ]
        )

        st.plotly_chart(fig, use_container_width=False)