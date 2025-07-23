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

st.altair_chart(combined_chart, use_container_width=True)

# --- Class colours for DPS bar chart --- #
CLASS_COLOURS = {
    "deathknight":  "#C41F3B",
    "demonhunter":  "#A330C9",
    "druid":        "#FF7D0A",
    "evoker":       "#33937F",
    "hunter":       "#ABD473",
    "mage":         "#69CCF0",
    "monk":         "#00FF96",
    "paladin":      "#F58CBA",
    "priest":       "#FFFFFF",
    "rogue":        "#FFF569",
    "shaman":       "#0070DE",
    "warlock":      "#9482C9",
    "warrior":      "#C79C6E"
}

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
                axis=alt.Axis(labelOverlap="parity")),
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
        width="container",
        height=400,
        title=f"dps per player on first {boss} kill"
    )
)

st.altair_chart(bar_chart, use_container_width=True)

# --- Filter HPS for the kill --- #
player_hps_filtered = player_hps[
    (player_hps["report_id"] == record_kill_report) &
    (player_hps["pull_number"] == record_kill_pull_number)
]
player_hps_filtered = player_hps_filtered.sort_values("damage_per_second", ascending=False)

# --- HPS Bar Chart --- #
bar_chart = (
    alt.Chart(player_hps_filtered)
    .mark_bar()
    .encode(
        y=alt.Y("player_name:N",
                sort=player_hps_filtered["player_name"].tolist(),
                title="",
                axis=alt.Axis(labelOverlap="parity")),
        x=alt.X("damage_per_second:Q", title="hps"),
        color=alt.Color("player_class:N", title="class",
                        scale=alt.Scale(domain=list(CLASS_COLOURS.keys()),
                                        range=list(CLASS_COLOURS.values()))).legend(None),
        tooltip=[
            alt.Tooltip("player_name", title="player"),
            alt.Tooltip("player_class", title="class"),
            alt.Tooltip("damage_per_second", title="hps"),
            alt.Tooltip("damage_done", title="damage done"),
        ],
    )
    .properties(
        width="container",
        height=400,
        title=f"hps per player on first {boss} kill"
    )
)

st.altair_chart(bar_chart, use_container_width=True)
