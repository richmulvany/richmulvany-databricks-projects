# home.py
import streamlit as st
import pandas as pd
import json
import requests
import altair as alt
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

# --- Streamlit UI --- #
logo_path = "https://pbs.twimg.com/profile_images/1490380290962952192/qZk9xi5l_200x200.jpg"

# Set tab config
st.set_page_config(page_title="players · sc-warcraftlogs", page_icon=logo_path, layout="wide")

# Function to workaround container size
def st_normal():
    _, col, _ = st.columns([1, 8.5, 1])
    return col

# Import SC logo
st.logo(
    logo_path,
    link="https://www.warcraftlogs.com/guild/id/586885"
)

# --- Main Title & Logo --- #
st.markdown(f"""
<div style="display: flex; align-items: center;">
    <a href="/" style="text-decoration: none;">
        <img src="{logo_path}" width="64" style="border-radius: 100%; border: 2px solid #FFFFFF; margin-right: 12px;">
    </a>
    <a href="/" style="text-decoration: none; color: inherit;">
        <h1 style="margin: 0;">sc warcraftlogs</h1>
    </a>
</div>
""", unsafe_allow_html=True)

st.header("player statistics")

with st.spinner("Loading data..."):
    # First-death records (one row per pull)
    pull_counts = load_csv("player_pull_counts.csv")
    guild_roster = load_csv("guild_roster.csv")
    class_data = load_csv("game_data_classes.csv")

    # Ensure numeric total_pulls
    pull_counts["total_pulls"] = pd.to_numeric(pull_counts["total_pulls"], errors="coerce")

    # Filter to known players
    pull_counts = pull_counts[pull_counts["player_name"].isin(guild_roster["player_name"])]

    # Player class map
    class_map = (
        guild_roster[["player_name", "player_class_id"]]
        .drop_duplicates()
        .merge(class_data, left_on="player_class_id", right_on="class_id", how="left")
        .assign(player_class=lambda df: df["class_name"].str.lower().str.strip())
        [["player_name", "player_class"]]
    )

    # Map class colours
    CLASS_COLOURS = load_json("class_colours.json")

# --- 1. Per-Boss Attendance Rate ---
pull_counts = pull_counts[pull_counts["raid_difficulty"] == "mythic"]
boss_totals = (
    pull_counts.groupby(['boss_name'])['total_pulls']
    .max()
    .reset_index()
    .rename(columns={'total_pulls': 'boss_max_pulls'})
)

df_with_boss_max = pull_counts.merge(boss_totals, on=['boss_name'], how='left')

df_with_boss_max['attendance_ratio_per_boss'] = (
    df_with_boss_max['total_pulls'] / df_with_boss_max['boss_max_pulls']
)

player_attendance_per_boss = (
    df_with_boss_max.groupby('player_name')['attendance_ratio_per_boss']
    .mean()
    .reset_index()
    .rename(columns={'attendance_ratio_per_boss': 'mean_boss_attendance'})
)

# --- 2. Overall Attendance Rate ---
player_totals = (
    pull_counts.groupby('player_name')['total_pulls']
    .sum()
    .reset_index()
)
max_total = player_totals['total_pulls'].max()
player_totals['overall_attendance'] = player_totals['total_pulls'] / max_total*100

# --- Merge & deduplicate ---
attendance_summary = (
    player_attendance_per_boss
    .merge(player_totals, on='player_name', how='left')
    .merge(class_map, on='player_name', how='left')
)

# Drop rows with missing attendance (just in case)
attendance_summary = attendance_summary.dropna(subset=["overall_attendance"])

# Collapse any accidental duplicates
attendance_summary = (
    attendance_summary
    .groupby("player_name", as_index=False)
    .agg({
        "overall_attendance": "max",
        "mean_boss_attendance": "mean",
        "player_class": "first"
    })
)

# Filter to valid classes
attendance_summary = attendance_summary[
    attendance_summary["player_class"].isin(CLASS_COLOURS.keys())
]

# --- Display in Streamlit ---

# Build chart
bar_chart = (
    alt.Chart(attendance_summary.sort_values("overall_attendance", ascending=False))
    .mark_bar()
    .encode(
        x=alt.X("player_name:N", sort=None, title="",
                axis=alt.Axis(labelOverlap=False)),
        y=alt.Y("overall_attendance:Q",
                title="player attendance (%)",
                scale=alt.Scale(domain=[0, 100])),
        color=alt.Color("player_class:N", title="class",
                        scale=alt.Scale(domain=list(CLASS_COLOURS.keys()),
                                        range=list(CLASS_COLOURS.values())),
                        legend=None),
        tooltip=[
            alt.Tooltip("player_name", title="player"),
            alt.Tooltip("player_class", title="class"),
            alt.Tooltip("overall_attendance", title="overall_attendance", format=".2f")
        ],
    )
    .properties(
        width="container",
        height=400,
        title="player attendance summary"
    )
)

# Present data
st.subheader("player attendance")
st.altair_chart(bar_chart, use_container_width=True)
