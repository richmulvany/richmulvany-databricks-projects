# deaths.py
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

# --- Streamlit UI ---
logo_path = "https://pbs.twimg.com/profile_images/1490380290962952192/qZk9xi5l_200x200.jpg"

# Set tab config
st.set_page_config(page_title="players · sc-warcraftlogs", page_icon=logo_path)

# Function to workaround container size
def st_normal():
    _, col, _ = st.columns([1, 8.5, 1])
    return col

st.logo(
    logo_path,
    link="https://www.warcraftlogs.com/guild/id/586885"
)

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

st.header("death statistics")

difficulty = st.sidebar.radio("raid difficulty:",["all", "mythic", "heroic", "normal"])

with st.spinner("Loading data..."):
    # First-death records (one row per pull)
    first_deaths = load_csv("player_first_deaths.csv")

    # Guild roster
    roster = load_csv("guild_roster.csv")

    # Pull counts
    pulls = load_csv("player_pull_counts.csv")
    if "total_pulls" in pulls.columns:
        pulls = pulls.rename(columns={"total_pulls": "boss_pulls"})

    # Player class map
    class_map = first_deaths[["player_name", "player_class"]].drop_duplicates()

    # Player disastrous deaths
    disastrous_deaths = load_csv("player_inting.csv")

# Remove non-guildies
valid_players = roster["player_name"].unique()
pulls = pulls[pulls["player_name"].isin(valid_players)]
first_deaths = first_deaths[first_deaths["player_name"].isin(valid_players)]

# Filter difficulty
if difficulty != "all":
    pulls = pulls[pulls["raid_difficulty"] == difficulty]
    first_deaths = first_deaths[first_deaths["raid_difficulty"] == difficulty]

# --- Aggregate first-death counts ---
first_death_counts = (
    first_deaths.groupby(["player_name", "boss_name", "raid_difficulty"])
    .size()
    .reset_index(name="first_death_count")
)

# --- Merge everything ---
df = (
    first_death_counts
    .merge(pulls, on=["player_name", "boss_name"], how="left")
    .merge(class_map, on="player_name", how="left")
)

# --- Compute percentage ---

# --- Boss filter ---
bosses = [
    "all bosses", 
    "vexie and the geargrinders",
    "cauldron of carnage",
    "rik reverb",
    "stix bunkjunker",
    "sprocketmonger lockenstock",
    "one-armed bandit",
    "mug'zee, heads of security",
    "chrome king gallywix"
]
selected_boss = st.selectbox("select a boss:", options=bosses)

if selected_boss != "all bosses":
    df["first_death_perc"] = round(100 * df["first_death_count"] / df["boss_pulls"], 2)
    df = df.dropna(subset=["first_death_perc"])  # Remove missing pull data
    chart_data = df[df["boss_name"] == selected_boss].sort_values("first_death_perc", ascending=False)
else:
    chart_data = df.groupby("player_name", as_index=False).agg({
        "first_death_count": "sum",
        "boss_pulls": "sum"
    })
    player_meta = df.drop_duplicates(subset="player_name")[["player_name", "player_class"]]
    chart_data = chart_data.merge(player_meta, on="player_name", how="left")
    chart_data["first_death_perc"] = round(100 * chart_data["first_death_count"] / chart_data["boss_pulls"], 2)
    chart_data = chart_data.dropna(subset=["first_death_perc"])  # Remove missing pull data
    chart_data = chart_data.sort_values("first_death_perc", ascending=False)
    chart_data = chart_data[chart_data["boss_pulls"] > 99]
# Remove holy priest
chart_data = chart_data[chart_data["player_name"] != "evereld"]

# Map class colours
CLASS_COLOURS = load_json("class_colours.json")

# Build chart
bar_chart = (
    alt.Chart(chart_data)
    .mark_bar()
    .encode(
        x=alt.X("player_name:N",
                sort=chart_data["player_name"].tolist(),
                title=""),
        y=alt.Y("first_death_perc:Q", title="% of pulls as first death"),
        color=alt.Color("player_class:N", title="class",
                        scale=alt.Scale(domain=list(CLASS_COLOURS.keys()),
                                        range=list(CLASS_COLOURS.values()))).legend(None),
        tooltip=[
            alt.Tooltip("player_name", title="player"),
            alt.Tooltip("player_class", title="class"),
            alt.Tooltip("first_death_count", title="first deaths"),
            alt.Tooltip("boss_pulls", title="pulls"),
            alt.Tooltip("first_death_perc", title="death %"),
        ],
    )
    .properties(
        width="container",
        height=400,
        title=f"first death % per player on {selected_boss}"
    )
)

# Present data
st.subheader(f"first deaths on {selected_boss}")

st.altair_chart(bar_chart, use_container_width=True)

st.caption("holy priests removed from deaths due to misrepresentative data")

st.dataframe(
    chart_data[["player_name", "player_class", "first_death_count", "boss_pulls", "first_death_perc"]],
    hide_index=True,
)

# --- Aggregate disastrous-death counts ---
disastrous_death_counts = (
    disastrous_deaths.groupby(["player_name", "boss_name"])
    .size()
    .reset_index(name="disastrous_death_count")
)

# --- Merge everything ---
df = (
    disastrous_death_counts
    .merge(pulls, on=["player_name", "boss_name"], how="left")
    .merge(class_map, on="player_name", how="left")
)

# --- Compute percentage ---
df["disastrous_death_perc"] = round(100 * df["disastrous_death_count"] / df["boss_pulls"], 2)
df = df.dropna(subset=["disastrous_death_perc"])  # Remove missing pull data

# --- Filter boss ---
chart_data = df[df["boss_name"] == selected_boss].sort_values("disastrous_death_perc", ascending=False)

st.subheader(f"disastrous deaths on {selected_boss}")
st.dataframe(
    chart_data[["player_name", "player_class", "disastrous_death_count", "boss_pulls", "disastrous_death_perc"]],
    hide_index=True,
)

# Build chart
bar_chart = (
    alt.Chart(chart_data)
    .mark_bar()
    .encode(
        x=alt.X("player_name:N",
                sort=chart_data["player_name"].tolist(),
                title=""),
        y=alt.Y("disastrous_death_perc:Q", title="% of pulls with disastrous death"),
        color=alt.Color("player_class:N", title="class",
                        scale=alt.Scale(domain=list(CLASS_COLOURS.keys()),
                                        range=list(CLASS_COLOURS.values()))).legend(None),
        tooltip=[
            alt.Tooltip("player_name", title="player"),
            alt.Tooltip("player_class", title="class"),
            alt.Tooltip("disastrous_death_count", title="disastrous deaths"),
            alt.Tooltip("boss_pulls", title="pulls"),
            alt.Tooltip("disastrous_death_perc", title="death %"),
        ],
    )
    .properties(
        width="container",
        height=400,
        title=f"disastrous death % per player on {selected_boss}"
    )
)

st.altair_chart(bar_chart, use_container_width=True)
