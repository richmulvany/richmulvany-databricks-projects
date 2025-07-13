# warcraftlogs_streamlit_app.py
import streamlit as st
import pandas as pd
import requests
import altair as alt
from io import StringIO

# --- GitHub raw base URL ---
REPO_URL = "https://raw.githubusercontent.com/richmulvany/richmulvany-databricks-projects/feat/warcraftlogs-create-gold-products/data-exports"

# --- Helper to load CSVs directly from GitHub ---
def load_csv(file_name: str) -> pd.DataFrame:
    url = f"{REPO_URL}/{file_name}"
    response = requests.get(url)
    response.raise_for_status()
    return pd.read_csv(StringIO(response.text))

# --- Streamlit UI ---
st.title("⚔️ SC WarcraftLogs: First Death % per Boss")

with st.spinner("Loading data..."):
    # First-death records (one row per pull)
    first_deaths = load_csv("player_first_deaths.csv")

    # Pull counts
    pulls = load_csv("player_pull_counts.csv")
    if "total_pulls" in pulls.columns:
        pulls = pulls.rename(columns={"total_pulls": "boss_pulls"})

    # Player class map
    class_map = first_deaths[["player_name", "player_class"]].drop_duplicates()

# --- Aggregate first-death counts ---
first_death_counts = (
    first_deaths.groupby(["player_name", "boss_name"])
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
df["first_death_perc"] = round(100 * df["first_death_count"] / df["boss_pulls"], 2)
df = df.dropna(subset=["first_death_perc"])  # Remove missing pull data

# --- Boss filter ---
bosses = sorted(df["boss_name"].unique())
selected_boss = st.selectbox("Select a boss:", options=bosses)

chart_data = df[df["boss_name"] == selected_boss].sort_values("first_death_perc", ascending=False)

st.subheader(f"📊 First Deaths on {selected_boss}")
st.dataframe(
    chart_data[["player_name", "player_class", "first_death_count", "boss_pulls", "first_death_perc"]],
    hide_index=True,
)

# Sort by descending first_death_perc
chart_data["player_sort_order"] = chart_data["first_death_perc"].rank(method="first", ascending=False)

# Map class colours
CLASS_COLOURS = {
    "death knight": "#C41F3B",
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

# Build chart
bar_chart = (
    alt.Chart(chart_data)
    .mark_bar()
    .encode(
        x=alt.X("player_name:N",
                sort=chart_data["player_name"].tolist(),
                title="Player"),
        y=alt.Y("first_death_perc:Q", title="% of Pulls as First Death"),
        color=alt.Color("player_class:N", title="Class",
                        scale=alt.Scale(domain=list(CLASS_COLOURS.keys()),
                                        range=list(CLASS_COLOURS.values()))).legend(None),
        tooltip=[
            alt.Tooltip("player_name", title="Player"),
            alt.Tooltip("player_class", title="Class"),
            alt.Tooltip("first_death_count", title="First Deaths"),
            alt.Tooltip("boss_pulls", title="Pulls"),
            alt.Tooltip("first_death_perc", title="Death %"),
        ],
    )
    .properties(
        width="container",
        height=400,
        title=f"First Death % per Player on {selected_boss}"
    )
)

st.altair_chart(bar_chart, use_container_width=True)
