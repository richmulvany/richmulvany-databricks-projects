import streamlit as st
import pandas as pd
import json
import requests
from io import StringIO
import altair as alt

# -------------------------------------------------------------------
# GitHub data access helpers
# -------------------------------------------------------------------
REPO_URL = "https://raw.githubusercontent.com/richmulvany/richmulvany-databricks-projects/main"


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

# -------------------------------------------------------------------
# Configure the Streamlit page
# -------------------------------------------------------------------
logo_path = "https://pbs.twimg.com/profile_images/1490380290962952192/qZk9xi5l_200x200.jpg"
st.set_page_config(page_title="deaths · sc-warcraftlogs", page_icon=logo_path)
st.logo(logo_path, link="https://www.warcraftlogs.com/guild/id/586885")

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

# -------------------------------------------------------------------
# Sidebar Filters
# -------------------------------------------------------------------
difficulty = st.sidebar.radio("raid difficulty:", ["all", "mythic", "heroic", "normal"], index=1)

# -------------------------------------------------------------------
# Load Data
# -------------------------------------------------------------------
with st.spinner("Loading data…"):
    player_deaths = load_csv("player_deaths.csv")
    first_deaths = load_csv("player_first_deaths.csv")
    disastrous_deaths = load_csv("player_inting.csv")
    pulls = load_csv("player_pull_counts.csv")
    roster = load_csv("guild_roster.csv")
    class_map = first_deaths[["player_name", "player_class"]].drop_duplicates()
    CLASS_COLOURS = load_json("class_colours.json")

# Filter to valid players
valid_players = roster["player_name"].unique()
first_deaths = first_deaths[first_deaths["player_name"].isin(valid_players)]
player_deaths = player_deaths[player_deaths["player_name"].isin(valid_players)]
disastrous_deaths = disastrous_deaths[disastrous_deaths["player_name"].isin(valid_players)]
pulls = pulls[pulls["player_name"].isin(valid_players)]

# Rename pulls column
if "total_pulls" in pulls.columns:
    pulls = pulls.rename(columns={"total_pulls": "boss_pulls"})

if difficulty != "all":
    first_deaths = first_deaths[first_deaths["raid_difficulty"] == difficulty]
    pulls = pulls[pulls["raid_difficulty"] == difficulty]
    player_deaths = player_deaths[player_deaths["raid_difficulty"] == difficulty]

# -------------------------------------------------------------------
# First Deaths Chart
# -------------------------------------------------------------------
first_death_counts = (
    first_deaths.groupby(["player_name", "boss_name", "raid_difficulty"], observed=True, as_index=False).size()
    .rename(columns={"size": "first_death_count"})
)

df = (
    first_death_counts.merge(pulls, on=["player_name", "boss_name", "raid_difficulty"], how="left")
    .merge(class_map, on="player_name", how="left")
)

available_bosses = sorted(df["boss_name"].dropna().unique())
selected_boss = st.selectbox("boss:", options=["all bosses"] + available_bosses, index=0)

if selected_boss != "all bosses":
    df = df[df["boss_name"] == selected_boss]

chart_data = (
    df.groupby("player_name", observed=True, as_index=False)
    .agg({"first_death_count": "sum", "boss_pulls": "sum"})
    .merge(class_map, on="player_name", how="left")
)

chart_data["first_death_perc"] = round(100 * chart_data["first_death_count"] / chart_data["boss_pulls"], 2)
chart_data = chart_data[chart_data["boss_pulls"] > 0]
chart_data = chart_data[chart_data["player_name"] != "evereld"]
chart_data = chart_data.sort_values("first_death_perc", ascending=False)

st.subheader(f"first deaths on {selected_boss}")

bar_chart = (
    alt.Chart(chart_data)
    .mark_bar()
    .encode(
        x=alt.X("player_name:N", sort=chart_data["player_name"].tolist(), title=""),
        y=alt.Y("first_death_perc:Q", title="% of pulls as first death"),
        color=alt.Color("player_class:N", scale=alt.Scale(domain=list(CLASS_COLOURS.keys()), range=list(CLASS_COLOURS.values()))).legend(None),
        tooltip=[
            alt.Tooltip("player_name", title="player"),
            alt.Tooltip("player_class", title="class"),
            alt.Tooltip("first_death_count", title="first deaths"),
            alt.Tooltip("boss_pulls", title="pulls"),
            alt.Tooltip("first_death_perc", title="death %"),
        ],
    )
    .properties(width="container", height=400, title=f"first death % per player on {selected_boss}")
)

st.altair_chart(bar_chart, use_container_width=True)

st.dataframe(
    chart_data[["player_name", "player_class", "first_death_count", "boss_pulls", "first_death_perc"]],
    hide_index=True,
)

st.caption("holy priests are removed from deaths due to misrepresentative data")

# -------------------------------------------------------------------
# Disastrous Deaths Chart
# -------------------------------------------------------------------
disastrous_counts = (
    disastrous_deaths.groupby(["player_name", "boss_name"], observed=True, as_index=False).size()
    .rename(columns={"size": "disastrous_death_count"})
)

df = (
    disastrous_counts.merge(pulls, on=["player_name", "boss_name"], how="left")
    .merge(class_map, on="player_name", how="left")
)

if selected_boss != "all bosses":
    df = df[df["boss_name"] == selected_boss]

chart_data = (
    df.groupby("player_name", observed=True, as_index=False)
    .agg({"disastrous_death_count": "sum", "boss_pulls": "sum"})
    .merge(class_map, on="player_name", how="left")
)

chart_data["disastrous_death_perc"] = round(100 * chart_data["disastrous_death_count"] / chart_data["boss_pulls"], 2)
chart_data = chart_data[chart_data["boss_pulls"] > 0]
chart_data = chart_data.sort_values("disastrous_death_perc", ascending=False)

st.subheader(f"disastrous deaths on {selected_boss}")

bar_chart = (
    alt.Chart(chart_data)
    .mark_bar()
    .encode(
        x=alt.X("player_name:N", sort=chart_data["player_name"].tolist(), title=""),
        y=alt.Y("disastrous_death_perc:Q", title="% of pulls with disastrous death"),
        color=alt.Color("player_class:N", scale=alt.Scale(domain=list(CLASS_COLOURS.keys()), range=list(CLASS_COLOURS.values()))).legend(None),
        tooltip=[
            alt.Tooltip("player_name", title="player"),
            alt.Tooltip("player_class", title="class"),
            alt.Tooltip("disastrous_death_count", title="disastrous deaths"),
            alt.Tooltip("boss_pulls", title="pulls"),
            alt.Tooltip("disastrous_death_perc", title="death %"),
        ],
    )
    .properties(width="container", height=400, title=f"disastrous death % per player on {selected_boss}")
)

st.altair_chart(bar_chart, use_container_width=True)

st.dataframe(
    chart_data[["player_name", "player_class", "disastrous_death_count", "boss_pulls", "disastrous_death_perc"]],
    hide_index=True,
)

st.caption("disastrous deaths are defined by deaths which are the first to occur in a chain reaction of more than 3 deaths")

# -------------------------------------------------------------------
# Death Cause Chart
# -------------------------------------------------------------------
st.subheader(f"cause of deaths on {selected_boss}")

death_causes = player_deaths.copy()
if selected_boss != "all bosses":
    death_causes = death_causes[death_causes["boss_name"] == selected_boss]

top_abilities = (
    death_causes.groupby("death_ability_name", observed=True, as_index=False).size()
    .rename(columns={"size": "death_count"})
    .sort_values("death_count", ascending=False)
    .head(15)
)

top_abilities["death_ability_name"] = top_abilities["death_ability_name"].astype(str)

if top_abilities.empty:
    st.info("No death cause data available for this selection.")
else:
    chart = (
        alt.Chart(top_abilities)
        .mark_bar(color="#BB86FC")
        .encode(
            x=alt.X("death_count:Q", title="death count"),
            y=alt.Y("death_ability_name:N", sort="-x", title="ability", axis=alt.Axis(labelOverlap=False)),
            tooltip=[
                alt.Tooltip("death_ability_name", title="ability"),
                alt.Tooltip("death_count", title="death count"),
            ],
        )
        .properties(width="container", height=400, title=f"top death-causing abilities on {selected_boss}")
    )
    st.altair_chart(chart, use_container_width=True)
