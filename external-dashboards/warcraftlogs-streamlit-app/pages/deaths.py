import streamlit as st
import pandas as pd
import json
import requests
from io import StringIO
import altair as alt

# Base URL for raw GitHub content
REPO_URL = "https://raw.githubusercontent.com/richmulvany/richmulvany-databricks-projects/main"

def load_csv(file_name: str) -> pd.DataFrame:
    """
    Helper to load a CSV file directly from the data‑exports folder of the repo.

    Parameters
    ----------
    file_name : str
        The file name within the ``data-exports`` directory.

    Returns
    -------
    pd.DataFrame
        Parsed CSV content as a DataFrame.
    """
    url = f"{REPO_URL}/data-exports/{file_name}"
    response = requests.get(url)
    response.raise_for_status()
    return pd.read_csv(StringIO(response.text))

def load_json(file_name: str) -> dict:
    """
    Helper to load a JSON file from the streamlit app directory of the repo.

    Parameters
    ----------
    file_name : str
        Name of the JSON file located alongside the Streamlit app.

    Returns
    -------
    dict
        Parsed JSON dictionary.
    """
    url = f"{REPO_URL}/external-dashboards/warcraftlogs-streamlit-app/{file_name}"
    response = requests.get(url)
    response.raise_for_status()
    return json.loads(response.text)

# -------------------------------------------------------------------
# Configure the Streamlit page
# -------------------------------------------------------------------
logo_path = "https://pbs.twimg.com/profile_images/1490380290962952192/qZk9xi5l_200x200.jpg"
st.set_page_config(page_title="deaths · sc-warcraftlogs", page_icon=logo_path)

# Helper column layout function used across pages
def st_normal():
    """Return a centred column occupying ~85 % of the available width."""
    _, col, _ = st.columns([1, 8.5, 1])
    return col

# Display the logo with a link back to the Warcraft Logs guild page
st.logo(logo_path, link="https://www.warcraftlogs.com/guild/id/586885")

# Title area
st.markdown(
    """




sc warcraftlogs 


""",
    unsafe_allow_html=True,
)
st.header("death statistics")

# -------------------------------------------------------------------
# Sidebar filters
# -------------------------------------------------------------------
difficulty = st.sidebar.radio(
    "raid difficulty:",
    ["all", "mythic", "heroic", "normal"],
    index=0,
)

include_wipes = st.sidebar.checkbox(
    "include wipes (all pulls)",
    value=False,
)

# The list of raids and bosses will be populated after loading the CSVs.
# To avoid expensive repeated downloads, wrap data loading in a spinner.
with st.spinner("Loading data…"):
    # Load core tables: death events, first deaths, pull counts and inting events
    deaths_df = load_csv("player_deaths.csv")
    first_df = load_csv("player_first_deaths.csv")
    pulls_df = load_csv("player_pull_counts.csv")
    inting_df = load_csv("player_inting.csv")
    details_df = load_csv("player_details.csv")
    # Load class colours
    CLASS_COLOURS = load_json("class_colours.json")

# Clean up pulls DataFrame: ensure consistent column names
if "total_pulls" in pulls_df.columns:
    pulls_df = pulls_df.rename(columns={"total_pulls": "boss_pulls"})

# Filter by difficulty on all datasets that have a difficulty column
if difficulty != "all":
    deaths_df = deaths_df[deaths_df["raid_difficulty"] == difficulty]
    first_df = first_df[first_df["raid_difficulty"] == difficulty]
    pulls_df = pulls_df[pulls_df["raid_difficulty"] == difficulty]
    # inting_df uses "boss_difficulty" to denote difficulty
    if "boss_difficulty" in inting_df.columns:
        inting_df = inting_df[inting_df["boss_difficulty"] == difficulty]

# -------------------------------------------------------------------
# Kill/wipe filtering
# -------------------------------------------------------------------
# When wipes are excluded, we assume the final pull (highest pull_number) for
# each boss and report corresponds to the kill.  We filter all datasets
# accordingly by joining on report_id, boss_name and pull_number.
if not include_wipes:
    kill_pulls = (
        deaths_df.groupby(["report_id", "boss_name"], as_index=False)["pull_number"]
        .max()
        .rename(columns={"pull_number": "kill_pull"})
    )
    deaths_df = deaths_df.merge(
        kill_pulls,
        left_on=["report_id", "boss_name", "pull_number"],
        right_on=["report_id", "boss_name", "kill_pull"],
        how="inner",
    )
    first_df = first_df.merge(
        kill_pulls,
        left_on=["report_id", "boss_name", "pull_number"],
        right_on=["report_id", "boss_name", "kill_pull"],
        how="inner",
    )
    inting_df = inting_df.merge(
        kill_pulls,
        left_on=["report_id", "boss_name", "pull_number"],
        right_on=["report_id", "boss_name", "kill_pull"],
        how="inner",
    )

# Determine available raids from the deaths data
available_raids = sorted(deaths_df["raid_name"].dropna().unique().tolist())
selected_raid = st.sidebar.selectbox(
    "raid:",
    options=["all raids"] + available_raids,
    index=0,
)

# Filter by raid if selected
if selected_raid != "all raids":
    deaths_df = deaths_df[deaths_df["raid_name"] == selected_raid]
    first_df = first_df[first_df["raid_name"] == selected_raid]
    # pulls_df does not contain raid_name, so cannot be filtered here

# Determine available bosses from the deaths data after raid/difficulty filtering
available_bosses = sorted(deaths_df["boss_name"].dropna().unique().tolist())
selected_boss = st.sidebar.selectbox(
    "boss:",
    options=["all bosses"] + available_bosses,
    index=0,
)

# Filter by boss if a specific boss is selected
if selected_boss != "all bosses":
    deaths_df = deaths_df[deaths_df["boss_name"] == selected_boss]
    first_df = first_df[first_df["boss_name"] == selected_boss]
    pulls_df = pulls_df[pulls_df["boss_name"] == selected_boss]
    inting_df = inting_df[inting_df["boss_name"] == selected_boss]

# Derive available player roles from the player details
roles = sorted(details_df["player_role"].dropna().unique().tolist())
selected_roles = st.sidebar.multiselect(
    "player roles:",
    options=roles,
    default=roles,
)

# Prepare mapping from player_name to role and spec
player_info = details_df.drop_duplicates(subset=["player_name"])[
    ["player_name", "player_class", "player_spec", "player_role"]
]

# -------------------------------------------------------------------
# Aggregations
# -------------------------------------------------------------------

# Compute total deaths per player
total_deaths = (
    deaths_df.groupby("player_name", as_index=False).size().rename(columns={"size": "death_count"})
)

# Compute first death counts per player
first_death_counts = (
    first_df.groupby("player_name", as_index=False).size().rename(columns={"size": "first_death_count"})
)

# Compute inting (disastrous death) counts per player
inting_counts = (
    inting_df.groupby("player_name", as_index=False).size().rename(columns={"size": "inting_count"})
)

# Merge counts into a single DataFrame
agg_df = total_deaths.merge(first_death_counts, on="player_name", how="outer")
agg_df = agg_df.merge(inting_counts, on="player_name", how="outer")

# Replace NaN with zeros for missing metrics
agg_df[["death_count", "first_death_count", "inting_count"]] = agg_df[["death_count", "first_death_count", "inting_count"]].fillna(0)

# Merge in pull counts to compute rates.  Use left join so players without pull counts are dropped.
agg_df = agg_df.merge(pulls_df.rename(columns={"boss_pulls": "total_pulls"}), on="player_name", how="left")

# Compute rates as percentages (deaths per pull * 100)
agg_df["death_perc"] = (agg_df["death_count"] / agg_df["total_pulls"]) * 100
agg_df["first_death_perc"] = (agg_df["first_death_count"] / agg_df["total_pulls"]) * 100
agg_df["inting_perc"] = (agg_df["inting_count"] / agg_df["total_pulls"]) * 100

# Drop players with no pulls after filtering (avoid divide by zero or NaN)
agg_df = agg_df.dropna(subset=["total_pulls"])

# Merge player class, spec and role information
agg_df = agg_df.merge(player_info, on="player_name", how="left")

# Filter by selected roles
if selected_roles:
    agg_df = agg_df[agg_df["player_role"].isin(selected_roles)]

# Sort by death percentage descending for presentation
chart_df = agg_df.sort_values("death_perc", ascending=False)

# If no data after filtering, inform the user gracefully
if chart_df.empty:
    st.info("No data available for the selected filters.")
    st.stop()

# Lowercase class names for colour mapping
chart_df["player_class_lower"] = chart_df["player_class"].str.lower()

# Compose a subtitle describing the selected raid, boss and whether wipes are included
pull_label = "all pulls" if include_wipes else "kill pulls only"
subtitle = (
    f"{'all bosses' if selected_boss == 'all bosses' else selected_boss} "
    f"in {'all raids' if selected_raid == 'all raids' else selected_raid}"
    f" ({pull_label})"
)

# -------------------------------------------------------------------
# Visualisations
# -------------------------------------------------------------------

st.subheader(f"death rate per player on {subtitle}")

# Bar chart for death percentage per player
death_chart = (
    alt.Chart(chart_df)
    .mark_bar()
    .encode(
        x=alt.X(
            "player_name:N",
            sort=chart_df["player_name"].tolist(),
            title="",
        ),
        y=alt.Y(
            "death_perc:Q",
            title="death rate (% of pulls)",
        ),
        color=alt.Color(
            "player_class_lower:N",
            title="class",
            scale=alt.Scale(
                domain=list(CLASS_COLOURS.keys()),
                range=list(CLASS_COLOURS.values())
            ),
        ).legend(None),
        tooltip=[
            alt.Tooltip("player_name:N", title="player"),
            alt.Tooltip("player_class:N", title="class"),
            alt.Tooltip("player_spec:N", title="spec"),
            alt.Tooltip("death_count:Q", title="deaths"),
            alt.Tooltip("total_pulls:Q", title="pulls"),
            alt.Tooltip("death_perc:Q", title="death %"),
        ],
    )
    # .properties(
    #     width="container",
    #     height=400,
    #     title=None,
    # )
)
st.altair_chart(death_chart, use_container_width=True)

# First death chart
st.subheader("first death rate per player")
first_chart = (
    alt.Chart(chart_df)
    .mark_bar()
    .encode(
        x=alt.X("player_name:N", sort=chart_df["player_name"].tolist(), title=""),
        y=alt.Y("first_death_perc:Q", title="first death rate (% of pulls)"),
        color=alt.Color(
            "player_class_lower:N",
            scale=alt.Scale(domain=list(CLASS_COLOURS.keys()), range=list(CLASS_COLOURS.values())),
            legend=None,
        ),
        tooltip=[
            alt.Tooltip("player_name:N", title="player"),
            alt.Tooltip("player_class:N", title="class"),
            alt.Tooltip("player_spec:N", title="spec"),
            alt.Tooltip("first_death_count:Q", title="first deaths"),
            alt.Tooltip("total_pulls:Q", title="pulls"),
            alt.Tooltip("first_death_perc:Q", title="first death %"),
        ],
    )
    # .properties(width="container", height=400, title=None)
)
st.altair_chart(first_chart, use_container_width=True)

# Inting chart
st.subheader("disastrous death rate per player")
inting_chart = (
    alt.Chart(chart_df)
    .mark_bar()
    .encode(
        x=alt.X("player_name:N", sort=chart_df["player_name"].tolist(), title=""),
        y=alt.Y("inting_perc:Q", title="disastrous death rate (% of pulls)"),
        color=alt.Color(
            "player_class_lower:N",
            scale=alt.Scale(domain=list(CLASS_COLOURS.keys()), range=list(CLASS_COLOURS.values())),
            legend=None,
        ),
        tooltip=[
            alt.Tooltip("player_name:N", title="player"),
            alt.Tooltip("player_class:N", title="class"),
            alt.Tooltip("player_spec:N", title="spec"),
            alt.Tooltip("inting_count:Q", title="disastrous deaths"),
            alt.Tooltip("total_pulls:Q", title="pulls"),
            alt.Tooltip("inting_perc:Q", title="disastrous death %"),
        ],
    )
    # .properties(width="container", height=400, title=None)
)
st.altair_chart(inting_chart, use_container_width=True)

# -------------------------------------------------------------------
# Tabular data display
# -------------------------------------------------------------------

st.subheader("player death summary")
display_df = chart_df[
    [
        "player_name",
        "player_class",
        "player_spec",
        "player_role",
        "death_count",
        "total_pulls",
        "death_perc",
        "first_death_count",
        "first_death_perc",
        "inting_count",
        "inting_perc",
    ]
].rename(
    columns={
        "player_name": "Player",
        "player_class": "Class",
        "player_spec": "Spec",
        "player_role": "Role",
        "death_count": "Deaths",
        "total_pulls": "Pulls",
        "death_perc": "Death %",
        "first_death_count": "First Deaths",
        "first_death_perc": "First Death %",
        "inting_count": "Disastrous Deaths",
        "inting_perc": "Disastrous Death %",
    }
)

st.dataframe(display_df, hide_index=True)

st.caption(
    "Deaths, first deaths and disastrous deaths are aggregated from the selected pulls. "
    "Rates are calculated against the number of pulls for each player. "
    "Players without any pull data in the selected filters are omitted."
)
