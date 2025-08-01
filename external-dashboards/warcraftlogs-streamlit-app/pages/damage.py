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
    """
    url = f"{REPO_URL}/data-exports/{file_name}"
    response = requests.get(url)
    response.raise_for_status()
    return pd.read_csv(StringIO(response.text))

def load_json(file_name: str) -> dict:
    """
    Helper to load a JSON file from the streamlit app directory of the repo.
    """
    url = f"{REPO_URL}/external-dashboards/warcraftlogs-streamlit-app/{file_name}"
    response = requests.get(url)
    response.raise_for_status()
    return json.loads(response.text)

# -------------------------------------------------------------------
# Configure the Streamlit page
# -------------------------------------------------------------------
logo_path = "https://pbs.twimg.com/profile_images/1490380290962952192/qZk9xi5l_200x200.jpg"
st.set_page_config(page_title="damage · sc-warcraftlogs", page_icon=logo_path)

# Display the logo with a link back to the Warcraft Logs guild page
st.logo(logo_path, link="https://www.warcraftlogs.com/guild/id/586885")

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

st.header("damage statistics")

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

# choose parse metric view
parse_choice = st.sidebar.radio(
    "parse metric to show:",
    ["average parse", "highest parse"],
    index=0,
    horizontal=True,
)
parse_field = "avg_parse" if parse_choice == "average parse" else "best_parse"
parse_label = "average parse (%)" if parse_field == "avg_parse" else "best parse (%)"

# The list of raids and bosses will be populated after loading the CSVs.
with st.spinner("Loading data…"):
    dps_df = load_csv("player_dps.csv")
    ranks_df = load_csv("ranks_dps.csv")
    details_df = load_csv("player_details.csv")
    CLASS_COLOURS = load_json("class_colours.json")

if difficulty != "all":
    dps_df = dps_df[dps_df["raid_difficulty"] == difficulty]
    valid_reports = dps_df["report_id"].unique()
    ranks_df = ranks_df[ranks_df["report_id"].isin(valid_reports)]

if not include_wipes:
    kill_pulls = (
        dps_df.groupby(["report_id", "boss_name"], as_index=False)["pull_number"]
        .max()
        .rename(columns={"pull_number": "kill_pull"})
    )
    dps_df = dps_df.merge(
        kill_pulls,
        left_on=["report_id", "boss_name", "pull_number"],
        right_on=["report_id", "boss_name", "kill_pull"],
        how="inner",
    )
    ranks_df = ranks_df.merge(
        kill_pulls,
        left_on=["report_id", "boss_name", "pull_number"],
        right_on=["report_id", "boss_name", "kill_pull"],
        how="inner",
    )

available_raids = sorted(dps_df["raid_name"].dropna().unique().tolist())
selected_raid = st.sidebar.selectbox(
    "raid:",
    options=["all raids"] + available_raids,
    index=0,
)

if selected_raid != "all raids":
    dps_df = dps_df[dps_df["raid_name"] == selected_raid]
    valid_reports = dps_df["report_id"].unique()
    ranks_df = ranks_df[ranks_df["report_id"].isin(valid_reports)]

available_bosses = sorted(dps_df["boss_name"].dropna().unique().tolist())
selected_boss = st.sidebar.selectbox(
    "boss:",
    options=["all bosses"] + available_bosses,
    index=0,
)

if selected_boss != "all bosses":
    dps_df = dps_df[dps_df["boss_name"] == selected_boss]
    ranks_df = ranks_df[ranks_df["boss_name"] == selected_boss]

roles = sorted(ranks_df["player_role"].dropna().unique().tolist())
selected_roles = st.sidebar.multiselect(
    "player roles:",
    options=roles,
    default=roles,
)

if selected_roles:
    ranks_df = ranks_df[ranks_df["player_role"].isin(selected_roles)]

# -------------------------------------------------------------------
# Aggregation for parse percent and DPS by player
# -------------------------------------------------------------------

ranks_df["parse_percent"] = pd.to_numeric(ranks_df["parse_percent"], errors="coerce")
ranks_df["parse_percent"] = ranks_df["parse_percent"].clip(upper=100)

agg_parse = (
    ranks_df.groupby(["player_name", "player_class", "player_spec"], as_index=False)
    .agg(
        avg_parse=("parse_percent", "mean"),
        best_parse=("parse_percent", "max"),
        parses=("parse_percent", "count"),
    )
)

dps_df["damage_per_second"] = pd.to_numeric(dps_df["damage_per_second"], errors="coerce")
avg_dps = (
    dps_df.groupby("player_name", as_index=False)["damage_per_second"].mean()
    .rename(columns={"damage_per_second": "avg_dps"})
)

# pick primary spec per player based on selected parse metric
primary_spec = (
    agg_parse
    .sort_values(["player_name", parse_field], ascending=[True, False])
    .groupby("player_name", as_index=False)
    .first()
)
agg_df = primary_spec.merge(avg_dps, on="player_name", how="left")

agg_df["player_class_lower"] = agg_df["player_class"].str.lower()
chart_df = agg_df.sort_values(parse_field, ascending=False)

if chart_df.empty:
    st.info("No data available for the selected filters.")
    st.stop()

# -------------------------------------------------------------------
# Visualisations
# -------------------------------------------------------------------

subtitle = f"{'all bosses' if selected_boss == 'all bosses' else selected_boss} in {'all raids' if selected_raid == 'all raids' else selected_raid}"

st.subheader(f"{parse_choice.capitalize()} per player on {subtitle}")

bar_chart = (
    alt.Chart(chart_df)
    .mark_bar()
    .encode(
        x=alt.X(
            "player_name:N",
            sort=chart_df["player_name"].tolist(),
            title="",
        ),
        y=alt.Y(f"{parse_field}:Q", title=parse_label),
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
            alt.Tooltip(f"{parse_field}:Q", title=parse_choice),
            alt.Tooltip("avg_parse:Q", title="average parse"),
            alt.Tooltip("best_parse:Q", title="best parse"),
            alt.Tooltip("avg_dps:Q", title="avg DPS"),
            alt.Tooltip("parses:Q", title="logs"),
        ],
    )
)
st.altair_chart(bar_chart, use_container_width=True)

st.subheader("damage vs parse overview")
scatter = (
    alt.Chart(chart_df)
    .mark_circle(size=60, opacity=1.0)
    .encode(
        x=alt.X("avg_dps:Q", title="average DPS"),
        y=alt.Y(f"{parse_field}:Q", title=parse_label),
        color=alt.Color(
            "player_class_lower:N",
            scale=alt.Scale(
                domain=list(CLASS_COLOURS.keys()),
                range=list(CLASS_COLOURS.values())
            ),
            legend=None,
        ),
        tooltip=[
            alt.Tooltip("player_name:N", title="player"),
            alt.Tooltip("player_class:N", title="class"),
            alt.Tooltip("player_spec:N", title="spec"),
            alt.Tooltip(f"{parse_field}:Q", title=parse_choice),
            alt.Tooltip("avg_dps:Q", title="avg DPS"),
            alt.Tooltip("parses:Q", title="logs"),
        ],
    )
)
st.altair_chart(scatter, use_container_width=True)

st.subheader("player DPS and parse summary")
display_df = chart_df[
    [
        "player_name",
        "player_class",
        "player_spec",
        "avg_parse",
        "best_parse",
        "avg_dps",
        "parses",
    ]
].rename(
    columns={
        "player_name": "Player",
        "player_class": "Class",
        "player_spec": "Spec",
        "avg_parse": "Avg Parse (%)",
        "best_parse": "Best Parse (%)",
        "avg_dps": "Avg DPS",
        "parses": "Log Count",
    }
)
st.dataframe(display_df, hide_index=True)
