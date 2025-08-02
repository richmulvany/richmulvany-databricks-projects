import streamlit as st
import pandas as pd
import altair as alt
import json
import requests
from io import StringIO


# --- GitHub raw base URL ---
REPO_URL = (
    "https://raw.githubusercontent.com/richmulvany/richmulvany-databricks-projects/main"
)

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

def build_attendance_chart(df: pd.DataFrame, class_colours: dict, title: str) -> alt.Chart:
    return (
        alt.Chart(df.sort_values("overall_attendance", ascending=False))
        .mark_bar()
        .encode(
            x=alt.X("player_name:N", sort=None, title="", axis=alt.Axis(labelOverlap=False)),
            y=alt.Y("overall_attendance:Q", title="attendance (%)", scale=alt.Scale(domain=[0, 100])),
            color=alt.Color(
                "player_class:N",
                scale=alt.Scale(domain=list(class_colours.keys()), range=list(class_colours.values())),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("player_name", title="player"),
                alt.Tooltip("player_class", title="class"),
                alt.Tooltip("overall_attendance", title="attendance", format=".2f"),
            ],
        )
        .properties(width="container", height=400, title=title)
    )

def main() -> None:
    logo_path = "https://pbs.twimg.com/profile_images/1490380290962952192/qZk9xi5l_200x200.jpg"
    st.set_page_config(page_title="players · sc-warcraftlogs", page_icon=logo_path, layout="wide")
    st.logo(logo_path, link="https://www.warcraftlogs.com/guild/id/586885")
    st.markdown(
        f"""
        <div style="display: flex; align-items: center;">
            <a href="/" style="text-decoration: none;">
                <img src="{logo_path}" width="64" style="border-radius: 100%; border: 2px solid #FFFFFF; margin-right: 12px;">
            </a>
            <a href="/" style="text-decoration: none; color: inherit;">
                <h1 style="margin: 0;">sc warcraftlogs</h1>
            </a>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.header("player statistics")

    # --- Load data ---
    with st.spinner("Loading data..."):
        pull_counts = load_csv("player_pull_counts.csv")
        player_details = load_csv("player_details.csv")
        player_dps = load_csv("player_dps.csv")
        guild_roster = load_csv("guild_roster.csv")
        class_data = load_csv("game_data_classes.csv")
        class_colours = load_json("class_colours.json")

    pull_counts["total_pulls"] = pd.to_numeric(pull_counts["total_pulls"], errors="coerce").fillna(0)

    # Filter to players under rank 8
    eligible_players = guild_roster[guild_roster["guild_rank"] < 8][["player_name"]]
    pull_counts = pull_counts.merge(eligible_players, on="player_name", how="inner")
    player_details = player_details.merge(eligible_players, on="player_name", how="inner")
    player_dps = player_dps.merge(eligible_players, on="player_name", how="inner")

    # Map player_class
    class_map = (
        guild_roster[["player_name", "player_class_id"]]
        .drop_duplicates()
        .merge(class_data, left_on="player_class_id", right_on="class_id", how="left")
        .assign(player_class=lambda df: df["class_name"].str.lower().str.strip())
        [["player_name", "player_class"]]
    )

    pull_counts = pull_counts.merge(class_map, on="player_name", how="left")

    # --- Sidebar filters ---
    st.sidebar.header("filters")
    difficulties = pull_counts["raid_difficulty"].dropna().unique().tolist()
    raids = player_dps["raid_name"].dropna().unique().tolist()

    selected_difficulty = st.selectbox("select difficulty", ["all"] + sorted(difficulties), index=2 if "mythic" in difficulties else 0)
    selected_raid = st.sidebar.selectbox("select raid", ["all"] + sorted(raids), index=0)

    if selected_raid == "all":
        bosses = player_dps["boss_name"].dropna().unique().tolist()
    else:
        bosses = player_dps[player_dps["raid_name"] == selected_raid]["boss_name"].dropna().unique().tolist()

    selected_boss = st.sidebar.selectbox("select boss", ["all"] + sorted(bosses), index=0)

    # --- Filtered pulls ---
    filtered_pulls = pull_counts.copy()
    if selected_difficulty != "all":
        filtered_pulls = filtered_pulls[filtered_pulls["raid_difficulty"] == selected_difficulty]
    if selected_raid != "all":
        filtered_pulls = filtered_pulls[filtered_pulls["raid_name"] == selected_raid]
    if selected_boss != "all":
        filtered_pulls = filtered_pulls[filtered_pulls["boss_name"] == selected_boss]

    # --- Attendance ---
    if filtered_pulls.empty:
        st.info("No attendance data available for the selected filters.")
    else:
        attendance_totals = (
            filtered_pulls.groupby(["player_name", "player_class"])["total_pulls"]
            .sum()
            .reset_index()
        )
        max_pulls = attendance_totals["total_pulls"].max() or 1
        attendance_totals["overall_attendance"] = (
            round(attendance_totals["total_pulls"] / max_pulls * 100, 2)
        )
        chart = build_attendance_chart(attendance_totals, class_colours, title="player attendance summary")
        st.subheader(
            f"attendance: {selected_raid if selected_raid != 'all' else 'all raids'}{'' if selected_boss == 'all' else ' · ' + selected_boss}"
        )
        st.altair_chart(chart, use_container_width=True)
        st.dataframe(
            attendance_totals.sort_values("overall_attendance", ascending=False)
            .rename(columns={
                "player_name": "player",
                "player_class": "class",
                "total_pulls": "pulls",
                "overall_attendance": "attendance_%",
            }),
            use_container_width=True,
            hide_index=True
        )

    # --- Item Level ---
    details = player_details.copy()
    details["date"] = pd.to_datetime(details["report_date"].str.split().str[0], errors="coerce")
    details["player_item_level"] = pd.to_numeric(details["player_item_level"], errors="coerce")
    plot_df = details.dropna(subset=["date", "player_item_level"])
    if plot_df.empty:
        st.info("No item level data available.")
    else:
        ilvl_chart = (
            alt.Chart(plot_df)
            .mark_line(point=True)
            .encode(
                x=alt.X("date:T", title=None),
                y=alt.Y("player_item_level:Q", title="item level", scale=alt.Scale(zero=False)),
                color=alt.Color(
                "player_class:N",
                scale=alt.Scale(domain=list(class_colours.keys()), range=list(class_colours.values())),
                legend=None,
            ),
                tooltip=[
                    alt.Tooltip("player_name", title="player"),
                    alt.Tooltip("date:T", title="date", format="%Y-%m-%d"),
                    alt.Tooltip("player_item_level", title="item level"),
                    alt.Tooltip("player_role", title="role"),
                    alt.Tooltip("player_class", title="class"),
                    alt.Tooltip("player_spec", title="spec"),
                ],
            )
            .properties(width="container", height=400, title="player ilvl over time")
        )
        st.subheader("item level progression")
        st.altair_chart(ilvl_chart, use_container_width=True)
        latest_ilvl = (
            plot_df.sort_values("date")
            .groupby("player_name").tail(1)
            .sort_values("player_item_level", ascending=False)
            [["player_name", "player_role", "player_class", "player_spec", "player_item_level"]]
            .rename(columns={"player_item_level": "latest_item_level"})
        )
        st.dataframe(
            latest_ilvl,
            use_container_width=True,
            hide_index=True
        )
    # --- Guild avg / min / max item level chart ---
    ilvl_agg = (
        plot_df.groupby("date")
        .agg(
            avg_ilvl=("player_item_level", "mean"),
            std_ilvl=("player_item_level", "std"),
        )
        .reset_index()
    )
    ilvl_agg["min_ilvl"] = round(ilvl_agg["avg_ilvl"] - ilvl_agg["std_ilvl"], 2)
    ilvl_agg["max_ilvl"] = round(ilvl_agg["avg_ilvl"] + ilvl_agg["std_ilvl"], 2)

    # Band (fill area)
    band = alt.Chart(ilvl_agg).mark_area(
        opacity=0.2,
        color="#BB86FC"
    ).encode(
        x="date:T",
        y="min_ilvl:Q",
        y2="max_ilvl:Q"
    )

    # Line for min_ilvl
    line_min = alt.Chart(ilvl_agg).mark_line(color="#BB86FC",interpolate="monotone").encode(
        x="date:T",
        y="min_ilvl:Q"
    )

    # Line for max_ilvl
    line_max = alt.Chart(ilvl_agg).mark_line(color="#BB86FC",interpolate="monotone").encode(
        x="date:T",
        y="max_ilvl:Q"
    )

    # Optional: average line (dashed black)
    avg_line = alt.Chart(ilvl_agg).mark_line(
        color="lightgrey", strokeDash=[5, 5],
        interpolate="monotone"
    ).encode(
        x=alt.X("date:T", title=None),
        y=alt.Y("avg_ilvl:Q", scale=alt.Scale(zero=False), title="item level")
    )

    # Combine all
    guild_ilvl_chart = (band + line_min + line_max + avg_line).properties(
        width="container",
        height=300,
        title="guild ilvl mean with standard deviation over time"
    )

    st.altair_chart(guild_ilvl_chart, use_container_width=True)

if __name__ == "__main__":
    main()