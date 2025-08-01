import streamlit as st
import pandas as pd
import altair as alt
import json
import requests
from io import StringIO
from typing import List


# --- GitHub raw base URL ---
REPO_URL = (
    "https://raw.githubusercontent.com/richmulvany/richmulvany-databricks-projects/main"
)


def load_csv(file_name: str) -> pd.DataFrame:
    """Load a CSV from the data-exports folder of the repository.

    Parameters
    ----------
    file_name : str
        File name relative to the ``data-exports`` directory.

    Returns
    -------
    pd.DataFrame
        DataFrame containing the CSV contents.
    """
    url = f"{REPO_URL}/data-exports/{file_name}"
    response = requests.get(url)
    response.raise_for_status()
    return pd.read_csv(StringIO(response.text))


def load_json(file_name: str) -> dict:
    """Load a JSON file from the Streamlit app directory of the repository.

    Parameters
    ----------
    file_name : str
        File name relative to ``external-dashboards/warcraftlogs-streamlit-app``.

    Returns
    -------
    dict
        Parsed JSON object.
    """
    url = f"{REPO_URL}/external-dashboards/warcraftlogs-streamlit-app/{file_name}"
    response = requests.get(url)
    response.raise_for_status()
    return json.loads(response.text)


def filter_to_kills(
    df: pd.DataFrame, include_wipes: bool, id_cols: List[str]
) -> pd.DataFrame:
    """Filter a DataFrame of pulls to include only the kill pull for each
    boss/report combination when ``include_wipes`` is False.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing at least ``report_id``, ``boss_name`` and
        ``pull_number`` columns.
    include_wipes : bool
        If True, return ``df`` unchanged.  Otherwise filter to rows where
        ``pull_number`` equals the maximum pull number per combination of
        ``id_cols``.
    id_cols : List[str]
        List of columns that uniquely identify a kill (e.g. report_id,
        boss_name, and raid_name).

    Returns
    -------
    pd.DataFrame
        DataFrame filtered to kill pulls if requested.
    """
    if include_wipes:
        return df.copy()
    # compute max pull per combination
    group_cols = id_cols + ["boss_name"]
    max_pulls = (
        df.groupby(group_cols)["pull_number"].max().reset_index().rename(
            columns={"pull_number": "kill_pull_number"}
        )
    )
    merged = df.merge(
        max_pulls,
        on=group_cols,
        how="left",
    )
    return merged[merged["pull_number"] == merged["kill_pull_number"]].drop(
        columns=["kill_pull_number"]
    )


def build_attendance_chart(
    df: pd.DataFrame, class_colours: dict, title: str
) -> alt.Chart:
    """Construct an Altair bar chart for attendance.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with columns ``player_name``, ``overall_attendance`` and
        ``player_class``.
    class_colours : dict
        Mapping of lowercase class name to hexadecimal colour codes.
    title : str
        Title for the chart.

    Returns
    -------
    alt.Chart
        Configured bar chart.
    """
    return (
        alt.Chart(df.sort_values("overall_attendance", ascending=False))
        .mark_bar()
        .encode(
            x=alt.X(
                "player_name:N",
                sort=None,
                title="",
                axis=alt.Axis(labelAngle=-45),
            ),
            y=alt.Y(
                "overall_attendance:Q",
                title="attendance (%)",
                scale=alt.Scale(domain=[0, 100]),
            ),
            color=alt.Color(
                "player_class:N",
                scale=alt.Scale(
                    domain=list(class_colours.keys()),
                    range=list(class_colours.values()),
                ),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("player_name", title="player"),
                alt.Tooltip("player_class", title="class"),
                alt.Tooltip("overall_attendance", title="attendance", format=".2f"),
            ],
        )
        # .properties(width="container", height=400, title=title)
    )


def build_parse_chart(
    df: pd.DataFrame, class_colours: dict, title: str
) -> alt.Chart:
    """Construct an Altair bar chart showing parse‐vs‐bracket differences.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with columns ``player_name``, ``avg_parse_percent``,
        ``avg_bracket_percent``, ``avg_diff`` and ``player_class``.
    class_colours : dict
        Mapping of lowercase class name to colour codes.
    title : str
        Title for the chart.

    Returns
    -------
    alt.Chart
        Bar chart highlighting differences between parse and bracket percentages.
    """
    base = df.sort_values("avg_diff", ascending=False)
    return (
        alt.Chart(base)
        .mark_bar()
        .encode(
            x=alt.X(
                "player_name:N",
                sort=None,
                title="",
                axis=alt.Axis(labelAngle=-45),
            ),
            y=alt.Y(
                "avg_diff:Q",
                title="average difference (parse − bracket)",
            ),
            color=alt.Color(
                "player_class:N",
                scale=alt.Scale(
                    domain=list(class_colours.keys()),
                    range=list(class_colours.values()),
                ),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("player_name", title="player"),
                alt.Tooltip("player_class", title="class"),
                alt.Tooltip("avg_parse_percent", title="avg parse", format=".1f"),
                alt.Tooltip("avg_bracket_percent", title="avg bracket", format=".1f"),
                alt.Tooltip("avg_diff", title="avg diff", format="+.1f"),
            ],
        )
        # .properties(width="container", height=400, title=title)
    )


def main() -> None:
    """Run the Streamlit player statistics page."""

    # --- Page setup ---
    logo_path = "https://pbs.twimg.com/profile_images/1490380290962952192/qZk9xi5l_200x200.jpg"
    st.set_page_config(
        page_title="players · sc-warcraftlogs", page_icon=logo_path, layout="wide"
    )
    # Header with logo and title
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
        ranks_dps = load_csv("ranks_dps.csv")
        guild_roster = load_csv("guild_roster.csv")
        class_data = load_csv("game_data_classes.csv")
        class_colours = load_json("class_colours.json")

    # Ensure numeric total pulls
    pull_counts["total_pulls"] = pd.to_numeric(
        pull_counts["total_pulls"], errors="coerce"
    ).fillna(0)

    # Map players to their classes (lowercase) using guild roster and game data
    class_map = (
        guild_roster[["player_id", "player_class_id"]]
        .drop_duplicates()
        .merge(
            class_data,
            left_on="player_class_id",
            right_on="class_id",
            how="left",
        )
        .assign(player_class=lambda df: df["class_name"].str.lower().str.strip())
        [["player_id", "player_class"]]
    )

    # Add player class to pull_counts via player_id (pull_counts may not have player_id; if not, join on player_name)
    if "player_id" in pull_counts.columns:
        pull_counts = pull_counts.merge(
            class_map, on="player_id", how="left"
        )
    else:
        # fallback join by player_name
        pull_counts = pull_counts.merge(
            guild_roster[["player_name", "player_class_id"]]
            .merge(
                class_data,
                left_on="player_class_id",
                right_on="class_id",
                how="left",
            )
            .assign(
                player_class=lambda df: df["class_name"].str.lower().str.strip()
            )[["player_name", "player_class"]],
            on="player_name",
            how="left",
        )

    # Determine unique filter values from data
    difficulties = (
        pull_counts["raid_difficulty"].dropna().unique().tolist()
        if "raid_difficulty" in pull_counts.columns
        else []
    )
    raids = (
        player_dps["raid_name"].dropna().unique().tolist()
        if "raid_name" in player_dps.columns
        else []
    )
    # Sidebar filters
    st.sidebar.header("filters")
    selected_difficulty = st.sidebar.selectbox(
        "select difficulty", options=["all"] + sorted(difficulties), index=1 if "mythic" in difficulties else 0
    )
    selected_raid = st.sidebar.selectbox(
        "select raid", options=["all"] + sorted(raids), index=0
    )
    # Determine bosses based on selected raid
    if selected_raid == "all":
        bosses = player_dps["boss_name"].dropna().unique().tolist()
    else:
        bosses = (
            player_dps[player_dps["raid_name"] == selected_raid]["boss_name"]
            .dropna()
            .unique()
            .tolist()
        )
    selected_boss = st.sidebar.selectbox(
        "select boss", options=["all"] + sorted(bosses), index=0
    )
    # Player role filter
    roles = player_details["player_role"].dropna().unique().tolist()
    selected_roles = st.sidebar.multiselect(
        "select roles", options=sorted(roles), default=roles
    )
    # Include wipes toggle
    include_wipes = st.sidebar.checkbox(
        "include wipes (all pulls)", value=False
    )
    # Metric selector
    metric_choice = st.sidebar.selectbox(
        "metric", options=["attendance", "parse vs bracket", "item level (coming soon)"]
    )

    # Filter data according to selections
    filtered_pulls = pull_counts.copy()
    if selected_difficulty != "all" and "raid_difficulty" in filtered_pulls.columns:
        filtered_pulls = filtered_pulls[
            filtered_pulls["raid_difficulty"] == selected_difficulty
        ]
    if selected_raid != "all" and "raid_name" in filtered_pulls.columns:
        filtered_pulls = filtered_pulls[
            filtered_pulls["raid_name"] == selected_raid
        ]
    if selected_boss != "all":
        filtered_pulls = filtered_pulls[
            filtered_pulls["boss_name"] == selected_boss
        ]

    # Filter by roles using player_details mapping to player_id
    if selected_roles and "player_id" in filtered_pulls.columns:
        role_map = (
            player_details[["player_id", "player_role"]]
            .drop_duplicates()
            .query("player_role in @selected_roles")
        )
        filtered_pulls = filtered_pulls.merge(role_map, on="player_id", how="inner")

    # Apply kill/wipe filter
    if not filtered_pulls.empty:
        # Use raid_name and report_id to ensure correct grouping when raid_name exists
        id_cols = [col for col in ["report_id", "raid_name"] if col in filtered_pulls.columns]
        filtered_pulls = filter_to_kills(
            filtered_pulls, include_wipes=include_wipes, id_cols=id_cols
        )

    # Attendance metric
    if metric_choice == "attendance":
        if filtered_pulls.empty:
            st.info(
                "No data available for the selected filters. Try adjusting the raid, boss or difficulty."
            )
        else:
            # Calculate attendance per player
            attendance_totals = (
                filtered_pulls.groupby(["player_name", "player_class"])["total_pulls"]
                .sum()
                .reset_index()
            )
            max_pulls = attendance_totals["total_pulls"].max() or 1
            attendance_totals["overall_attendance"] = (
                attendance_totals["total_pulls"] / max_pulls * 100
            )
            chart = build_attendance_chart(
                attendance_totals, class_colours, title="player attendance summary"
            )
            st.subheader(
                f"attendance: {selected_raid if selected_raid != 'all' else 'all raids'}{'' if selected_boss == 'all' else ' · ' + selected_boss}"
            )
            st.altair_chart(chart, use_container_width=True)
            # Display table for more detail
            st.dataframe(
                attendance_totals.sort_values(
                    "overall_attendance", ascending=False
                ).rename(
                    columns={
                        "player_name": "player",
                        "player_class": "class",
                        "total_pulls": "pulls",
                        "overall_attendance": "attendance_%",
                    }
                ),
                use_container_width=True,
            )

    # Parse vs bracket metric
    elif metric_choice == "parse vs bracket":
        # Join ranks_dps with player_dps to obtain raid/difficulty information
        ranks = ranks_dps.copy()
        # ensure numeric
        ranks["parse_percent"] = pd.to_numeric(ranks["parse_percent"], errors="coerce")
        ranks["bracket_percent"] = pd.to_numeric(ranks["bracket_percent"], errors="coerce")
        # join with player_dps to bring raid_name and raid_difficulty
        join_cols = ["report_id", "pull_number", "player_id", "boss_name"]
        ranks = ranks.merge(
            player_dps[[
                col
                for col in [
                    "report_id",
                    "pull_number",
                    "player_id",
                    "boss_name",
                    "raid_name",
                    "raid_difficulty",
                    "player_class",
                    "player_spec",
                ]
                if col in player_dps.columns
            ]],
            on=join_cols,
            how="left",
            suffixes=("", "_dps"),
        )
        # Apply filters
        if selected_difficulty != "all" and "raid_difficulty" in ranks.columns:
            ranks = ranks[ranks["raid_difficulty"] == selected_difficulty]
        if selected_raid != "all" and "raid_name" in ranks.columns:
            ranks = ranks[ranks["raid_name"] == selected_raid]
        if selected_boss != "all":
            ranks = ranks[ranks["boss_name"] == selected_boss]
        if selected_roles:
            ranks = ranks[ranks["player_role"].isin(selected_roles)]
        # Remove NaNs
        ranks = ranks.dropna(subset=["parse_percent", "bracket_percent"])
        if ranks.empty:
            st.info(
                "No parse data available for the selected filters. Try adjusting your selections."
            )
        else:
            # Clip parse_percent and bracket_percent to [0, 100]
            ranks["parse_percent"] = ranks["parse_percent"].clip(upper=100)
            ranks["bracket_percent"] = ranks["bracket_percent"].clip(upper=100)
            ranks["diff"] = ranks["parse_percent"] - ranks["bracket_percent"]
            # Aggregate per player
            parse_summary = (
                ranks.groupby(
                    ["player_name", "player_class", "player_spec"]
                )
                .agg(
                    avg_parse_percent=("parse_percent", "mean"),
                    avg_bracket_percent=("bracket_percent", "mean"),
                    avg_diff=("diff", "mean"),
                    log_count=("parse_percent", "size"),
                )
                .reset_index()
            )
            chart = build_parse_chart(
                parse_summary, class_colours, title="average parse vs bracket"
            )
            st.subheader(
                f"parse vs bracket: {selected_raid if selected_raid != 'all' else 'all raids'}{'' if selected_boss == 'all' else ' · ' + selected_boss}"
            )
            st.altair_chart(chart, use_container_width=True)
            st.dataframe(
                parse_summary.sort_values("avg_diff", ascending=False)
                .rename(
                    columns={
                        "player_name": "player",
                        "player_class": "class",
                        "player_spec": "spec",
                        "avg_parse_percent": "avg_parse_%",
                        "avg_bracket_percent": "avg_bracket_%",
                        "avg_diff": "avg_diff",
                        "log_count": "logs",
                    }
                ),
                use_container_width=True,
            )

    # Item level (placeholder)
    else:
        st.subheader("item level (coming soon)")
        st.info(
            "Item level data is not yet available in the exported datasets. "
            "The Warcraft Logs API exposes item level through combatantinfo "
            "events, which record each player's gear and corresponding item levels "
            "at the start of a pull. A silver table for events_combatant exists "
            "in the medallion architecture, but no gold notebook currently aggregates "
            "this information.\n\n"
            "To build this feature, create a new gold notebook that reads the "
            "combatantinfo events from `02_silver.staging.warcraftlogs_events_combatant` "
            "or similar, parses the list of gear objects to compute an average or "
            "median item level for each player per report. Joining with the fights "
            "table will add raid, boss and difficulty context. Export the result to "
            "`data-exports/player_ilvl.csv` with columns like `report_date`, "
            "`raid_name`, `boss_name`, `raid_difficulty`, `player_name`, `player_class`, "
            "`player_spec`, and `item_level`. Once available, this page can load "
            "`player_ilvl.csv` and display line charts of item level progression for "
            "individual players or comparisons across the raid."
        )


# if __name__ == "__main__":
#     main()
