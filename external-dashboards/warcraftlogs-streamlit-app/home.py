import streamlit as st
import pandas as pd
import json
import requests
import altair as alt
import plotly.graph_objects as go
from io import StringIO

# --- Constants --- #
REPO_URL = "https://raw.githubusercontent.com/richmulvany/richmulvany-databricks-projects/main"
LOGO_URL = "https://pbs.twimg.com/profile_images/1490380290962952192/qZk9xi5l_200x200.jpg"
BANNER_URL = "https://pbs.twimg.com/profile_banners/1485002107849789443/1716388990/1500x500"
SOCIAL_LINKS = [
    ("https://raider.io/guilds/eu/twisting-nether/Student%20Council", "https://cdn.raiderio.net/images/brand/Mark_White.png"),
    ("https://discord.gg/studentcouncil", "https://cdn.prod.website-files.com/6257adef93867e50d84d30e2/66e3d7f4ef6498ac018f2c55_Symbol.svg"),
    ("https://x.com/SCTNGuild", "https://upload.wikimedia.org/wikipedia/commons/thumb/b/b7/X_logo.jpg/1200px-X_logo.jpg"),
    ("https://www.warcraftlogs.com/guild/id/586885", "https://assets.rpglogs.com/cms/WCL_White_Icon_01_9b25d38cba.png")
]

# --- Page Config --- #
st.set_page_config(page_title="players · sc-warcraftlogs", page_icon=LOGO_URL, layout="wide")

# --- UI Utilities --- #
def st_normal():
    _, col, _ = st.columns([1, 8.5, 1])
    return col

# --- Loaders --- #
def load_csv(filename: str) -> pd.DataFrame:
    url = f"{REPO_URL}/data-exports/{filename}"
    response = requests.get(url)
    response.raise_for_status()
    return pd.read_csv(StringIO(response.text))

def load_json(filename: str) -> dict:
    url = f"{REPO_URL}/external-dashboards/warcraftlogs-streamlit-app/{filename}"
    response = requests.get(url)
    response.raise_for_status()
    return json.loads(response.text)

# --- Header --- #
def render_header():
    st.logo(LOGO_URL, link="https://www.warcraftlogs.com/guild/id/586885")
    st.image(BANNER_URL)

    st.markdown(f"""
    <div style="text-align: center;">
        <div style="display: flex; align-items: center; justify-content: center;">
            <a href="/" style="text-decoration: none;">
                <img src="{LOGO_URL}" width="64" style="border-radius: 100%; border: 2px solid #FFFFFF; margin-right: 12px;">
            </a>
            <a href="/" style="text-decoration: none; color: inherit;">
                <h1 style="margin: 0;">sc warcraftlogs</h1>
            </a>
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("""
    <div style="display: flex; justify-content: center; align-items: center; flex-wrap: wrap; gap: 20px; margin-top: 10px;">
    """ + "".join(
        f'<a href="{link}" target="_blank"><img src="{img}" width="32" style="top: 0px;"/></a>'
        for link, img in SOCIAL_LINKS
    ) + "</div>", unsafe_allow_html=True)

    st.divider()

# --- Visualizations --- #
def render_kill_summary(boss: str, date: str):
    with st_normal():
        st.markdown(f"## last boss kill: **{boss}**")
        st.image("https://pbs.twimg.com/media/GwAb3VQWEAEou3T?format=jpg&name=medium", use_container_width=True)
        st.caption(f"killed {date}")

def render_progression_chart(df: pd.DataFrame, boss: str):
    df = df[(df["boss_name"] == boss) & (df["raid_difficulty"] == "mythic")]

    panel_df = (
        df.groupby("report_date")["encounter_order"]
        .agg(["min", "max"])
        .reset_index()
        .rename(columns={"min": "first_pull", "max": "last_pull"})
        .assign(panel_index=lambda d: d.index)
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

    x_axis = alt.X("encounter_order:Q", title="pull number")
    y_axis = alt.Y("boss_hp_remaining_pct:Q", title="boss hp remaining (%)")

    actual_line = alt.layer(
        alt.Chart(df).mark_line(color="lightgrey").encode(
            x=x_axis, y=y_axis,
            tooltip=["report_date", "fight_outcome", "boss_hp_remaining_pct"]
        ),
        alt.Chart(df).mark_point(color="lightgrey", filled=True).encode(
            x=x_axis, y=y_axis,
            tooltip=["report_date", "fight_outcome", "boss_hp_remaining_pct"]
        )
    )

    lowest_line = alt.Chart(df).mark_line(color="#BB86FC").encode(
        x="encounter_order:Q",
        y="boss_hp_lowest_pull:Q",
        tooltip=["encounter_order", "boss_hp_lowest_pull"]
    )

    combined_chart = alt.layer(panel_chart, actual_line, lowest_line).properties(
        title=f"{boss} progression"
    )

    with st_normal():
        st.altair_chart(combined_chart, use_container_width=True)

def render_kill_composition(df: pd.DataFrame, boss: str, report_id: str, class_colours: dict):
    spec_range_type = {
        ("hunter", "beast mastery"): "ranged", ("hunter", "marksmanship"): "ranged", ("hunter", "survival"): "melee",
        ("mage", "arcane"): "ranged", ("mage", "fire"): "ranged", ("mage", "frost"): "ranged",
        ("warlock", "destruction"): "ranged", ("warlock", "affliction"): "ranged", ("warlock", "demonology"): "ranged",
        ("druid", "balance"): "ranged", ("druid", "feral"): "melee",
        ("shaman", "elemental"): "ranged", ("shaman", "enhancement"): "melee",
        ("evoker", "devastation"): "ranged", ("evoker", "augmentation"): "ranged",
        ("priest", "shadow"): "ranged",
        ("rogue", "assassination"): "melee", ("rogue", "subtlety"): "melee", ("rogue", "outlaw"): "melee",
        ("monk", "windwalker"): "melee",
        ("demonhunter", "havoc"): "melee",
        ("warrior", "arms"): "melee", ("warrior", "fury"): "melee",
        ("deathknight", "unholy"): "melee", ("deathknight", "frost"): "melee",
        ("paladin", "retribution"): "melee",
    }

    df = df[df["report_id"] == report_id].drop_duplicates()
    df["player_class_lower"] = df["player_class"].str.lower()
    df["player_spec_lower"] = df["player_spec"].str.lower()

    df["range_type"] = df.apply(
        lambda row: spec_range_type.get((row["player_class_lower"], row["player_spec_lower"]), "ranged"),
        axis=1
    )

    df.loc[df["player_role"] == "dps", "player_role"] = (
        df[df["player_role"] == "dps"]["range_type"].apply(lambda x: f"{x} dps")
    )

    role_order = ["tank", "healer", "melee dps", "ranged dps"]
    df["player_role"] = pd.Categorical(df["player_role"], categories=role_order, ordered=True)
    df["dot_x"] = df.groupby("player_role").cumcount() + 1

    chart = alt.Chart(df).mark_circle(size=400, opacity=1.0).encode(
        x=alt.X("dot_x:O", title=None, axis=None),
        y=alt.Y("player_role:N", sort=role_order, title=None),
        color=alt.Color(
            "player_class:N",
            scale=alt.Scale(domain=list(class_colours.keys()), range=list(class_colours.values())),
            legend=None
        ),
        tooltip=["player_name", "player_role", "player_class", "player_spec"]
    ).properties(
        width=400,
        height=200,
        title=f"{boss} kill composition"
    )

    with st_normal():
        st.altair_chart(chart, use_container_width=True)

# --- Donut Stats --- #
def render_donut_stats(player_deaths, guild_progression, boss):
    color_palette = ["#BB86FC", "#3700B3", "#03DAC6"]

    deaths_filtered = player_deaths[
        (player_deaths["boss_name"] == boss) &
        (player_deaths["raid_difficulty"] == "mythic")
    ]
    top_3_deaths = deaths_filtered["death_ability_name"].value_counts().nlargest(3).reset_index()
    top_3_deaths.columns = ["category", "value"]

    boss_prog = guild_progression[
        (guild_progression["boss_name"] == boss) &
        (guild_progression["raid_difficulty"] == "mythic")
    ].copy().drop_duplicates(subset=["report_id", "pull_number"])
    boss_prog["pull_start_time"] = pd.to_datetime(boss_prog["pull_start_time"], unit="ms")
    boss_prog["pull_end_time"] = pd.to_datetime(boss_prog["pull_end_time"], unit="ms")

    timing = boss_prog.groupby("report_id").agg({
        "pull_start_time": "min",
        "pull_end_time": "max",
        "fight_duration_sec": "sum"
    }).rename(columns={
        "pull_start_time": "raid_start",
        "pull_end_time": "raid_end",
        "fight_duration_sec": "pull_time"
    })
    timing["raid_time"] = (timing["raid_end"] - timing["raid_start"]).dt.total_seconds()
    timing["yap_time"] = (timing["raid_time"] - timing["pull_time"]).clip(lower=0)

    raid_time = round(timing["raid_time"].sum() / 3600, 1)
    pull_time = round(timing["pull_time"].sum() / 3600, 1)
    yap_time = round(timing["yap_time"].sum() / 3600, 1)
    time_df = pd.DataFrame({"category": ["pulling", "yapping"], "value": [pull_time, yap_time]})

    pulls = boss_prog.drop_duplicates(subset=["report_id", "pull_number"]).copy()
    def label_phase(row):
        if row["last_phase_is_intermission"]:
            return "phase 2"
        elif row["last_phase"] == 2:
            return "phase 3"
        return f"phase {row['last_phase']}"
    pulls["category"] = pulls.apply(label_phase, axis=1)
    phase_counts = pulls["category"].value_counts().reset_index()
    phase_counts.columns = ["category", "value"]
    total_pulls = len(pulls)

    charts_data = [
        {"data": phase_counts, "label": f"boss pulls:<br>{total_pulls}"},
        {"data": top_3_deaths, "label": f"boss deaths:<br>{deaths_filtered.shape[0]}"},
        {"data": time_df, "label": f"boss time:<br>{raid_time}hr"}
    ]

    cols = st.columns(3)
    for i, col in enumerate(cols):
        with col:
            data = charts_data[i]["data"].copy()
            label = charts_data[i]["label"]
            data["category"] = data["category"].str.replace("_", " ").str.replace(r"\\s+", "<br>", n=1, regex=True)

            fig = go.Figure(data=[
                go.Pie(
                    labels=data["category"],
                    values=data["value"],
                    hole=0.5,
                    textinfo="label+percent",
                    textposition="inside", 
                    marker=dict(colors=color_palette[:len(data)]),
                    showlegend=False,
                    sort=True,
                    direction="clockwise",
                    automargin=False,
                    rotation=0
                )
            ])
            fig.update_traces(
                textfont=dict(size=12),
                insidetextorientation="radial"
            )
            fig.update_layout(
                height=300,
                width=300,
                margin=dict(t=20, b=20, l=20, r=20),
                annotations=[dict(text=label, x=0.5, y=0.5, font_size=14, showarrow=False, font_color="white")],
                uniformtext_minsize=10,
                uniformtext_mode="hide"
            )
            st.plotly_chart(fig, use_container_width=True)

# --- ItemLevel Distribution --- #
def render_ilvl_chart(df, report_id):
    df = df[df["report_id"] == report_id].copy()
    df["player_item_level"] = pd.to_numeric(df["player_item_level"], errors="coerce")
    df = df.dropna(subset=["player_item_level"])

    base = alt.Chart(df).transform_density(
        "player_item_level",
        as_=["item_level", "density"],
        bandwidth=1
    )

    # density = base.mark_area(opacity=1.0, color='#BB86FC').encode(
    #     x=alt.X("item_level:Q", title="item level"),
    #     y=alt.Y("density:Q", title="density")
    # )

    histogram = alt.Chart(df).mark_bar(opacity=1.0, color='#BB86FC').encode(
        x=alt.X("player_item_level:Q", bin=alt.Bin(maxbins=8), title="item level"),
        y=alt.Y("count()", title="player count")
    )

    chart = (histogram).properties(
        width=800,
        height=300,
        title="item level distribution"
    )

    with st_normal():
        st.altair_chart(chart, use_container_width=True)

# --- Player Breakdown --- #
def render_player_breakdown(df, report_id):
    df = df[df["report_id"] == report_id].copy()
    breakdown = df[[
        "player_name","player_role","player_class","player_spec","player_item_level"
        ]
    ].sort_values("player_item_level", ascending=False)

    with st_normal():
        st.dataframe(
        breakdown,
        hide_index=True,
    )

# --- Additional Charts --- #
def render_dps_chart(df, report_id, pull_number, boss, class_colours):
    df = df[(df["report_id"] == report_id) & (df["pull_number"] == pull_number)]
    df = df.sort_values("damage_per_second", ascending=False)

    chart = (
        alt.Chart(df).mark_bar().encode(
            y=alt.Y("player_name:N", sort="-x", title="", axis=alt.Axis(labelOverlap=False)),
            x=alt.X("damage_per_second:Q", title="dps"),
            color=alt.Color("player_class:N", scale=alt.Scale(domain=list(class_colours.keys()), range=list(class_colours.values())), legend=None),
            tooltip=[
                alt.Tooltip("player_name", title="player"),
                alt.Tooltip("player_class", title="class"),
                alt.Tooltip("damage_per_second", title="dps"),
                alt.Tooltip("damage_done", title="damage done"),
            ]
        ).properties(width=800, height=25 * len(df), title=f"dps per player on first {boss} kill")
    )

    with st_normal():
        st.altair_chart(chart, use_container_width=True)

def render_hps_chart(df, report_id, pull_number, boss, class_colours):
    df = df[(df["report_id"] == report_id) & (df["pull_number"] == pull_number)]
    df = df.sort_values("healing_per_second", ascending=False)

    chart = (
        alt.Chart(df).mark_bar().encode(
            y=alt.Y("player_name:N", sort="-x", title="", axis=alt.Axis(labelOverlap=False)),
            x=alt.X("healing_per_second:Q", title="hps"),
            color=alt.Color("player_class:N", scale=alt.Scale(domain=list(class_colours.keys()), range=list(class_colours.values())), legend=None),
            tooltip=[
                alt.Tooltip("player_name", title="player"),
                alt.Tooltip("player_class", title="class"),
                alt.Tooltip("healing_per_second", title="hps"),
                alt.Tooltip("healing_done", title="healing done"),
            ]
        ).properties(width=800, height=25 * len(df), title=f"hps per player on first {boss} kill")
    )

    with st_normal():
        st.altair_chart(chart, use_container_width=True)

# --- Main --- #
def main():
    render_header()

    # Load data
    with st.spinner("Loading data..."):
        dfs = {name.replace(".csv", ""): load_csv(name) for name in [
            "guild_progression.csv", "player_dps.csv", "player_hps.csv",
            "player_deaths.csv", "composition.csv", "player_details.csv"]}
        class_colours = load_json("class_colours.json")

    # Extract recent kill info
    kills = dfs["guild_progression"][dfs["guild_progression"]["fight_outcome"] == "kill"]
    first_kills = kills.sort_values("report_date").drop_duplicates(["boss_name", "raid_difficulty"])
    recent_kill = first_kills.sort_values("report_date", ascending=False).iloc[0]

    boss, date, report_id, pull_number = (
        recent_kill["boss_name"], recent_kill["report_date"].split()[0],
        recent_kill["report_id"], recent_kill["pull_number"]
    )

    render_kill_summary(boss, date)
    render_progression_chart(dfs["guild_progression"], boss)
    render_kill_composition(dfs["composition"], boss, report_id, class_colours)
    render_ilvl_chart(dfs["player_details"], report_id)
    render_player_breakdown(dfs["player_details"], report_id)
    render_dps_chart(dfs["player_dps"], report_id, pull_number, boss, class_colours)
    render_hps_chart(dfs["player_hps"], report_id, pull_number, boss, class_colours)
    render_donut_stats(dfs["player_deaths"], dfs["guild_progression"], boss)


if __name__ == "__main__":
    main()
