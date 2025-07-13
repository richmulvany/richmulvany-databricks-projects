# warcraftlogs_streamlit_app.py
import streamlit as st
import pandas as pd
import requests
import altair as alt
from io import StringIO

# --- GitHub raw base URL ---
REPO_URL = "https://github.com/richmulvany/richmulvany-databricks-projects/tree/main/data-exports"

# --- Streamlit UI ---
logo_path = "https://pbs.twimg.com/profile_images/1490380290962952192/qZk9xi5l_200x200.jpg"

st.logo(
    logo_path,
    link="https://www.warcraftlogs.com/guild/id/586885"
)

st.markdown(f"""
<div style="display: flex; align-items: center;">
    <img src={logo_path} width="64" style="border-radius: 100%; border: 2px solid #FFFFFF; margin-right: 12px;">
    <h1 style="margin: 0;">sc warcraftlogs</h1>
</div>
""", unsafe_allow_html=True)

st.header("death statistics")

page = st.sidebar.radio("data type:",["deaths", "damage", "healing"])
