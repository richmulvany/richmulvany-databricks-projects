# home.py
import streamlit as st
import pandas as pd
import requests
import altair as alt
from io import StringIO

# --- GitHub raw base URL ---
REPO_URL = "https://raw.githubusercontent.com/richmulvany/richmulvany-databricks-projects/test/warcraftlogs-gold-data-exports/data-exports"

# --- Streamlit UI ---
logo_path = "https://pbs.twimg.com/profile_images/1490380290962952192/qZk9xi5l_200x200.jpg"

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

st.header("home page")
