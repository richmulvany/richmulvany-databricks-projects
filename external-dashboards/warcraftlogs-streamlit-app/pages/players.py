
# home.py
import streamlit as st
import pandas as pd
import requests
import altair as alt
from io import StringIO

# --- GitHub raw base URL ---
REPO_URL = "https://raw.githubusercontent.com/richmulvany/richmulvany-databricks-projects/main/data-exports"

# --- Helper to load CSVs directly from GitHub --- #
def load_csv(file_name: str) -> pd.DataFrame:
    url = f"{REPO_URL}/{file_name}"
    response = requests.get(url)
    response.raise_for_status()
    return pd.read_csv(StringIO(response.text))

# --- Streamlit UI --- #
logo_path = "https://pbs.twimg.com/profile_images/1490380290962952192/qZk9xi5l_200x200.jpg"

# Set tab config
st.set_page_config(page_title="players · sc-warcraftlogs", page_icon=logo_path)

# Import SC logo
st.logo(
    logo_path,
    link="https://www.warcraftlogs.com/guild/id/586885"
)

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

st.header("player statistics")

