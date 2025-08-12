# Real-Time Big Data ELT Dashboard
# 100% Free & Open Source | Works on Streamlit Cloud Free Tier

import streamlit as st
import pandas as pd
import plotly.express as px
import requests
from io import StringIO

# -----------------------
# App Title & Description
# -----------------------
st.set_page_config(page_title="Real-Time Big Data ELT Dashboard", layout="wide")
st.title("📊 Real-Time Big Data ELT Dashboard")
st.markdown("""
This tool fetches live data from a public GitHub dataset, transforms it, and displays visual analytics.
You’ll see **real-time ELT in action** — a core skill for future data engineers and AI-driven companies.
""")

# -----------------------
# Data Source (Example CSV from GitHub)
# -----------------------
DATA_URL = "https://raw.githubusercontent.com/datablist/sample-csv-files/main/files/people/people-100.csv"

try:
    response = requests.get(DATA_URL)
    response.raise_for_status()
    csv_data = StringIO(response.text)
    df = pd.read_csv(csv_data)
except Exception as e:
    st.error(f"❌ Failed to fetch data: {e}")
    st.stop()

# -----------------------
# ELT Process (Extract, Load, Transform)
# -----------------------
# Example transformation: Group by Country and count entries
country_count = df["Country"].value_counts().reset_index()
country_count.columns = ["Country", "Count"]

# -----------------------
# Display Raw Data
# -----------------------
with st.expander("📂 View Raw Data"):
    st.dataframe(df)

# -----------------------
# Analytics Visualization
# -----------------------
col1, col2 = st.columns(2)

with col1:
    fig_pie = px.pie(country_count, names="Country", values="Count", title="Distribution by Country")
    st.plotly_chart(fig_pie, use_container_width=True)

with col2:
    fig_bar = px.bar(country_count.head(10), x="Country", y="Count", title="Top 10 Countries")
    st.plotly_chart(fig_bar, use_container_width=True)

# -----------------------
# Time-Series Example (Randomized for Demo)
# -----------------------
df["JoinDate"] = pd.to_datetime(df["Date of birth"], errors="coerce")
df_time = df.groupby(df["JoinDate"].dt.year).size().reset_index(name="Count")

fig_line = px.line(df_time, x="JoinDate", y="Count", title="Sign-ups Over Time (Sample Data)")
st.plotly_chart(fig_line, use_container_width=True)

# -----------------------
# Instructions for Users
# -----------------------
st.markdown("""
---
### 🛠 How to Use This Dashboard
1. The app fetches fresh data from GitHub every time it loads.
2. It processes the data in real time (ELT pipeline).
3. Visuals update instantly to show the latest trends.
---
""")
