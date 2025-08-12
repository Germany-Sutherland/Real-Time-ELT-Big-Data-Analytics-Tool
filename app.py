
---

## 3) `app.py` — Full code (copy & paste)
```python
# app.py
# Real-time ELT demo using USGS earthquake feed (public GeoJSON)
# Streamlit app: Extract -> Load -> Transform -> Visualize -> Agentic Analysis
# Free open-source stack. Designed for Streamlit Cloud (no heavy deps).

import warnings
warnings.filterwarnings("ignore")  # keep console/UI clean for demo

import streamlit as st
import pandas as pd
import numpy as np
import requests
from datetime import datetime, timezone
import plotly.express as px
from io import StringIO

# -------------------------
# Config
# -------------------------
st.set_page_config(page_title="Real-time ELT — USGS Stream Demo", layout="wide", page_icon="🌐")

USGS_URL_HOUR = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"
USGS_URL_DAY = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"

# -------------------------
# Helper: fetch USGS feed
# -------------------------
@st.cache_data(ttl=60)
def fetch_usgs(url: str):
    """
    Fetch a USGS GeoJSON feed and return a normalized DataFrame.
    Cached for 60s to avoid too frequent calls during demo.
    """
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    data = r.json()
    features = data.get("features", [])
    rows = []
    for f in features:
        props = f.get("properties", {})
        geom = f.get("geometry", {}) or {}
        coords = geom.get("coordinates", [None, None, None])
        ts_ms = props.get("time")
        ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc) if ts_ms else None
        rows.append({
            "id": f.get("id"),
            "time_utc": ts,
            "place": props.get("place"),
            "mag": props.get("mag"),
            "url": props.get("url"),
            "status": props.get("status"),
            "tsunami": props.get("tsunami"),
            "lon": coords[0] if len(coords) > 0 else None,
            "lat": coords[1] if len(coords) > 1 else None,
            "depth_km": coords[2] if len(coords) > 2 else None
        })
    df = pd.DataFrame(rows)
    # ensure types
    if not df.empty:
        df['mag'] = pd.to_numeric(df['mag'], errors='coerce').fillna(0.0)
        df['depth_km'] = pd.to_numeric(df['depth_km'], errors='coerce').fillna(0.0)
        df['time_utc'] = pd.to_datetime(df['time_utc'])
    return df

# -------------------------
# Session-store initialization
# -------------------------
def init_store():
    if "store" not in st.session_state:
        st.session_state.store = pd.DataFrame()  # empty store
    if "last_fetch" not in st.session_state:
        st.session_state.last_fetch = None

init_store()

# -------------------------
# Sidebar controls (Extract)
# -------------------------
st.sidebar.header("Data Source & Controls")
source_option = st.sidebar.selectbox("USGS feed", ["Past hour (live)", "Past day (live)"])
fetch_button = st.sidebar.button("Fetch latest events")
auto_refresh = st.sidebar.checkbox("Auto-refresh every 60s (cached)", value=False)
clear_store = st.sidebar.button("Clear in-memory store")
download_store = st.sidebar.button("Download current store CSV")

if clear_store:
    st.session_state.store = pd.DataFrame()
    st.success("In-memory store cleared.")

# -------------------------
# Main header & how-to
# -------------------------
st.title("🌐 Real-time ELT Demo — USGS Earthquake Stream")
st.write(
    "This demo shows an Extract → Load → Transform → Visualize pipeline using a live public feed (USGS). "
    "Use the sidebar to fetch live events and watch data flow through the pipeline."
)
st.markdown("""
**How to use (quick):**  
1. Choose the feed (past hour or past day) in the sidebar.  
2. Click **Fetch latest events** to extract and load new events into the in-memory store.  
3. Run Transform/Visualize steps below to compute features and draw charts.
""")

# -------------------------
# Extract & Load step
# -------------------------
with st.spinner("Fetching events from USGS..."):
    if fetch_button or (auto_refresh and source_option and st.experimental_rerun is None):
        try:
            url = USGS_URL_HOUR if source_option.startswith("Past hour") else USGS_URL_DAY
            df_new = fetch_usgs(url)
            if df_new is None:
                st.error("Failed to fetch data.")
            else:
                # Load: append deduplicated by 'id'
                store = st.session_state.store
                if store.empty:
                    combined = df_new
                else:
                    combined = pd.concat([store, df_new], ignore_index=True)
                    combined = combined.drop_duplicates(subset=["id"], keep="last").reset_index(drop=True)
                st.session_state.store = combined
                st.session_state.last_fetch = datetime.now(timezone.utc)
                st.success(f"Fetched {len(df_new)} events; store size {len(st.session_state.store)}")
        except Exception as e:
            st.error(f"Fetch failed: {e}")

# Also allow manual poll even without clicking (button optional)
if not st.session_state.store.empty:
    st.metric("Rows in store", int(len(st.session_state.store)))
    if st.session_state.last_fetch:
        st.metric("Last fetch (UTC)", st.session_state.last_fetch.strftime("%Y-%m-%d %H:%M:%S"))

# -------------------------
# Transform step
# -------------------------
st.markdown("---")
st.header("Transform: Feature Engineering & Quality Checks")

if st.button("Run Transform"):
    try:
        df = st.session_state.store.copy()
        if df.empty:
            st.info("Store is empty. Fetch events first.")
        else:
            # Feature: hour of day, magnitude bucket, depth bucket
            df['hour_utc'] = df['time_utc'].dt.hour
            df['mag_bucket'] = pd.cut(df['mag'], bins=[-1,1,2,3,4,5,10], labels=["<1","1-2","2-3","3-4","4-5",">=5"])
            df['depth_bucket'] = pd.cut(df['depth_km'], bins=[-1,10,50,200,1000], labels=["shallow","intermediate","deep","very_deep"])
            # Basic quality metrics
            missing_loc = df['lat'].isna().sum() + df['lon'].isna().sum()
            st.success(f"Transform done. Rows: {len(df)}. Missing loc entries: {missing_loc}")
            st.session_state.store = df  # update store with features
    except Exception as e:
        st.error(f"Transform failed: {e}")

# show small preview of transformed store
if not st.session_state.store.empty:
    st.subheader("Store preview (latest events)")
    st.dataframe(st.session_state.store.sort_values("time_utc", ascending=False).head(50))

# -------------------------
# Visualize step (Plotly charts)
# -------------------------
st.markdown("---")
st.header("Visualize: Interactive Charts")

if st.session_state.store.empty:
    st.info("No events to visualize. Fetch and transform events first.")
else:
    df_vis = st.session_state.store.copy()
    # Map: scatter geo (mag as size)
    try:
        fig_map = px.scatter_geo(
            df_vis,
            lat="lat",
            lon="lon",
            size="mag",
            color="mag",
            hover_name="place",
            hover_data={"time_utc":True, "depth_km":True},
            projection="natural earth",
            title="Recent Events (size = magnitude)"
        )
        st.plotly_chart(fig_map, use_container_width=True)
    except Exception:
        st.warning("Map not available (missing lat/lon).")

    # Time series: count by hour
    try:
        ts = df_vis.set_index("time_utc").resample("15min").size().rename("count").reset_index()
        fig_ts = px.line(ts, x="time_utc", y="count", title="Event count over time (15min bins)")
        st.plotly_chart(fig_ts, use_container_width=True)
    except Exception:
        st.warning("Time series not available.")

    # Histogram magnitude
    try:
        fig_hist = px.histogram(df_vis, x="mag", nbins=20, title="Magnitude distribution")
        st.plotly_chart(fig_hist, use_container_width=True)
    except Exception:
        st.warning("Histogram not available.")

    # Pie: depth buckets share
    if "depth_bucket" in df_vis.columns:
        fig_pie = px.pie(df_vis, names="depth_bucket", title="Depth bucket distribution")
        st.plotly_chart(fig_pie, use_container_width=True)

# -------------------------
# Agents: Analysis & Strategy
# -------------------------
st.markdown("---")
st.header("Agentic Analysis (explainable)")

def analysis_agent(df):
    thoughts = []
    thoughts.append("Analysis Agent: computing summary statistics and detecting spikes.")
    total = len(df)
    avg_mag = float(df['mag'].mean()) if total > 0 else 0.0
    max_mag = float(df['mag'].max()) if total > 0 else 0.0
    thoughts.append(f"Total events in store: {total}. Average magnitude: {avg_mag:.2f}. Max magnitude: {max_mag:.2f}.")
    # detect recent strong events (mag >= 5)
    strong = df[df['mag'] >= 5.0]
    thoughts.append(f"Number of strong events (mag >= 5): {len(strong)}.")
    return thoughts, strong

def strategy_agent(strong_events):
    thoughts = []
    if strong_events.empty:
        thoughts.append("Strategy Agent: No strong events — continue monitoring.")
        action = "Monitor"
    else:
        top = strong_events.sort_values("mag", ascending=False).iloc[0]
        thoughts.append(f"Strategy Agent: Strong event detected at {top['place']} (mag {top['mag']}). Recommend alerting regional operations.")
        action = f"Alert for {top['place']}"
    return thoughts, action

if st.button("Run Agents"):
    if st.session_state.store.empty:
        st.info("No data to analyze. Fetch & transform first.")
    else:
        a_thoughts, strong = analysis_agent(st.session_state.store)
        st.subheader("Analysis Agent — thoughts")
        for t in a_thoughts:
            st.write("- " + t)

        s_thoughts, action = strategy_agent(strong)
        st.subheader("Strategy Agent — thoughts")
        for t in s_thoughts:
            st.write("- " + t)

        st.markdown(f"**Recommended action:** {action}")

# -------------------------
# Download store CSV
# -------------------------
if download_store and not st.session_state.store.empty:
    csv = st.session_state.store.to_csv(index=False).encode("utf-8")
    st.download_button("Download current store", csv, "usgs_store.csv", "text/csv")

# -------------------------
# Bottom: explanation & ELT summary
# -------------------------
st.markdown("---")
st.header("About this ELT demo")
st.markdown(
    """
This demo shows how a real-time **ELT** (Extract → Load → Transform) pipeline works using a public live feed.
- **Extract**: we fetch the live USGS GeoJSON feed (no authentication, public).
- **Load**: new events are appended into an in-memory store (simulating a streaming buffer or small data lake).
- **Transform**: we compute derived fields (hour, magnitude buckets, depth buckets) and quality checks.
- **Visualize**: interactive charts (map, time-series, histograms, pie) help operators spot trends.
- **Agents**: two simple agents (Analysis & Strategy) compute summaries and recommend actions; this demonstrates agentic reasoning and explainability.

This is a lightweight, deployable demo suitable for showcasing real-time ELT & analytics skills.
"""
)
