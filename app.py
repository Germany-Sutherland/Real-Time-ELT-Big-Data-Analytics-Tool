# app.py — Real-Time Big Data ELT Dashboard (COVID-19 data source)

import warnings
warnings.filterwarnings("ignore")

import streamlit as st
import pandas as pd
import plotly.express as px
from io import StringIO
import requests

st.set_page_config(page_title="Real-Time COVID-19 ELT Dashboard", layout="wide")
st.title("🌍 Real-Time Big Data ELT Dashboard — COVID-19 Global Metrics")
st.write(
    "Live ETL demo using the latest global COVID-19 data. "
    "Extract → Load → Transform → Visualize with interactive charts."
)
st.markdown("""
**How to use:**  
- Click **Fetch Latest Data** to retrieve current stats.  
- Watch the data flow through ETL into interactive visuals below.
""")

# Extract step: fetch live CSV from OWID GitHub
CSV_URL = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/latest/owid-covid-latest.csv"
fetch = st.button("Fetch Latest Data")

if 'df_store' not in st.session_state:
    st.session_state.df_store = pd.DataFrame()

if fetch:
    try:
        r = requests.get(CSV_URL, timeout=10)
        r.raise_for_status()
        df_new = pd.read_csv(StringIO(r.text))
        st.session_state.df_store = df_new
        st.success(f"Fetched {len(df_new):,} rows of COVID-19 data")
    except Exception as e:
        st.error(f"Error fetching data: {e}")

if st.session_state.df_store.empty:
    st.info("No data loaded. Click 'Fetch Latest Data' to begin.")
    st.stop()

df = st.session_state.df_store.copy()

# Transform: derive per-million metrics
df['cases_per_million'] = df['total_cases_per_million']
df['vaccinations_per_hundred'] = df['people_vaccinated_per_hundred'].fillna(0)
df['population_millions'] = df['population'] / 1e6

# Show raw top rows
st.subheader("Sample Data (first 10 rows)")
st.dataframe(df[['location','total_cases','total_deaths','total_vaccinations','population']].head(10))

# Visuals
col1, col2 = st.columns(2)
with col1:
    fig_bar = px.bar(
        df.nlargest(10, 'total_cases'),
        x='location', y='total_cases',
        title="Top 10 Countries by Total Cases"
    )
    st.plotly_chart(fig_bar, use_container_width=True)

with col2:
    fig_scatter = px.scatter(
        df,
        x='vaccinations_per_hundred', y='cases_per_million',
        size='population_millions', hover_name='location',
        title="Vaccination vs Cases per Million"
    )
    st.plotly_chart(fig_scatter, use_container_width=True)

# Time-series simulation: ask user to upload their own historical data CSV
st.markdown("---")
st.subheader("Time-Series (Requires historical data)")
hist = st.file_uploader("Upload OWID time-series CSV (e.g., owid-covid-data.csv) for trend charts", type="csv")
if hist:
    hist_df = pd.read_csv(hist, parse_dates=['date'])
    top = st.selectbox("Select country for time-series", hist_df['location'].unique())
    td = hist_df[hist_df['location'] == top]
    fig_line = px.line(
        td, x='date', y='new_cases_smoothed_per_million',
        title=f"7-day Avg New Cases per Million — {top}"
    )
    st.plotly_chart(fig_line, use_container_width=True)

st.markdown("---")
st.markdown("""
### About this demo  
- **Extract**: gets up-to-date COVID-19 data from OWID’s GitHub repository.  
- **Load**: stores it in memory for analysis.  
- **Transform**: computes meaningful per-capita metrics.  
- **Visualize**: outputs bar chart and scatter plot for insights.  
This simulates how real-time analytics workflows work in production.
""")
