# Real-Time-ELT-Big-Data-Analytics-Tool

# Real-time ELT Demo — USGS Stream (Streamlit)

Demo app showing an ELT pipeline using a live public feed (USGS earthquake GeoJSON).
- Extract → Load → Transform → Visualize → Analyze
- Agents show their reasoning and recommend actions.
- All free & open-source. Deploy on Streamlit Cloud.

## Files
- `app.py` — main Streamlit app
- `requirements.txt` — dependencies

## Run locally
```bash
python -m venv .venv
source .venv/bin/activate        # Windows PowerShell: .venv\Scripts\Activate.ps1
pip install -r requirements.txt
streamlit run app.py
