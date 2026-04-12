import os
import sys
import pickle
import numpy as np
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from datetime import datetime

# ─────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="NYC Taxi Anomaly Detector",
    page_icon="🚕",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ─────────────────────────────────────────────
# LOAD CSS FROM TEMPLATE
# ─────────────────────────────────────────────
def load_css(template_path: str = "templates/style.html") -> None:
    """Load CSS from external HTML template file."""
    if os.path.exists(template_path):
        with open(template_path, "r") as f:
            st.markdown(f.read(), unsafe_allow_html=True)
    else:
        # Fallback minimal CSS so app doesn't break if template missing
        st.markdown("""
        <style>
            .stApp { background-color: #0a0a0f; color: #e8e8f0; }
            .section-header { font-size: 1.1rem; font-weight: 700;
                              color: #fff; border-left: 3px solid #ffc700;
                              padding-left: 0.8rem; margin: 1.5rem 0 1rem 0; }
            .metric-grid { display: grid; grid-template-columns: repeat(4,1fr); gap: 1rem; margin-bottom: 2rem; }
            .metric-card { background: #0f0f1a; border: 1px solid #1e1e3a;
                           border-radius: 12px; padding: 1.4rem 1.6rem; }
            .metric-label { font-size: 0.7rem; color: #6666aa; text-transform: uppercase; }
            .metric-value { font-size: 2rem; font-weight: 800; color: #fff; }
            .metric-value.red { color: #ff4455; } .metric-value.yellow { color: #ffc700; }
            .metric-value.blue { color: #00b4ff; } .metric-value.green { color: #00e5a0; }
            .hero-banner { background: #0d0d1f; border: 1px solid #2a2a5a;
                           border-radius: 16px; padding: 2rem 3rem; margin-bottom: 2rem; }
            .hero-title { font-size: 2.4rem; font-weight: 800; color: #fff; }
            .hero-title span { color: #ffc700; }
            .hero-subtitle { font-size: 0.8rem; color: #6666aa; text-transform: uppercase; }
            .info-box { background: #0f0f1a; border: 1px solid #1e1e3a;
                        border-radius: 10px; padding: 1rem 1.4rem;
                        font-size: 0.78rem; color: #8888bb; line-height: 1.8; }
            #MainMenu, footer, header { visibility: hidden; }
        </style>
        """, unsafe_allow_html=True)

load_css()


# ─────────────────────────────────────────────
# CONSTANTS — paths relative to artifact dir
# ─────────────────────────────────────────────
MODEL_REL_PATH     = os.path.join("model_trainer",       "trained_model",       "model.pkl")
SCALER_REL_PATH    = os.path.join("data_transformation", "preprocessing_object","preprocessing.pkl")
CSV_REL_PATH       = os.path.join("data_ingestion",      "feature_store",       "taxiNYC.csv")
ARTIFACTS_BASE_DIR = "Artifacts"
TIMESTAMP_FORMAT   = "%d_%m_%Y_%H_%M_%S"


# ─────────────────────────────────────────────
# HELPER — Auto-detect latest artifact folder
# ─────────────────────────────────────────────
def get_latest_artifact_dir(base_dir: str = ARTIFACTS_BASE_DIR):
    """
    Auto-detects the latest timestamp folder inside Artifacts/.
    Returns (full_path, timestamp_string) or (None, None).
    Works even if Artifacts/ doesn't exist yet.
    """
    if not os.path.exists(base_dir):
        return None, None

    folders = [
        f for f in os.listdir(base_dir)
        if os.path.isdir(os.path.join(base_dir, f))
    ]
    if not folders:
        return None, None

    # Filter only valid timestamp folders — skip any other folders
    valid = []
    for f in folders:
        try:
            datetime.strptime(f, TIMESTAMP_FORMAT)
            valid.append(f)
        except ValueError:
            continue

    if not valid:
        return None, None

    valid.sort(key=lambda x: datetime.strptime(x, TIMESTAMP_FORMAT), reverse=True)
    latest = valid[0]
    return os.path.join(base_dir, latest), latest


# ─────────────────────────────────────────────
# HELPER — Load files safely
# ─────────────────────────────────────────────
def load_model(artifact_dir: str):
    path = os.path.join(artifact_dir, MODEL_REL_PATH)
    if not os.path.exists(path):
        return None, f"Model not found at: `{path}`"
    try:
        with open(path, "rb") as f:
            return pickle.load(f), None
    except Exception as e:
        return None, str(e)


def load_scaler(artifact_dir: str):
    path = os.path.join(artifact_dir, SCALER_REL_PATH)
    if not os.path.exists(path):
        return None, f"Scaler not found at: `{path}`"
    try:
        with open(path, "rb") as f:
            return pickle.load(f), None
    except Exception as e:
        return None, str(e)


def load_raw_data(artifact_dir: str):
    """
    Loads CSV and always restores timestamp as DatetimeIndex.
    Handles all edge cases: no file, wrong format, missing column.
    """
    path = os.path.join(artifact_dir, CSV_REL_PATH)
    if not os.path.exists(path):
        return None, f"CSV not found at: `{path}`"
    try:
        df = pd.read_csv(path)

        # ✅ Case 1: timestamp is a regular column
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)

        # ✅ Case 2: timestamp saved as 'Unnamed: 0' (index=True without name)
        elif 'Unnamed: 0' in df.columns:
            df.rename(columns={'Unnamed: 0': 'timestamp'}, inplace=True)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)

        # ✅ Case 3: index is already datetime string
        else:
            try:
                df.index = pd.to_datetime(df.index)
            except Exception:
                pass

        df.index.name = 'timestamp'

        # Validate required columns exist
        required = ['passengers', 'hour', 'day_of_week', 'rolling_mean', 'rolling_std']
        missing = [c for c in required if c not in df.columns]
        if missing:
            return None, f"Missing columns in CSV: {missing}"

        return df, None

    except Exception as e:
        return None, str(e)


def run_predictions(model, scaler, df: pd.DataFrame):
    """Scale data and predict anomalies. Returns enriched DataFrame."""
    try:
        scaled      = scaler.transform(df)
        predictions = model.predict(scaled)          # 1=normal, -1=anomaly
        scores      = model.decision_function(scaled) # lower = more anomalous
        df = df.copy()
        df['anomaly']       = predictions
        df['anomaly_score'] = scores
        df['is_anomaly']    = df['anomaly'] == -1
        return df, None
    except Exception as e:
        return None, str(e)


# ─────────────────────────────────────────────
# PLOTLY THEME
# ─────────────────────────────────────────────
PLOT_BG     = "#0a0a0f"
PAPER_BG    = "#0a0a0f"
GRID_COLOR  = "#1a1a2e"
TEXT_COLOR  = "#8888bb"
FONT_FAMILY = "Space Mono, monospace"

def base_layout(title=""):
    return dict(
        title=dict(text=title, font=dict(family="Syne, sans-serif", size=15, color="#ffffff")),
        plot_bgcolor=PLOT_BG,
        paper_bgcolor=PAPER_BG,
        font=dict(family=FONT_FAMILY, color=TEXT_COLOR, size=11),
        xaxis=dict(gridcolor=GRID_COLOR, linecolor=GRID_COLOR, tickcolor=TEXT_COLOR),
        yaxis=dict(gridcolor=GRID_COLOR, linecolor=GRID_COLOR, tickcolor=TEXT_COLOR),
        legend=dict(bgcolor="rgba(0,0,0,0)", bordercolor=GRID_COLOR, borderwidth=1),
        margin=dict(l=20, r=20, t=50, b=20),
        hovermode="x unified"
    )


# ─────────────────────────────────────────────
# LOAD DATA — fail proof with clear messages
# ─────────────────────────────────────────────
artifact_dir, timestamp_str = get_latest_artifact_dir()

if artifact_dir is None:
    st.error("❌ No Artifacts folder found.")
    st.info("""
        **How to fix:**
        1. Run the training pipeline first: `python main.py`
        2. Make sure you're running Streamlit from your project root directory
        3. The `Artifacts/` folder must be in the same directory as `app.py`
    """)
    st.stop()

# Load model
model, model_err = load_model(artifact_dir)
if model_err:
    st.error(f"❌ Model loading failed: {model_err}")
    st.info("Run `python main.py` to train and save the model first.")
    st.stop()

# Load scaler
scaler, scaler_err = load_scaler(artifact_dir)
if scaler_err:
    st.error(f"❌ Scaler loading failed: {scaler_err}")
    st.info("Run `python main.py` to generate the scaler first.")
    st.stop()

# Load data
df_raw, data_err = load_raw_data(artifact_dir)
if data_err:
    st.error(f"❌ Data loading failed: {data_err}")
    st.info("Run `python main.py` to generate the feature store CSV first.")
    st.stop()

# Run predictions
df, pred_err = run_predictions(model, scaler, df_raw)
if pred_err:
    st.error(f"❌ Prediction failed: {pred_err}")
    st.info("There may be a mismatch between the scaler and model. Re-run `python main.py`.")
    st.stop()

anomalies = df[df['is_anomaly'] == True]
normals   = df[df['is_anomaly'] == False]


# ─────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
        <div style='font-family:Space Mono,monospace;font-size:0.7rem;
                    color:#6666aa;letter-spacing:2px;text-transform:uppercase;
                    margin-bottom:1.5rem;padding-bottom:1rem;border-bottom:1px solid #1e1e3a'>
            🚕 NYC TAXI MONITOR
        </div>
    """, unsafe_allow_html=True)

    st.markdown(f"""
        <div class='info-box'>
            <div>📁 ARTIFACT RUN</div>
            <div style='color:#ffc700;margin-top:4px;word-break:break-all'>{timestamp_str}</div>
            <div style='margin-top:12px'>📊 TOTAL RECORDS</div>
            <div style='color:#ffffff;margin-top:4px'>{len(df):,}</div>
            <div style='margin-top:12px'>🔴 TOTAL ANOMALIES</div>
            <div style='color:#ff4455;margin-top:4px'>{len(anomalies):,}</div>
        </div>
    """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("<div style='font-family:Space Mono,monospace;font-size:0.7rem;color:#6666aa;letter-spacing:1px'>FILTER BY TIME RANGE</div>", unsafe_allow_html=True)

    date_min = df.index.min().date()
    date_max = df.index.max().date()
    start_date = st.date_input("Start Date", value=date_min, min_value=date_min, max_value=date_max)
    end_date   = st.date_input("End Date",   value=date_max, min_value=date_min, max_value=date_max)

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("<div style='font-family:Space Mono,monospace;font-size:0.7rem;color:#6666aa;letter-spacing:1px'>FILTER BY HOUR</div>", unsafe_allow_html=True)
    hour_range = st.slider("Hour of Day", 0, 23, (0, 23))

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("<div style='font-family:Space Mono,monospace;font-size:0.7rem;color:#6666aa;letter-spacing:1px'>VIEW OPTIONS</div>", unsafe_allow_html=True)
    show_rolling = st.checkbox("Show Rolling Mean", value=True)
    show_scores  = st.checkbox("Show Anomaly Scores", value=True)


# ─────────────────────────────────────────────
# FILTER DATA
# ─────────────────────────────────────────────
mask = (
    (df.index.date >= start_date) &
    (df.index.date <= end_date) &
    (df['hour'] >= hour_range[0]) &
    (df['hour'] <= hour_range[1])
)
df_filtered   = df[mask]
anom_filtered = df_filtered[df_filtered['is_anomaly'] == True]
norm_filtered = df_filtered[df_filtered['is_anomaly'] == False]

if len(df_filtered) == 0:
    st.warning("⚠️ No data found for the selected filters. Please adjust the date or hour range.")
    st.stop()


# ─────────────────────────────────────────────
# HERO BANNER
# ─────────────────────────────────────────────
st.markdown("""
    <div class='hero-banner'>
        <div class='hero-title'>🚕 NYC Taxi <span>Anomaly</span> Detector</div>
        <div class='hero-subtitle'>Isolation Forest · Time Series · Anomaly Detection</div>
    </div>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────
# METRIC CARDS
# ─────────────────────────────────────────────
anomaly_pct       = round((len(anom_filtered) / len(df_filtered)) * 100, 2)
avg_passengers    = int(df_filtered['passengers'].mean())
peak_anomaly_hour = int(anom_filtered['hour'].mode()[0]) if len(anom_filtered) > 0 else 0

st.markdown(f"""
    <div class='metric-grid'>
        <div class='metric-card yellow'>
            <div class='metric-label'>Total Records</div>
            <div class='metric-value yellow'>{len(df_filtered):,}</div>
        </div>
        <div class='metric-card red'>
            <div class='metric-label'>Anomalies Found</div>
            <div class='metric-value red'>{len(anom_filtered):,}</div>
        </div>
        <div class='metric-card blue'>
            <div class='metric-label'>Anomaly Rate</div>
            <div class='metric-value blue'>{anomaly_pct}%</div>
        </div>
        <div class='metric-card green'>
            <div class='metric-label'>Avg Passengers</div>
            <div class='metric-value green'>{avg_passengers:,}</div>
        </div>
    </div>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────
# CHART 1 — PASSENGER TRAFFIC WITH ANOMALIES
# ─────────────────────────────────────────────
st.markdown("<div class='section-header'>📈 Passenger Traffic with Anomalies</div>", unsafe_allow_html=True)

fig1 = go.Figure()
fig1.add_trace(go.Scatter(
    x=norm_filtered.index, y=norm_filtered['passengers'],
    mode='lines', name='Normal',
    line=dict(color='#00b4ff', width=1.2), opacity=0.8
))
if show_rolling and 'rolling_mean' in df_filtered.columns:
    fig1.add_trace(go.Scatter(
        x=df_filtered.index, y=df_filtered['rolling_mean'],
        mode='lines', name='Rolling Mean',
        line=dict(color='#ffc700', width=1.5, dash='dot'), opacity=0.7
    ))
fig1.add_trace(go.Scatter(
    x=anom_filtered.index, y=anom_filtered['passengers'],
    mode='markers', name='Anomaly',
    marker=dict(color='#ff4455', size=7, symbol='circle',
                line=dict(color='#ff8899', width=1))
))
fig1.update_layout(**base_layout("Passenger Count Over Time"), height=380)
st.plotly_chart(fig1, use_container_width=True)


# ─────────────────────────────────────────────
# CHART 2 — ANOMALY SCORE OVER TIME
# ─────────────────────────────────────────────
if show_scores:
    st.markdown("<div class='section-header'>🎯 Anomaly Score Over Time</div>", unsafe_allow_html=True)
    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(
        x=df_filtered.index, y=df_filtered['anomaly_score'],
        mode='lines', name='Anomaly Score',
        line=dict(color='#aa44ff', width=1.2),
        fill='tozeroy', fillcolor='rgba(170,68,255,0.05)'
    ))
    fig2.add_hline(y=0, line=dict(color='#ff4455', width=1.5, dash='dash'),
                   annotation_text="Anomaly Threshold", annotation_font_color="#ff4455")
    fig2.update_layout(**base_layout("Decision Score (Below 0 = Anomaly)"), height=280)
    st.plotly_chart(fig2, use_container_width=True)


# ─────────────────────────────────────────────
# CHART 3 & 4 — ANOMALY DISTRIBUTION
# ─────────────────────────────────────────────
st.markdown("<div class='section-header'>🔍 Anomaly Distribution Analysis</div>", unsafe_allow_html=True)
col1, col2 = st.columns(2)

with col1:
    hourly = df_filtered.groupby('hour')['is_anomaly'].sum().reset_index()
    hourly.columns = ['hour', 'anomaly_count']
    fig3 = go.Figure(go.Bar(
        x=hourly['hour'], y=hourly['anomaly_count'],
        marker=dict(color=hourly['anomaly_count'],
                    colorscale=[[0,'#1a1a3a'],[0.5,'#aa44ff'],[1,'#ff4455']],
                    showscale=False),
        hovertemplate='Hour %{x}: %{y} anomalies<extra></extra>'
    ))
    fig3.update_layout(**base_layout("Anomalies by Hour of Day"), height=300)
    st.plotly_chart(fig3, use_container_width=True)

with col2:
    day_names = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun']
    daily = df_filtered.groupby('day_of_week')['is_anomaly'].sum().reset_index()
    daily.columns = ['day_of_week', 'anomaly_count']
    daily['day_name'] = daily['day_of_week'].apply(lambda x: day_names[x] if x < 7 else str(x))
    fig4 = go.Figure(go.Bar(
        x=daily['day_name'], y=daily['anomaly_count'],
        marker=dict(color=daily['anomaly_count'],
                    colorscale=[[0,'#1a1a3a'],[0.5,'#ffc700'],[1,'#ff4455']],
                    showscale=False),
        hovertemplate='%{x}: %{y} anomalies<extra></extra>'
    ))
    fig4.update_layout(**base_layout("Anomalies by Day of Week"), height=300)
    st.plotly_chart(fig4, use_container_width=True)


# ─────────────────────────────────────────────
# CHART 5 — ROLLING STD VOLATILITY
# ─────────────────────────────────────────────
st.markdown("<div class='section-header'>📊 Traffic Volatility (Rolling Std)</div>", unsafe_allow_html=True)
fig5 = go.Figure()
fig5.add_trace(go.Scatter(
    x=df_filtered.index, y=df_filtered['rolling_std'],
    mode='lines', name='Rolling Std',
    line=dict(color='#00e5a0', width=1.2),
    fill='tozeroy', fillcolor='rgba(0,229,160,0.05)'
))
fig5.add_trace(go.Scatter(
    x=anom_filtered.index, y=anom_filtered['rolling_std'],
    mode='markers', name='Anomaly',
    marker=dict(color='#ff4455', size=6, symbol='circle')
))
fig5.update_layout(**base_layout("Rolling Standard Deviation Over Time"), height=280)
st.plotly_chart(fig5, use_container_width=True)


# ─────────────────────────────────────────────
# ANOMALY TABLE
# ─────────────────────────────────────────────
st.markdown("<div class='section-header'>🔴 Detected Anomaly Records</div>", unsafe_allow_html=True)

if len(anom_filtered) == 0:
    st.info("✅ No anomalies found in the selected time range and filters.")
else:
    display_df = anom_filtered[['passengers','hour','day_of_week',
                                 'rolling_mean','rolling_std','anomaly_score']].copy()
    display_df.index = display_df.index.strftime('%Y-%m-%d %H:%M')
    display_df.columns = ['Passengers','Hour','Day of Week',
                           'Rolling Mean','Rolling Std','Anomaly Score']
    display_df = display_df.round(3)

    st.dataframe(
        display_df.style
            .background_gradient(subset=['Anomaly Score'], cmap='RdPu_r')
            .format({'Passengers':'{:,.0f}','Rolling Mean':'{:,.1f}',
                     'Rolling Std':'{:,.1f}','Anomaly Score':'{:.4f}'}),
        use_container_width=True,
        height=350
    )
    csv_bytes = display_df.to_csv().encode('utf-8')
    st.download_button(
        label="⬇️ Download Anomalies as CSV",
        data=csv_bytes,
        file_name=f"anomalies_{timestamp_str}.csv",
        mime='text/csv'
    )


# ─────────────────────────────────────────────
# FOOTER
# ─────────────────────────────────────────────
st.markdown("""
    <div style='text-align:center;margin-top:3rem;padding-top:1.5rem;
                border-top:1px solid #1e1e3a;
                font-family:Space Mono,monospace;font-size:0.7rem;color:#3a3a6a'>
        NYC TAXI ANOMALY DETECTOR · ISOLATION FOREST · BUILT WITH STREAMLIT
    </div>
""", unsafe_allow_html=True)
