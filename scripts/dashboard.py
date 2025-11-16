"""
Smart City IoT Sensor Dashboard
Real-time monitoring with anomaly detection and predictive analytics
"""

import time
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from sqlalchemy import create_engine, text

# Page configuration
st.set_page_config(
    page_title="Smart City IoT Dashboard", 
    layout="wide",
    page_icon="🏙️"
)

# Custom CSS for better styling
st.markdown("""
    <style>
    .main > div {
        padding-top: 2rem;
    }
    .stMetric {
        background-color: #f0f2f6;
        padding: 15px;
        border-radius: 10px;
    }
    </style>
""", unsafe_allow_html=True)

st.title("🏙️ Smart City IoT Sensor Dashboard")
st.markdown("**Real-time monitoring of city sensors with AI-powered anomaly detection**")

DATABASE_URL = "postgresql://kafka_user:kafka_password@localhost:5432/kafka_db"


@st.cache_resource
def get_engine(url: str):
    return create_engine(url, pool_pre_ping=True)


engine = get_engine(DATABASE_URL)


def load_sensor_data(zone_filter: str = "All", limit: int = 500) -> pd.DataFrame:
    """Load recent sensor data from PostgreSQL."""
    base_query = "SELECT * FROM sensor_data"
    params = {}
    
    if zone_filter and zone_filter != "All":
        base_query += " WHERE zone = :zone"
        params["zone"] = zone_filter
    
    base_query += " ORDER BY timestamp DESC LIMIT :limit"
    params["limit"] = limit

    try:
        df = pd.read_sql_query(text(base_query), con=engine.connect(), params=params)
        if not df.empty and "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df
    except Exception as e:
        st.error(f"Error loading sensor data: {e}")
        return pd.DataFrame()


def load_window_metrics(window_type: str = "1min", limit: int = 100) -> pd.DataFrame:
    """Load windowed aggregation metrics."""
    table_name = f"sensor_metrics_{window_type}"
    query = f"SELECT * FROM {table_name} ORDER BY window_start DESC LIMIT :limit"
    
    try:
        df = pd.read_sql_query(text(query), con=engine.connect(), params={"limit": limit})
        if not df.empty:
            df["window_start"] = pd.to_datetime(df["window_start"])
            df["window_end"] = pd.to_datetime(df["window_end"])
        return df
    except Exception as e:
        return pd.DataFrame()


def get_available_zones() -> list:
    """Get list of available zones."""
    try:
        query = "SELECT DISTINCT zone FROM sensor_data ORDER BY zone"
        df = pd.read_sql_query(query, con=engine.connect())
        return ["All"] + df["zone"].tolist()
    except:
        return ["All"]


# Sidebar controls
st.sidebar.header("⚙️ Dashboard Controls")

available_zones = get_available_zones()
selected_zone = st.sidebar.selectbox("Filter by Zone", available_zones)

update_interval = st.sidebar.slider(
    "Update Interval (seconds)", min_value=2, max_value=30, value=5
)

limit_records = st.sidebar.number_input(
    "Number of records to display", min_value=100, max_value=5000, value=500, step=100
)

show_anomalies_only = st.sidebar.checkbox("Show Anomalies Only", value=False)

window_type = st.sidebar.selectbox(
    "Aggregation Window",
    ["1min", "5min"],
    help="Select time window for aggregated metrics"
)

if st.sidebar.button("🔄 Refresh Now"):
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.markdown("### 📊 Dashboard Info")
st.sidebar.info(
    "This dashboard displays real-time IoT sensor data from smart city infrastructure. "
    "Metrics include temperature, humidity, air quality, noise levels, and traffic flow."
)

# Main dashboard loop
placeholder = st.empty()

while True:
    # Load data
    df_sensors = load_sensor_data(selected_zone, limit=int(limit_records))
    df_metrics = load_window_metrics(window_type, limit=100)

    with placeholder.container():
        if df_sensors.empty:
            st.warning("⏳ Waiting for sensor data... Please ensure the producer and consumer are running.")
            time.sleep(update_interval)
            continue

        # Filter anomalies if requested
        if show_anomalies_only:
            df_sensors = df_sensors[df_sensors["is_anomaly"] == True]
            if df_sensors.empty:
                st.info("✅ No anomalies detected in the current dataset!")
                time.sleep(update_interval)
                continue

        # Calculate KPIs
        total_sensors = len(df_sensors)
        anomaly_count = df_sensors["is_anomaly"].sum()
        anomaly_rate = (anomaly_count / total_sensors * 100) if total_sensors > 0 else 0.0
        
        avg_temp = df_sensors["temperature"].mean()
        avg_aqi = df_sensors["air_quality_index"].mean()
        avg_humidity = df_sensors["humidity"].mean()
        avg_noise = df_sensors["noise_level"].mean()
        avg_traffic = df_sensors["traffic_flow"].mean()
        
        # Max values
        max_aqi = df_sensors["air_quality_index"].max()
        max_temp = df_sensors["temperature"].max()
        max_noise = df_sensors["noise_level"].max()

        # Display header with zone info
        col_header1, col_header2 = st.columns([3, 1])
        with col_header1:
            st.subheader(f"📍 Zone: {selected_zone} | 📊 {total_sensors} sensor readings")
        with col_header2:
            if anomaly_count > 0:
                st.error(f"🚨 {anomaly_count} ANOMALIES ({anomaly_rate:.1f}%)")
            else:
                st.success("✅ All Systems Normal")

        # KPI Metrics
        st.markdown("### 📈 Real-Time Metrics")
        kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)
        
        kpi1.metric(
            "🌡️ Avg Temperature",
            f"{avg_temp:.1f}°C",
            f"{max_temp:.1f}°C max",
            delta_color="off"
        )
        kpi2.metric(
            "💨 Avg Air Quality",
            f"{avg_aqi:.0f} AQI",
            f"{max_aqi:.0f} max",
            delta_color="inverse"
        )
        kpi3.metric(
            "💧 Avg Humidity",
            f"{avg_humidity:.1f}%",
            delta_color="off"
        )
        kpi4.metric(
            "🔊 Avg Noise",
            f"{avg_noise:.1f} dB",
            f"{max_noise:.1f} max",
            delta_color="off"
        )
        kpi5.metric(
            "🚗 Avg Traffic",
            f"{avg_traffic:.0f}",
            delta_color="off"
        )

        # Anomaly Alerts Section
        if anomaly_count > 0:
            st.markdown("### 🚨 Recent Anomalies")
            anomalies = df_sensors[df_sensors["is_anomaly"] == True].head(5)
            
            for idx, row in anomalies.iterrows():
                with st.expander(f"⚠️ {row['zone']} - {row['timestamp'].strftime('%H:%M:%S')}"):
                    col1, col2, col3 = st.columns(3)
                    col1.write(f"**Temperature:** {row['temperature']:.2f}°C")
                    col1.write(f"**Humidity:** {row['humidity']:.2f}%")
                    col2.write(f"**Air Quality:** {row['air_quality_index']:.2f} AQI")
                    col2.write(f"**Noise Level:** {row['noise_level']:.2f} dB")
                    col3.write(f"**Traffic Flow:** {row['traffic_flow']}")
                    col3.write(f"**Location:** ({row['latitude']:.4f}, {row['longitude']:.4f})")

        # Charts Section
        st.markdown("### 📊 Sensor Analytics")
        
        # Time series charts
        chart_col1, chart_col2 = st.columns(2)
        
        with chart_col1:
            # Temperature over time by zone
            if not df_sensors.empty:
                fig_temp = px.scatter(
                    df_sensors.sort_values('timestamp'),
                    x='timestamp',
                    y='temperature',
                    color='zone',
                    size='air_quality_index',
                    hover_data=['humidity', 'noise_level'],
                    title='🌡️ Temperature Trends by Zone',
                    labels={'temperature': 'Temperature (°C)', 'timestamp': 'Time'}
                )
                fig_temp.update_traces(marker=dict(line=dict(width=0.5, color='DarkSlateGrey')))
                st.plotly_chart(fig_temp, use_container_width=True)
        
        with chart_col2:
            # Air Quality over time
            if not df_sensors.empty:
                fig_aqi = px.line(
                    df_sensors.sort_values('timestamp'),
                    x='timestamp',
                    y='air_quality_index',
                    color='zone',
                    title='💨 Air Quality Index Over Time',
                    labels={'air_quality_index': 'AQI', 'timestamp': 'Time'}
                )
                fig_aqi.add_hline(y=100, line_dash="dash", line_color="orange", 
                                 annotation_text="Unhealthy for Sensitive Groups")
                fig_aqi.add_hline(y=200, line_dash="dash", line_color="red", 
                                 annotation_text="Unhealthy")
                st.plotly_chart(fig_aqi, use_container_width=True)

        chart_col3, chart_col4 = st.columns(2)
        
        with chart_col3:
            # Traffic flow by zone
            traffic_by_zone = df_sensors.groupby('zone')['traffic_flow'].mean().reset_index()
            traffic_by_zone = traffic_by_zone.sort_values('traffic_flow', ascending=False)
            
            fig_traffic = px.bar(
                traffic_by_zone,
                x='zone',
                y='traffic_flow',
                title='🚗 Average Traffic Flow by Zone',
                labels={'traffic_flow': 'Avg Traffic Flow', 'zone': 'Zone'},
                color='traffic_flow',
                color_continuous_scale='Reds'
            )
            st.plotly_chart(fig_traffic, use_container_width=True)
        
        with chart_col4:
            # Noise levels distribution
            fig_noise = px.box(
                df_sensors,
                x='zone',
                y='noise_level',
                title='🔊 Noise Level Distribution by Zone',
                labels={'noise_level': 'Noise Level (dB)', 'zone': 'Zone'},
                color='zone'
            )
            fig_noise.add_hline(y=85, line_dash="dash", line_color="orange",
                               annotation_text="OSHA Limit")
            st.plotly_chart(fig_noise, use_container_width=True)

        # Windowed Aggregations Section
        if not df_metrics.empty:
            st.markdown(f"### ⏱️ Windowed Aggregations ({window_type})")
            
            # Display recent window metrics
            window_col1, window_col2 = st.columns(2)
            
            with window_col1:
                # Average metrics by zone over windows
                fig_window_temp = px.line(
                    df_metrics,
                    x='window_start',
                    y='avg_temperature',
                    color='zone',
                    title=f'Average Temperature per {window_type} Window',
                    labels={'avg_temperature': 'Avg Temperature (°C)', 'window_start': 'Window Start'}
                )
                st.plotly_chart(fig_window_temp, use_container_width=True)
            
            with window_col2:
                # Anomaly count per window
                fig_window_anomalies = px.bar(
                    df_metrics.head(20),
                    x='window_start',
                    y='anomaly_count',
                    color='zone',
                    title=f'Anomalies Detected per {window_type} Window',
                    labels={'anomaly_count': 'Anomaly Count', 'window_start': 'Window Start'}
                )
                st.plotly_chart(fig_window_anomalies, use_container_width=True)

        # Heatmap: Multi-metric correlation
        st.markdown("### 🔥 Sensor Correlations")
        
        numeric_cols = ['temperature', 'humidity', 'air_quality_index', 'noise_level', 'traffic_flow']
        correlation_matrix = df_sensors[numeric_cols].corr()
        
        fig_corr = px.imshow(
            correlation_matrix,
            text_auto=True,
            aspect="auto",
            title="Correlation Matrix of Sensor Metrics",
            color_continuous_scale='RdBu_r',
            zmin=-1,
            zmax=1
        )
        st.plotly_chart(fig_corr, use_container_width=True)

        # Recent Data Table
        st.markdown("### 📋 Recent Sensor Readings")
        display_columns = ['timestamp', 'zone', 'temperature', 'humidity', 
                          'air_quality_index', 'noise_level', 'traffic_flow', 'is_anomaly']
        
        df_display = df_sensors[display_columns].head(20).copy()
        df_display['is_anomaly'] = df_display['is_anomaly'].map({True: '🚨', False: '✅'})
        
        st.dataframe(
            df_display.style.background_gradient(
                subset=['temperature', 'air_quality_index', 'traffic_flow'],
                cmap='YlOrRd'
            ),
            use_container_width=True,
            height=400
        )

        # Footer
        st.markdown("---")
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        st.caption(
            f"📡 Last updated: {current_time} | "
            f"🔄 Auto-refresh: {update_interval}s | "
            f"💾 Total records: {total_sensors:,}"
        )

    time.sleep(update_interval)
