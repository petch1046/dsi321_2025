import os
import pandas as pd
import plotly.express as px
import streamlit as st
import s3fs
from datetime import timedelta
from zoneinfo import ZoneInfo
import pyarrow.parquet as pq
import numpy as np
import time

# Set timezone
os.environ['TZ'] = 'Asia/Bangkok'
time.tzset()

# Streamlit config
st.set_page_config(page_title="Air Quality Dashboard", layout="wide")

# Set up LakeFS connection
lakefs_endpoint = os.getenv("LAKEFS_ENDPOINT", "http://lakefs-dev:8000")
ACCESS_KEY = os.getenv("LAKEFS_ACCESS_KEY")
SECRET_KEY = os.getenv("LAKEFS_SECRET_KEY")

fs = s3fs.S3FileSystem(
    key=ACCESS_KEY,
    secret=SECRET_KEY,
    client_kwargs={'endpoint_url': lakefs_endpoint}
)

@st.cache_data(ttl=600)
def load_data():
    lakefs_path = "s3://air-quality/main/airquality.parquet/year=2025"
    data_list = fs.glob(f"{lakefs_path}/*/*/*/*")
    df_all = pd.concat([pd.read_parquet(f"s3://{path}", filesystem=fs) for path in data_list], ignore_index=True)
    df_all['lat'] = pd.to_numeric(df_all['lat'], errors='coerce')
    df_all['long'] = pd.to_numeric(df_all['long'], errors='coerce')
    df_all['year'] = df_all['year'].astype(int)
    df_all['month'] = df_all['month'].astype(int)
    df_all.drop_duplicates(inplace=True)
    df_all['PM25.aqi'] = df_all['PM25.aqi'].mask(df_all['PM25.aqi'] < 0, pd.NA)
    df_all['PM25.aqi'] = df_all.groupby('stationID')['PM25.aqi'].transform(lambda x: x.fillna(method='ffill'))
    return df_all

def filter_data(df, start_date, end_date, station):
    df_filtered = df.copy()
    df_filtered = df_filtered[
        (df_filtered['timestamp'].dt.date >= start_date) &
        (df_filtered['timestamp'].dt.date <= end_date)
    ]
    if station != "All Stations":
        df_filtered = df_filtered[df_filtered['nameTH'] == station]
    df_filtered = df_filtered[df_filtered['PM25.aqi'] >= 0]
    return df_filtered

def classify_aqi(aqi):
    if aqi <= 50: return 'Good'
    elif aqi <= 100: return 'Moderate'
    elif aqi <= 150: return 'Unhealthy for Sensitive'
    elif aqi <= 200: return 'Unhealthy'
    elif aqi <= 300: return 'Very Unhealthy'
    else: return 'Hazardous'

def sample_stations(station_series):
    examples = sorted(set(station_series))[:3]
    return ', '.join(examples) + ('...' if len(set(station_series)) > 3 else '')

st.title("Air Quality Dashboard from LakeFS")
df = load_data()

with st.sidebar:
    st.title("Air4Thai Dashboard")
    st.header("⚙️ Settings")

    max_date = df['timestamp'].max().date()
    min_date = df['timestamp'].min().date()

    start_date = st.date_input("Start date", min_value=min_date, max_value=max_date, value=min_date)
    end_date = st.date_input("End date", min_value=min_date, max_value=max_date, value=max_date)

    station_name = df['nameTH'].dropna().unique().tolist()
    station_name.sort()
    station_name.insert(0, "All Stations")
    station = st.selectbox("Select Station", station_name)

df_filtered = filter_data(df, start_date, end_date, station)
df_filtered['AQI Category'] = df_filtered['PM25.aqi'].apply(classify_aqi)

category_summary = (
    df_filtered.groupby('AQI Category')
    .agg(Count=('nameTH', 'count'), ExampleStations=('nameTH', sample_stations))
    .reset_index()
)

refresh_interval_sec = 3600
st.markdown(f"""<meta http-equiv="refresh" content="{refresh_interval_sec}">""", unsafe_allow_html=True)

placeholder = st.empty()

with placeholder.container():
    if not df_filtered.empty:
        avg_aqi = df_filtered['PM25.aqi'].mean()
        avg_color = df_filtered['PM25.color_id'].mean()

        prev_day = end_date - pd.Timedelta(days=1)
        df_prev_day = filter_data(df, prev_day, prev_day, station)
        prev_avg_aqi = df_prev_day['PM25.aqi'].mean()
        prev_avg_color = df_prev_day['PM25.color_id'].mean()

        delta_aqi = None if pd.isna(prev_avg_aqi) else avg_aqi - prev_avg_aqi
        delta_color = None if pd.isna(prev_avg_color) else avg_color - prev_avg_color

        area_highest_aqi = df_filtered.groupby('areaTH')['PM25.aqi'].mean().idxmax()
        area_highest_aqi_val = df_filtered.groupby('areaTH')['PM25.aqi'].mean().max()

        delta_area_aqi = None
        if not df_prev_day.empty:
            area_prev_highest_aqi_val = df_prev_day.groupby('areaTH')['PM25.aqi'].mean().max()
            delta_area_aqi = area_highest_aqi_val - area_prev_highest_aqi_val

        kpi1, kpi2, kpi3 = st.columns(3)
        kpi1.metric("☁️ Average quality of PM2.5", f"{avg_aqi:.2f}", f"{delta_aqi:+.2f}" if delta_aqi else None)
        kpi2.metric("🇹🇭 Average PM2.5 levels in Thailand", f"{avg_color:.2f}", f"{delta_color:+.2f}" if delta_color else None)
        kpi3.metric("📍 Areas with the highest PM2.5 levels", area_highest_aqi, f"{delta_area_aqi:+.2f}" if delta_area_aqi else None)

        st.subheader("AQI Category Summary")
        st.dataframe(category_summary, use_container_width=True)

        st.subheader("PM2.5 Time Series (Smoothed)")
        df_filtered['date'] = df_filtered['timestamp'].dt.date
        df_daily = df_filtered.groupby(['date', 'nameTH'], as_index=False)['PM25.aqi'].mean()

        top_stations = df_filtered.groupby('nameTH')['PM25.aqi'].mean().nlargest(5).index
        df_top_stations = df_daily[df_daily['nameTH'].isin(top_stations)].copy()
        df_top_stations['Rolling_AQI'] = df_top_stations.groupby('nameTH')['PM25.aqi'].transform(lambda x: x.rolling(7, min_periods=1).mean())

        fig_time = px.line(
            df_top_stations,
            x='date', y='Rolling_AQI', color='nameTH',
            title='Smoothed PM2.5 AQI Over Time (Top 5 Stations)',
            labels={'Rolling_AQI': '7-Day Rolling Avg PM2.5 AQI'}
        )
        fig_time.update_layout(
            xaxis_title='Date', yaxis_title='PM2.5 AQI',
            xaxis=dict(tickangle=45), template='plotly_dark',
            plot_bgcolor='rgba(0,0,0,0)', showlegend=True
        )
        st.plotly_chart(fig_time, use_container_width=True)

        st.subheader("AQI Map")
        df_map = df_filtered.dropna(subset=['lat', 'long']).nlargest(10, 'PM25.aqi')
        fig_map = px.scatter_mapbox(
            df_map, lat='lat', lon='long', color='PM25.aqi',
            size='PM25.aqi', size_max=30, zoom=5,
            center={"lat": df_map['lat'].mean(), "lon": df_map['long'].mean()},
            color_continuous_scale='RdYlGn_r',
            hover_name='nameTH', hover_data={'PM25.aqi': ':.2f'},
            mapbox_style='open-street-map', height=500,
            title='Top 10 PM2.5 AQI Map by Station')
        st.plotly_chart(fig_map, use_container_width=True)

        st.subheader("Average AQI by Area")
        df_area = df_filtered.groupby('areaTH', as_index=False)['PM25.aqi'].mean().nlargest(10, 'PM25.aqi')
        fig_bar = px.bar(df_area, x='areaTH', y='PM25.aqi', color='PM25.aqi', title='Top 10 Average PM2.5 AQI by Area')
        st.plotly_chart(fig_bar, use_container_width=True)

        st.subheader("Station Distribution by AQI Category")
        fig_pie = px.pie(
            category_summary, names='AQI Category', values='Count',
            title='Proportion of AQI Categories', hover_data=['ExampleStations'])
        fig_pie.update_traces(hovertemplate="<b>%{label}</b><br>Stations: %{customdata[0]}<br>Count: %{value}")
        st.plotly_chart(fig_pie, use_container_width=True)
    else:
        st.warning("No data found for the selected time or station.")

if st.button("Download Data"):
    csv = df_filtered.to_csv(index=False)
    st.download_button(
        label="Download CSV",
        data=csv,
        file_name='filtered_data.csv',
        mime='text/csv',
        help="Click to download the filtered data as a CSV file."
    )
    st.success("Data downloaded successfully!")
    st.balloons()