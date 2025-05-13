import os
import pandas as pd
import streamlit as st
import pyarrow.parquet as pq
import s3fs

# Set up a connection to LakeFS
lakefs_endpoint = os.getenv("LAKEFS_ENDPOINT", "http://lakefs-dev:8000")
ACCESS_KEY = os.getenv("LAKEFS_ACCESS_KEY")
SECRET_KEY = os.getenv("LAKEFS_SECRET_KEY")

# Set up S3FileSystem to access LakeFS
fs = s3fs.S3FileSystem(
    key=ACCESS_KEY,
    secret=SECRET_KEY,
    client_kwargs={'endpoint_url': lakefs_endpoint}
)

# Parquet file location in LakeFS
lakefs_s3_path = 's3a://air-quality/main/airquality.parquet/year=2025/month=5/day=4/hour=2/7400decd6afd4d1caa00b19993ed2e7a-0.parquet'

@st.cache_data()
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
    # Fill value "Previous Record" Group By stationID
    df_all['PM25.aqi'] = df_all.groupby('stationID')['PM25.aqi'].transform(lambda x: x.fillna(method='ffill'))
    return df_all

def filter_data(df, start_date, end_date, station):
    df_filtered = df.copy()

    # Filter by date
    df_filtered = df_filtered[
        (df_filtered['timestamp'].dt.date >= start_date) &
        (df_filtered['timestamp'].dt.date <= end_date)
    ]

    # Filter by station
    if station != "ทั้งหมด":
        df_filtered = df_filtered[df_filtered['nameTH'] == station]

    # Remove invalid AQI
    df_filtered = df_filtered[df_filtered['PM25.aqi'] >= 0]

    return df_filtered

st.title("Air Quality Dashboard from LakeFS")
df = load_data()

# Sidebar settings
with st.sidebar:
    st.title("Air4Thai Dashboard")
    st.header("⚙️ Settings")

    max_date = df['timestamp'].max().date()
    min_date = df['timestamp'].min().date()
    default_start_date = min_date
    default_end_date = max_date

    start_date = st.date_input(
        "Start date",
        default_start_date,
        min_value=min_date,
        max_value=max_date
    )

    end_date = st.date_input(
        "End date",
        default_end_date,
        min_value=min_date,
        max_value=max_date
    )

    station_name = df['nameTH'].dropna().unique().tolist()
    station_name.sort()
    station_name.insert(0, "ทั้งหมด")
    station = st.selectbox("Select Station", station_name)

df_filtered = filter_data(df, start_date, end_date, station)

# Container for KPI and main content
placeholder = st.empty()

with placeholder.container():

    if not df_filtered.empty:
        # AVG for Selection Interval
        avg_aqi = df_filtered['PM25.aqi'].mean()
        avg_color = df_filtered['PM25.color_id'].mean()

        # Previous Day
        prev_day = end_date - pd.Timedelta(days=1)
        df_prev_day = filter_data(df, prev_day, prev_day, station)

        # AVG of Previous Day
        prev_avg_aqi = df_prev_day['PM25.aqi'].mean()
        prev_avg_color = df_prev_day['PM25.color_id'].mean()

        # Delta
        delta_aqi = None if pd.isna(prev_avg_aqi) else avg_aqi - prev_avg_aqi
        delta_color = None if pd.isna(prev_avg_color) else avg_color - prev_avg_color

        # Area that have the Most AQI
        area_highest_aqi = df_filtered.groupby('areaTH')['PM25.aqi'].mean().idxmax()
        area_highest_aqi_val = df_filtered.groupby('areaTH')['PM25.aqi'].mean().max()

        # Area Most AQI of Previous
        if not df_prev_day.empty:
            # area_prev_highest_aqi = df_prev_day.groupby('areaTH')['PM25.aqi'].mean().idxmax()
            area_prev_highest_aqi_val = df_prev_day.groupby('areaTH')['PM25.aqi'].mean().max()
            delta_area_aqi = area_highest_aqi_val - area_prev_highest_aqi_val
        else:
            delta_area_aqi = None

        # Scorecards
        kpi1, kpi2, kpi3 = st.columns(3)
        kpi1.metric(
            label="☁️ Average quality of PM2.5",
            value=f"{avg_aqi:.2f}",
            delta=f"{delta_aqi:+.2f}" if delta_aqi is not None else None
        )
        kpi2.metric(
            label="🇹🇭 Average PM2.5 levels in Thailand",
            value=f"{avg_color:.2f}",
            delta=f"{delta_color:+.2f}" if delta_color is not None else None
        )
        kpi3.metric(
            label="📍 Areas with the highest PM2.5 levels",
            value=area_highest_aqi,3
            delta=f"{delta_area_aqi:+.2f}" if delta_area_aqi is not None else None
        )
    else:
        st.warning("No data found for the selected time or station.")