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

@st.cache_data
def load_data():
    # Load Parquet files from LakeFS
    parquet_file = pq.ParquetFile(fs.open(lakefs_s3_path))
    df = parquet_file.read().to_pandas()
    return df

st.title("Air Quality Dashboard from LakeFS")
df = load_data()
st.write(df.head())