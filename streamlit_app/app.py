import os
import boto3
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Sports Demo – Gold Layer", layout="wide")

AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
S3_BUCKET = os.getenv("S3_BUCKET", "sports-demo-dev-sport-data")
GOLD_LATEST_PREFIX = os.getenv("GOLD_LATEST_PREFIX", "gold/latest")

s3 = boto3.client("s3", region_name=AWS_REGION)

st.title("🏟️ Sports Analytics Demo – Gold Outputs")

@st.cache_data(ttl=60)
def list_latest_files():
    resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=GOLD_LATEST_PREFIX + "/")
    contents = resp.get("Contents", [])
    # only csv
    keys = [c["Key"] for c in contents if c["Key"].endswith(".csv")]
    return sorted(keys)

@st.cache_data(ttl=60)
def load_csv(key):
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    return pd.read_csv(obj["Body"])

keys = list_latest_files()

if not keys:
    st.warning("No files found in gold/latest yet. Trigger the pipeline and refresh.")
    st.stop()

selected = st.selectbox("Select a latest output file", keys)

df = load_csv(selected)

col1, col2, col3 = st.columns(3)
col1.metric("Rows", df.shape[0])
col2.metric("Columns", df.shape[1])
col3.write(f"**S3 key:** `{selected}`")

st.dataframe(df, use_container_width=True)
