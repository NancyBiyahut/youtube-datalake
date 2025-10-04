import pandas as pd
import json
import os
from scripts.upload_to_s3 import upload_file_to_s3

def transform_data():
    # Paths
    input_path = "/opt/airflow/dags/scripts/include/watch-history.json"
    staging_path = "/opt/airflow/dags/scripts/data/staging_watch_history.csv"
    curated_parquet_path = "/opt/airflow/dags/scripts/data/curated_watch_history.parquet"

    if not os.path.exists(input_path):
        raise FileNotFoundError("watch-history.json not found!")

    # Load JSON
    with open(input_path, 'r') as file:
        data = json.load(file)

    # Normalize / extract relevant fields
    records = []
    for item in data:
        if "titleUrl" in item:
            records.append({
                "title": item.get("title"),
                "titleUrl": item.get("titleUrl"),
                "time": item.get("time"),
                "channel_name": item.get("subtitles", [{}])[0].get("name"),
                "channel_url": item.get("subtitles", [{}])[0].get("url")
            })

    df = pd.DataFrame(records)

    # Convert time fields
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df["year"] = df["time"].dt.year
    df["month"] = df["time"].dt.month
    df["day"] = df["time"].dt.day
    df["weekday"] = df["time"].dt.day_name()
    df["hour"] = df["time"].dt.hour

    # --- Save Staging Layer ---
    df.to_csv(staging_path, index=False)
    print(f"Staging CSV saved: {staging_path}")
    upload_file_to_s3(staging_path, "staging/processed_watch_history.csv")

    # --- Save Curated Layer (Partitioned Parquet) ---
    df.to_parquet(curated_parquet_path, index=False, partition_cols=["year","month"])
    print(f"Curated Parquet saved: {curated_parquet_path}")
    upload_file_to_s3(curated_parquet_path, "curated/watch_history.parquet")

    # Raw Layer: copy original JSON to S3/raw
    upload_file_to_s3(input_path, "raw/watch-history.json")
    print("Raw JSON uploaded to S3/raw")
