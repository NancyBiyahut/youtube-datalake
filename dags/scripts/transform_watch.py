import pandas as pd
import json
import os

def transform_data():
    """
    Transforms the YouTube watch history JSON file into structured data.
    Saves processed files locally for later upload by separate DAG tasks.
    """

    input_path = "/opt/airflow/dags/scripts/include/watch-history.json"
    staging_path = "/opt/airflow/dags/scripts/data/processed_watch_history.csv"
    curated_parquet_path = "/opt/airflow/dags/scripts/data/watch_history.parquet"

    if not os.path.exists(input_path):
        raise FileNotFoundError("watch-history.json not found!")

    with open(input_path, 'r') as file:
        data = json.load(file)

    # Normalize YouTube JSON
    records = []
    for item in data:
        if "titleUrl" in item:
            record = {
                "title": item.get("title"),
                "titleUrl": item.get("titleUrl"),
                "time": item.get("time"),
                "channel_name": item.get("subtitles", [{}])[0].get("name"),
                "channel_url": item.get("subtitles", [{}])[0].get("url"),
            }
            records.append(record)

    df = pd.DataFrame(records)

    # Add derived columns
    df["time"] = pd.to_datetime(df["time"], errors='coerce')
    df["year"] = df["time"].dt.year
    df["month"] = df["time"].dt.month
    df["day"] = df["time"].dt.day
    df["weekday"] = df["time"].dt.day_name()
    df["hour"] = df["time"].dt.hour

    
    # --- Save local staging CSV ---
    # df.to_csv(staging_path, index=False)
    print(f"✅ Staging CSV saved → {staging_path}")

    # --- Save local curated Parquet ---
    df.to_parquet(curated_parquet_path, index=False)
    print(f"✅ Curated Parquet saved → {curated_parquet_path}")

    print("🎯 Transformation complete. Files ready for upload.")
