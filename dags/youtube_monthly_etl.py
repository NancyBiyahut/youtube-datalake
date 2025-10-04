from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

from scripts.transform_watch import transform_data
from scripts.upload_to_s3 import upload_file_to_s3


# S3 and local paths
BUCKET = "nancy-youtube"

RAW_PATH = "/opt/airflow/dags/scripts/include/watch-history.json"
STAGING_PATH = "/opt/airflow/dags/scripts/data/processed_watch_history.csv"
CURATED_PATH = "/opt/airflow/dags/scripts/data/watch_history.parquet"

RAW_KEY = "raw/watch-history.json"
STAGING_KEY = "staging/processed_watch_history.csv"
CURATED_KEY = "curated/watch_history.parquet"


# 🧩 Step 1 — Check if file exists locally
def check_file_exists(file_path: str):
    """Check if the YouTube watch history file exists before transformation."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(
            f" Missing input file: {file_path}\n"
            "Please download your YouTube watch-history.json and place it in /include/"
        )
    print(f"✅ Found file: {file_path}")


#DAG definition
with DAG(
    dag_id="youtube_monthly_etl",
    description="Modular YouTube ETL Pipeline: check, transform, and upload to S3",
    schedule_interval="@monthly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["youtube", "etl", "s3"]
) as dag:

    #  Check if raw JSON exists
    check_file = PythonOperator(
        task_id="check_watch_history_file",
        python_callable=check_file_exists,
        op_args=[RAW_PATH],
    )

    #  Transform and save locally
    transform = PythonOperator(
        task_id="transform_watch_history",
        python_callable=transform_data,
    )

    #  Upload raw layer
    upload_raw = PythonOperator(
        task_id="upload_raw_to_s3",
        python_callable=upload_file_to_s3,
        op_args=[RAW_PATH, RAW_KEY, BUCKET],
    )

    #  Upload staging layer
    upload_staging = PythonOperator(
        task_id="upload_staging_to_s3",
        python_callable=upload_file_to_s3,
        op_args=[STAGING_PATH, STAGING_KEY, BUCKET],
    )

    #  Upload curated layer
    upload_curated = PythonOperator(
        task_id="upload_curated_to_s3",
        python_callable=upload_file_to_s3,
        op_args=[CURATED_PATH, CURATED_KEY, BUCKET],
    )

    # DAG Flow
    check_file >> transform >> [upload_raw, upload_staging, upload_curated]
