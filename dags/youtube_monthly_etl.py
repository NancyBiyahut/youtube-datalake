from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

from scripts.check_file import check_file_exists
from scripts.transform_watch import transform_data
#from scripts.youtube_api_enrich import enrich_youtube_data
from scripts.upload_to_s3 import upload_file_to_s3
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

with DAG(
    dag_id="youtube_monthly_etl",
    description="YouTube Monthly ETL Pipeline with S3 and Athena",
    schedule_interval="@monthly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["youtube","etl"]
) as dag:

    # 1) Check input file exists
    check_input = PythonOperator(
        task_id="check_input_file",
        python_callable=check_file_exists
    )

    # # 2) Upload raw watch-history.json to S3/raw
    # upload_raw = PythonOperator(
    #     task_id="upload_raw_to_s3",
    #     python_callable=lambda: upload_file_to_s3(
    #         local_path="/opt/airflow/dags/scripts/include/watch-history.json",
    #         s3_key="raw/watch-history.json"
    #     )
    # )

    # 3) Transform and enrich watch history
    transform = PythonOperator(
        task_id="transform_watch_history",
        python_callable=transform_data
    )

    # 4) Enrich top videos/channels via YouTube API
    # enrich = PythonOperator(
    #     task_id="enrich_youtube_data",
    #     python_callable=enrich_youtube_data
    # )

    # 5) Optional: Athena query to validate or summarize data
    # athena_query = AthenaOperator(
    #     task_id="athena_top_channels",
    #     query="""
    #         SELECT channel_name, COUNT(*) as videos_watched
    #         FROM curated_videos
    #         GROUP BY channel_name
    #         ORDER BY videos_watched DESC
    #         LIMIT 10;
    #     """,
    #     database="youtube_curated_db",
    #     output_location="s3://your-bucket/query-results/",
    #     aws_conn_id="aws_default"
    # )

    # Define task order
    check_input >> transform
