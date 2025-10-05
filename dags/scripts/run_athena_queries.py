import boto3
import os
import time

def run_athena_queries():
    athena = boto3.client(
        "athena",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "ap-south-1")
    )

    output_location = "s3://nancy-youtube/athena-results/"
    database = "youtube_db"

    queries = [
        ("Top Channels",
         "SELECT channel_name, COUNT(*) AS videos_watched FROM youtube_curated "
         "GROUP BY channel_name ORDER BY videos_watched DESC LIMIT 5;"),

        ("Monthly Activity",
         "SELECT year, month, COUNT(*) AS videos FROM youtube_curated "
         "GROUP BY year, month ORDER BY year DESC, month DESC;"),

        ("Most Active Days",
         "SELECT day_of_week, COUNT(*) AS views FROM youtube_curated "
         "GROUP BY day_of_week ORDER BY views DESC;")
    ]

    for name, query in queries:
        print(f"▶️ Running Athena Query: {name}")
        response = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": output_location}
        )

        query_execution_id = response["QueryExecutionId"]

        # Wait for completion
        while True:
            result = athena.get_query_execution(QueryExecutionId=query_execution_id)
            status = result["QueryExecution"]["Status"]["State"]

            if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                print(f"✅ Query '{name}' finished with status: {status}")
                break
            time.sleep(5)
