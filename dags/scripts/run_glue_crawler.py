import boto3
import os
import time
import sys


def run_glue_crawler(crawler_name="youtube_crawler" , timeout=60):
    glue = boto3.client(
        "glue",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "ap-south-1")
    )

    print(f"Starting Glue Crawler: {crawler_name}")
    glue.start_crawler(Name=crawler_name)

    start_time = time.time()
    while True:
        try:
            response = glue.get_crawler(Name=crawler_name)
            status = response["Crawler"]["State"]
            print(f"Crawler status: {status}")

            if status == "READY":
                print("Glue Crawler completed successfully.")
                break

            # Exit if timeout exceeded
            if (time.time() - start_time) > timeout:
                print(f" Timeout: Crawler '{crawler_name}' did not complete within {timeout} seconds.")
                sys.exit(1)

            time.sleep(10)

        except glue.exceptions.EntityNotFoundException:
            print(f"Crawler '{crawler_name}' not found.")
            sys.exit(1)
        except Exception as e:
            print(f"Error checking crawler status: {e}")
            sys.exit(1)
