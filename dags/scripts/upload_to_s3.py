import boto3
import os

def upload_file_to_s3(local_file: str, s3_key: str, bucket: str = None):
    """
    Upload a file to S3.

    Args:
        local_file (str): Local path of the file to upload.
        s3_key (str): Key (path) to save in S3, e.g. 'raw/file.json' or 'staging/file.csv'.
        bucket (str, optional): Target S3 bucket name. 
                                If not provided, defaults to AWS_S3_BUCKET from env.
    """

    # Fallback to environment variable if bucket not passed
    if bucket is None:
        bucket = os.getenv("S3_BUCKET")

    if not bucket:
        raise ValueError("S3 bucket not specified and AWS_S3_BUCKET not set in environment.")

    # Load credentials from environment (Docker will inject them)
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

    if not aws_access_key_id or not aws_secret_access_key:
        raise ValueError("AWS credentials not found in environment variables")

    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_region,
    )

    try:
        s3.upload_file(local_file, bucket, s3_key)
        print(f" Uploaded {local_file} → s3://{bucket}/{s3_key}")
    except Exception as e:
        print(f" Error uploading {local_file} → s3://{bucket}/{s3_key}: {str(e)}")
        raise
