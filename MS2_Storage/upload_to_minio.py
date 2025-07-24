import os
import boto3
from botocore.exceptions import NoCredentialsError

# Configuration
MINIO_ENDPOINT = "minio:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
BUCKET_NAME = "btc-raw-data"
DATA_DIR = "/app/data/monthly_raw_data"

def upload_file_to_minio(s3_client, bucket_name, file_path, object_name):
    try:
        s3_client.upload_file(file_path, bucket_name, object_name)
        print(f"Uploaded: {object_name}")
    except Exception as e:
        print(f"Failed to upload {object_name}: {e}")

def main():
    print("Connecting to MinIO...")

    s3 = boto3.client(
        's3',
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )

    try:
        s3.head_bucket(Bucket=BUCKET_NAME)
    except:
        s3.create_bucket(Bucket=BUCKET_NAME)
        print(f"Created bucket: {BUCKET_NAME}")

    # Upload CSV files
    for filename in sorted(os.listdir(DATA_DIR)):
        if filename.endswith(".csv"):
            local_path = os.path.join(DATA_DIR, filename)
            upload_file_to_minio(s3, BUCKET_NAME, local_path, filename)

if __name__ == "__main__":
    main()