import boto3
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import io
import os
import time

# MinIO config
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET_NAME = os.getenv("MINIO_BUCKET", "btc-raw-data")

# Postgres config
PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")
PG_DB = os.getenv("POSTGRES_DB", "btc_data")

# Connect to MinIO
s3 = boto3.client(
    "s3",
    endpoint_url=f"http://{MINIO_ENDPOINT}",
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
)

def wait_for_upload_complete(bucket, key="upload_complete.txt", timeout=300, interval=2):
    print(f"Waiting for {key} to appear in bucket...")
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        try:
            s3.head_object(Bucket=bucket, Key=key)
            print("Upload complete file detected. Starting processing immediately...")
            return
        except s3.exceptions.ClientError:
            print("Still waiting for upload_complete.txt...")
            time.sleep(interval)

    raise TimeoutError(f"Timed out waiting for {key}")

def preprocess(df):
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df.dropna(inplace=True)
    return df

def main():
    # Wait for signal from storage microservice
    wait_for_upload_complete(BUCKET_NAME)

    # Fetch file list
    response = s3.list_objects_v2(Bucket=BUCKET_NAME)
    file_keys = [
        f["Key"] for f in response.get("Contents", [])
        if f["Key"] != "upload_complete.txt"
    ]

    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD, dbname=PG_DB
    )
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS btc_preprocessed (
            timestamp TIMESTAMP,
            open NUMERIC,
            high NUMERIC,
            low NUMERIC,
            close NUMERIC,
            volume NUMERIC
        )
    """)
    conn.commit()

    # Process and insert data
    for file_key in file_keys:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=file_key)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()))
        df = preprocess(df)

        if not df.empty:
            rows = df[["timestamp", "open", "high", "low", "close", "volume"]].values.tolist()
            execute_values(cursor, """
                INSERT INTO btc_preprocessed (timestamp, open, high, low, close, volume)
                VALUES %s
            """, rows)
            conn.commit()
            print(f"Processed: {file_key}")
        else:
            print(f"Skipped empty or invalid file: {file_key}")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()