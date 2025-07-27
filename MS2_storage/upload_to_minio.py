import os
import time
import pandas as pd
import psycopg2
from minio import Minio
from io import BytesIO

DB_CONFIG = {
    "dbname": "btc_data",
    "user": "user",
    "password": "password",
    "host": "postgres",
    "port": "5432"
}

MINIO_CONFIG = {
    "endpoint": "minio:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin",
    "bucket": "btc-raw-data"
}

def connect_db():
    return psycopg2.connect(**DB_CONFIG)

def wait_for_table(conn, table_name="btc_prices", timeout=60):
    print(f"Waiting for table `{table_name}` to be ready in database...")
    with conn.cursor() as cur:
        for i in range(timeout):
            try:
                cur.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
                cur.fetchall()
                print(f"Table `{table_name}` is ready.")
                return
            except psycopg2.Error:
                conn.rollback()
                print(f"Table not found yet, retrying... ({i+1}/{timeout})")
                time.sleep(1)
        raise TimeoutError(f"Table `{table_name}` not available after {timeout} seconds.")

def wait_for_ingestion_done(conn, timeout=60):
    print("Waiting for ingestion_status = 'done' in DB...")

    with conn.cursor() as cur:
        for i in range(timeout):
            try:
                # Check if table exists
                cur.execute("SELECT to_regclass('ingestion_status');")
                if cur.fetchone()[0] is None:
                    raise psycopg2.ProgrammingError("ingestion_status table does not exist yet")

                # If table exists, check for 'done' status
                cur.execute("""
                    SELECT status FROM ingestion_status
                    ORDER BY timestamp DESC LIMIT 1
                """)
                row = cur.fetchone()
                if row and row[0] == 'done':
                    print("Ingestion status is 'done'.")
                    return
            except psycopg2.Error:
                conn.rollback()
            time.sleep(1)

    raise TimeoutError("Timed out waiting for ingestion to complete.")

def upload_csv_to_minio(minio_client, bucket_name, filename, df):
    buffer = BytesIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)
    minio_client.put_object(
        bucket_name,
        filename,
        buffer,
        length=len(buffer.getvalue()),
        content_type='application/csv'
    )
    print(f"Uploaded: {filename}")

def main():
    conn = connect_db()
    wait_for_table(conn, "btc_prices")
    wait_for_ingestion_done(conn, timeout=300)

    minio_client = Minio(
        MINIO_CONFIG["endpoint"],
        access_key=MINIO_CONFIG["access_key"],
        secret_key=MINIO_CONFIG["secret_key"],
        secure=False
    )

    if not minio_client.bucket_exists(MINIO_CONFIG["bucket"]):
        minio_client.make_bucket(MINIO_CONFIG["bucket"])

    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT DATE_TRUNC('month', timestamp) FROM btc_prices ORDER BY 1")
        months = cur.fetchall()

    for month_start, in months:
        month_str = month_start.strftime("%Y-%m")
        query = f"""
            SELECT * FROM btc_prices
            WHERE DATE_TRUNC('month', timestamp) = DATE '{month_start.date()}'
        """
        df = pd.read_sql(query, conn)
        if not df.empty:
            filename = f"BTC_{month_str}.csv"
            upload_csv_to_minio(minio_client, MINIO_CONFIG["bucket"], filename, df)

    minio_client.put_object(
        MINIO_CONFIG["bucket"],
        "upload_complete.txt",
        BytesIO(b"done"),
        length=len(b"done"),
        content_type="text/plain"
    )
    print("Upload complete signal file created: upload_complete.txt")

if __name__ == "__main__":
    main()
