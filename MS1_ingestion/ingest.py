import os
import pandas as pd
import psycopg2
from io import StringIO

DATA_DIR = "/app/data/monthly_raw_data"

DB_CONFIG = {
    "dbname": "btc_data",
    "user": "user",
    "password": "password",
    "host": "postgres",
    "port": "5432"
}

def connect_db():
    return psycopg2.connect(**DB_CONFIG)

def create_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS btc_prices (
                timestamp TIMESTAMP,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume FLOAT
            )
        """)
        conn.commit()

def copy_from_stringio(conn, df):
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    with conn.cursor() as cur:
        cur.copy_expert("""
            COPY btc_prices (timestamp, open, high, low, close, volume)
            FROM STDIN WITH CSV
        """, buffer)
        conn.commit()

def ingest_files(conn):
    print(f"Checking files inside {DATA_DIR}")
    print(os.listdir(DATA_DIR))

    for file in sorted(os.listdir(DATA_DIR)):
        if file.endswith(".csv"):
            print(f"Ingesting {file}")
            try:
                df = pd.read_csv(os.path.join(DATA_DIR, file))
            except Exception as e:
                print(f"Failed to read {file}: {e}")
                continue

            df.columns = [c.lower() for c in df.columns]
            expected_cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            if not all(col in df.columns for col in expected_cols):
                missing = [col for col in expected_cols if col not in df.columns]
                print(f"Missing columns in {file}: {missing}")
                continue

            df = df[expected_cols].dropna()
            try:
                copy_from_stringio(conn, df)
            except Exception as e:
                print(f"COPY failed for {file}: {e}")
                conn.rollback()

if __name__ == "__main__":
    print("ingest.py is starting...")
    conn = connect_db()
    create_table(conn)
    ingest_files(conn)
    conn.close()
