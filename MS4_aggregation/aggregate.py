import os
import time
import psycopg2
import pandas as pd

# PostgreSQL config
DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "btc_data")
DB_USER = os.getenv("POSTGRES_USER", "user")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")


def wait_for_table(conn, table_name="btc_preprocessed"):
    print(f"Waiting indefinitely for table `{table_name}` to be ready in database...")
    with conn.cursor() as cur:
        i = 0
        while True:
            try:
                cur.execute("SELECT to_regclass(%s);", (table_name,))
                exists = cur.fetchone()[0]
                if exists:
                    print(f"Table `{table_name}` is ready.")
                    return
                else:
                    print(f"Table not found yet, retrying... ({i+1})")
            except psycopg2.Error as e:
                conn.rollback()
                print(f"DB error while checking for table: {e} (retrying... {i+1})")
            time.sleep(1)
            i += 1


def wait_for_preprocessing_done(conn):
    print("Waiting indefinitely for preprocessing_status = 'done' in DB...")
    with conn.cursor() as cur:
        i = 0
        while True:
            try:
                cur.execute("SELECT to_regclass('preprocessing_status');")
                if cur.fetchone()[0] is None:
                    raise psycopg2.ProgrammingError("preprocessing_status table does not exist yet")

                cur.execute("""
                    SELECT status FROM preprocessing_status
                    ORDER BY timestamp DESC LIMIT 1
                """)
                row = cur.fetchone()
                if row and row[0] == 'done':
                    print("Preprocessing status is 'done'.")
                    return
            except psycopg2.Error:
                conn.rollback()
            print(f"Still waiting... ({i+1})")
            time.sleep(1)
            i += 1


def fetch_aggregated_data(conn, freq):
    if freq == "W":
        trunc = "week"
    elif freq == "Q":
        trunc = "quarter"
    else:
        raise ValueError("Unsupported frequency")

    query = f"""
        WITH base AS (
            SELECT 
                *,
                DATE_TRUNC('{trunc}', timestamp) AS period
            FROM btc_preprocessed
        ),
        first_last AS (
            SELECT DISTINCT ON (period) 
                period AS timestamp,
                open,
                close
            FROM base
            ORDER BY period, timestamp
        ),
        aggregated AS (
            SELECT
                DATE_TRUNC('{trunc}', timestamp) AS period,
                MAX(high) AS high,
                MIN(low) AS low,
                SUM(volume) AS volume,
                MAX(high) - MIN(low) AS range
            FROM btc_preprocessed
            GROUP BY DATE_TRUNC('{trunc}', timestamp)
        )
        SELECT 
            f.timestamp,
            f.open,
            a.high,
            a.low,
            f.close,
            a.volume,
            a.range
        FROM first_last f
        JOIN aggregated a ON f.timestamp = a.period
        ORDER BY f.timestamp
    """
    return pd.read_sql(query, conn)


def write_to_db(df, table_name):
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )
    cur = conn.cursor()

    cur.execute(f"""
        DROP TABLE IF EXISTS {table_name};
    """)
    conn.commit()

    cur.execute(f"""
        CREATE TABLE {table_name} (
            timestamp TIMESTAMP PRIMARY KEY,
            open NUMERIC,
            high NUMERIC,
            low NUMERIC,
            close NUMERIC,
            volume NUMERIC,
            range NUMERIC
        )
    """)
    conn.commit()

    for _, row in df.iterrows():
        cur.execute(f"""
            INSERT INTO {table_name} (timestamp, open, high, low, close, volume, range)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, tuple(row))
    conn.commit()
    conn.close()


def mark_aggregation_done():
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS aggregation_status (
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status TEXT
        )
    """)
    conn.commit()

    cur.execute("INSERT INTO aggregation_status (status) VALUES ('done')")
    conn.commit()
    conn.close()


def main():
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )

    wait_for_preprocessing_done(conn)
    wait_for_table(conn, "btc_preprocessed")

    weekly = fetch_aggregated_data(conn, "W")
    quarterly = fetch_aggregated_data(conn, "Q")
    conn.close()

    write_to_db(weekly, "btc_weekly")
    write_to_db(quarterly, "btc_quarterly")

    print("Aggregation complete.")
    mark_aggregation_done()


if __name__ == "__main__":
    main()