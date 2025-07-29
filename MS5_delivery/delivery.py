from flask import Flask, request, jsonify
import psycopg2
import pandas as pd
import os
import time

app = Flask(__name__)

# PostgreSQL config from environment variables
DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "btc_data")
DB_USER = os.getenv("POSTGRES_USER", "user")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")


def wait_for_aggregation_done():
    print("Waiting for aggregation_status = 'done' in DB...")
    i = 0
    while True:
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            cur = conn.cursor()
            cur.execute("SELECT to_regclass('aggregation_status');")
            if cur.fetchone()[0] is not None:
                cur.execute("""
                    SELECT status FROM aggregation_status
                    ORDER BY timestamp DESC LIMIT 1
                """)
                row = cur.fetchone()
                if row and row[0] == 'done':
                    print("Aggregation is complete.")
                    conn.close()
                    return
            conn.close()
        except Exception as e:
            print(f"DB not ready yet: {e}")

        i += 1
        print(f"Waiting for aggregation... attempt {i}")
        time.sleep(5)


def query_table(table, start=None, end=None):
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )

    query = f"SELECT * FROM {table}"
    params = []

    if start and end:
        query += " WHERE timestamp BETWEEN %s AND %s ORDER BY timestamp"
        params = [start, end]
    else:
        query += " ORDER BY timestamp"

    df = pd.read_sql(query, conn, params=params)
    conn.close()
    return df.to_dict(orient="records")


@app.route("/weekly", methods=["GET"])
def get_weekly():
    start = request.args.get("start")
    end = request.args.get("end")
    data = query_table("btc_weekly", start, end)
    return jsonify(data)


@app.route("/quarterly", methods=["GET"])
def get_quarterly():
    start = request.args.get("start")
    end = request.args.get("end")
    data = query_table("btc_quarterly", start, end)
    return jsonify(data)


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    wait_for_aggregation_done()
    app.run(host="0.0.0.0", port=5000)