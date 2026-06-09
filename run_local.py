"""
Standalone end-to-end test script.
Runs fetch -> transform -> load -> log against a local PostgreSQL instance.
Use this to validate the pipeline without a running Airflow instance.

Requirements:
  - PostgreSQL running on localhost:5432 (see docker-compose.yml)
  - Tables created via sql/init.sql
  - pip install requests psycopg2-binary

Usage:
  python run_local.py
  python run_local.py --host localhost --port 5432 --db weather --user airflow --password airflow
"""

import argparse
import json
import sys
import os
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras

sys.path.insert(0, os.path.dirname(__file__))

from utils.fetch import fetch_weather_for_all_cities
from utils.transform import transform_weather_responses

CITIES = [
    {"name": "Paris",    "latitude": 48.8566, "longitude": 2.3522},
    {"name": "Lyon",     "latitude": 45.7640, "longitude": 4.8357},
    {"name": "Bordeaux", "latitude": 44.8378, "longitude": -0.5792},
    {"name": "Lille",    "latitude": 50.6292, "longitude": 3.0573},
]

INSERT_WEATHER_SQL = """
INSERT INTO weather_hourly (
    city, timestamp, temperature_celsius, humidity_pct,
    precipitation_mm, wind_speed_kmh, wind_direction_deg, wmo_weather_code
)
VALUES (
    %(city)s, %(timestamp)s, %(temperature_celsius)s, %(humidity_pct)s,
    %(precipitation_mm)s, %(wind_speed_kmh)s, %(wind_direction_deg)s, %(wmo_weather_code)s
)
ON CONFLICT (city, timestamp) DO UPDATE SET
    temperature_celsius = EXCLUDED.temperature_celsius,
    humidity_pct        = EXCLUDED.humidity_pct,
    precipitation_mm    = EXCLUDED.precipitation_mm,
    wind_speed_kmh      = EXCLUDED.wind_speed_kmh,
    wind_direction_deg  = EXCLUDED.wind_direction_deg,
    wmo_weather_code    = EXCLUDED.wmo_weather_code,
    loaded_at           = NOW();
"""

INSERT_LOG_SQL = """
INSERT INTO ingestion_log (
    dag_id, run_id, execution_date, source,
    cities_fetched, records_loaded, status, error_message
)
VALUES (
    %(dag_id)s, %(run_id)s, %(execution_date)s, %(source)s,
    %(cities_fetched)s, %(records_loaded)s, %(status)s, %(error_message)s
);
"""


def get_connection(args) -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=args.host,
        port=args.port,
        dbname=args.db,
        user=args.user,
        password=args.password,
    )


def run(args):
    execution_date = datetime.now(tz=timezone.utc)
    run_id = f"local__{execution_date.strftime('%Y%m%dT%H%M%S')}"
    status = "success"
    error_message = None
    records_loaded = 0

    print(f"[run_local] Starting run: {run_id}")

    try:
        print("[1/3] Fetching weather data...")
        raw_data = fetch_weather_for_all_cities(CITIES)
        print(f"      Cities fetched: {len(raw_data)}")

        print("[2/3] Transforming records...")
        records = transform_weather_responses(raw_data)
        print(f"      Records produced: {len(records)}")

        print("[3/3] Loading to PostgreSQL...")
        conn = get_connection(args)
        cursor = conn.cursor()

        psycopg2.extras.execute_batch(cursor, INSERT_WEATHER_SQL, records, page_size=200)
        records_loaded = len(records)
        conn.commit()
        print(f"      Rows upserted: {records_loaded}")

    except Exception as exc:
        status = "failure"
        error_message = str(exc)
        print(f"[ERROR] {exc}", file=sys.stderr)

    finally:
        try:
            cursor.execute(
                INSERT_LOG_SQL,
                {
                    "dag_id": "weather_pipeline",
                    "run_id": run_id,
                    "execution_date": execution_date,
                    "source": "open-meteo",
                    "cities_fetched": len(raw_data) if "raw_data" in dir() else 0,
                    "records_loaded": records_loaded,
                    "status": status,
                    "error_message": error_message,
                },
            )
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as log_exc:
            print(f"[WARNING] Could not write ingestion log: {log_exc}", file=sys.stderr)

    print(f"\n[run_local] Done. Status: {status}")

    print("\n--- Proof of load (last 5 rows per city) ---")
    conn2 = get_connection(args)
    cursor2 = conn2.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cursor2.execute("""
        SELECT city, timestamp, temperature_celsius, wmo_weather_code, loaded_at
        FROM weather_hourly
        ORDER BY city, timestamp DESC
        LIMIT 20;
    """)
    rows = cursor2.fetchall()
    print(json.dumps([dict(r) for r in rows], default=str, indent=2))

    cursor2.execute("SELECT * FROM ingestion_log ORDER BY logged_at DESC LIMIT 3;")
    logs = cursor2.fetchall()
    print("\n--- Ingestion log (last 3 runs) ---")
    print(json.dumps([dict(r) for r in logs], default=str, indent=2))

    cursor2.close()
    conn2.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the weather pipeline locally.")
    parser.add_argument("--host",     default="localhost")
    parser.add_argument("--port",     type=int, default=5432)
    parser.add_argument("--db",       default="weather")
    parser.add_argument("--user",     default="airflow")
    parser.add_argument("--password", default="airflow")
    run(parser.parse_args())
