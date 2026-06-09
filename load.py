import logging
from typing import Any

logger = logging.getLogger(__name__)

INSERT_WEATHER_SQL = """
INSERT INTO weather_hourly (
    city,
    timestamp,
    temperature_celsius,
    humidity_pct,
    precipitation_mm,
    wind_speed_kmh,
    wind_direction_deg,
    wmo_weather_code
)
VALUES (
    %(city)s,
    %(timestamp)s,
    %(temperature_celsius)s,
    %(humidity_pct)s,
    %(precipitation_mm)s,
    %(wind_speed_kmh)s,
    %(wind_direction_deg)s,
    %(wmo_weather_code)s
)
ON CONFLICT (city, timestamp)
DO UPDATE SET
    temperature_celsius  = EXCLUDED.temperature_celsius,
    humidity_pct         = EXCLUDED.humidity_pct,
    precipitation_mm     = EXCLUDED.precipitation_mm,
    wind_speed_kmh       = EXCLUDED.wind_speed_kmh,
    wind_direction_deg   = EXCLUDED.wind_direction_deg,
    wmo_weather_code     = EXCLUDED.wmo_weather_code,
    loaded_at            = NOW();
"""

INSERT_LOG_SQL = """
INSERT INTO ingestion_log (
    dag_id,
    run_id,
    execution_date,
    source,
    cities_fetched,
    records_loaded,
    status,
    error_message
)
VALUES (
    %(dag_id)s,
    %(run_id)s,
    %(execution_date)s,
    %(source)s,
    %(cities_fetched)s,
    %(records_loaded)s,
    %(status)s,
    %(error_message)s
);
"""


def load_records_to_postgres(
    records: list[dict[str, Any]],
    postgres_hook,
) -> int:
    if not records:
        logger.warning("No records to load.")
        return 0

    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.executemany(INSERT_WEATHER_SQL, records)
        conn.commit()
        loaded = cursor.rowcount
        logger.info("Upserted %d rows into weather_hourly.", loaded)
        return loaded
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def write_ingestion_log(
    postgres_hook,
    dag_id: str,
    run_id: str,
    execution_date,
    cities_fetched: int,
    records_loaded: int,
    status: str,
    error_message: str | None = None,
) -> None:
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute(
            INSERT_LOG_SQL,
            {
                "dag_id": dag_id,
                "run_id": run_id,
                "execution_date": execution_date,
                "source": "open-meteo",
                "cities_fetched": cities_fetched,
                "records_loaded": records_loaded,
                "status": status,
                "error_message": error_message,
            },
        )
        conn.commit()
        logger.info("Ingestion log written: status=%s, records=%d", status, records_loaded)
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()
