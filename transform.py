import logging
from typing import Any

logger = logging.getLogger(__name__)

FIELD_RENAME_MAP = {
    "temperature_2m": "temperature_celsius",
    "relative_humidity_2m": "humidity_pct",
    "precipitation": "precipitation_mm",
    "wind_speed_10m": "wind_speed_kmh",
    "wind_direction_10m": "wind_direction_deg",
    "weather_code": "wmo_weather_code",
}


def extract_hourly_records(city_name: str, raw_response: dict) -> list[dict[str, Any]]:
    hourly = raw_response.get("hourly", {})
    timestamps = hourly.get("time", [])

    if not timestamps:
        logger.warning("No hourly data found for city: %s", city_name)
        return []

    records = []

    for index, timestamp in enumerate(timestamps):
        record = {
            "city": city_name,
            "timestamp": timestamp,
        }

        for original_field, target_field in FIELD_RENAME_MAP.items():
            values = hourly.get(original_field, [])
            record[target_field] = values[index] if index < len(values) else None

        records.append(record)

    return records


def transform_weather_responses(city_payloads: list[dict]) -> list[dict[str, Any]]:
    all_records = []

    for payload in city_payloads:
        city_name = payload.get("city_name", "unknown")
        raw_response = payload.get("raw_response", {})

        records = extract_hourly_records(city_name, raw_response)
        all_records.extend(records)

        logger.info("Transformed %d records for city: %s", len(records), city_name)

    logger.info("Total records after transformation: %d", len(all_records))
    return all_records
