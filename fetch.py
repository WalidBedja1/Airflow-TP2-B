import logging
import time
from typing import Any

import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://api.open-meteo.com/v1/forecast"

REQUESTED_FIELDS = {
    "hourly": [
        "temperature_2m",
        "relative_humidity_2m",
        "precipitation",
        "wind_speed_10m",
        "wind_direction_10m",
        "weather_code",
    ]
}

REQUEST_TIMEOUT = 10
RETRY_DELAY = 2


def build_request_params(city: dict) -> dict:
    return {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "hourly": ",".join(REQUESTED_FIELDS["hourly"]),
        "forecast_days": 1,
        "timezone": "Europe/Paris",
    }


def fetch_weather_for_city(city: dict) -> dict[str, Any]:
    params = build_request_params(city)
    logger.info("Fetching weather for city: %s", city["name"])

    response = requests.get(BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    return {
        "city_name": city["name"],
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "raw_response": response.json(),
    }


def fetch_weather_for_all_cities(cities: list[dict]) -> list[dict[str, Any]]:
    results = []

    for city in cities:
        try:
            result = fetch_weather_for_city(city)
            results.append(result)
        except requests.HTTPError as exc:
            logger.error("HTTP error for city %s: %s", city["name"], exc)
        except requests.ConnectionError as exc:
            logger.error("Connection error for city %s: %s", city["name"], exc)
        except requests.Timeout:
            logger.error("Timeout for city %s", city["name"])

        time.sleep(0.2)

    logger.info("Fetched data for %d/%d cities", len(results), len(cities))
    return results
