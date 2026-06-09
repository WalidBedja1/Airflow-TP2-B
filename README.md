# Weather Pipeline

Full Airflow pipeline: Open-Meteo API -> transformation -> PostgreSQL load + ingestion log.

Extends the `weather_ingestion` project (TP 2A) with a load layer and observability.

---

## Project structure

```
weather_pipeline/
  dags/
    weather_pipeline_dag.py    # Airflow DAG: 4 tasks chained
  utils/
    fetch.py                   # HTTP calls to Open-Meteo
    transform.py               # Flatten hourly arrays, rename fields
    load.py                    # Upsert to weather_hourly, write ingestion_log
  sql/
    init.sql                   # DDL: weather_hourly + ingestion_log tables
  output/
    weather_preview.json       # Sample records (reference)
  run_local.py                 # End-to-end test without Airflow
  docker-compose.yml           # Local PostgreSQL instance
  requirements.txt
  README.md
```

---

## DAG overview

```
fetch_weather >> transform_weather >> load_weather >> log_ingestion
```

| Task | Module | Role |
|---|---|---|
| `fetch_weather` | `utils/fetch.py` | Calls Open-Meteo for each city, pushes raw JSON via XCom |
| `transform_weather` | `utils/transform.py` | Flattens hourly arrays into flat records |
| `load_weather` | `utils/load.py` | Upserts records into `weather_hourly` |
| `log_ingestion` | `utils/load.py` | Writes one row to `ingestion_log` (trigger_rule=all_done) |

`log_ingestion` uses `trigger_rule="all_done"` so it always runs, even on upstream failure.

---

## Tables

### weather_hourly

Stores one row per city per hour. Upsert on `(city, timestamp)`.

| Column | Type | Source |
|---|---|---|
| city | VARCHAR | Injected at fetch time |
| timestamp | TIMESTAMP | `hourly.time` |
| temperature_celsius | NUMERIC | `temperature_2m` |
| humidity_pct | SMALLINT | `relative_humidity_2m` |
| precipitation_mm | NUMERIC | `precipitation` |
| wind_speed_kmh | NUMERIC | `wind_speed_10m` |
| wind_direction_deg | SMALLINT | `wind_direction_10m` |
| wmo_weather_code | SMALLINT | `weather_code` |
| loaded_at | TIMESTAMP | Set by DB on insert/update |

### ingestion_log

One row per pipeline run. Tracks volume and outcome for observability.

| Column | Purpose |
|---|---|
| dag_id / run_id | Airflow run identification |
| execution_date | Logical date of the run |
| cities_fetched | Number of cities with a successful API response |
| records_loaded | Rows upserted in that run |
| status | `success` or `failure` |
| error_message | Populated if the run failed |

---

## PostgreSQL connection

The DAG references the Airflow connection ID `postgres_weather`.

Create it in the Airflow UI (Admin > Connections) or via CLI:

```bash
airflow connections add postgres_weather \
  --conn-type postgres \
  --conn-host localhost \
  --conn-login airflow \
  --conn-password airflow \
  --conn-port 5432 \
  --conn-schema weather
```

---

## Running locally (without Airflow)

Start a local PostgreSQL instance:

```bash
docker-compose up -d
```

Install dependencies and run:

```bash
pip install -r requirements.txt
python run_local.py
```

Optional flags:

```bash
python run_local.py --host myhost --port 5432 --db weather --user myuser --password mypassword
```

The script prints a proof-of-load query showing the last rows inserted and the last 3 ingestion log entries.

---

## Proof of load (sample output)

```json
[
  {
    "city": "Bordeaux",
    "timestamp": "2024-06-09T12:00:00",
    "temperature_celsius": 22.8,
    "wmo_weather_code": 1,
    "loaded_at": "2024-06-09T14:03:21.441Z"
  }
]
```

```json
[
  {
    "dag_id": "weather_pipeline",
    "run_id": "local__20240609T140321",
    "execution_date": "2024-06-09T14:03:21Z",
    "source": "open-meteo",
    "cities_fetched": 4,
    "records_loaded": 96,
    "status": "success",
    "error_message": null
  }
]
```

---

## Cities configured

| City | Latitude | Longitude |
|---|---|---|
| Paris | 48.8566 | 2.3522 |
| Lyon | 45.7640 | 4.8357 |
| Bordeaux | 44.8378 | -0.5792 |
| Lille | 50.6292 | 3.0573 |

To add a city, append to the `CITIES` list in `dags/weather_pipeline_dag.py` and `run_local.py`.
"# Airflow-TP2-B" 
