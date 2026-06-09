from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.fetch import fetch_weather_for_all_cities
from utils.transform import transform_weather_responses
from utils.load import load_records_to_postgres, write_ingestion_log

CITIES = [
    {"name": "Paris",    "latitude": 48.8566, "longitude": 2.3522},
    {"name": "Lyon",     "latitude": 45.7640, "longitude": 4.8357},
    {"name": "Bordeaux", "latitude": 44.8378, "longitude": -0.5792},
    {"name": "Lille",    "latitude": 50.6292, "longitude": 3.0573},
]

POSTGRES_CONN_ID = "postgres_weather"

DEFAULT_ARGS = {
    "owner": "data-team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def task_fetch(**context):
    raw_data = fetch_weather_for_all_cities(CITIES)
    context["ti"].xcom_push(key="raw_weather", value=raw_data)
    context["ti"].xcom_push(key="cities_fetched", value=len(raw_data))


def task_transform(**context):
    raw_data = context["ti"].xcom_pull(key="raw_weather", task_ids="fetch_weather")
    records = transform_weather_responses(raw_data)
    context["ti"].xcom_push(key="transformed_records", value=records)


def task_load(**context):
    records = context["ti"].xcom_pull(key="transformed_records", task_ids="transform_weather")
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    loaded = load_records_to_postgres(records, hook)
    context["ti"].xcom_push(key="records_loaded", value=loaded)


def task_log(**context):
    ti = context["ti"]
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    cities_fetched = ti.xcom_pull(key="cities_fetched", task_ids="fetch_weather") or 0
    records_loaded = ti.xcom_pull(key="records_loaded", task_ids="load_weather") or 0

    write_ingestion_log(
        postgres_hook=hook,
        dag_id=context["dag"].dag_id,
        run_id=context["run_id"],
        execution_date=context["execution_date"],
        cities_fetched=cities_fetched,
        records_loaded=records_loaded,
        status="success",
        error_message=None,
    )


with DAG(
    dag_id="weather_pipeline",
    default_args=DEFAULT_ARGS,
    description="Full pipeline: fetch Open-Meteo, transform, load to PostgreSQL, log run.",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["weather", "pipeline", "postgres"],
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_weather",
        python_callable=task_fetch,
    )

    transform_task = PythonOperator(
        task_id="transform_weather",
        python_callable=task_transform,
    )

    load_task = PythonOperator(
        task_id="load_weather",
        python_callable=task_load,
    )

    log_task = PythonOperator(
        task_id="log_ingestion",
        python_callable=task_log,
        trigger_rule="all_done",
    )

    fetch_task >> transform_task >> load_task >> log_task
