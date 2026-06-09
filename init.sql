-- =============================================================
-- Table: weather_hourly
-- Stores one row per city per hour from Open-Meteo.
-- Primary key is (city, timestamp) to allow idempotent upserts.
-- =============================================================

CREATE TABLE IF NOT EXISTS weather_hourly (
    city                  VARCHAR(100)        NOT NULL,
    timestamp             TIMESTAMP           NOT NULL,
    temperature_celsius   NUMERIC(5, 2),
    humidity_pct          SMALLINT,
    precipitation_mm      NUMERIC(6, 2),
    wind_speed_kmh        NUMERIC(6, 2),
    wind_direction_deg    SMALLINT,
    wmo_weather_code      SMALLINT,
    loaded_at             TIMESTAMP           NOT NULL DEFAULT NOW(),
    PRIMARY KEY (city, timestamp)
);


-- =============================================================
-- Table: ingestion_log
-- One row per DAG run. Tracks source, volume, and outcome.
-- Used for observability without relying solely on Airflow logs.
-- =============================================================

CREATE TABLE IF NOT EXISTS ingestion_log (
    id                SERIAL              PRIMARY KEY,
    dag_id            VARCHAR(200)        NOT NULL,
    run_id            VARCHAR(200)        NOT NULL,
    execution_date    TIMESTAMP           NOT NULL,
    source            VARCHAR(100)        NOT NULL DEFAULT 'open-meteo',
    cities_fetched    SMALLINT,
    records_loaded    INTEGER,
    status            VARCHAR(50)         NOT NULL,
    error_message     TEXT,
    logged_at         TIMESTAMP           NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ingestion_log_execution_date
    ON ingestion_log (execution_date DESC);
