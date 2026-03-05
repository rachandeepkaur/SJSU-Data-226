from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any, Dict, List

import pandas as pd
import requests

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


DAG_ID = "open_meteo_etl_daily"


def _get_open_meteo_base_url() -> str:
    """
    Reads the HTTP Airflow Connection:
      Conn Id: open_meteo_api
      Host: https://api.open-meteo.com
    """
    conn = BaseHook.get_connection("open_meteo_api")
    return conn.host.rstrip("/")


def fetch_open_meteo_daily(**context) -> List[Dict[str, Any]]:
    """
    Calls Open-Meteo API for each configured location and returns daily data rows
    mapped to your Snowflake WEATHER_RAW_DAILY column names.
    """
    locations = json.loads(Variable.get("weather_locations"))
    history_days = int(Variable.get("weather_history_days"))  # you will set this to 60 in UI

    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=history_days)

    base_url = _get_open_meteo_base_url()
    endpoint = f"{base_url}/v1/forecast"

    rows: List[Dict[str, Any]] = []

    for loc in locations:
        params = {
            "latitude": loc["lat"],
            "longitude": loc["lon"],
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "daily": [
                "temperature_2m_max",
                "temperature_2m_min",
                "temperature_2m_mean",
                "precipitation_sum",
            ],
            "timezone": "UTC",
        }

        r = requests.get(endpoint, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()

        daily = data.get("daily", {})
        dates = daily.get("time", [])

        # Map Open-Meteo fields -> YOUR Snowflake columns
        for i, d in enumerate(dates):
            rows.append(
                {
                    "LOCATION_NAME": loc["name"],
                    "LATITUDE": float(loc["lat"]),
                    "LONGITUDE": float(loc["lon"]),
                    "DATE": d,  # your column is DATE (not OBS_DATE)
                    "TEMP_MAX": daily.get("temperature_2m_max", [None])[i],
                    "TEMP_MIN": daily.get("temperature_2m_min", [None])[i],
                    "TEMP_MEAN": daily.get("temperature_2m_mean", [None])[i],
                    "PRECIP_MM": daily.get("precipitation_sum", [None])[i],
                }
            )

    # Save to XCom for the load task
    context["ti"].xcom_push(key="raw_rows", value=rows)
    return rows


def upsert_weather_raw_to_snowflake(**context) -> None:
    """
    Loads fetched rows into Snowflake:
      - inserts into temp table
      - MERGE into WEATHER_RAW_DAILY
    Uses SQL transaction + try/except with rollback.
    """
    rows = context["ti"].xcom_pull(key="raw_rows", task_ids="fetch_open_meteo_daily")
    if not rows:
        return

    df = pd.DataFrame(rows)
    df["DATE"] = pd.to_datetime(df["DATE"]).dt.date

    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        # Temp table to stage data (keeps MERGE clean and idempotent)
        cur.execute("""
            CREATE OR REPLACE TEMP TABLE TMP_WEATHER_RAW_DAILY (
              LOCATION_NAME STRING,
              LATITUDE FLOAT,
              LONGITUDE FLOAT,
              DATE DATE,
              TEMP_MAX FLOAT,
              TEMP_MIN FLOAT,
              TEMP_MEAN FLOAT,
              PRECIP_MM FLOAT
            );
        """)

        cur.execute("BEGIN;")
        cur.execute("TRUNCATE TABLE TMP_WEATHER_RAW_DAILY;")

        insert_sql = """
            INSERT INTO TMP_WEATHER_RAW_DAILY
              (LOCATION_NAME, LATITUDE, LONGITUDE, DATE, TEMP_MAX, TEMP_MIN, TEMP_MEAN, PRECIP_MM)
            VALUES
              (%(LOCATION_NAME)s, %(LATITUDE)s, %(LONGITUDE)s, %(DATE)s, %(TEMP_MAX)s, %(TEMP_MIN)s, %(TEMP_MEAN)s, %(PRECIP_MM)s);
        """
        cur.executemany(insert_sql, df.to_dict("records"))

        merge_sql = """
            MERGE INTO WEATHER_RAW_DAILY t
            USING TMP_WEATHER_RAW_DAILY s
              ON t.LOCATION_NAME = s.LOCATION_NAME
             AND t.DATE = s.DATE
            WHEN MATCHED THEN UPDATE SET
              t.LATITUDE = s.LATITUDE,
              t.LONGITUDE = s.LONGITUDE,
              t.TEMP_MAX = s.TEMP_MAX,
              t.TEMP_MIN = s.TEMP_MIN,
              t.TEMP_MEAN = s.TEMP_MEAN,
              t.PRECIP_MM = s.PRECIP_MM,
              t.INGESTED_AT = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT
              (LOCATION_NAME, LATITUDE, LONGITUDE, DATE, TEMP_MAX, TEMP_MIN, TEMP_MEAN, PRECIP_MM)
            VALUES
              (s.LOCATION_NAME, s.LATITUDE, s.LONGITUDE, s.DATE, s.TEMP_MAX, s.TEMP_MIN, s.TEMP_MEAN, s.PRECIP_MM);
        """
        cur.execute(merge_sql)
        cur.execute("COMMIT;")

    except Exception:
        cur.execute("ROLLBACK;")
        raise
    finally:
        cur.close()
        conn.close()


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["lab1", "open-meteo", "etl"],
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_open_meteo_daily",
        python_callable=fetch_open_meteo_daily,
    )

    load_task = PythonOperator(
        task_id="upsert_weather_raw_to_snowflake",
        python_callable=upsert_weather_raw_to_snowflake,
    )

    # Optional: trigger your forecasting DAG after ETL succeeds
    trigger_forecast = TriggerDagRunOperator(
        task_id="trigger_forecast_dag",
        trigger_dag_id="weather_forecast_daily",
        wait_for_completion=False,
    )

    fetch_task >> load_task >> trigger_forecast