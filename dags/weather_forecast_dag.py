# weather_forecast_dag.py
# Airflow DAG for weather forecast pipeline

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any, Dict, List

import numpy as np
import pandas as pd

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


DAG_ID = "weather_forecast_daily"


def _fit_and_predict_rolling_mean(y: pd.Series, horizon: int) -> np.ndarray:
    """
    Simple baseline model:
    Forecast = mean of last 7 observed TEMP_MAX values.
    """
    y = y.dropna()
    if y.empty:
        return np.array([0.0] * horizon)

    window = min(7, len(y))
    pred = float(y.tail(window).mean())
    return np.array([pred] * horizon)


def train_predict_and_upsert(**context) -> None:
    """
    1) Read last N days from WEATHER_RAW_DAILY
    2) Forecast next H days for each location
    3) MERGE into WEATHER_FORECAST_DAILY
    Uses SQL transaction + try/except.
    """
    locations = json.loads(Variable.get("weather_locations"))
    history_days = int(Variable.get("weather_history_days"))          # you set this to 60
    horizon_days = int(Variable.get("weather_forecast_horizon_days")) # e.g., 7

    # This matches your DB naming choices
    target_col = Variable.get("weather_target_metric")  # should be "TEMP_MAX"
    model_name = "rolling_mean_7"

    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        # Stage table for clean MERGE
        cur.execute("""
            CREATE OR REPLACE TEMP TABLE TMP_WEATHER_FORECAST_DAILY (
              LOCATION_NAME STRING,
              FORECAST_DATE DATE,
              PREDICTED_TEMP_MAX FLOAT,
              MODEL_NAME STRING,
              TRAIN_START_DATE DATE,
              TRAIN_END_DATE DATE
            );
        """)

        cur.execute("BEGIN;")
        cur.execute("TRUNCATE TABLE TMP_WEATHER_FORECAST_DAILY;")

        all_rows: List[Dict[str, Any]] = []

        for loc in locations:
            loc_name = loc["name"]

            # Pull last N days of actuals
            sql = f"""
                SELECT DATE, {target_col}
                FROM WEATHER_RAW_DAILY
                WHERE LOCATION_NAME = %s
                  AND DATE >= DATEADD(day, -{history_days}, CURRENT_DATE())
                ORDER BY DATE;
            """
            cur.execute(sql, (loc_name,))
            data = cur.fetchall()

            df = pd.DataFrame(data, columns=["DATE", target_col])
            df["DATE"] = pd.to_datetime(df["DATE"])

            train_start = df["DATE"].min().date() if not df.empty else None
            train_end = df["DATE"].max().date() if not df.empty else None

            preds = _fit_and_predict_rolling_mean(df[target_col], horizon=horizon_days)

            # Forecast starts the day after the last observed day
            if not df.empty:
                start_fc_date = df["DATE"].max().date() + timedelta(days=1)
            else:
                start_fc_date = datetime.utcnow().date()

            for i in range(horizon_days):
                all_rows.append(
                    {
                        "LOCATION_NAME": loc_name,
                        "FORECAST_DATE": start_fc_date + timedelta(days=i),
                        "PREDICTED_TEMP_MAX": float(preds[i]),
                        "MODEL_NAME": model_name,
                        "TRAIN_START_DATE": train_start,
                        "TRAIN_END_DATE": train_end,
                    }
                )

        # Insert forecast rows into temp stage
        insert_tmp = """
            INSERT INTO TMP_WEATHER_FORECAST_DAILY
              (LOCATION_NAME, FORECAST_DATE, PREDICTED_TEMP_MAX, MODEL_NAME, TRAIN_START_DATE, TRAIN_END_DATE)
            VALUES
              (%(LOCATION_NAME)s, %(FORECAST_DATE)s, %(PREDICTED_TEMP_MAX)s, %(MODEL_NAME)s, %(TRAIN_START_DATE)s, %(TRAIN_END_DATE)s);
        """
        if all_rows:
            cur.executemany(insert_tmp, all_rows)

        # MERGE into your forecast table (idempotent)
        merge_sql = """
            MERGE INTO WEATHER_FORECAST_DAILY t
            USING TMP_WEATHER_FORECAST_DAILY s
              ON t.LOCATION_NAME = s.LOCATION_NAME
             AND t.FORECAST_DATE = s.FORECAST_DATE
            WHEN MATCHED THEN UPDATE SET
              t.PREDICTED_TEMP_MAX = s.PREDICTED_TEMP_MAX,
              t.MODEL_NAME = s.MODEL_NAME,
              t.TRAIN_START_DATE = s.TRAIN_START_DATE,
              t.TRAIN_END_DATE = s.TRAIN_END_DATE,
              t.CREATED_AT = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT
              (LOCATION_NAME, FORECAST_DATE, PREDICTED_TEMP_MAX, MODEL_NAME, TRAIN_START_DATE, TRAIN_END_DATE)
            VALUES
              (s.LOCATION_NAME, s.FORECAST_DATE, s.PREDICTED_TEMP_MAX, s.MODEL_NAME, s.TRAIN_START_DATE, s.TRAIN_END_DATE);
        """
        cur.execute(merge_sql)

        cur.execute("COMMIT;")
    except Exception:
        cur.execute("ROLLBACK;")
        raise
    finally:
        cur.close()
        conn.close()


def rebuild_final_table_union(**context) -> None:
    """
    Assignment requirement:
    Forecasting job must UNION:
      - ETL table (WEATHER_RAW_DAILY)
      - Forecast table (WEATHER_FORECAST_DAILY)
    and create the final table WEATHER_FINAL_DAILY.

    Uses SQL transaction + try/except.
    """
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        cur.execute("BEGIN;")

        # Rebuild final table each run (simple for grading)
        cur.execute("TRUNCATE TABLE WEATHER_FINAL_DAILY;")

        union_sql = """
            INSERT INTO WEATHER_FINAL_DAILY
              (location_name, date, temp_max, temp_min, temp_mean, is_forecast, model_name)
            -- Actuals from ETL
            SELECT
              LOCATION_NAME AS location_name,
              DATE AS date,
              TEMP_MAX AS temp_max,
              TEMP_MIN AS temp_min,
              TEMP_MEAN AS temp_mean,
              FALSE AS is_forecast,
              NULL AS model_name
            FROM WEATHER_RAW_DAILY

            UNION ALL

            -- Forecasts from ML table (only temp_max forecast is available)
            SELECT
              LOCATION_NAME AS location_name,
              FORECAST_DATE AS date,
              PREDICTED_TEMP_MAX AS temp_max,
              NULL AS temp_min,
              NULL AS temp_mean,
              TRUE AS is_forecast,
              MODEL_NAME AS model_name
            FROM WEATHER_FORECAST_DAILY;
        """
        cur.execute(union_sql)

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
    schedule=None,  # triggered by ETL DAG; or change to "@daily"
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["lab1", "forecast"],
) as dag:

    t1 = PythonOperator(
        task_id="train_predict_and_upsert_forecast",
        python_callable=train_predict_and_upsert,
    )

    t2 = PythonOperator(
        task_id="rebuild_final_table_union",
        python_callable=rebuild_final_table_union,
    )

    t1 >> t2