# Lab 1 - Weather Analytics

## Project Structure

```
lab1-weather-analytics/
  dags/
    open_meteo_etl_dag.py      # ETL pipeline DAG
    weather_forecast_dag.py    # Forecast pipeline DAG
  sql/
    01_create_tables.sql       # Table creation scripts
    02_merge_union_final.sql   # Merge & union logic
  src/
    open_meteo_client.py       # Open-Meteo API client
    feature_engineering.py     # Feature engineering
    forecast_model.py          # Forecast model
  report/
    report.md                  # Lab report
    system_diagram.png         # System architecture diagram
    airflow_ui_screenshot.png  # Airflow UI screenshot
  requirements.txt
  README.md
```

## Setup

```bash
pip install -r requirements.txt
```

## Usage

Add DAGs to your Airflow DAGs folder and trigger them from the Airflow UI.
