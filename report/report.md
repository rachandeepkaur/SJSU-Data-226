# Lab 1 Weather Analytics Report

## Overview
This project aims to design and implement a weather prediction analytics system leveraging Snowflake, Apache Airflow, and the Open-Meteo API. By automating historical weather data ingestion and machine learning forecasting pipelines, we efficiently predict the next 7-day maximum temperature for multiple locations. The project demonstrates the integration of ETL and ML workflows within a cloud-based data warehouse architecture using transactional safety and automated orchestration.
## System Diagram

```mermaid
flowchart LR
    API["☁️ Open Meteo API"]

    subgraph DAG1["Airflow DAG 1"]
        extract["extract"] --> transform["transform"] --> etl["open_meteo_etl_daily"]
    end

    subgraph DAG2["Airflow DAG 2"]
        train["train_model"] --> forecast["weather_forecast_daily"]
    end

    API --> extract
    DAG1 -.-> DAG2

    etl --> SF1[("❄️ Snowflake\nraw.weather_raw_daily")]
    etl --> train
    forecast --> SF2[("❄️ Snowflake\nraw.weather_forecast_daily")]
```


## Results

## Airflow UI Screenshots
