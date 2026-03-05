-- 01_create_tables.sql
-- SQL script to create tables for weather analytics

-- ============================================================
-- Create WEATHER_RAW_DAILY
-- ============================================================

CREATE OR REPLACE TABLE WEATHER_RAW_DAILY (
  LOCATION_NAME      STRING      NOT NULL,
  LATITUDE           FLOAT       NOT NULL,
  LONGITUDE          FLOAT       NOT NULL,
  DATE               DATE        NOT NULL,

  TEMP_MAX           FLOAT,
  TEMP_MIN           FLOAT,
  TEMP_MEAN          FLOAT,
  PRECIP_MM          FLOAT,

  INGESTED_AT        TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),

  CONSTRAINT PK_WEATHER_RAW PRIMARY KEY (LOCATION_NAME, DATE)
);

-- ============================================================
-- Create WEATHER_FORECAST_DAILY
-- ============================================================

CREATE OR REPLACE TABLE WEATHER_FORECAST_DAILY (
  LOCATION_NAME        STRING        NOT NULL,
  FORECAST_DATE        DATE          NOT NULL,
  PREDICTED_TEMP_MAX   FLOAT         NOT NULL,
  MODEL_NAME           STRING        NOT NULL,
  TRAIN_START_DATE     DATE,
  TRAIN_END_DATE       DATE,
  CREATED_AT           TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),

  CONSTRAINT PK_WEATHER_FC PRIMARY KEY (LOCATION_NAME, FORECAST_DATE)
);

-- ============================================================
-- Create WEATHER_FINAL_DAILY
-- ============================================================

CREATE OR REPLACE TABLE WEATHER_FINAL_DAILY (
  LOCATION_NAME STRING,
  DATE DATE,
  TEMP_MAX FLOAT,
  TEMP_MIN FLOAT,
  TEMP_MEAN FLOAT,
  IS_FORECAST BOOLEAN,
  MODEL_NAME STRING,
  CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

  CONSTRAINT PK_WEATHER_FINAL PRIMARY KEY (LOCATION_NAME, DATE, IS_FORECAST)
);