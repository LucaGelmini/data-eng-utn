# Data Engineering Pipeline - UTN FRBA

A data engineering project for the Data Engineering course at **Universidad Tecnológica Nacional (UTN)** - Facultad Regional de Buenos Aires (FRBA).

## Overview

This project implements a complete **Medallion Architecture** data pipeline that extracts meteorological data from Open-Meteo APIs and processes it through three layers:

- **Bronze Layer**: Raw data extraction from weather APIs (historical, forecast, air quality, stations)
- **Silver Layer**: Enriched transformations with aggregations and AQI calculation
- **Gold Layer**: Business intelligence with health alerts, allergy risk, and outdoor activity scores

Supports **3 cities** (Buenos Aires, Rosario, Córdoba) with data stored on AWS S3 using Delta Lake.

## Data Sources

- Weather forecast data (temperature, precipitation, wind speed)
- Historical weather data
- Air quality data
- Nearby weather stations

## Setup

### Prerequisites

- Python 3.9+
- `uv` package manager

### Installation

1. **Install uv** (if not already installed):
   ```bash
   pip install uv
   ```

2. **Install dependencies**:
   ```bash
   uv sync
   ```

3. **Configure environment variables** (in `.env`):
   ```
   METEO_STAT_API_KEY=your_api_key
   METEO_STAT_HOST=your_host
   BUCKET_NAME=your_s3_bucket
   AWS_ACCESS_KEY_ID=your_key
   AWS_SECRET_ACCESS_KEY=your_secret
   ```

### API Setup

**For Nearby Stations Endpoint (RapidAPI)**:
1. Create an account at [RapidAPI](https://rapidapi.com/)
2. Subscribe to the [Meteostat API](https://rapidapi.com/view/meteostat)
3. Get your API key from the RapidAPI dashboard
4. Set `METEO_STAT_API_KEY` and `METEO_STAT_HOST` in your `.env`

**API Documentation**:
- [Open-Meteo API](https://open-meteo.com/en/docs) - Free weather forecast, historical, and air quality data
- [Meteostat API](https://dev.meteostat.net/api/) - Weather station data

## Running Tasks

Tasks are managed with `invoke`. Run any task using:

```bash
uv run invoke <task_name> [options]
```

### Pipeline Tasks

**Complete Pipeline Run:**
```bash
# 1. Extract raw data (Bronze layer)
uv run inv run-extraction-pipeline

# 2. Transform to enriched data (Silver layer)
uv run inv run-transformation-pipeline

# 3. Create business intelligence (Gold layer)
uv run inv run-gold-pipeline
```

**Individual Extraction Tasks:**
```bash
# Fetch specific data for a single location
uv run inv fetch-forecast --latitude=-34.603722 --longitude=-58.381592
uv run inv fetch-historic --latitude=-34.603722 --longitude=-58.381592 --days=7
uv run inv fetch-air-quality --latitude=-34.603722 --longitude=-58.381592
uv run inv fetch-stations --latitude=-34.603722 --longitude=-58.381592
```

## Architecture

```
BRONZE (Raw)          SILVER (Enriched)              GOLD (Business)
─────────────────────────────────────────────────────────────────────

📦 historical      →  weather_summary
                   →  hourly_historical_analysis

📦 forecast        →  weather_forecast             ┐
📦 air_quality     →  air_quality_daily            ├→ forecast_combined
                                                    ┘  • health_alert
                                                       • allergy_risk
                                                       • outdoor_score
```

**5 Transformers** (4 Silver + 1 Gold):
- `WeatherSummaryTransformer`: Historical weather aggregations
- `WeatherForecastTransformer`: Forecast weather aggregations
- `AirQualityDailyTransformer`: Air quality aggregations + AQI calculation
- `HourlyHistoricalAnalysisTransformer`: Hourly temperature patterns
- `ForecastCombinedTransformer`: Business intelligence with health alerts

**Features:**
- Delta Lake for ACID transactions and time-travel
- Idempotent `merge_upsert` operations
- Partitioned by city for query performance
- Multi-city support (3 cities)

## Data Schemas

### Bronze Layer (Raw Data)

#### `bronze.historical`
Historical weather data from weather stations.
- `time`: Timestamp of the measurement (e.g., "2025-12-01T00:00")
- `temperature_2m`: Temperature at 2 meters height (°C)
- `precipitation`: Precipitation amount (mm)
- `windspeed_10m`: Wind speed at 10 meters height (km/h)
- `station_coordinates`: Weather station coordinates [lat, lon]
- `requested_coordinates`: Original query coordinates [lat, lon]
- `date_retrieved`: Date when data was retrieved
- `city`: City name (partitioned)

#### `bronze.forecast`
Weather forecast data for upcoming days.
- `time`: Forecast timestamp (e.g., "2025-12-09T00:00")
- `temperature_2m`: Forecasted temperature at 2m (°C)
- `precipitation`: Forecasted precipitation (mm)
- `windspeed_10m`: Forecasted wind speed at 10m (km/h)
- `station_coordinates`: Forecast station coordinates [lat, lon]
- `requested_coordinates`: Query coordinates [lat, lon]
- `date_retrieved`: Date when forecast was retrieved
- `city`: City name (partitioned)

#### `bronze.air_quality`
Air quality forecast data with pollutant concentrations.
- `time`: Forecast timestamp (e.g., "2025-12-09T00:00")
- `pm10`: Particulate Matter 10μm (μg/m³)
- `pm2_5`: Particulate Matter 2.5μm (μg/m³)
- `carbon_monoxide`: CO concentration (μg/m³)
- `station_coordinates`: Measurement station coordinates [lat, lon]
- `requested_coordinates`: Query coordinates [lat, lon]
- `date_retrieved`: Date when data was retrieved
- `city`: City name (partitioned)

#### `bronze.nearest_stations`
Metadata about nearby weather stations.
- `id`: Station unique identifier
- `name`: Station name
- `distance`: Distance from query point (meters)
- `generated_at`: When station info was generated
- `name_language`: Language of station name
- `query_coordinates`: Query coordinates tuple (lat, lon)
- `date_retrieved`: Date when data was retrieved
- `city`: City name (partitioned)

### Silver Layer (Enriched Data)

#### `silver.weather_summary`
Daily aggregated historical weather data with enrichments.
- `date`: Date of the summary
- `city`: City name
- `geohash`: Geohash of location (precision 7)
- `temp_min`: Minimum daily temperature (°C)
- `temp_max`: Maximum daily temperature (°C)
- `temp_avg`: Average daily temperature (°C)
- `total_precipitation`: Total daily precipitation (mm)
- `avg_windspeed`: Average daily wind speed (km/h)
- `latitude`: Station latitude
- `longitude`: Station longitude
- `date_retrieved`: Date when data was retrieved
- `temp_range`: Temperature range (max - min)

#### `silver.weather_forecast`
Daily aggregated weather forecast with enrichments.
- `date`: Forecast date
- `city`: City name
- `geohash`: Geohash of location (precision 7)
- `temp_min`: Minimum forecasted temperature (°C)
- `temp_max`: Maximum forecasted temperature (°C)
- `temp_avg`: Average forecasted temperature (°C)
- `total_precipitation`: Total forecasted precipitation (mm)
- `avg_windspeed`: Average forecasted wind speed (km/h)
- `latitude`: Forecast station latitude
- `longitude`: Forecast station longitude
- `date_retrieved`: Date when forecast was retrieved
- `temp_range`: Temperature range (max - min)

#### `silver.air_quality_daily`
Daily aggregated air quality data with simplified AQI.
- `date`: Date of measurement
- `city`: City name
- `geohash`: Geohash of location (precision 7)
- `pm10_min`: Minimum PM10 concentration (μg/m³)
- `pm10_max`: Maximum PM10 concentration (μg/m³)
- `pm10_avg`: Average PM10 concentration (μg/m³)
- `pm2_5_min`: Minimum PM2.5 concentration (μg/m³)
- `pm2_5_max`: Maximum PM2.5 concentration (μg/m³)
- `pm2_5_avg`: Average PM2.5 concentration (μg/m³)
- `co_min`: Minimum CO concentration (μg/m³)
- `co_max`: Maximum CO concentration (μg/m³)
- `co_avg`: Average CO concentration (μg/m³)
- `latitude`: Station latitude
- `longitude`: Station longitude
- `date_retrieved`: Date when data was retrieved
- `aqi_simplified`: **Simplified AQI** (0-100+, based on PM2.5) - **Note: NOT EPA compliant**

**Important Note on AQI Calculation**: The `aqi_simplified` field uses a simplified formula `(PM2.5 / 25.0) * 100` capped at 100. This is **NOT** the official AQI (Air Quality Index) calculation used by the U.S. Environmental Protection Agency (EPA) for air quality monitoring. The official EPA AQI uses a piecewise linear function with specific breakpoints (0-12.0 μg/m³ = Good, 12.1-35.4 = Moderate, 35.5-55.4 = Unhealthy for Sensitive Groups, etc.). The correct formula is: `AQI = ((Ih - Il) / (BPh - BPl)) × (Cp - BPl) + Il`, where Cp is the pollutant concentration and breakpoints define category ranges. For production use, implement the official EPA formula as described in the [EPA Technical Assistance Document](https://document.airnow.gov/technical-assistance-document-for-the-reporting-of-daily-air-quailty.pdf).

### Gold Layer (Business Intelligence)

#### `gold.forecast_combined`
Combined weather and air quality forecast with actionable insights.
- `date`: Forecast date
- `city`: City name
- `geohash`: Geohash of location (precision 7)
- `temp_min`: Minimum forecasted temperature (°C)
- `temp_max`: Maximum forecasted temperature (°C)
- `temp_avg`: Average forecasted temperature (°C)
- `temp_range`: Temperature range (max - min)
- `total_precipitation`: Total forecasted precipitation (mm)
- `avg_windspeed`: Average forecasted wind speed (km/h)
- `pm10_avg`: Average PM10 concentration (μg/m³)
- `pm2_5_avg`: Average PM2.5 concentration (μg/m³)
- `co_avg`: Average CO concentration (μg/m³)
- `aqi_simplified`: Simplified AQI (0-100+)
- `latitude`: Station latitude
- `longitude`: Station longitude
- `date_retrieved`: Date when forecast was retrieved
- `health_alert`: Health alert level - `GOOD`, `LOW_ALERT`, `MODERATE_ALERT`, `HIGH_ALERT`
- `allergy_risk`: Allergy risk assessment - `LOW`, `MODERATE`, `HIGH`
- `outdoor_score`: Outdoor activity score (0-100, higher is better)

**Business Logic**:
- `health_alert`: Based on AQI and temperature (e.g., HIGH_ALERT if AQI ≥ 75)
- `allergy_risk`: Based on PM10, PM2.5, and wind speed
- `outdoor_score`: Composite score considering AQI, temperature, precipitation, and wind

#### `gold.hourly_historical_analysis`
Hourly aggregated historical weather patterns across all days.
- `hour`: Hour of day (0-23)
- `city`: City name
- `temp_min`: Minimum temperature for this hour across all days (°C)
- `temp_max`: Maximum temperature for this hour across all days (°C)
- `temp_avg`: Average temperature for this hour across all days (°C)
- `precipitation_avg`: Average precipitation for this hour (mm)
- `windspeed_avg`: Average wind speed for this hour (km/h)
- `days_count`: Number of days included in the aggregation

## Demo

Run the Jupyter notebook to see the complete pipeline in action:

```bash
jupyter notebook demo_pipeline.ipynb
```

The notebook demonstrates:
- Complete data flow (Bronze → Silver → Gold)
- Idempotency verification
- Business intelligence queries (health alerts, outdoor scores, allergy risk)
