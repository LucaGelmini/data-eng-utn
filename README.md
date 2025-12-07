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

## Demo

Run the Jupyter notebook to see the complete pipeline in action:

```bash
jupyter notebook demo_pipeline.ipynb
```

The notebook demonstrates:
- Complete data flow (Bronze → Silver → Gold)
- Idempotency verification
- Business intelligence queries (health alerts, outdoor scores, allergy risk)

## Documentation

See [COMPLETE_ARCHITECTURE.md](COMPLETE_ARCHITECTURE.md) for detailed architecture documentation.