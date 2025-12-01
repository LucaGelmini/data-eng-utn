# Data Engineering Pipeline - UTN FRBA

A data engineering project for the Data Engineering course at **Universidad Tecnológica Nacional (UTN)** - Facultad Regional de Buenos Aires (FRBA).

## Overview

This project implements a medallion architecture data pipeline that extracts meteorological data from free APIs and processes it through multiple layers:

- **Bronze Layer**: Raw data extraction from weather APIs
- **Silver Layer**: Data transformations and cleaning

Data can be stored locally or on AWS S3.

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

### Available Tasks

- **`fetch_forecast`** - Fetch weather forecast data
  ```bash
  uv run invoke fetch-forecast --latitude=-34.603722 --longitude=-58.381592
  ```

- **`fetch_historic`** - Fetch historical weather data (default: last 7 days)
  ```bash
  uv run invoke fetch-historic --latitude=-34.603722 --longitude=-58.381592 --days=7
  ```

- **`fetch_air_quality`** - Fetch air quality data
  ```bash
  uv run invoke fetch-air-quality --latitude=-34.603722 --longitude=-58.381592
  ```

- **`fetch_stations`** - Fetch nearby weather stations
  ```bash
  uv run invoke fetch-stations --latitude=-34.603722 --longitude=-58.381592
  ```

- **`run_extraction_pipeline`** - Run the complete extraction and load pipeline
  ```bash
  uv run invoke run-extraction-pipeline --latitude=-34.603722 --longitude=-58.381592
  ```

## Architecture

- **Bronze Layer**: Raw data ingestion from APIs into Delta Lake
- **Silver Layer**: Data transformations and quality checks (coming soon)

Data is stored in Delta Lake tables for ACID compliance and time-travel capabilities.