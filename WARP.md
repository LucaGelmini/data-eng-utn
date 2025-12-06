# WARP.md

This file provides guidance to WARP (warp.dev) when working with code in this repository.

## Project Overview

Data Engineering pipeline implementing medallion architecture for meteorological data extraction from free weather APIs. Academic project for UTN FRBA Data Engineering course.

**Data Flow**: APIs → Bronze Layer (Delta Lake) → Silver Layer (planned)

## Environment & Dependencies

**Package Manager**: `uv` (required)
**Python Version**: 3.13+

Always use `uv run` to execute Python commands:
```bash
uv run python script.py
uv run invoke <task>
uv run jupyter lab
```

## Common Commands

### Setup
```bash
# Install dependencies
uv sync

# Setup environment (copy .env.example to .env and configure)
cp .env.example .env
```

### Data Extraction Tasks
All tasks use `invoke` and accept optional `--latitude` and `--longitude` parameters.

```bash
# Fetch 7-day weather forecast
uv run invoke fetch-forecast --latitude=-34.603722 --longitude=-58.381592

# Fetch historical data (default: last 7 days)
uv run invoke fetch-historic --latitude=-34.603722 --longitude=-58.381592 --days=7

# Fetch air quality data (7-day forecast)
uv run invoke fetch-air-quality --latitude=-34.603722 --longitude=-58.381592

# Fetch nearby weather stations (RapidAPI - requires auth)
uv run invoke fetch-stations --latitude=-34.603722 --longitude=-58.381592

# Run complete extraction pipeline (all cities)
uv run invoke run-extraction-pipeline
```

### Development
```bash
# Run extractors directly (for testing)
uv run python src/extractors.py

# Start Jupyter for analysis
uv run jupyter lab
```

## Architecture

### Code Structure

**`src/config.py`**: Environment configuration and AWS S3 storage options
- Loads `.env` variables for API keys and AWS credentials
- Configures Delta Lake storage (local `./out/` or S3)
- Sets `storage_options` dict used by loaders

**`src/extractors.py`**: Data extraction from weather APIs
- **Base classes**: `BaseAuth`, `BaseExractor` (abstract patterns)
- **Concrete extractors**: 
  - `ForecastExtractor` - 7-day forecast from Open-Meteo
  - `HistoricalExtractor` - Historical data with date range
  - `AirQualityExtractor` - PM10, PM2.5, CO forecast
  - `NearestStationsExtractor` - Weather stations via RapidAPI/Meteostat
- **Pattern**: Each extractor defines `request` + `session` as class variables, implements `wrangler()` for data transformation
- **Metadata enrichment**: All extractors add `station_coordinates`, `requested_coordinates`, `date_retrieved`

**`src/loaders.py`**: Delta Lake persistence layer
- `DeltaLakeLoader` - Writes to Delta tables with ACID properties
- **Methods**:
  - `merge_upsert()` - Merge/insert based on predicate (creates table if missing)
  - `delete_insert()` - Partition replacement strategy (deletes existing partition, appends new data)
- Handles both local (`./out/`) and S3 storage based on config

**`src/pipelines.py`**: Orchestration logic
- `run_extraction_load_bronze_pipeline()` - Multi-city extraction pipeline
- **Default cities**: Buenos Aires, Córdoba, Rosario
- **Strategy**: Delete-insert pattern per city/date_retrieved partition
- **Bronze tables**: `forecast`, `historical`, `air_quality`, `nearest_stations`

**`src/transformers.py`**: Silver layer transformations (not yet implemented)

**`tasks.py`**: Invoke CLI task definitions
- Maps CLI commands to extractor/pipeline functions
- Provides individual task wrappers for testing extractors

### Data Storage

**Local mode** (no `BUCKET_NAME` in `.env`):
- Output: `./out/bronze/{table}/`
- Partitioned by: `date_retrieved`, `city`

**S3 mode** (when `BUCKET_NAME` configured):
- Output: `s3://{BUCKET_NAME}/bronze/{table}/`
- Same partitioning scheme

**Tables**:
- `bronze/forecast` - 7-day hourly forecast data
- `bronze/historical` - Historical hourly weather (configurable lookback)
- `bronze/air_quality` - 7-day hourly air quality forecast
- `bronze/nearest_stations` - Weather station metadata

### Key Design Patterns

**Extractor pattern**:
1. Class-level `request` and `session` define API contract
2. Instance initialization sets coordinates in session params
3. `extract()` orchestrates: `_extract_raw()` → `wrangler()` → DataFrame
4. Subclasses only implement `wrangler()` for data transformation

**Loader pattern**:
- **Delete-insert**: Used for time-series data where full partition replacement is desired (idempotent pipeline runs)
- **Merge-upsert**: Alternative strategy for incremental updates (currently unused but available)

**Pipeline pattern**:
- Loop over cities
- Extract all data sources per city
- Add `city` column for partitioning
- Use `delete_insert()` with `date_retrieved` as filter (ensures idempotency)

### Configuration Notes

**Required for all extractions**:
- `METEO_STAT_HOST`, `METEO_STAT_API_KEY` - Only needed for `fetch-stations` (RapidAPI Meteostat subscription required)

**Optional AWS S3 configuration**:
- If `BUCKET_NAME` not set → writes to `./out/` locally
- If set → writes to S3 with configured credentials and Delta Lake options

**API Sources**:
- Open-Meteo API (free, no auth) - forecast, historical, air quality
- Meteostat via RapidAPI (requires subscription) - weather stations

## Common Workflows

**Testing a single extractor**:
```bash
# Option 1: Use invoke task
uv run invoke fetch-forecast

# Option 2: Run extractor file directly
uv run python src/extractors.py
```

**Running full pipeline locally**:
```bash
# Ensure .env has no BUCKET_NAME set
uv run invoke run-extraction-pipeline
# Check output: ls -R ./out/
```

**Running full pipeline to S3**:
```bash
# Ensure .env has BUCKET_NAME and AWS credentials
uv run invoke run-extraction-pipeline
```

**Analyzing data**:
```bash
uv run jupyter lab
# Open qa_analysis.ipynb
```
