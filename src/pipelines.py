from datetime import date, timedelta
from typing import Callable

import src.config as cfg
from src.extractors import (
    ForecastExtractor,
    HistoricalExtractor,
    AirQualityExtractor,
    NearestStationsExtractor,
)

from src.loaders import DeltaLakeLoader
from src.transformers import (
    WeatherSummaryTransformer,
    AirQualityDailyTransformer,
    WeatherForecastTransformer,
    HourlyHistoricalAnalysisTransformer,
    ForecastCombinedTransformer
)

LOOKBACK_WINDOW_DAYS = 7
END_DATE = date.today()
COORDINATES = {
    "buenos_aires": {
        "longitude": -58.417309,
        "latitude": -34.611778,
    },
    "cordoba": {
        "longitude": -64.181056,
        "latitude": -31.413500,
    },
    "rosario": {
        "longitude": -60.639321,
        "latitude": -32.944242,
    }
}

def get_city_coordinates(city_name: str) -> dict[str, float]:
    # Here can be a better sanitization
    city_name = city_name.lower().strip()

    return cfg.CITIES_COORDINATES_MAP[city_name]

def run_extraction_load_bronze_pipeline(
    lookback_window_days: int = LOOKBACK_WINDOW_DAYS,
    end_date: date = END_DATE,
    cities: list[str] = ["buenos_aires", "rosario", "cordoba"],
    city_to_coordinates_mapper: Callable[[str], dict[str, float]] = get_city_coordinates,
):
    """Runs the extraction and load pipeline for multiple cities."""

    datalake_layer = 'bronze'
    start_date = end_date - timedelta(days=lookback_window_days)
    loader = DeltaLakeLoader(table_root_path=f"{datalake_layer}", storage_options=cfg.storage_options)

    for city in cities:
        coords = city_to_coordinates_mapper(city)

        longitude = coords["longitude"]
        latitude = coords["latitude"]

        print(f"Processing data for {city}...")

        # Forecast Data Extraction and Loading
        forecast_extractor = ForecastExtractor(longitude, latitude)
        forecast_data = forecast_extractor.extract()
        forecast_data["city"] = city
        loader.insert_overwrite("forecast", forecast_data, partition_by=["date_retrieved", "city"])


        # Historical Weather Data Extraction and Loading
        historical_extractor = HistoricalExtractor(start_date, end_date, longitude, latitude)
        historical_data = historical_extractor.extract()
        historical_data["city"] = city
        loader.insert_overwrite("historical", historical_data, partition_by=["date_retrieved", "city"])

        # Air Quality Data Extraction and Loading
        air_quality_extractor = AirQualityExtractor(longitude, latitude)
        air_quality_data = air_quality_extractor.extract()
        air_quality_data["city"] = city
        loader.insert_overwrite("air_quality", air_quality_data, partition_by=["date_retrieved", "city"])

        # Nearest Stations Data Extraction and Loading
        nearest_stations_extractor = NearestStationsExtractor(longitude, latitude)
        nearest_stations_data = nearest_stations_extractor.extract()
        nearest_stations_data["city"] = city
        loader.insert_overwrite("nearest_stations", nearest_stations_data, partition_by=["date_retrieved", "city"])


def run_transformation_silver_pipeline(
    cities: list[str] = ["buenos_aires", "rosario", "cordoba"],
    storage_options: dict | None = cfg.storage_options
):
    """Runs the transformation pipeline to create enriched silver layer tables."""

    datalake_layer = 'silver'
    loader = DeltaLakeLoader(table_root_path=datalake_layer, storage_options=storage_options)

    for city in cities:
        print(f"Processing silver layer for {city}...")

        weather_transformer = WeatherSummaryTransformer(storage_options=storage_options, city=city)
        weather_summary_data = weather_transformer.transform()
        if not weather_summary_data.empty:
            loader.merge_upsert(
                location="weather_summary",
                data=weather_summary_data,
                predicate="tgt.date = src.date AND tgt.city = src.city",
                partition_by=["city"]
            )

        air_quality_transformer = AirQualityDailyTransformer(storage_options=storage_options, city=city)
        air_quality_daily_data = air_quality_transformer.transform()
        if not air_quality_daily_data.empty:
            loader.merge_upsert(
                location="air_quality_daily",
                data=air_quality_daily_data,
                predicate="tgt.date = src.date AND tgt.city = src.city",
                partition_by=["city"]
            )

        weather_forecast_transformer = WeatherForecastTransformer(storage_options=storage_options, city=city)
        weather_forecast_data = weather_forecast_transformer.transform()
        if not weather_forecast_data.empty:
            loader.merge_upsert(
                location="weather_forecast",
                data=weather_forecast_data,
                predicate="tgt.date = src.date AND tgt.city = src.city",
                partition_by=["city"]
            )

        hourly_analysis_transformer = HourlyHistoricalAnalysisTransformer(storage_options=storage_options, city=city)
        hourly_analysis_data = hourly_analysis_transformer.transform()
        if not hourly_analysis_data.empty:
            loader.merge_upsert(
                location="hourly_historical_analysis",
                data=hourly_analysis_data,
                predicate="tgt.hour = src.hour AND tgt.city = src.city",
                partition_by=["city"]
            )


def run_transformation_gold_pipeline(
    cities: list[str] = ["buenos_aires", "rosario", "cordoba"],
    storage_options: dict | None = cfg.storage_options
):
    """Runs the gold layer transformation pipeline with business-ready data.

    Creates actionable insights including health alerts, allergy risk, and outdoor scores.
    """

    datalake_layer = 'gold'
    loader = DeltaLakeLoader(table_root_path=datalake_layer, storage_options=storage_options)

    for city in cities:
        print(f"Processing gold layer for {city}...")

        forecast_combined_transformer = ForecastCombinedTransformer(storage_options=storage_options, city=city)
        forecast_combined_data = forecast_combined_transformer.transform()
        if not forecast_combined_data.empty:
            loader.merge_upsert(
                location="forecast_combined",
                data=forecast_combined_data,
                predicate="tgt.date = src.date AND tgt.city = src.city",
                partition_by=["city"]
            )