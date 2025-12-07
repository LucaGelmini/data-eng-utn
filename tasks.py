from invoke.tasks import task
from src.extractors import (
    ForecastExtractor,
    HistoricalExtractor,
    AirQualityExtractor,
    NearestStationsExtractor
)
from src.loaders import DeltaLakeLoader
import pendulum
from src.pipelines import run_extraction_load_bronze_pipeline, run_transformation_silver_pipeline, run_transformation_gold_pipeline

@task
def fetch_forecast(ctx, latitude: float = -34.603722, longitude: float = -58.381592):
    """Fetch weather forecast for given coordinates."""
    extractor = ForecastExtractor(longitude=longitude, latitude=latitude)
    forecast_data = extractor.extract()
    print("Forecast Data Shape:", forecast_data.shape)
    print("Forecast Data Info:")
    print(forecast_data.info())


@task
def fetch_historic(ctx, latitude: float = -34.603722, longitude: float = -58.381592, days: int = 7):
    """Fetch historic weather data for the past `days` days."""
    start_date = pendulum.now().subtract(days=days).date()
    end_date = pendulum.now().date()
    extractor = HistoricalExtractor(
        start_date=start_date,
        end_date=end_date,
        longitude=longitude,
        latitude=latitude
    )
    historical_data = extractor.extract()
    print("Historical Data Shape:", historical_data.shape)
    print("Historical Data Info:")
    print(historical_data.info())


@task
def fetch_air_quality(ctx, latitude: float = -34.603722, longitude: float = -58.381592):
    """Fetch air quality data for given coordinates."""
    extractor = AirQualityExtractor(longitude=longitude, latitude=latitude)
    air_quality_data = extractor.extract()
    print("Air Quality Data Shape:", air_quality_data.shape)
    print("Air Quality Data Info:")
    print(air_quality_data.info())


@task
def fetch_stations(ctx, latitude: float = -34.603722, longitude: float = -58.381592):
    """Fetch nearby weather stations for given coordinates."""
    extractor = NearestStationsExtractor(longitude=longitude, latitude=latitude)
    stations_data = extractor.extract()
    print("Stations Data Shape:", stations_data.shape)
    print("Stations Data Info:")
    print(stations_data.info())
        
@task
def run_extraction_pipeline(ctx):
    """Run the complete extraction and load pipeline for default params."""
    run_extraction_load_bronze_pipeline()
    
@task
def run_transformation_pipeline(ctx):
    """Run the complete transformation and load pipeline for default params."""
    run_transformation_silver_pipeline()

@task
def run_gold_pipeline(ctx):
    """Run the gold layer transformation pipeline."""
    run_transformation_gold_pipeline()