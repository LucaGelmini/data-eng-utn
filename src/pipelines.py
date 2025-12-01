from datetime import date, timedelta

from src.extractors import (
    ForecastExtractor,
    HistoricalExtractor,
    AirQualityExtractor,
    NearestStationsExtractor,
)

from src.loaders import DeltaLakeLoader

LOOKBACK_WINDOW_DAYS = 7
END_DATE = date.today()
PATH_ROOT = "bronze"
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

def run_extraction_load_bronze_pipeline(
    path_root: str = PATH_ROOT,
    lookback_window_days: int = LOOKBACK_WINDOW_DAYS,
    end_date: date = END_DATE,
    coordinates: dict = COORDINATES
):
    """Runs the extraction and load pipeline for multiple cities."""
    
    start_date = end_date - timedelta(days=lookback_window_days)
    
    for city, coords in coordinates.items():
        longitude = coords["longitude"]
        latitude = coords["latitude"]
        loader = DeltaLakeLoader(table_root_path=f"{path_root}")
        
        print(f"Processing data for {city}...")
    
        # Forecast Data Extraction and Loading
        forecast_extractor = ForecastExtractor(longitude, latitude)
        forecast_data = forecast_extractor.extract()
        forecast_data["city"] = city
        loader.delete_insert("forecast", forecast_data, filter_col="date_retrieved", partition_by=["date_retrieved", "city"])
        

        # Historical Weather Data Extraction and Loading
        historical_extractor = HistoricalExtractor(start_date, end_date, longitude, latitude)
        historical_data = historical_extractor.extract()
        historical_data["city"] = city
        loader.delete_insert("historical", historical_data, filter_col="date_retrieved", partition_by=["date_retrieved", "city"])

        # Air Quality Data Extraction and Loading
        air_quality_extractor = AirQualityExtractor(longitude, latitude)
        air_quality_data = air_quality_extractor.extract()
        air_quality_data["city"] = city
        loader.delete_insert("air_quality", air_quality_data, filter_col="date_retrieved", partition_by=["date_retrieved", "city"])

        # Nearest Stations Data Extraction and Loading
        nearest_stations_extractor = NearestStationsExtractor(longitude, latitude)
        nearest_stations_data = nearest_stations_extractor.extract()
        nearest_stations_data["city"] = city
        loader.delete_insert("nearest_stations", nearest_stations_data, filter_col="date_retrieved", partition_by=["date_retrieved", "city"])