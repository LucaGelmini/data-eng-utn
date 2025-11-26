from invoke.tasks import task
from src.TP1.main import OpenMeteoAPIClient
from typing import Literal
import pendulum

@task
def fetch_weather(
    ctx,
    endpoint: Literal["forecast", "historic", "air_quality","stations"],
    latitude: float = 	-34.603722,
    longitude: float = -58.381592):
    """
    Data from OpenMeteo API: fetch-weather --endpoint=<endpoint> [--latitude=<latitude>] [--longitude=<longitude>]
    """
    client = OpenMeteoAPIClient(latitude=latitude, longitude=longitude)
    
    if endpoint == "forecast":
        weather_data = client.get_weather_forecast()
        print("Metadata:\n", weather_data[0])
        print("Data:\n", weather_data[1])
    elif endpoint == "historic":
        start_date = pendulum.now().subtract(days=7).to_date_string()
        end_date = pendulum.now().to_date_string()
        historical_data = client.get_weather_historical(start_date, end_date)
        print("Metadata:\n", historical_data[0])
        print("Data:\n", historical_data[1])
    elif endpoint == "air_quality":
        air_quality_data = client.get_air_quality_data()
        print("Metadata:\n", air_quality_data[0])
        print("Data:\n", air_quality_data[1])
    elif endpoint == "stations":
        stations_data = client.get_nearby_stations()
        print("Metadata:\n", stations_data[0])
        print("Data:\n", stations_data[1])
    else:
        raise ValueError(f"Unknown endpoint: {endpoint}")
        
@task
def run_extraction_pipeline(ctx):
    pass