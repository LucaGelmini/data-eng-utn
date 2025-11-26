import os
import requests
from typing import ClassVar, Any
import pandas as pd
from deltalake import DeltaTable, write_deltalake

import src.config as cfg




class OpenMeteoAPIClient:
    BASE_URL: ClassVar[str] = "api.open-meteo.com/v1"
    WEATHER_VARS: ClassVar[list[str]] = [
        "temperature_2m",
        "precipitation",
        "windspeed_10m",
    ]
    AIR_QUALITY_VARS: ClassVar[list[str]] = [
        "pm10",
        "pm2_5",
        "carbon_monoxide"
    ]
    
    def __init__(self,latitude: float, longitude: float):
        self.latitude = latitude
        self.longitude = longitude

    def get_weather_forecast(self) -> tuple[dict, pd.DataFrame]:
        endpoint = f"https://{self.BASE_URL}/forecast"
        vars = ','.join(self.WEATHER_VARS)
        params = {
            "latitude": self.latitude,
            "longitude": self.longitude,
            "hourly": vars,
        }
        response = requests.get(endpoint, params=params)
        response.raise_for_status()
        res_body: dict[str, Any] = response.json()
        data = res_body.pop("hourly")
        metadata = res_body
        
        data = pd.DataFrame(data)
        data["latitude"] = metadata["latitude"]
        data["longitude"] = metadata["longitude"]
        return metadata, data
        
        
    
    def get_weather_historical(self, start_date: str, end_date: str) -> tuple[dict, pd.DataFrame]:
        endpoint = f"https://archive-{self.BASE_URL}/archive"
        vars = ','.join(self.WEATHER_VARS)
        params = {
            "latitude": self.latitude,
            "longitude": self.longitude,
            "start_date": start_date,
            "end_date": end_date,
            "hourly":vars,
        }
        print(f"Fetching historical data: {params}")
        response = requests.get(endpoint, params=params)
        response.raise_for_status()
        res_body: dict[str, Any] = response.json()
        
        data = res_body.pop("hourly")
        metadata = res_body
        data = pd.DataFrame(data)
        data["latitude"] = metadata["latitude"]
        data["longitude"] = metadata["longitude"]
        
        return metadata, data
    
    def get_air_quality_data(self) -> tuple[dict, pd.DataFrame]:
        endpoint = f"https://air-quality-{self.BASE_URL}/air-quality"
        vars = ','.join(self.AIR_QUALITY_VARS)
        params = {
            "latitude": self.latitude,
            "longitude": self.longitude,
            "hourly": vars,
        }
        response = requests.get(endpoint, params=params)
        response.raise_for_status()
        res_body: dict[str, Any] = response.json()
        
        data = res_body.pop("hourly")
        metadata = res_body
        data = pd.DataFrame(data)
        data["latitude"] = metadata["latitude"]
        data["longitude"] = metadata["longitude"]
        
        return metadata, data

    def get_nearby_stations(self) -> tuple[dict, pd.DataFrame]:
        """No habia de open meteo asi que use meteo stat"""
        endpoint = f"https://{cfg.METEO_STAT_HOST}/stations/nearby"
        headers = {
            "X-RapidAPI-Host": cfg.METEO_STAT_HOST,
            "X-RapidAPI-Key": cfg.METEO_STAT_API_KEY,
        }
        params = {
            "lat": self.latitude,
            "lon": self.longitude,
            "limit": 10,
            "radius": 100000,
        }
        response = requests.get(endpoint, headers=headers, params=params)
        response.raise_for_status()
        res_body: dict[str, Any] = response.json()
        
        data = res_body.pop("data")
        metadata = res_body
        data = pd.DataFrame(data)
        data["generated_at"] = metadata["meta"]["generated"]
        data["name_language"] = data["name"].apply(lambda x: list(x.keys())[0])
        data["name"] = data["name"].apply(lambda x: list(x.values())[0])
        
        return metadata, data


class DeltaLakeLoader:
    pass

