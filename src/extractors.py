from requests import Request, Session
from typing import ClassVar
import pandas as pd
from abc import ABC, abstractmethod
import pendulum
from datetime import date

import src.config as cfg


class BaseAuth(ABC):
    @abstractmethod
    def authenticate(self, session: Session): ...

class BaseExractor(ABC):
    request: ClassVar[Request]
    session: ClassVar[Session]
    
    def __init__(self, longitude: float, latitude: float) -> None:
        if not self.request:
            raise ValueError("Subclasses must define a 'request' class variable")
        self.longitude = longitude
        self.latitude = latitude
        self.session.params = {
            "longitude": self.longitude,
            "latitude": self.latitude,
        }
        
    @abstractmethod
    def wrangler(self, raw_data: dict) -> pd.DataFrame: ...
        
    def _extract_raw(self)->dict:          
        prepped = self.session.prepare_request(self.request)
        response = self.session.send(prepped)
        response.raise_for_status()
        data = response.json()
        return data
    
    def extract(self) -> pd.DataFrame:
        raw_data = self._extract_raw()
        return self.wrangler(raw_data)

class ForecastExtractor(BaseExractor):
    """Extractor for weather forecast data from MeteoStat API.
    
    Each extracion get 7 days of forecast data from 00:00 of the current day.
    """
    

    session = Session()
    request = Request(
        method="GET",
        url="https://api.open-meteo.com/v1/forecast",
        params={
            "hourly": ",".join([
                "temperature_2m",
                "precipitation",
                "windspeed_10m"
            ]),
        }
    )
    
    def wrangler(self, raw_data: dict) -> pd.DataFrame:
        data = raw_data.pop("hourly")
        metadata = raw_data
        
        data = pd.DataFrame(data)
        data["station_coordinates"] = [(metadata["latitude"], metadata["longitude"])] * len(data)
        data["requested_coordinates"] = [(self.latitude, self.longitude)] * len(data)
        data["date_retrieved"] = pendulum.now().to_date_string()
        return data

class HistoricalExtractor(BaseExractor):
    """Extractor for historical weather data from MeteoStat API.
    
     Each extraction requires start_date and end_date parameters in 'YYYY-MM-DD' format.
    """

    session = Session()
    request = Request(
        method="GET",
        url="https://archive-api.open-meteo.com/v1/archive",
        params={
            "hourly": ",".join([
                "temperature_2m",
                "precipitation",
                "windspeed_10m"
            ]),
        }
    )
    
    def __init__(
        self,
        start_date: date,
        end_date: date,
        longitude: float,
        latitude: float
        ) -> None:
        super().__init__(longitude, latitude)
        if start_date > end_date:
            raise ValueError("start_date must be earlier than or equal to end_date")
        
        self.request.params["start_date"] = pendulum.instance(start_date).to_date_string()
        self.request.params["end_date"] = pendulum.instance(end_date).to_date_string()
        
    
    def wrangler(self, raw_data: dict) -> pd.DataFrame:
        data = raw_data.pop("hourly")
        metadata = raw_data
        
        data = pd.DataFrame(data)
        data["station_coordinates"] = [(metadata["latitude"], metadata["longitude"])] * len(data)
        data["requested_coordinates"] = [(self.latitude, self.longitude)] * len(data)
        data["date_retrieved"] = pendulum.now().to_date_string()
        return data

class AirQualityExtractor(BaseExractor):
    """Extractor for air quality data from MeteoStat API.
    
    Each extraction get 7 days forecast of air quality data from 00:00 of the current day.
    """

    session = Session()
    request = Request(
        method="GET",
        url=f"https://air-quality-api.open-meteo.com/v1/air-quality",
        params={
            "hourly": ",".join([
                "pm10",
                "pm2_5",
                "carbon_monoxide"
            ]),
        }
    )
    
    def wrangler(self, raw_data: dict) -> pd.DataFrame:
        data = raw_data.pop("hourly")
        metadata = raw_data
        
        data = pd.DataFrame(data)
        data["station_coordinates"] = [(metadata["latitude"], metadata["longitude"])] * len(data)
        data["requested_coordinates"] = [(self.latitude, self.longitude)] * len(data)
        data["date_retrieved"] = pendulum.now().to_date_string()
        return data

class RapidAPIAuthenticator(BaseAuth):
    host: ClassVar[str] = cfg.METEO_STAT_HOST
    api_key: ClassVar[str] = cfg.METEO_STAT_API_KEY
    
    def authenticate(self, session: Session):
        session.headers.update({
            "X-RapidAPI-Host": self.host,
            "X-RapidAPI-Key": self.api_key,
        })
        
class NearestStationsExtractor(RapidAPIAuthenticator, BaseExractor):
    """Extractor for nearest weather stations from MeteoStat API.
    """

    session = Session()
    request = Request(
        method="GET",
        url=f"https://{cfg.METEO_STAT_HOST}/stations/nearby",
        params={
            "limit": 50,
            "radius": 100000,
        }
    )
    
    def __init__(self, longitude, latitude) -> None:
        super().__init__(longitude, latitude)
        self.session.params = {
            "lat": self.latitude,
            "lon": self.longitude,
        }
        self.authenticate(self.session)
    
    def wrangler(self, raw_data: dict) -> pd.DataFrame:
        data = raw_data.pop("data")
        metadata = raw_data
        
        data = pd.DataFrame(data)
        data["generated_at"] = metadata["meta"]["generated"]
        data["name_language"] = data["name"].apply(lambda x: list(x.keys())[0])
        data["name"] = data["name"].apply(lambda x: list(x.values())[0])
        data["query_coordinates"] = [str((self.latitude, self.longitude))] * len(data)
        data["date_retrieved"] = pendulum.now().to_date_string()
        
        return data
    
if __name__ == "__main__":
    BUENOS_AIRES_LONGITUDE = -58.417309
    BUENOS_AIRES_LATITUDE = -34.611778
    
    forecast = ForecastExtractor(BUENOS_AIRES_LONGITUDE, BUENOS_AIRES_LATITUDE)
    historic = HistoricalExtractor(
        start_date=pendulum.now().subtract(days=7).date(),
        end_date=pendulum.now().date(),
        longitude=BUENOS_AIRES_LONGITUDE,
        latitude=BUENOS_AIRES_LATITUDE
    )
    air_quality = AirQualityExtractor(BUENOS_AIRES_LONGITUDE, BUENOS_AIRES_LATITUDE)
    stations = NearestStationsExtractor(BUENOS_AIRES_LONGITUDE, BUENOS_AIRES_LATITUDE)
    
    print("Forecast info:")
    print(forecast.extract().info())
    print("\nHistorical info:")
    print(historic.extract().info())
    print("\nAir Quality info:")
    print(air_quality.extract().info())
    print("\nNearest Stations info:")
    print(stations.extract().info())