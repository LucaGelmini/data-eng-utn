import pygeohash as gh
import pandas as pd
from deltalake import DeltaTable
from typing import ClassVar
from abc import ABC, abstractmethod
import src.config as cfg

class BaseTransformer(ABC):
    source_layer: ClassVar[str] = 'bronze'
    bucket_name: ClassVar[str | None] = cfg.BUCKET_NAME

    def __init__(self, storage_options: dict | None = None, city: str | None = None):
        self.storage_options = storage_options
        self.city = city

    def _get_table_path(self, layer: str, table_name: str) -> str:
        """Build the path to a Delta table."""
        if self.storage_options:
            return f"s3://{self.bucket_name}/{layer}/{table_name}"
        return f"./out/{layer}/{table_name}"

    @property
    @abstractmethod
    def datasets(self) -> dict[str, DeltaTable]:
        """Read the deltalake to extract the datasets to use in the process.

        Returns a dict with the name of the dataset and its delta table object.
        """

    @abstractmethod
    def transform(self) -> pd.DataFrame:
        """Loads and transforms the delta tables.

        Returns a pandas dataframe"""


class WeatherSummaryTransformer(BaseTransformer):
    """Transforms historical weather data into daily aggregated summaries.

    Enrichments:
    - Daily aggregations (min, max, avg temperature, total precipitation, avg wind speed)
    - Temperature range calculation
    - Geohash for location indexing
    """

    @property
    def datasets(self) -> dict[str, DeltaTable]:
        historical_path = self._get_table_path(self.source_layer, 'historical')
        return {
            'historical': DeltaTable(historical_path, storage_options=self.storage_options)
        }

    def transform(self) -> pd.DataFrame:
        historical_df = self.datasets['historical'].to_pandas()

        if self.city:
            historical_df = historical_df[historical_df['city'] == self.city]

        if historical_df.empty:
            return pd.DataFrame(columns=[
                'date', 'city', 'geohash', 'temp_min', 'temp_max', 'temp_avg',
                'total_precipitation', 'avg_windspeed', 'latitude', 'longitude',
                'date_retrieved', 'temp_range'
            ])

        historical_df['time'] = pd.to_datetime(historical_df['time'])
        historical_df['date'] = pd.to_datetime(historical_df['time']).dt.date

        historical_df['latitude'] = historical_df['station_coordinates'].apply(lambda x: x[0])
        historical_df['longitude'] = historical_df['station_coordinates'].apply(lambda x: x[1])

        historical_df['geohash'] = historical_df.apply(
            lambda row: gh.encode(row['latitude'], row['longitude'], precision=7),
            axis=1
        )

        agg_df = historical_df.groupby(['date', 'city', 'geohash']).agg({
            'temperature_2m': ['min', 'max', 'mean'],
            'precipitation': 'sum',
            'windspeed_10m': 'mean',
            'latitude': 'first',
            'longitude': 'first',
            'date_retrieved': 'first'
        }).reset_index()

        agg_df.columns = [
            'date', 'city', 'geohash',
            'temp_min', 'temp_max', 'temp_avg',
            'total_precipitation', 'avg_windspeed',
            'latitude', 'longitude', 'date_retrieved'
        ]

        agg_df['temp_range'] = agg_df['temp_max'] - agg_df['temp_min']
        agg_df['date'] = agg_df['date'].astype(str)

        return agg_df


class AirQualityDailyTransformer(BaseTransformer):
    """Transforms air quality forecast data into daily aggregated summaries.

    Note: The bronze.air_quality table contains forecast data (future dates).

    Enrichments:
    - Daily aggregations (min, max, avg for pollutants)
    - Air quality index calculation (simplified)
    - Geohash for location indexing
    """

    @property
    def datasets(self) -> dict[str, DeltaTable]:
        air_quality_path = self._get_table_path(self.source_layer, 'air_quality')
        return {
            'air_quality': DeltaTable(air_quality_path, storage_options=self.storage_options)
        }

    def transform(self) -> pd.DataFrame:
        air_quality_df = self.datasets['air_quality'].to_pandas()

        if self.city:
            air_quality_df = air_quality_df[air_quality_df['city'] == self.city]

        if air_quality_df.empty:
            return pd.DataFrame(columns=[
                'date', 'city', 'geohash', 'pm10_min', 'pm10_max', 'pm10_avg',
                'pm2_5_min', 'pm2_5_max', 'pm2_5_avg', 'co_min', 'co_max', 'co_avg',
                'latitude', 'longitude', 'date_retrieved', 'aqi_simplified'
            ])

        air_quality_df['time'] = pd.to_datetime(air_quality_df['time'])
        air_quality_df['date'] = pd.to_datetime(air_quality_df['time']).dt.date

        air_quality_df['latitude'] = air_quality_df['station_coordinates'].apply(lambda x: x[0])
        air_quality_df['longitude'] = air_quality_df['station_coordinates'].apply(lambda x: x[1])

        air_quality_df['geohash'] = air_quality_df.apply(
            lambda row: gh.encode(row['latitude'], row['longitude'], precision=7),
            axis=1
        )

        agg_df = air_quality_df.groupby(['date', 'city', 'geohash']).agg({
            'pm10': ['min', 'max', 'mean'],
            'pm2_5': ['min', 'max', 'mean'],
            'carbon_monoxide': ['min', 'max', 'mean'],
            'latitude': 'first',
            'longitude': 'first',
            'date_retrieved': 'first'
        }).reset_index()

        agg_df.columns = [
            'date', 'city', 'geohash',
            'pm10_min', 'pm10_max', 'pm10_avg',
            'pm2_5_min', 'pm2_5_max', 'pm2_5_avg',
            'co_min', 'co_max', 'co_avg',
            'latitude', 'longitude', 'date_retrieved'
        ]

        # IMPORTANT: This is a SIMPLIFIED AQI calculation for demonstration purposes only.
        # AQI (Air Quality Index) is the index used by the U.S. Environmental Protection
        # Agency (EPA) for air quality monitoring.
        # See the non simplified index methodology here:
        # https://document.airnow.gov/technical-assistance-document-for-the-reporting-of-daily-air-quailty.pdf
        
        agg_df['aqi_simplified'] = agg_df['pm2_5_avg'].apply(
            lambda x: min(100, (x / 25.0) * 100) if pd.notnull(x) else None
        )

        agg_df['date'] = agg_df['date'].astype(str)

        return agg_df


class WeatherForecastTransformer(BaseTransformer):
    """Transforms forecast weather data into daily aggregated summaries.

    Note: The bronze.forecast table contains forecast data (future dates).

    Enrichments:
    - Daily aggregations for forecast data
    - Temperature range calculation
    - Geohash for location indexing
    """

    @property
    def datasets(self) -> dict[str, DeltaTable]:
        forecast_path = self._get_table_path(self.source_layer, 'forecast')
        return {
            'forecast': DeltaTable(forecast_path, storage_options=self.storage_options)
        }

    def transform(self) -> pd.DataFrame:
        forecast_df = self.datasets['forecast'].to_pandas()

        if self.city:
            forecast_df = forecast_df[forecast_df['city'] == self.city]

        if forecast_df.empty:
            return pd.DataFrame(columns=[
                'date', 'city', 'geohash', 'temp_min', 'temp_max', 'temp_avg',
                'total_precipitation', 'avg_windspeed', 'latitude', 'longitude',
                'date_retrieved', 'temp_range'
            ])

        forecast_df['time'] = pd.to_datetime(forecast_df['time'])
        forecast_df['date'] = pd.to_datetime(forecast_df['time']).dt.date

        forecast_df['latitude'] = forecast_df['station_coordinates'].apply(lambda x: x[0])
        forecast_df['longitude'] = forecast_df['station_coordinates'].apply(lambda x: x[1])

        forecast_df['geohash'] = forecast_df.apply(
            lambda row: gh.encode(row['latitude'], row['longitude'], precision=7),
            axis=1
        )

        agg_df = forecast_df.groupby(['date', 'city', 'geohash']).agg({
            'temperature_2m': ['min', 'max', 'mean'],
            'precipitation': 'sum',
            'windspeed_10m': 'mean',
            'latitude': 'first',
            'longitude': 'first',
            'date_retrieved': 'first'
        }).reset_index()

        agg_df.columns = [
            'date', 'city', 'geohash',
            'temp_min', 'temp_max', 'temp_avg',
            'total_precipitation', 'avg_windspeed',
            'latitude', 'longitude', 'date_retrieved'
        ]

        agg_df['temp_range'] = agg_df['temp_max'] - agg_df['temp_min']
        agg_df['date'] = agg_df['date'].astype(str)

        return agg_df


class ForecastCombinedTransformer(BaseTransformer):
    """Joins weather forecast and air quality forecast with health alerts.

    Gold Layer Transformer - Business-ready data with actionable insights.

    Enrichments:
    - Combined forecast metrics
    - Health alert categories based on AQI and weather conditions
    - Allergy risk assessment
    - Outdoor activity score (0-100)
    """

    source_layer = 'silver'

    @property
    def datasets(self) -> dict[str, DeltaTable]:
        weather_path = self._get_table_path(self.source_layer, 'weather_forecast')
        air_quality_path = self._get_table_path(self.source_layer, 'air_quality_daily')
        return {
            'weather': DeltaTable(weather_path, storage_options=self.storage_options),
            'air_quality': DeltaTable(air_quality_path, storage_options=self.storage_options)
        }

    def _calculate_health_alert(self, row: pd.Series) -> str:
        """Calculate health alert level based on AQI and weather."""
        aqi = row['aqi_simplified']
        temp = row['temp_avg']

        if aqi >= 75:
            return 'HIGH_ALERT'
        elif aqi >= 50 and (temp > 30 or temp < 10):
            return 'MODERATE_ALERT'
        elif aqi >= 50 or temp > 35 or temp < 5:
            return 'LOW_ALERT'
        else:
            return 'GOOD'

    def _calculate_allergy_risk(self, row: pd.Series) -> str:
        """Calculate allergy risk based on air quality and weather."""
        pm25 = row['pm2_5_avg']
        pm10 = row['pm10_avg']
        wind = row['avg_windspeed']

        if pm10 > 50 and wind > 20:
            return 'HIGH'
        elif pm25 > 15 or (pm10 > 30 and wind > 10):
            return 'MODERATE'
        else:
            return 'LOW'

    def _calculate_outdoor_score(self, row: pd.Series) -> int:
        """Calculate outdoor activity score (0-100, higher is better)."""
        score = 100

        score -= row['aqi_simplified'] * 0.5

        if row['temp_avg'] > 30:
            score -= (row['temp_avg'] - 30) * 2
        elif row['temp_avg'] < 15:
            score -= (15 - row['temp_avg']) * 1.5

        if row['total_precipitation'] > 0:
            score -= min(20, row['total_precipitation'] * 10)

        if row['avg_windspeed'] > 30:
            score -= (row['avg_windspeed'] - 30)

        return max(0, min(100, int(score)))

    def transform(self) -> pd.DataFrame:
        weather_df = self.datasets['weather'].to_pandas()
        air_quality_df = self.datasets['air_quality'].to_pandas()

        if self.city:
            weather_df = weather_df[weather_df['city'] == self.city]
            air_quality_df = air_quality_df[air_quality_df['city'] == self.city]

        if weather_df.empty or air_quality_df.empty:
            return pd.DataFrame(columns=[
                'date', 'city', 'geohash', 'temp_min', 'temp_max', 'temp_avg',
                'temp_range', 'total_precipitation', 'avg_windspeed', 'pm10_avg',
                'pm2_5_avg', 'co_avg', 'aqi_simplified', 'health_alert',
                'allergy_risk', 'outdoor_score', 'latitude', 'longitude',
                'date_retrieved'
            ])

        joined_df = pd.merge(
            weather_df,
            air_quality_df,
            on=['date', 'city'],
            how='inner',
            suffixes=('_weather', '_air')
        )

        result_df = joined_df[[
            'date', 'city',
            'geohash_weather',
            'temp_min', 'temp_max', 'temp_avg', 'temp_range',
            'total_precipitation', 'avg_windspeed',
            'pm10_avg', 'pm2_5_avg', 'co_avg',
            'aqi_simplified',
            'latitude_weather', 'longitude_weather',
            'date_retrieved_weather'
        ]].copy()

        result_df = result_df.rename(columns={
            'geohash_weather': 'geohash',
            'latitude_weather': 'latitude',
            'longitude_weather': 'longitude',
            'date_retrieved_weather': 'date_retrieved'
        })

        result_df['health_alert'] = result_df.apply(self._calculate_health_alert, axis=1)
        result_df['allergy_risk'] = result_df.apply(self._calculate_allergy_risk, axis=1)
        result_df['outdoor_score'] = result_df.apply(self._calculate_outdoor_score, axis=1)

        return result_df


class HourlyHistoricalAnalysisTransformer(BaseTransformer):
    """Analyzes historical data by hour across days.

    Enrichments:
    - Hour-of-day aggregations (24 hours)
    - Temperature patterns by hour
    - Multi-day hourly statistics
    """

    @property
    def datasets(self) -> dict[str, DeltaTable]:
        historical_path = self._get_table_path(self.source_layer, 'historical')
        return {
            'historical': DeltaTable(historical_path, storage_options=self.storage_options)
        }

    def transform(self) -> pd.DataFrame:
        historical_df = self.datasets['historical'].to_pandas()

        if self.city:
            historical_df = historical_df[historical_df['city'] == self.city]

        if historical_df.empty:
            return pd.DataFrame(columns=[
                'hour', 'city', 'temp_min', 'temp_max', 'temp_avg',
                'precipitation_avg', 'windspeed_avg', 'days_count'
            ])

        historical_df['time'] = pd.to_datetime(historical_df['time'])
        historical_df['hour'] = historical_df['time'].dt.hour # type: ignore
        historical_df['date'] = historical_df['time'].dt.date # type: ignore

        agg_df = historical_df.groupby(['hour', 'city']).agg({
            'temperature_2m': ['min', 'max', 'mean'],
            'precipitation': 'mean',
            'windspeed_10m': 'mean',
            'date': 'nunique'
        }).reset_index()

        agg_df.columns = [
            'hour', 'city',
            'temp_min', 'temp_max', 'temp_avg',
            'precipitation_avg', 'windspeed_avg',
            'days_count'
        ]

        return agg_df
