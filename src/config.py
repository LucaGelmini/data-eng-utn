import os
import dotenv
from enum import Enum

dotenv.load_dotenv()

# Api keys and hosts
meteo_stat_host = os.getenv("METEO_STAT_HOST")
meteo_stat_api_key = os.getenv("METEO_STAT_API_KEY")
if not meteo_stat_host or not meteo_stat_api_key:
    raise ValueError("METEO_STAT_HOST and METEO_STAT_API_KEY must be set in the environment variables.")
METEO_STAT_HOST = meteo_stat_host
METEO_STAT_API_KEY = meteo_stat_api_key

# AWS S3 Configuration
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_ALLOW_HTTP = os.getenv("AWS_ALLOW_HTTP", "true").lower()
AWS_S3_ALLOW_UNSAFE_RENAME = os.getenv("AWS_S3_ALLOW_UNSAFE_RENAME", "true").lower()
AWS_CONDITIONAL_PUT = os.getenv("AWS_CONDITIONAL_PUT", "etag")
BUCKET_NAME = os.getenv("BUCKET_NAME", None)


storage_options = {
    'AWS_ENDPOINT_URL': AWS_ENDPOINT_URL,
    'AWS_ACCESS_KEY_ID': AWS_ACCESS_KEY_ID,
    'AWS_SECRET_ACCESS_KEY': AWS_SECRET_ACCESS_KEY,
    'AWS_ALLOW_HTTP': AWS_ALLOW_HTTP,
    'aws_conditional_put': AWS_CONDITIONAL_PUT,
    'AWS_S3_ALLOW_UNSAFE_RENAME': AWS_S3_ALLOW_UNSAFE_RENAME
} if BUCKET_NAME else None

CITIES_COORDINATES_MAP = {
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
    },
    "mendoza": {
        "longitude": -68.827171,
        "latitude": -32.889458,
    },
    "san_miguel_de_tucuman": {
        "longitude": -65.209747,
        "latitude": -26.808285,
    },
    "salta": {
        "longitude": -65.411596,
        "latitude": -24.782127,
    },
    "santa_fe": {
        "longitude": -60.699889,
        "latitude": -31.624176,
    },
    "san_juan": {
        "longitude": -68.536850,
        "latitude": -31.537450,
    },
    "resistencia": {
        "longitude": -58.983890,
        "latitude": -27.451233,
    },
    "corrientes": {
        "longitude": -58.834413,
        "latitude": -27.469770,
    },
    "posadas": {
        "longitude": -55.896240,
        "latitude": -27.367030,
    },
    "bahia_blanca": {
        "longitude": -62.265610,
        "latitude": -38.718430,
    },
    "mar_del_plata": {
        "longitude": -57.557543,
        "latitude": -38.002297,
    },
    "neuquen": {
        "longitude": -68.059053,
        "latitude": -38.951650,
    },
}