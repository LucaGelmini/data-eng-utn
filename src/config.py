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
AWS_CONDITIONAL_PUT = os.getenv("AWS_CONDITIONAL_PUT", "none")
BUCKET_NAME = os.getenv("BUCKET_NAME", None)

