import os
import dotenv
from enum import Enum

dotenv.load_dotenv()

# Api keys and hosts
METEO_STAT_HOST = os.getenv("METEO_STAT_HOST")
METEO_STAT_API_KEY = os.getenv("METEO_STAT_API_KEY")

# AWS S3 Configuration
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_ALLOW_HTTP = os.getenv("AWS_ALLOW_HTTP", "true").lower()
AWS_S3_ALLOW_UNSAFE_RENAME = os.getenv("AWS_S3_ALLOW_UNSAFE_RENAME", "true").lower()
AWS_CONDITIONAL_PUT = os.getenv("AWS_CONDITIONAL_PUT", "none")

