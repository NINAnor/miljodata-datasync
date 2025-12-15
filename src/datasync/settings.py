import pathlib

import environ

from .logger import configure_logger, logging

env = environ.Env()
BASE_DIR = pathlib.Path(__file__).parent.parent.parent

environ.Env.read_env(str(BASE_DIR / ".env"))

log = configure_logger(
    logging.DEBUG if env.bool("DEBUG", default=False) else logging.INFO
)


# NVA
NVA_BASE_URL = env("NVA_BASE_URL", default="https://api.nva.unit.no/")
NVA_DUCKDB_NAME = env("NVA_DUCKDB_FILE_NAME", default="nva_sync")
NVA_INSTITUTION_CODE = env("NVA_INSTITUTION_CODE", default="7511.0.0.0")
NVA_ACCESS_KEY = env("NVA_ACCESS_KEY", default="")
NVA_SECRET_KEY = env("NVA_SECRET_KEY", default="")
NVA_ENDPOINT = env("NVA_ENDPOINT", default="")
NVA_BUCKET = env("NVA_BUCKET", default="")
NVA_PREFIX = env("NVA_PREFIX", default="nva-test")
NVA_REGION = env("NVA_REGION", default="us-east-1")
CRISTIN_DB_PATH = env("CRISTIN_DB_PATH", default="")


# UBW
UBW_BASE_URL = env("UBW_BASE_URL", default="")
UBW_BASIC_AUTH = env("UBW_BASIC_AUTH", default="")
UBW_ACCESS_KEY = env("UBW_ACCESS_KEY", default="")
UBW_SECRET_KEY = env("UBW_SECRET_KEY", default="")
UBW_AWS_ENDPOINT = env("UBW_AWS_ENDPOINT", default="")
UBW_BUCKET = env("UBW_BUCKET", default="")
UBW_PREFIX = env("UBW_PREFIX", default="")


# DMS
DMS_DATASETS_BASE = env("DMS_DATASETS_BASE", default="")
DMS_ACCESS_KEY = env("DMS_ACCESS_KEY", default="")
DMS_SECRET_KEY = env("DMS_SECRET_KEY", default="")
DMS_AWS_ENDPOINT = env("DMS_AWS_ENDPOINT", default="")
DMS_BUCKET = env("DMS_BUCKET", default="")
DMS_GEOAPI_PREFIX = env("DMS_GEOAPI_PREFIX", default="/geoapi/")
DMS_CSW_PREFIX = env("DMS_CSW_PREFIX", default="/csw/dms-metadata.parquet")
