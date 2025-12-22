import pathlib

import duckdb
import fsspec
from jinja2 import Environment, FileSystemLoader, select_autoescape
from s3fs import S3FileSystem

from ..settings import env
from ..settings import log as logging

IPT_URL = env.str("IPT_URL", default="https://ipt.nina.no")
CACHE_PATH: str = env.str("IPT_CACHE_PATH", default=".dwca_cache/")
CONNECTION: str = ":memory:"
RESOURCES_PREFIX: str = "/ipt/datasets/"
GEOAPI_PATH: str = "/geoapi/ipt-resources.json"
GEOAPI_PUBLISH_URL: str = env.str("GEOAPI_PUBLISH_URL", default=None)
CSW_PATH: str = "/csw/ipt-metadata.parquet"
S3_URL_STYLE: str = "path"
S3_BUCKET: str = env.str("AWS_BUCKET")
AWS_SECRET_KEY: str = env.str("IPT_AWS_SECRET_KEY")
AWS_ENDPOINT_URL: str = env.str("IPT_AWS_ENDPOINT_URL")
AWS_ACCESS_KEY: str = env.str("IPT_AWS_ACCESS_KEY")
DMS_PROJECT_ID: str = env.str("IPT_DMS_PROJECT_ID", default="911200")

templates = Environment(
    loader=FileSystemLoader(pathlib.Path(__file__).parent / "templates"),
    autoescape=select_autoescape(),
)

fs = fsspec.filesystem("file")
s3fs = S3FileSystem(endpoint_url=AWS_ENDPOINT_URL, anon=True)

conn = duckdb.connect(CONNECTION)


def get_connection():
    return conn


if not pathlib.Path(CACHE_PATH).exists():
    logging.info("cache folder not found, try creating it")
    pathlib.Path(CACHE_PATH).mkdir(parents=True)


def duckdb_install_extensions():
    logging.info("install extensions")
    conn.execute("""
        INSTALL zipfs FROM community;
    """).fetchall()
    conn.install_extension("spatial")
    conn.install_extension("httpfs")


def duckdb_load_extensions():
    logging.info("load extensions")
    conn.load_extension("zipfs")
    conn.load_extension("spatial")
    conn.load_extension("httpfs")


def duckdb_load_s3_credentials():
    logging.info(
        "load secrets",
    )
    conn.execute(f"""
        CREATE OR REPLACE SECRET secret (
            TYPE s3,
            REGION 'eu-west-1',
            KEY_ID '{AWS_ACCESS_KEY}',
            SECRET '{AWS_SECRET_KEY}',
            ENDPOINT '{AWS_ENDPOINT_URL.replace(r"https://", "")}',
            URL_STYLE '{S3_URL_STYLE}'
        );
    """).fetchall()
