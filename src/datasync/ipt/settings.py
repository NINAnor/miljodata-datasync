import os
import pathlib

import duckdb
import fsspec
import s3fs
from jinja2 import Environment, FileSystemLoader, select_autoescape

from ..settings import env
from ..settings import log as logging

IPT_URL = env.str("IPT_URL", default="https://ipt.nina.no")
CACHE_PATH: str = env.str("IPT_CACHE_PATH", default=".dwca_cache/")
CONNECTION: str = ":memory:"
RESOURCES_PREFIX: str = "/ipt/datasets/"
GEOAPI_PUBLISH_URL: str = env.str("IPT_GEOAPI_PUBLISH_URL", default=None)
OGC_RECORDS_PUBLISH_URL: str = env.str("IPT_OGC_RECORDS_PUBLISH_URL", default=None)
S3_URL_STYLE: str = "path"
S3_BUCKET: str = env.str("IPT_AWS_BUCKET", default=None)
AWS_SECRET_KEY: str = env.str("IPT_AWS_SECRET_KEY", default=None)
AWS_ENDPOINT_URL: str = env.str("IPT_AWS_ENDPOINT_URL", default=None)
AWS_ACCESS_KEY: str = env.str("IPT_AWS_ACCESS_KEY", default=None)
DMS_PROJECT_ID: str = env.str("IPT_DMS_PROJECT_ID", default="911200")


os.environ["AWS_REQUEST_CHECKSUM_CALCULATION"] = "WHEN_REQUIRED"
os.environ["AWS_RESPONSE_CHECKSUM_VALIDATION"] = "WHEN_REQUIRED"

templates = Environment(
    loader=FileSystemLoader(pathlib.Path(__file__).parent / "templates"),
    autoescape=select_autoescape(),
)

fs = fsspec.filesystem("file")
s3 = s3fs.S3FileSystem(
    endpoint_url=AWS_ENDPOINT_URL,
    key=AWS_ACCESS_KEY,
    secret=AWS_SECRET_KEY,
)

conn = duckdb.connect(CONNECTION)

if not pathlib.Path(CACHE_PATH).exists():
    logging.info("cache folder not found, try creating it")
    pathlib.Path(CACHE_PATH).mkdir(parents=True)

logging.info("install extensions")
conn.execute("""
    INSTALL zipfs FROM community;
""").fetchall()
conn.install_extension("spatial")
conn.install_extension("httpfs")

logging.info("load extensions")
conn.load_extension("zipfs")
conn.load_extension("spatial")
conn.load_extension("httpfs")

logging.info(
    "load secrets",
)
conn.execute(f"""
    CREATE OR REPLACE SECRET secret (
        TYPE s3,
        PROVIDER config,
        KEY_ID '{AWS_ACCESS_KEY}',
        SECRET '{AWS_SECRET_KEY}',
        ENDPOINT '{AWS_ENDPOINT_URL.replace(r"https://", "")}',
        URL_STYLE '{S3_URL_STYLE}',
        SCOPE 's3://{S3_BUCKET}'
    );
""").fetchall()
