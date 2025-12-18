import dlt
import typer
from dlt.destinations.impl.filesystem.factory import filesystem
from dlt.sources.credentials import AwsCredentials
from dlt.sources.rest_api import rest_api_source

from .settings import (
    env,
    log,
)

app = typer.Typer()

UBW_BASE_URL = env("UBW_BASE_URL", default="")
UBW_BASIC_AUTH = env("UBW_BASIC_AUTH", default="")
UBW_ACCESS_KEY = env("UBW_ACCESS_KEY", default="")
UBW_SECRET_KEY = env("UBW_SECRET_KEY", default="")
UBW_AWS_ENDPOINT = env("UBW_AWS_ENDPOINT", default="")
UBW_BUCKET = env("UBW_BUCKET", default="")
UBW_PREFIX = env("UBW_PREFIX", default="")


@app.command()
def run(
    access_key: str = UBW_ACCESS_KEY,
    secret_key: str = UBW_SECRET_KEY,
    endpoint_url: str = UBW_AWS_ENDPOINT,
    bucket: str = UBW_BUCKET,
    prefix: str = UBW_PREFIX,
    base_url: str = UBW_BASE_URL,
    auth: str = UBW_BASIC_AUTH,
):
    log.info(base_url)
    source = rest_api_source(
        {
            "client": {
                "base_url": base_url,
                "headers": {
                    "Authorization": f"Basic {auth}",
                    "Accept": "application/json",
                },
            },
            "resources": [
                {
                    "name": "budget",
                    "endpoint": {
                        "path": "objects/ninaprojectsbudmdapis",
                        "paginator": "single_page",
                    },
                },
                {
                    "name": "projects",
                    "endpoint": {
                        "path": "objects/ninaprojectsmdapis",
                        "paginator": "single_page",
                    },
                },
                {
                    "name": "resources",
                    "endpoint": {
                        "path": "objects/ninaprojectresmdapis",
                        "paginator": "single_page",
                    },
                },
                {
                    "name": "units",
                    "endpoint": {
                        "path": "objects/ninaorgunitmdapis",
                        "paginator": "single_page",
                    },
                },
            ],
        }
    )

    credentials = AwsCredentials(
        s3_url_style="path",
        endpoint_url=endpoint_url,
        aws_secret_access_key=secret_key,
        aws_access_key_id=access_key,
    )

    pipeline = dlt.pipeline(
        pipeline_name="ubw",
        destination=filesystem(
            bucket_url=f"s3://{bucket}/" + prefix,
            credentials=credentials,
            layout="{table_name}.{ext}",
        ),
        dataset_name="ubw",
    )

    log.info(
        pipeline.run(source, loader_file_format="parquet", write_disposition="replace")
    )


if __name__ == "__main__":
    app()
