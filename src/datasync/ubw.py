import dlt
import typer
from dlt.destinations.impl.filesystem.factory import filesystem
from dlt.sources.credentials import AwsCredentials
from dlt.sources.rest_api import rest_api_source

from .settings import log

app = typer.Typer(help="Export UBW APIs to Parquet in S3 bucket")


@app.command()
def run(
    access_key: str = typer.Option(
        ...,
        envvar="UBW_ACCESS_KEY",
        help="AWS S3 access key",
    ),
    secret_key: str = typer.Option(
        ...,
        envvar="UBW_SECRET_KEY",
        help="AWS S3 secret key",
    ),
    endpoint_url: str = typer.Option(
        ...,
        envvar="UBW_AWS_ENDPOINT",
        help="AWS S3 endpoint URL",
    ),
    bucket: str = typer.Option(
        ...,
        envvar="UBW_BUCKET",
        help="AWS S3 bucket name",
    ),
    prefix: str = typer.Option(
        ...,
        envvar="UBW_PREFIX",
        help="AWS S3 prefix (folder path) for storing data",
    ),
    base_url: str = typer.Option(
        ...,
        envvar="UBW_BASE_URL",
        help="Base URL for the UBW API",
    ),
    auth: str = typer.Option(
        ...,
        envvar="UBW_BASIC_AUTH",
        help="Basic auth credentials for the UBW API",
    ),
):
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
                {
                    "name": "employees",
                    "endpoint": {
                        "path": "objects/ninaansattdhapis",
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
