from collections import defaultdict

import dlt
import typer
from dlt.destinations.impl.filesystem.factory import filesystem
from dlt.sources.credentials import AwsCredentials
from dlt.sources.rest_api import rest_api_source
from dlt.sources.rest_api.config_setup import PageNumberPaginator

from .settings import log

app = typer.Typer(help="Export Doffin notices to Parquet in S3 bucket")


@app.command()
def run(
    api_key: str = typer.Option(
        "", envvar="DOFFIN_API_KEY", help="Doffin API subscription key"
    ),
    base_url: str = typer.Option(
        "https://api.doffin.no/public/v2/",
        envvar="DOFFIN_BASE_URL",
        help="Base URL for the Doffin API",
    ),
    page_size: int = typer.Option(
        100, envvar="DOFFIN_PAGE_SIZE", help="Number of hits per page"
    ),
    access_key: str = typer.Option(
        ..., envvar="DOFFIN_AWS_ACCESS_KEY", help="AWS S3 access key"
    ),
    secret_key: str = typer.Option(
        ..., envvar="DOFFIN_AWS_SECRET_KEY", help="AWS S3 secret key"
    ),
    endpoint_url: str = typer.Option(
        ..., envvar="DOFFIN_AWS_ENDPOINT", help="AWS S3 endpoint URL"
    ),
    bucket: str = typer.Option(
        ..., envvar="DOFFIN_AWS_BUCKET", help="AWS S3 bucket name"
    ),
    prefix: str = typer.Option(
        "doffin",
        envvar="DOFFIN_AWS_PREFIX",
        help="AWS S3 prefix (folder path) for storing data",
    ),
    region: str = typer.Option(
        "us-east-1", envvar="DOFFIN_S3_REGION", help="AWS S3 region"
    ),
    params: list[str] | None = typer.Option(  # noqa: B008
        None,
        "--param",
        help="Additional API query parameters (format: key=value). "
        "Supported: sortBy, searchString, type, status, cpvCode, location, "
        "issueDateFrom, issueDateTo, estimatedValueFrom, estimatedValueTo. "
        "Can specify multiple params.",
    ),
):
    """
    Export Doffin notices to Parquet in S3 bucket.

    Examples:
        # Export all notices
        datasync doffin run

        # Filter by status and CPV code
        datasync doffin run \\
            --param status=active \\
            --param cpvCode=45000000 \\
            --param issueDateFrom=2024-01-01
    """
    params_by_key = defaultdict(list)
    params_by_key["numHitsPerPage"].append(str(page_size))

    if params:
        for param_str in params:
            if "=" in param_str:
                key, value = param_str.split("=", 1)
                params_by_key[key.strip()].append(value.strip())
            else:
                log.warning(
                    f"Ignoring invalid param format (expected key=value): {param_str}"
                )

    source = rest_api_source(
        {
            "client": {
                "base_url": base_url,
                "headers": {"Ocp-Apim-Subscription-Key": api_key},
            },
            "resources": [
                {
                    "name": "notices",
                    "endpoint": {
                        "path": "search",
                        "params": params_by_key,
                        "data_selector": "hits",
                        "paginator": PageNumberPaginator(
                            page_param="page",
                            base_page=1,
                            total_path=None,
                            # doffin api doesn't allow more than 1.000 rows
                            # being fetched
                            maximum_page=1000 // page_size,
                        ),
                    },
                }
            ],
        }
    )

    credentials = AwsCredentials(
        s3_url_style="path",
        endpoint_url=endpoint_url,
        aws_secret_access_key=secret_key,
        aws_access_key_id=access_key,
        region_name=region,
    )

    pipeline = dlt.pipeline(
        pipeline_name="doffin",
        destination=filesystem(
            bucket_url=f"s3://{bucket}/{prefix}",
            credentials=credentials,
            layout="{table_name}.{ext}",
        ),
        dataset_name="doffin",
    )

    load_info = pipeline.run(
        source, loader_file_format="parquet", write_disposition="replace"
    )

    row_counts = {}
    if pipeline.last_trace and pipeline.last_trace.last_normalize_info:
        row_counts = pipeline.last_trace.last_normalize_info.row_counts or {}

    notices_rows_loaded = row_counts.get("notices", 0)

    log.info(
        "doffin load complete",
        path=f"s3://{bucket}/{prefix}/notices.parquet",
        load_id=load_info.loads_ids[0] if load_info.loads_ids else None,
        rows_loaded=notices_rows_loaded,
        row_counts=row_counts,
    )


if __name__ == "__main__":
    app()
