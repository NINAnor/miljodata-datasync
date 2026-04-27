import typer

from ..settings import (
    log,
)
from .settings import (
    VALID_PARAMS_NVA_API,
)
from .utils import (
    apply_filter_transformation,
    create_pipeline,
    create_s3_credentials,
    nva_search_source,
    setup_duckdb_s3_connection,
    write_timestamp,
)

app = typer.Typer(help="Fetching NVA resources with customizable search parameters")


@app.command()
def search_resources_api(
    resource_name: str = typer.Option(..., help="Name for the DLT resource"),
    base_url: str = typer.Option(
        default="https://api.nva.unit.no/",
        envvar="NVA_BASE_URL",
        help="Base URL for the NVA API",
    ),
    # search parameters
    filters: list[str] = typer.Option(  # noqa: B008
        [],
        "--filter",
        "-f",
        help="Filter parameters as key=value pairs. Valid keys:"
        f" {VALID_PARAMS_NVA_API}",
    ),
    apply_filter: bool = typer.Option(
        True, help="Apply extract_useful_info.sql transformation"
    ),
    storage_endpoint_url: str = typer.Option(
        ...,
        envvar="NVA_S3_ENDPOINT_URL",
        help="S3 endpoint URL for storage",
    ),
    storage_access_key: str = typer.Option(
        ..., envvar="NVA_S3_ACCESS_KEY", help="S3 access key for storage"
    ),
    storage_secret_key: str = typer.Option(
        ..., envvar="NVA_S3_SECRET_KEY", help="S3 secret key for storage"
    ),
    storage_bucket: str = typer.Option(
        ..., envvar="NVA_S3_BUCKET", help="S3 bucket for storage"
    ),
    storage_prefix: str = typer.Option(
        ..., envvar="NVA_S3_PREFIX", help="S3 prefix for storage"
    ),
    storage_region: str = typer.Option(
        "us-east-1", "--storage-region", help="S3 region for storage"
    ),
):
    """
    Get resources from NVA API

    This will first fetch the data from the NVA API based on the search parameters
    then write the data to the specified S3 location

    Args:
        resource_name: Will be used for the output files on S3, avoid using '-'.
                It will be replaced with _ in the output file names due to DLT naming
                conventions

    Example usage:
    # Fetch by project:
    uv run datasync nva search-resources-api \
        --resource-name "renew-hydro-resources" \
        --filter project="https://api.nva.unit.no/cristin/project/2732649"

    # Fetch by publisher:
    uv run datasync nva search-resources-api \
        --resource-name "salmon-advisory-resources" \
        --filter publisher="Vitenskapelig råd for lakseforvaltning"

    # Multiple filters with category and date range:
    uv run datasync nva search-resources-api \
        --resource-name "nina-journal-articles" \
        --filter publisher=NINA \
        --filter unit=7511.0.0.0 \
        --filter category=JournalArticle \
        --filter published_since=2020-01-01

    # Filter by contributor and publication year:
    uv run datasync nva search-resources-api \
        --resource-name "author-publications" \
        --filter contributor="https://api.nva.unit.no/cristin/person/1773250" \\
        --filter publication_year_since=2020
    """
    log.debug("Starting NVA API search with parameters", filters=filters)
    if storage_prefix == "":
        log.error(
            "Storage prefix must be provided (--storage-prefix) or set via environment"
            " variable NVA_STORAGE_PREFIX"
        )
        raise typer.Exit(code=1)

    if "-" in resource_name:
        resource_name = resource_name.replace("-", "_")
        log.warning(
            "Resource name contains hyphens, which will be replaced with underscores "
            "in the output file names due to DLT naming conventions."
        )

    search_params = {}
    for filter_str in filters:
        key, value = filter_str.split("=", 1)
        key = key.strip()
        value = value.strip()

        if key not in VALID_PARAMS_NVA_API:
            log.error("Valid filters", valid_filters=VALID_PARAMS_NVA_API)
            raise typer.Exit(code=1)

        search_params[key] = value
    log.debug("Parsed search parameters", search_params=search_params)
    if not search_params:
        log.error("Valid filters", valid_filters=VALID_PARAMS_NVA_API)
        raise typer.Exit(code=1)

    log.info(f"Fetching NVA data with search parameters: {search_params}")

    credentials = create_s3_credentials(
        endpoint_url=storage_endpoint_url,
        access_key=storage_access_key,
        secret_key=storage_secret_key,
        region=storage_region,
    )

    pipeline = create_pipeline(
        pipeline_name=f"nva_resources_{resource_name}",
        bucket=storage_bucket,
        prefix=storage_prefix,
        credentials=credentials,
        region=storage_region,
    )

    log.info(f"Fetching resources from {base_url}")
    load_info = pipeline.run(
        nva_search_source(
            base_url=base_url,
            resource_name=resource_name,
            search_params=search_params,
        ),
        write_disposition="replace",
        loader_file_format="parquet",
    )

    log.info("Pipeline run completed", load_info=load_info)

    if apply_filter:
        con = setup_duckdb_s3_connection(
            endpoint_url=storage_endpoint_url,
            access_key=storage_access_key,
            secret_key=storage_secret_key,
            region=storage_region,
        )

        apply_filter_transformation(
            con=con,
            bucket=storage_bucket,
            prefix=storage_prefix,
            resource_name=resource_name,
            check_additional_identifiers=True,
        )

    write_timestamp(con, storage_bucket, storage_prefix)
    con.close()

    log.info("NVA data sync completed")
    log.info(
        f"Data available at: {storage_endpoint_url}/{storage_bucket}/{storage_prefix}/"
        f"main/{resource_name}.parquet"
    )


if __name__ == "__main__":
    app()
