import datetime
from importlib.resources import files

import dlt
import duckdb
from dlt.destinations.impl.filesystem.factory import filesystem
from dlt.sources.credentials import AwsCredentials
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator

from ..settings import log


def get_nva_resources(
    client: RESTClient,
    search_params: dict[str, list[str]],
):
    """
    Function to fetch resources from NVA search API.
    The API docs can be found here https://swagger-ui.nva.unit.no/

    Args:
        client: RESTClient instance
        search_params: Dictionary of search parameters (e.g., {'project': '...',})

    Yields:
        Resources from the NVA API
    """
    log.debug(f"Fetching resources with params: {search_params}")
    yield from client.paginate(
        "search/resources",
        method="GET",
        params=search_params,
    )


@dlt.source()
def nva_search_source(
    base_url: str,
    resource_name: str = "resources",
    **kwargs: list[str],
):
    """
    DLT source for fetching resources from NVA search API.

    Args:
        base_url: NVA API base URL
        resource_name: Name for the DLT resource
        **kwargs: Search parameters as keyword arguments (project, publisher, etc.)

    Example:
    # For project-based search:
    nva_search_source(
        resource_name="renew_hydro_resources",
        project=["https://api.nva.unit.no/cristin/project/2732649"]
    )

    # For publisher-based search:
    nva_search_source(
        resource_name="atlantic_salmon_resources",
        publisher=["Vitenskapelig råd for lakseforvaltning"]
    )

    # For more parameters: https://swagger-ui.nva.unit.no/
    """
    client = RESTClient(
        base_url=base_url,
        paginator=JSONLinkPaginator(next_url_path="nextResults"),
        data_selector="hits",
    )

    yield dlt.resource(
        get_nva_resources(client, kwargs),
        name=resource_name,
        write_disposition="replace",
        primary_key="identifier",
        max_table_nesting=1,
    )


def create_s3_credentials(
    endpoint_url: str,
    access_key: str,
    secret_key: str,
    region: str,
) -> AwsCredentials:
    """Create AWS credentials for S3 access."""
    return AwsCredentials(
        s3_url_style="path",
        endpoint_url=endpoint_url,
        aws_secret_access_key=secret_key,
        aws_access_key_id=access_key,
        region_name=region,
    )


def create_pipeline(
    pipeline_name: str,
    bucket: str,
    prefix: str,
    credentials: AwsCredentials,
    region: str,
) -> dlt.Pipeline:
    """Create a DLT pipeline with filesystem destination."""
    return dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=filesystem(
            region_name=region,
            bucket_url=f"s3://{bucket}/{prefix.rstrip('/')}",
            credentials=credentials,
            layout="{table_name}.{ext}",
        ),
        dataset_name="main",
        progress="log",
    )


def setup_duckdb_s3_connection(
    endpoint_url: str,
    access_key: str,
    secret_key: str,
    region: str,
) -> duckdb.DuckDBPyConnection:
    """Set up DuckDB connection with S3 support."""
    con = duckdb.connect()
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")

    # Clean endpoint URL
    clean_endpoint = endpoint_url.replace("https://", "").replace("http://", "")

    con.execute(f"""
        CREATE OR REPLACE SECRET (
            TYPE S3,
            KEY_ID '{access_key}',
            SECRET '{secret_key}',
            ENDPOINT '{clean_endpoint}',
            REGION '{region}',
            URL_STYLE 'path'
        );
    """)

    return con


def write_timestamp(
    con: duckdb.DuckDBPyConnection,
    bucket: str,
    prefix: str,
) -> str:
    """Write last successful run timestamp to S3."""
    timestamp = datetime.datetime.now().isoformat()
    prefix = prefix.rstrip("/")
    con.execute(f"""
        COPY (SELECT '{timestamp}' as last_successful_run)
        TO 's3://{bucket}/{prefix}/last_successful_run.parquet'
        (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    log.info("Last successful run timestamp written", time=timestamp)
    return timestamp


def apply_filter_transformation(
    con: duckdb.DuckDBPyConnection,
    bucket: str,
    prefix: str,
    resource_name: str,
    check_additional_identifiers: bool = True,
) -> None:
    """
    Apply extract_useful_info.sql transformation to parquet data.
    This will extract meaningful information from the raw NVA API response by adding
    new columns based on the logic defined in the SQL file.

    Args:
        con: DuckDB connection
        bucket: S3 bucket name
        prefix: S3 prefix path
        resource_name: Name of the resource to filter
        check_additional_identifiers: Whether to check for additional_identifiers table
    """
    log.info("Applying extract_useful_info.sql transformation to the data")

    normalized_name = resource_name.replace("-", "_")

    resources = con.read_parquet(  # noqa: F841
        f"s3://{bucket}/{prefix}/main/{normalized_name}.parquet"
    )

    additional_identifiers = con.read_parquet(  # noqa: F841
        f"s3://{bucket}/{prefix}/main/{normalized_name}__additional_identifiers.parquet"
    )

    extract_info_query = files("datasync.nva.queries").joinpath(
        "extract_useful_info.sql"
    )
    filtered_resources = con.sql(extract_info_query.read_text())

    filtered_resources.write_parquet(
        f"s3://{bucket}/{prefix}/main/{normalized_name}.parquet",
        compression="zstd",
        overwrite=True,
    )
    log.info(
        "Filter applied and data written back to S3",
        number_of_resources=len(filtered_resources),
    )
