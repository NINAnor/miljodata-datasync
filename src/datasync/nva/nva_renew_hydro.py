# used to display data on renewhydro.nina.no

import datetime
from importlib.resources import files

import dlt
import duckdb
import typer
from dlt.destinations.impl.filesystem.factory import filesystem
from dlt.sources.credentials import AwsCredentials
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator

from ..settings import (
    env,
    log,
)

NVA_BASE_URL = env("NVA_BASE_URL", default="https://api.nva.unit.no/")
NVA_RENEW_HYDRO_STORAGE_ACCESS_KEY = env(
    "NVA_RENEW_HYDRO_STORAGE_ACCESS_KEY", default=""
)
NVA_RENEW_HYDRO_STORAGE_SECRET_KEY = env(
    "NVA_RENEW_HYDRO_STORAGE_SECRET_KEY", default=""
)
NVA_RENEW_HYDRO_STORAGE_BUCKET = env("NVA_RENEW_HYDRO_STORAGE_BUCKET", default="")

NVA_RENEW_HYDRO_STORAGE_PREFIX = env(
    "NVA_RENEW_HYDRO_STORAGE_PREFIX", default="nva-renew-hydro"
)
NVA_RENEW_HYDRO_STORAGE_REGION = env(
    "NVA_RENEW_HYDRO_STORAGE_REGION", default="us-east-1"
)
NVA_RENEW_HYDRO_STORAGE_ENDPOINT_URL = env(
    "NVA_RENEW_HYDRO_STORAGE_ENDPOINT_URL", default=""
)
NVA_RENEW_HYDRO_PATH_PARQUET_OUTPUT = env(
    "NVA_RENEW_HYDRO_PATH_PARQUET_OUTPUT", default=""
)

app = typer.Typer(
    help="Create NINA specific tables from NVA data and export to parquet on a "
    "S3 Bucket"
)


def get_renew_hydro_resources(
    client: RESTClient,
    project_id: str = "https://api.nva.unit.no/cristin/project/2732649",
):
    """Fetch resources from NVA API for a specific project."""
    log.info(f"Fetching resources for project {project_id}")
    yield from client.paginate(
        "search/resources",
        method="GET",
        params={
            "project": project_id,
        },
    )


@dlt.source()
def nva_renew_hydro_source(
    base_url: str = NVA_BASE_URL,
    project_id: str = "https://api.nva.unit.no/cristin/project/2732649",
):
    """DLT source for fetching Renew Hydro resources from NVA API."""
    client = RESTClient(
        base_url=base_url,
        paginator=JSONLinkPaginator(next_url_path="nextResults"),
        data_selector="hits",
    )

    yield dlt.resource(
        get_renew_hydro_resources(client, project_id),
        name="renew_hydro_resources",
        write_disposition="replace",
        primary_key="identifier",
        max_table_nesting=1,
    )


@app.command()
def renew_hydro_filter_data(
    base_url: str = NVA_BASE_URL,
    project_id: str = "https://api.nva.unit.no/cristin/project/2732649",
    storage_access_key: str = NVA_RENEW_HYDRO_STORAGE_ACCESS_KEY,
    storage_secret_key: str = NVA_RENEW_HYDRO_STORAGE_SECRET_KEY,
    storage_bucket: str = NVA_RENEW_HYDRO_STORAGE_BUCKET,
    storage_prefix: str = NVA_RENEW_HYDRO_STORAGE_PREFIX,
    storage_endpoint_url: str = NVA_RENEW_HYDRO_STORAGE_ENDPOINT_URL,
    storage_region: str = NVA_RENEW_HYDRO_STORAGE_REGION,
):
    """
    Fetch resources from NVA API for Renew Hydro project using dlt
    and export to parquet files on S3 Bucket.
    The parquet files are being used to display publications on the RenewHydro website.
    """
    if not storage_endpoint_url:
        log.error("S3 endpoint URL is not provided")
        raise typer.Exit(code=1)
    if not storage_access_key:
        log.error("S3 access key is not provided")
        raise typer.Exit(code=1)
    if not storage_secret_key:
        log.error("S3 secret key is not provided")
        raise typer.Exit(code=1)

    log.info("Fetching NVA Renew Hydro data from API and exporting to parquet on S3")
    log.info(
        f"Data will be available at: {storage_endpoint_url}/{storage_bucket}/"
        f"{storage_prefix}"
    )

    credentials = AwsCredentials(
        s3_url_style="path",
        endpoint_url=storage_endpoint_url,
        aws_secret_access_key=storage_secret_key,
        aws_access_key_id=storage_access_key,
        region_name=storage_region,
    )

    pipeline = dlt.pipeline(
        pipeline_name="nva_renew_hydro",
        destination=filesystem(
            region_name=storage_region,
            bucket_url=f"s3://{storage_bucket}/{storage_prefix}",
            credentials=credentials,
            layout="{table_name}.{ext}",
        ),
        dataset_name="main",
        progress="log",
    )

    log.info(f"Fetching resources for project {project_id} from {base_url}")
    load_info = pipeline.run(
        nva_renew_hydro_source(
            base_url=base_url,
            project_id=project_id,
        ),
        write_disposition="replace",
        loader_file_format="parquet",
    )

    log.info("Pipeline run completed", load_info=load_info)

    log.info("Applying filter_cols.sql transformation to the data")
    con = duckdb.connect()
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")

    con.execute(f"""
        CREATE OR REPLACE SECRET secret (
            TYPE s3,
            REGION '{storage_region}',
            KEY_ID '{storage_access_key}',
            SECRET '{storage_secret_key}',
            ENDPOINT '{storage_endpoint_url.replace(r"https://", "")}',
            URL_STYLE 'path'
        );
    """).fetchall()

    resources = con.read_parquet(  # noqa: F841
        f"s3://{storage_bucket}/{storage_prefix}/main/renew_hydro_resources.parquet"
    )

    additional_identifiers = con.read_parquet(  # noqa: F841
        f"s3://{storage_bucket}/{storage_prefix}/main/renew_hydro_resources__additional_identifiers.parquet"
    )

    filter_query = files("datasync.nva.queries").joinpath("filter_cols.sql")
    filtered_resources = con.sql(filter_query.read_text())

    filtered_resources.write_parquet(
        f"s3://{storage_bucket}/{storage_prefix}/main/renew_hydro_resources.parquet",
        compression="zstd",
        overwrite=True,
    )

    log.info("Filter applied")

    # write timestamp
    timestamp = datetime.datetime.now().isoformat()

    con.execute(f"""
        COPY (SELECT '{timestamp}' as last_successful_run)
        TO 's3://{storage_bucket}/{storage_prefix}/last_successful_run.parquet'
        (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    log.info(f"Last successful run timestamp written: {timestamp}")

    con.close()

    log.info("NVA Renew Hydro data sync completed")
    log.info(
        f"Data available at: {storage_endpoint_url}/{storage_bucket}/{storage_prefix}/"
        f"main/renew_hydro_resources.parquet"
    )


if __name__ == "__main__":
    app()
