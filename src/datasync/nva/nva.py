import datetime

import dlt
import typer
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator

from ..settings import env, log
from .utils import (
    create_pipeline,
    create_s3_credentials,
    setup_duckdb_s3_connection,
    write_timestamp,
)

NVA_DUCKDB_NAME = env("NVA_DUCKDB_FILE_NAME", default="nva_sync")

app = typer.Typer(help="Export NVA APIs to Parquet on a S3 Bucket")


def get_funding_sources(client: RESTClient):
    log.debug("Fetching funding sources")
    yield from client.paginate(
        "cristin/funding-sources",
        method="GET",
    )


def get_persons(client: RESTClient, institution_code: str):
    log.debug("Fetching persons")
    yield from client.paginate(
        f"cristin/organization/{institution_code}/persons",
        method="GET",
    )


def get_projects(client: RESTClient, institution_code: str):
    log.debug("Fetching projects")
    yield from client.paginate(
        f"cristin/organization/{institution_code}/projects",
        method="GET",
    )


def get_categories(client: RESTClient):
    log.debug("Fetching categories")
    yield from client.paginate(
        "cristin/category/project",
        method="GET",
    )


def get_resources(client: RESTClient, institution_code: str):
    for year in set(range(1979, datetime.datetime.now().year + 1)):
        log.debug("Fetching resources for year", year=year)
        yield from client.paginate(
            "search/resources",
            method="GET",
            params={
                "unit": institution_code,
                "publicationYearSince": year,
                "publicationYearBefore": year + 1,
            },
        )


@dlt.source()
def nva(
    base_url,
    institution_code,
    resources: bool = False,
    projects: bool = False,
    persons: bool = False,
    categories: bool = False,
    funding_sources: bool = False,
):
    client = RESTClient(
        base_url=base_url,
        paginator=JSONLinkPaginator(next_url_path="nextResults"),
        data_selector="hits",
    )

    if resources:
        yield dlt.resource(
            get_resources(client, institution_code),
            name="resources",
            write_disposition="replace",
            primary_key="id",
            max_table_nesting=1,
        )

    if projects:
        yield dlt.resource(
            get_projects(client, institution_code),
            name="projects",
            write_disposition="replace",
            primary_key="id",
            max_table_nesting=1,
        )

    if persons:
        yield dlt.resource(
            get_persons(client, institution_code),
            name="persons",
            write_disposition="replace",
            primary_key="id",
            max_table_nesting=1,
        )
    if categories:
        yield dlt.resource(
            get_categories(client),
            name="categories",
            write_disposition="replace",
            primary_key="_dlt_id",
            max_table_nesting=1,
        )

    if funding_sources:
        yield dlt.resource(
            get_funding_sources(client),
            name="funding_sources",
            primary_key="identifier",
            write_disposition="replace",
            max_table_nesting=1,
        )

    return nva


@app.command()
def run(
    resources: bool = False,
    projects: bool = False,
    persons: bool = False,
    categories: bool = False,
    funding_sources: bool = False,
    base_url: str = typer.Option(
        default="https://api.nva.unit.no/",
        envvar="NVA_BASE_URL",
        help="Base URL for the NVA API",
    ),
    duckdb_name: str = NVA_DUCKDB_NAME,
    institution_code: str = typer.Option(
        "7511.0.0.0", envvar="NVA_INSTITUTION_CODE", help="NVA institution code"
    ),
    endpoint_url: str = typer.Option(
        ...,
        envvar="NVA_S3_ENDPOINT_URL",
        help="AWS S3 endpoint URL",
    ),
    access_key: str = typer.Option(
        ...,
        envvar="NVA_S3_ACCESS_KEY",
        help="AWS S3 access key",
    ),
    secret_key: str = typer.Option(
        ...,
        envvar="NVA_S3_SECRET_KEY",
        help="AWS S3 secret key",
    ),
    bucket: str = typer.Option(
        ...,
        envvar="NVA_S3_BUCKET",
        help="AWS S3 bucket name",
    ),
    prefix: str = typer.Option(
        ...,
        envvar="NVA_S3_PREFIX",
        help="AWS S3 prefix (folder path) for storing data",
    ),
    region: str = typer.Option(
        "us-east-1",
        envvar="NVA_S3_REGION",
        help="AWS S3 region",
    ),
):
    log.info("Starting NVA data sync")
    log.info(f"Data will be available at: {endpoint_url}/{bucket}/{prefix}")

    credentials = create_s3_credentials(
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key,
        region=region,
    )

    pipeline = create_pipeline(
        pipeline_name=duckdb_name,
        bucket=bucket,
        prefix=prefix,
        credentials=credentials,
        region=region,
    )

    log.info(
        pipeline.run(
            nva(
                base_url=base_url,
                institution_code=institution_code,
                resources=resources,
                projects=projects,
                persons=persons,
                categories=categories,
                funding_sources=funding_sources,
            ),
            write_disposition="replace",
            loader_file_format="parquet",
        )
    )

    log.info("NVA data sync completed")
    log.info(f"Data available at: {endpoint_url}/{bucket}/{prefix}")

    con = setup_duckdb_s3_connection(
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key,
        region=region,
    )

    write_timestamp(con, bucket, prefix)
    con.close()


if __name__ == "__main__":
    app()
