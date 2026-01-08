import datetime

import dlt
import typer
from dlt.destinations.impl.filesystem.factory import filesystem
from dlt.sources.credentials import AwsCredentials
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator

from .settings import (
    env,
    log,
)

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

app = typer.Typer(help="Export NVA APIs to Parquet on a S3 Bucket")


def get_funding_sources(client: RESTClient):
    log.debug("Fetching funding sources")
    yield from client.paginate(
        "cristin/funding-sources",
        method="get",
    )


def get_persons(client: RESTClient, institution_code: str):
    log.debug("Fetching persons")
    yield from client.paginate(
        f"cristin/organization/{institution_code}/persons",
        method="get",
    )


def get_projects(client: RESTClient, institution_code: str):
    log.debug("Fetching projects")
    yield from client.paginate(
        f"cristin/organization/{institution_code}/projects",
        method="get",
    )


def get_categories(client: RESTClient):
    log.debug("Fetching categories")
    yield from client.paginate(
        "cristin/category/project",
        method="get",
    )


def get_resources(client: RESTClient, institution_code: str):
    for year in set(range(1979, datetime.datetime.now().year + 1)):
        log.debug("Fetching resources for year", year=year)
        yield from client.paginate(
            "search/resources",
            method="get",
            params={
                "unit": institution_code,
                "publicationYearSince": year,
                "publicationYearBefore": year + 1,
            },
        )


@dlt.source()
def nva(
    base_url: str = NVA_BASE_URL,
    institution_code: str = NVA_INSTITUTION_CODE,
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
            max_table_nesting=1,
        )

    if projects:
        yield dlt.resource(
            get_projects(client, institution_code),
            name="projects",
            write_disposition="replace",
            max_table_nesting=1,
        )

    if persons:
        yield dlt.resource(
            get_persons(client, institution_code),
            name="persons",
            write_disposition="replace",
            max_table_nesting=1,
        )
    if categories:
        yield dlt.resource(
            get_categories(client),
            name="categories",
            write_disposition="replace",
            max_table_nesting=1,
        )

    if funding_sources:
        yield dlt.resource(
            get_funding_sources(client),
            name="funding_sources",
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
    base_url: str = NVA_BASE_URL,
    duckdb_name: str = NVA_DUCKDB_NAME,
    institution_code: str = NVA_INSTITUTION_CODE,
    endpoint_url: str = NVA_ENDPOINT,
    access_key: str = NVA_ACCESS_KEY,
    secret_key: str = NVA_SECRET_KEY,
    bucket: str = NVA_BUCKET,
    prefix: str = NVA_PREFIX,
    region: str = NVA_REGION,
):
    credentials = AwsCredentials(
        s3_url_style="path",
        endpoint_url=endpoint_url,
        aws_secret_access_key=secret_key,
        aws_access_key_id=access_key,
        region_name=region,
    )

    pipeline = dlt.pipeline(
        pipeline_name=duckdb_name,
        destination=filesystem(
            region_name=region,
            bucket_url=f"s3://{bucket}/" + prefix,
            credentials=credentials,
            layout="{table_name}.{ext}",
        ),
        dataset_name="main",
        progress="log",
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


if __name__ == "__main__":
    app()
