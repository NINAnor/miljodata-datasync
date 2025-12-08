#!/usr/bin/env python3

"""Biomark PIT registering salmon data synchronization."""

from datetime import datetime, timedelta

import dlt
import requests
import typer
from dlt.destinations.impl.filesystem.factory import filesystem
from dlt.sources.credentials import AwsCredentials
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth

from .settings import (
    BIOMARK_ACCESS_KEY,
    BIOMARK_API_EMAIL,
    BIOMARK_API_PWD,
    BIOMARK_AWS_ENDPOINT,
    BIOMARK_BASE_URL,
    BIOMARK_BUCKET,
    BIOMARK_PREFIX,
    BIOMARK_REGION,
    BIOMARK_SECRET_KEY,
    log,
)

app = typer.Typer()

SITES = {
    "kongsfjord": "0NK",
    "sylte": "0NS",
    "vigda": "0NV",
    "agdenes": "0NA",
    "vatne": "0NO",
}


def hex_to_decimal_tag(hex_tag):
    """
    Convert hexadecimal PIT tag format to ISO decimal format.

    Args:
        hex_tag (str): Hex tag in format like '3DD.003E550755'

    Returns:
        str: ISO decimal tag like '989.001045759829'
    """
    if not hex_tag or not isinstance(hex_tag, str):
        return None

    try:
        # Split on the dot
        if "." not in hex_tag:
            return None
        left_hex, right_hex = hex_tag.split(".")
        # convert left part (manufacturer code) to decimal
        left_decimal = int(left_hex, 16)
        # convert right part to decimal and format as fractional part
        right_decimal = int(right_hex, 16)
        # combine with proper formatting
        iso_decimal = f"{left_decimal}.{right_decimal:012d}"
        return iso_decimal
    except (ValueError, TypeError):
        return None


def get_bearer_token():
    """Get bearer token from Biomark API."""
    url = BIOMARK_BASE_URL + "token/"

    header = {
        "Content-Type": "application/json",
    }
    payload = {
        "email": BIOMARK_API_EMAIL,
        "password": BIOMARK_API_PWD,
    }

    response = requests.post(url, json=payload, headers=header, timeout=10)
    response.raise_for_status()
    token = response.json().get("access")
    return token


def get_environmental_data(
    client: RESTClient, location_code: str, begin_date: str, end_date: str
):
    """Fetch environmental data from Biomark API."""
    log.debug("Fetching environmental data for location", location_code=location_code)
    yield from client.paginate(
        f"enviro/{location_code}",
        method="get",
        params={
            "begin_dt": begin_date,
            "end_dt": end_date,
        },
    )


def get_tags_data(
    client: RESTClient, location_code: str, begin_date: str, end_date: str
):
    """Fetch tags data from Biomark API."""
    log.debug("Fetching tags data for location", location_code=location_code)
    yield from client.paginate(
        f"tags/{location_code}",
        method="get",
        params={
            "begin_dt": begin_date,
            "end_dt": end_date,
        },
    )


def get_readers_voltage_data(
    client: RESTClient, location_code: str, begin_date: str, end_date: str
):
    """Fetch readers voltage data from Biomark API."""
    log.debug("Fetching readers voltage data for location", location_code=location_code)
    yield from client.paginate(
        f"reader/{location_code}",
        method="get",
        params={
            "begin_dt": begin_date,
            "end_dt": end_date,
        },
    )


@dlt.transformer(primary_key=["tag", "detected_at"])
def add_decimal_tags(items):
    """Transform tags to include decimal format."""
    for item in items:
        if isinstance(item, dict) and "tag" in item:
            item["tag_decimal"] = hex_to_decimal_tag(item["tag"])
        yield item


@dlt.source()
def biomark_pit_salmon(
    base_url: str = BIOMARK_BASE_URL,
    begin_date: str = None,
    end_date: str = None,
    locations: list = None,
    tags: bool = False,
    readers: bool = False,
    environment: bool = False,
):
    """Biomark PIT salmon data source."""

    # get authentication token
    token = get_bearer_token()

    # create authenticated REST client
    client = RESTClient(
        base_url=base_url,
        auth=BearerTokenAuth(token),
    )

    # set default dates if not provided
    if begin_date is None or end_date is None:
        log.info("Start date or end date is None, setting to yesterday")
        begin_date = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
        end_date = datetime.today().strftime("%Y-%m-%d")

    for location_name in locations:
        location_code = SITES.get(location_name)
        if not location_code:
            log.warning(f"Unknown location: {location_name}")
            continue

        try:
            if tags:
                tags_resource = dlt.resource(
                    get_tags_data(client, location_code, begin_date, end_date),
                    name=f"tags_{location_name}",
                    primary_key=["tag", "detected_at"],
                    write_disposition="append",
                )
                # Apply decimal tag transformation
                yield tags_resource | add_decimal_tags.with_name(
                    f"tags_{location_name}"
                )

            if readers:
                yield dlt.resource(
                    get_readers_voltage_data(
                        client, location_code, begin_date, end_date
                    ),
                    name=f"readers_voltage_{location_name}",
                    primary_key=["read_at"],
                    write_disposition="append",
                )

            if environment:
                yield dlt.resource(
                    get_environmental_data(client, location_code, begin_date, end_date),
                    name=f"environment_data_{location_name}",
                    primary_key=["read_at"],
                    write_disposition="append",
                )

        except Exception as e:
            log.error(f"Error processing {location_name}: {e}")
            # continue with next location


@app.command()
def run(
    place: str = typer.Option(
        None, help="Site location (kongsfjord, sylte, vigda, agdenes, vatne)"
    ),
    begin_date: str = typer.Option(
        None, help="Start date for data download in YYYY-MM-DD format"
    ),
    end_date: str = typer.Option(
        None, help="End date for data download in YYYY-MM-DD format"
    ),
    tags: bool = typer.Option(False, help="Download tags data"),
    readers: bool = typer.Option(False, help="Download readers voltage data"),
    environment: bool = typer.Option(False, help="Download environment data"),
    all_locations: bool = typer.Option(
        False, help="Download data from all accessible locations"
    ),
    base_url: str = BIOMARK_BASE_URL,
    bucket: str = BIOMARK_BUCKET,
    prefix: str = BIOMARK_PREFIX,
    endpoint_url: str = BIOMARK_AWS_ENDPOINT,
    access_key: str = BIOMARK_ACCESS_KEY,
    secret_key: str = BIOMARK_SECRET_KEY,
    region: str = BIOMARK_REGION,
):
    """Biomark PIT registering salmon data synchronization."""

    # validate that either place or all_locations is specified
    if not all_locations and not place:
        raise typer.BadParameter(
            "Either --place must be specified or --all-locations flag must be used"
        )

    # validate that at least one data type is selected
    if not any([tags, readers, environment]):
        raise typer.BadParameter(
            "At least one data type must be selected: "
            "--tags, --readers, or --environment"
        )

    if all_locations:
        # skip 'vatne' (0NO) as it returns 403 Forbidden
        accessible_sites = {k: v for k, v in SITES.items() if k != "vatne"}
        locations = list(accessible_sites.keys())
        log.info("Processing accessible locations", locations=locations)
    else:
        locations = [place]
        log.info("Processing single location", location=place)

    credentials = AwsCredentials(
        s3_url_style="path",
        endpoint_url=endpoint_url,
        aws_secret_access_key=secret_key,
        aws_access_key_id=access_key,
        region_name=region,
    )

    # Set up DLT pipeline
    pipeline = dlt.pipeline(
        pipeline_name="biomark_pit_registering_salmon",
        destination=filesystem(
            bucket_url=f"s3://{bucket}/" + prefix,
            credentials=credentials,
            layout="{table_name}.{ext}",
        ),
        dataset_name="main",
        progress="log",
    )

    # Run the pipeline
    log.info(
        pipeline.run(
            biomark_pit_salmon(
                base_url=base_url,
                begin_date=begin_date,
                end_date=end_date,
                locations=locations,
                tags=tags,
                readers=readers,
                environment=environment,
            ),
            write_disposition="append",
            loader_file_format="parquet",
        )
    )


if __name__ == "__main__":
    app()
