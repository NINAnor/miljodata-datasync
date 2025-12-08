#!/usr/bin/env python3

"""Main script."""

from datetime import datetime, timedelta

import click
import dlt
import environ
import requests
from dlt.destinations.impl.filesystem.factory import filesystem
from dlt.sources.credentials import AwsCredentials
from dlt.sources.rest_api import rest_api_source

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
)
from .settings import log as logger

env = environ.Env()


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


DEBUG = env.bool("DEBUG", default=False)
DUCK_DB_NAME = env("DUCK_DB_NAME", default="pit_data_v4")


SITES = {
    "kongsfjord": "0NK",
    "sylte": "0NS",
    "vigda": "0NV",
    "agdenes": "0NA",
    "vatne": "0NO",
}


@click.command()
@click.option(
    "--place",
    help=(
        "Site location to download data from (kongsfjord, sylte, vigda, agdenes, vatne)"
        "Not required if --all-locations is used."
    ),
    type=click.Choice(list(SITES.keys())),
)
@click.option("--begin_date", help="Start date for data download in YYYY-MM-DD format")
@click.option("--end_date", help="End date for data download in YYYY-MM-DD format")
@click.option("--tags", is_flag=True, help="Download only tags data")
@click.option("--readers", is_flag=True, help="Download only readers voltage data")
@click.option("--environment", is_flag=True, help="Download only environment data")
@click.option(
    "--all_locations", is_flag=True, help="Download data from all accessible locations"
)
@click.option(
    "--skip_errors",
    is_flag=True,
    help="Continue processing other locations if one fails",
)
def main(
    place, begin_date, end_date, tags, readers, environment, all_locations, skip_errors
) -> None:
    """Start the application."""

    # validate that either place or all_locations is specified
    if not all_locations and not place:
        raise click.UsageError(
            "Either --place must be specified or --all-locations flag must be used"
        )

    # validate that at least one data type is selected
    if not any([tags, readers, environment]):
        raise click.UsageError(
            "At least one data type must be selected: "
            "--tags, --readers, or --environment"
        )

    token = get_bearer_token()

    # if there is no date set, take current day
    if begin_date is None or end_date is None:
        logger.info("Start date or end date is None, setting to today")
        begin_date = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
        end_date = datetime.today().strftime("%Y-%m-%d")

    # determine which locations to process
    if all_locations:
        # skip 'vatne' (0NO) as it returns 403 Forbidden
        accessible_sites = {k: v for k, v in SITES.items() if k != "vatne"}
        locations_to_process = list(accessible_sites.items())
        logger.info(
            "Processing accessible locations: %s", list(accessible_sites.keys())
        )
        if skip_errors:
            logger.info("Error handling enabled - will skip failed locations")
    else:
        locations_to_process = [(place, SITES[place])]
        logger.info("Processing single location: %s", place)

    # create resources for all selected locations
    all_resources = []

    for location_name, location_code in locations_to_process:
        try:
            if tags:
                all_resources.append(
                    {
                        "name": f"tags_{location_name}",
                        "write_disposition": "merge",
                        "primary_key": ["tag", "detected_at"],
                        "endpoint": {
                            "path": f"tags/{location_code}",
                            "params": {
                                "begin_dt": begin_date,
                                "end_dt": end_date,
                            },
                        },
                    }
                )

            if readers:
                all_resources.append(
                    {
                        "name": f"readers_voltage_{location_name}",
                        "write_disposition": "merge",
                        "primary_key": ["read_at"],
                        "endpoint": {
                            "path": f"reader/{location_code}",
                            "params": {
                                "begin_dt": begin_date,
                                "end_dt": end_date,
                            },
                        },
                    }
                )

            if environment:
                all_resources.append(
                    {
                        "name": f"environment_data_{location_name}",
                        "write_disposition": "merge",
                        "primary_key": ["read_at"],
                        "endpoint": {
                            "path": f"enviro/{location_code}",
                            "params": {
                                "begin_dt": begin_date,
                                "end_dt": end_date,
                            },
                        },
                    }
                )

        except Exception as e:
            if skip_errors:
                logger.warning(f"Skipping {location_name} due to error: {e}")
                continue
            else:
                raise

    source = rest_api_source(
        {
            "client": {
                "base_url": BIOMARK_BASE_URL,
                "paginator": "single_page",
                "auth": {
                    "type": "bearer",
                    "token": token,
                },
            },
            "resources": all_resources,
        }
    )

    credentials = AwsCredentials(
        s3_url_style="path",
        endpoint_url=BIOMARK_AWS_ENDPOINT,
        aws_secret_access_key=BIOMARK_SECRET_KEY,
        aws_access_key_id=BIOMARK_ACCESS_KEY,
        region_name=BIOMARK_REGION,
    )

    pipeline = dlt.pipeline(
        pipeline_name="biomark_pit_registering_salmon",
        destination=filesystem(
            bucket_url=f"s3://{BIOMARK_BUCKET}/" + BIOMARK_PREFIX,
            credentials=credentials,
            layout="{table_name}.{ext}",
        ),
        dataset_name="main",
        progress="log",
    )

    if tags:

        @dlt.transformer(primary_key=["tag", "detected_at"])
        def add_decimal_tags(items):
            """Transform tags to include decimal format."""
            for item in items:
                if isinstance(item, dict) and "tag" in item:
                    item["tag_decimal"] = hex_to_decimal_tag(item["tag"])
                yield item

        # create list of resources, transforming tag resources
        final_resources = []
        for name, resource in source.resources.items():
            if name.startswith("tags"):
                # transform tag resources and keep the original name
                transformed = resource | add_decimal_tags.with_name(name)
                final_resources.append(transformed)
            else:
                final_resources.append(resource)

        load_info = pipeline.run(final_resources)
    else:
        load_info = pipeline.run(source)

    logger.info(load_info)


def get_bearer_token():
    """Get bearer token from API."""

    url = BIOMARK_BASE_URL + "token/"

    header = {
        "Content-Type": "application/json",
    }
    payload = {
        "email": BIOMARK_API_EMAIL,
        "password": BIOMARK_API_PWD,
    }

    logger.info(payload)

    response = requests.post(url, json=payload, headers=header, timeout=10)
    response.raise_for_status()
    token = response.json().get("access")
    return token


if __name__ == "__main__":
    main()
