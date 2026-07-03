#!/usr/bin/env python3

"""Biomark PIT registering salmon data synchronization."""

import os
from datetime import datetime, timedelta
from pathlib import Path

import dlt
import duckdb
import requests
import typer
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import SinglePagePaginator
from sling import Replication

from .settings import (
    env,
    log,
)

BIOMARK_API_EMAIL = env("BIOMARK_API_EMAIL", default="")
BIOMARK_API_PWD = env("BIOMARK_API_PWD", default="")
BIOMARK_AWS_ENDPOINT = env("BIOMARK_AWS_ENDPOINT", default="")
BIOMARK_BUCKET = env("BIOMARK_BUCKET", default="")
BIOMARK_ACCESS_KEY = env("BIOMARK_ACCESS_KEY", default="")
BIOMARK_SECRET_KEY = env("BIOMARK_SECRET_KEY", default="")
BIOMARK_PREFIX = env("BIOMARK_PREFIX", default="tables")
BIOMARK_REGION = env("BIOMARK_REGION", default="us-east-1")
BIOMARK_BASE_URL = env("BIOMARK_BASE_URL", default="https://data3.biomark.com/api/v1/")

BIOMARK_DUCKDB_PATH = env(
    "BIOMARK_DUCKDB_PATH", default="biomark_pit_registering_salmon_v1.duckdb"
)
BIOMARK_DATASET_NAME = env("BIOMARK_DATASET_NAME", default="main")

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
    client: RESTClient, locations: list[str], begin_date: str, end_date: str
):
    """Fetch environmental data from Biomark API."""
    for location_name in locations:
        location_code = SITES.get(location_name)
        log.debug(
            "Fetching environmental data for location", location_code=location_code
        )
        try:
            yield from client.paginate(
                f"enviro/{location_code}",
                method="GET",
                params={
                    "begin_dt": begin_date,
                    "end_dt": end_date,
                },
            )
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 403:
                log.warning(
                    f"403 Forbidden error for environmental data at location "
                    f"{location_code}. Skipping this location.",
                    location_code=location_code,
                    error=str(e),
                )
            else:
                log.error(
                    f"HTTP error fetching environmental data for location "
                    f"{location_code}",
                    location_code=location_code,
                    status_code=e.response.status_code if e.response else None,
                    error=str(e),
                )
            continue
        except Exception as e:
            log.error(
                f"Unexpected error fetching environmental data for location "
                f"{location_code}",
                location_code=location_code,
                error=str(e),
            )
            continue


def get_tags_data(
    client: RESTClient, locations: list[str], begin_date: str, end_date: str
):
    """Fetch tags data from Biomark API."""
    for location_name in locations:
        location_code = SITES.get(location_name)
        log.debug("Fetching tags data for location", location_code=location_code)
        try:
            yield from client.paginate(
                f"tags/{location_code}",
                method="GET",
                params={
                    "begin_dt": begin_date,
                    "end_dt": end_date,
                },
            )
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 403:
                log.warning(
                    f"403 Forbidden error for tags data at location "
                    f"{location_code}. Skipping this location.",
                    location_code=location_code,
                    error=str(e),
                )
            else:
                log.error(
                    f"HTTP error fetching tags data for location {location_code}",
                    location_code=location_code,
                    status_code=e.response.status_code if e.response else None,
                    error=str(e),
                )
            continue
        except Exception as e:
            log.error(
                f"Unexpected error fetching tags data for location {location_code}",
                location_code=location_code,
                error=str(e),
            )
            continue


def get_readers_voltage_data(
    client: RESTClient, locations: list[str], begin_date: str, end_date: str
):
    """Fetch readers voltage data from Biomark API."""

    for location_name in locations:
        location_code = SITES.get(location_name)
        log.debug(
            "Fetching readers voltage data for location", location_code=location_code
        )
        try:
            yield from client.paginate(
                f"reader/{location_code}",
                method="GET",
                params={
                    "begin_dt": begin_date,
                    "end_dt": end_date,
                },
            )
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 403:
                log.warning(
                    f"403 Forbidden error for readers voltage data at location "
                    f"{location_code}. Skipping this location.",
                    location_code=location_code,
                    error=str(e),
                )
            else:
                log.error(
                    f"HTTP error fetching readers voltage data for location"
                    f"{location_code}",
                    location_code=location_code,
                    status_code=e.response.status_code if e.response else None,
                    error=str(e),
                )
            continue
        except Exception as e:
            log.error(
                f"Unexpected error fetching readers voltage data for location "
                f"{location_code}",
                location_code=location_code,
                error=str(e),
            )
            continue


@dlt.transformer(primary_key=["tag", "detected_at"])
def add_decimal_tags(items):
    """Transform tags to include decimal format."""
    for item in items:
        if isinstance(item, dict) and "tag" in item:
            item["tag_decimal"] = hex_to_decimal_tag(item["tag"])
        yield item


@dlt.source()
def biomark_pit_salmon(
    begin_date: str,
    end_date: str,
    locations: list[str],
    base_url: str = BIOMARK_BASE_URL,
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
        paginator=SinglePagePaginator(),
    )

    try:
        if tags:
            tags_resource = dlt.resource(
                get_tags_data(client, locations, begin_date, end_date),
                name="tags",
                primary_key=["tag", "detected_at"],
            )
            yield tags_resource | add_decimal_tags.with_name("tags")

        if readers:
            readers_resource = dlt.resource(
                get_readers_voltage_data(client, locations, begin_date, end_date),
                name="readers_voltage",
                primary_key=["reader__site__slug", "read_at"],
                write_disposition="merge",
            )
            yield readers_resource

        if environment:
            env_resource = dlt.resource(
                get_environmental_data(client, locations, begin_date, end_date),
                name="environment_data",
                primary_key=["read_at"],
                write_disposition="append",
            )
            env_resource.apply_hints(incremental=dlt.sources.incremental("read_at"))
            yield env_resource

    except Exception as e:
        log.error(f"Error processing locations: {e}")
        # continue with next location


@app.command()
def run(
    duckdb_path: str = BIOMARK_DUCKDB_PATH,
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
    yesterday: bool = typer.Option(False, help="Set date range to yesterday only"),
    dataset_name: str = BIOMARK_DATASET_NAME,
):
    """Download PIT data from BioMark's API to a .duckdb file."""

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

    if yesterday:
        begin_date = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
        end_date = (datetime.today()).strftime("%Y-%m-%d")
        log.info("Setting date range to yesterday", date=begin_date)

    if all_locations:
        # skip 'vatne' (0NO) as it returns 403 Forbidden
        accessible_sites = {k: v for k, v in SITES.items() if k != "vatne"}
        locations = list(accessible_sites.keys())
        log.info("Processing accessible locations", locations=locations)
    else:
        locations = [place]
        log.info("Processing single location", location=place)

    pipeline = dlt.pipeline(
        pipeline_name=duckdb_path.replace(".duckdb", ""),
        destination="duckdb",
        dataset_name=dataset_name,
        progress="log",
    )

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
        )
    )


def create_stream_config(config: dict, location: str) -> dict:
    """Create a stream configuration for a specific data type and location."""
    return {
        "primary_key": config["primary_key"],
        "update_key": config["update_key"],
        "object": (
            f"tables/{config['table_name']}/location={location}/"
            f"{{part_year}}/{{part_month}}/{{part_day}}"
        ),
        "mode": "incremental",
        "target_options": {"format": "parquet"},
        "sql": (
            f"select * replace ("  # noqa: S608
            f"{config['time_column']} at time zone 'UTC' as "
            f"{config['time_column']}) "
            f"from {config['table_name']} where "
            f"{config['location']} = '{location}' "
            f"and {{incremental_where_cond}}"
        ),
    }


def check_table_has_data(db_path: str, table_name: str, dataset_name: str) -> bool:
    """
    Check if a table exists and has data in the DuckDB database.

    Args:
        db_path: Path to the DuckDB database file
        table_name: Name of the table to check
        dataset_name: Name of the schema/dataset

    Returns:
        bool: True if table exists and has at least one row, False otherwise
    """
    try:
        conn = duckdb.connect(db_path, read_only=True)
        try:
            log.info(f"Checking table {table_name} existence in schema {dataset_name}")

            check_table_exists = """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = ? AND table_name = ?
            """
            result = conn.execute(
                check_table_exists, [dataset_name, table_name]
            ).fetchone()

            if result is None:
                log.info(f"Table {table_name} does not exist in schema {dataset_name}")
                return False

            qualified_table = f"{dataset_name}.{table_name}"
            table_relation = conn.table(qualified_table)
            count_result = table_relation.count("*").fetchone()

            row_count = count_result[0] if count_result else 0

            log.info(f"Table {table_name} has {row_count} rows")
            return row_count > 0

        finally:
            conn.close()

    except Exception as e:
        log.error(f"Error checking table {table_name}: {e}")
        return False


@app.command()
def replicate(
    bucket: str = BIOMARK_BUCKET,
    endpoint_url: str = BIOMARK_AWS_ENDPOINT,
    access_key: str = BIOMARK_ACCESS_KEY,
    secret_key: str = BIOMARK_SECRET_KEY,
    duckdb_path: str = BIOMARK_DUCKDB_PATH,
    region: str = BIOMARK_REGION,
    dataset_name: str = BIOMARK_DATASET_NAME,
    tags: bool = typer.Option(False, help="Add tags data to S3"),
    readers: bool = typer.Option(False, help="Add readers voltage data to S3"),
    environment: bool = typer.Option(False, help="Add environment data to S3"),
):
    """Upload data from .duckdb to S3 bucket."""
    os.environ["NINAS3"] = f"{{type: s3, bucket: {bucket}, use_environment: true }}"
    os.environ["AWS_ACCESS_KEY_ID"] = access_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = secret_key
    os.environ["AWS_REGION"] = region
    os.environ["AWS_ENDPOINT"] = endpoint_url
    os.environ["DUCKDB"] = f"{{type: duckdb, instance: {duckdb_path}}}"

    if not any([readers, tags, environment]):
        raise typer.BadParameter(
            "Error: At least one data type must be selected "
            "(--readers, --tags, or --environment)"
        )

    if not Path(duckdb_path).exists():
        raise typer.BadParameter(f"Error: DuckDB file not found at {duckdb_path}")

    # only include configs for enabled data types that have data
    stream_configs = {}
    if readers and check_table_has_data(duckdb_path, "readers_voltage", dataset_name):
        stream_configs["readers"] = {
            "table_name": f"{dataset_name}.readers_voltage",
            "primary_key": ["read_at"],
            "update_key": "read_at",
            "time_column": "read_at",
            "location": "reader__site__slug",
        }
        log.info("Added readers_voltage to stream config (has data)")
    elif readers:
        log.warning("--readers_voltage flag is True but table has no data, skipping")

    if tags and check_table_has_data(duckdb_path, "tags", dataset_name):
        stream_configs["tags"] = {
            "table_name": f"{dataset_name}.tags",
            "primary_key": ["detected_at", "tag"],
            "update_key": "detected_at",
            "time_column": "detected_at",
            "location": "antenna__reader__site__slug",
        }
        log.info("Added tags to stream config (has data)")
    elif tags:
        log.warning("--tags flag is set but table has no data, skipping")

    if environment and check_table_has_data(
        duckdb_path, "environment_data", dataset_name
    ):
        stream_configs["environment"] = {
            "table_name": f"{dataset_name}.environment_data",
            "primary_key": ["read_at"],
            "update_key": "read_at",
            "time_column": "read_at",
            "location": "reader__site__slug",
        }
        log.info("Added environment_data to stream config (has data)")
    elif environment:
        log.warning("--environment flag is set, but table has no data, skipping")

    if not stream_configs:
        log.warning("No tables with data found to replicate, skipping")
        return

    streams = {}
    for data_type, config in stream_configs.items():
        type_streams = {
            f"{data_type}__{location}": create_stream_config(config, location)
            for location in SITES.values()
        }
        streams.update(type_streams)
        typer.echo(f"Added {len(type_streams)} {data_type} streams")

    typer.echo(f"Total streams to replicate: {len(streams)}")

    Replication(
        source="DUCKDB",
        target="NINAS3",
        defaults={
            "object": "tables/{stream_name}/{part_year}/{part_month}/{part_day}",
            "mode": "incremental",
            "target_options": {"format": "parquet"},
        },
        streams=streams,
        env={"SLING_STATE": "NINAS3/data/sling", "SLING_DIRECT_INSERT": "True"},
        debug=True,
    ).run()


if __name__ == "__main__":
    app()
