import json
from datetime import datetime

import dlt
import typer
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import OffsetPaginator

from .settings import log

app = typer.Typer(help="Export COAT CKAN packages to Parquet")

PAGE_SIZE = 100
DEFAULT_BASE_URL = "https://data.coat.no/api/3/action"
BASE_DOMAIN = DEFAULT_BASE_URL.rsplit("/api/", 1)[0]

MIXED = ("author_email", "maintainer", "maintainer_email")
SPLIT = (
    "associated_parties",
    "datasets",
    "funding",
    "location",
    "persons",
    "protocol",
    "scientific_name",
)
DATE_FIELDS = ("embargo", "temporal_start", "temporal_end")


def normalize_record(record):
    """Normalize a package record in-place."""
    org = record.get("organization") or {}

    # Handle mixed fields (can be string or list)
    for field in MIXED:
        value = record.get(field)
        if isinstance(value, list):
            record[field] = json.dumps(value)
        elif isinstance(value, str) and value.startswith("["):
            try:
                parsed = json.loads(value)
                if isinstance(parsed, list) and len(parsed) == 1:
                    record[field] = parsed[0]
            except json.JSONDecodeError:
                pass

    # Split comma-separated fields into arrays
    for field in SPLIT:
        value = record.get(field)
        record[field] = [v.strip() for v in value.split(",")] if value else []

    # Parse date fields
    for field in DATE_FIELDS:
        value = record.get(field)
        if value:
            try:
                record[field] = datetime.strptime(value, "%Y-%m-%d").date()
            except (ValueError, TypeError):
                pass

    # Extract organization info
    record["organization_name"] = org.get("name")
    record["organization_title"] = org.get("title")

    # Flatten extras to JSON object
    record["extras"] = {e["key"]: e["value"] for e in record.get("extras", [])}

    # Extract tag names
    record["tags"] = [t["name"] for t in record.get("tags", [])]

    # Build URL
    record["url"] = f"{BASE_DOMAIN}/dataset/{record.get('name', '')}"

    return record


@dlt.resource(
    name="packages",
    primary_key="id",
    write_disposition="replace",
)
def packages(
    api_key: str = "",
    base_url: str = DEFAULT_BASE_URL,
):
    """Fetch packages from CKAN API and normalize them."""
    log.info(f"Starting package extraction from {base_url}")

    client = RESTClient(
        base_url=base_url,
        headers={"Authorization": api_key, "Accept": "application/json"},
    )

    paginator = OffsetPaginator(
        limit=PAGE_SIZE,
        offset_param="start",
        limit_param="rows",
        total_path="result.count",
    )

    total_records = 0
    for page in client.paginate(
        "/ckan_package_search",
        params={"include_private": "true"},
        paginator=paginator,
    ):
        for record in page:
            yield normalize_record(record)
            total_records += 1

    log.info(f"Extraction complete. Total records: {total_records}")


@dlt.transformer(
    data_from=packages,
    name="resources",
    primary_key="id",
    write_disposition="replace",
)
def resources(pkg):
    """Extract resources from packages."""
    for res in pkg.get("resources", []):
        yield {
            "package_name": pkg["name"],
            "package_id": pkg["id"],
            **res,
        }


@dlt.source(name="coat", max_table_nesting=0)
def coat_source():
    """Define the COAT data source."""
    return packages, resources


@app.command()
def run(
    bucket_url: str = typer.Option(
        default="data",
        envvar="COAT_BUCKET_URL",
        help="Destination bucket URL (local path or s3://...)",
    ),
    api_key: str = typer.Option(
        ...,
        envvar="COAT_API_KEY",
        help="CKAN API key",
    ),
    base_url: str = typer.Option(
        default="https://data.coat.no/api/3/action",
        envvar="COAT_BASE_URL",
        help="COAT CKAN API base URL",
    ),
):
    """Run the COAT extraction pipeline."""
    pipeline = dlt.pipeline(
        pipeline_name="coat",
        destination=dlt.destinations.filesystem(
            bucket_url=bucket_url, layout="{table_name}.{ext}"
        ),
        dataset_name="coat",
    )
    pipeline.run(coat_source(), loader_file_format="parquet")
    log.info(f"COAT pipeline available at: {bucket_url}/coat")


if __name__ == "__main__":
    app()
