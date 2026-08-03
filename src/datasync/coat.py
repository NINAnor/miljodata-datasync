import json
from datetime import datetime

import dlt
import requests
import typer
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import OffsetPaginator

from .settings import log

app = typer.Typer(help="Export COAT CKAN packages to Parquet")

PLAUSIBLE_BASE_URL = "https://plausible.io/api/v2"
PLAUSIBLE_SITE_ID = "data.coat.no"
PLAUSIBLE_METRICS = ["visitors", "visits", "pageviews", "bounce_rate", "visit_duration"]

_BREAKDOWN_DIMENSIONS = [
    ("page", "event:page"),
    ("source", "visit:source"),
    ("country", "visit:country_name"),
    ("device", "visit:device"),
    ("browser", "visit:browser"),
    ("os", "visit:os"),
]

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
def get_packages_resources(
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


def _plausible_paginate(api_key: str, base_payload: dict, page_size: int = 10_000):
    """Yield all result rows from a Plausible v2 query, handling pagination."""
    offset = 0
    while True:
        payload = {
            **base_payload,
            "include": {"total_rows": True},
            "pagination": {"limit": page_size, "offset": offset},
        }
        response = requests.post(
            f"{PLAUSIBLE_BASE_URL}/query",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=60,
        )
        response.raise_for_status()
        data = response.json()
        results = data.get("results", [])
        if not results:
            break
        yield from results
        total_rows = data.get("meta", {}).get("total_rows", 0)
        offset += len(results)
        if offset >= total_rows:
            break


@dlt.resource(
    name="plausible_timeseries",
    primary_key="date",
    write_disposition="replace",
)
def plausible_timeseries(api_key: str, site_id: str = PLAUSIBLE_SITE_ID):
    """Fetch daily timeseries stats from Plausible Analytics."""
    log.info(f"Fetching Plausible timeseries for {site_id}")
    for row in _plausible_paginate(
        api_key,
        {
            "site_id": site_id,
            "metrics": PLAUSIBLE_METRICS,
            "date_range": "all",
            "dimensions": ["time:day"],
            "order_by": [["time:day", "asc"]],
        },
    ):
        keys = ["date", *PLAUSIBLE_METRICS]
        vals = [*row["dimensions"], *row["metrics"]]
        yield dict(zip(keys, vals, strict=True))


@dlt.resource(
    name="plausible_breakdown",
    primary_key=["dimension", "value"],
    write_disposition="replace",
)
def plausible_breakdown(api_key: str, site_id: str = PLAUSIBLE_SITE_ID):
    """Fetch breakdowns by page, source, country, device, browser, and OS."""
    for dim_name, dim_key in _BREAKDOWN_DIMENSIONS:
        log.info(f"Fetching Plausible breakdown: {dim_name} ({dim_key})")
        for row in _plausible_paginate(
            api_key,
            {
                "site_id": site_id,
                "metrics": PLAUSIBLE_METRICS,
                "date_range": "all",
                "dimensions": [dim_key],
            },
        ):
            yield {
                "dimension": dim_name,
                "value": row["dimensions"][0],
                **dict(zip(PLAUSIBLE_METRICS, row["metrics"], strict=True)),
            }


@dlt.source(name="coat_plausible", max_table_nesting=0)
def plausible_source(api_key: str, site_id: str = PLAUSIBLE_SITE_ID):
    """Define the Plausible Analytics data source for a COAT site."""
    return (
        plausible_timeseries(api_key=api_key, site_id=site_id),
        plausible_breakdown(api_key=api_key, site_id=site_id),
    )


@app.command()
def get_plausible_analytics(
    bucket_url: str = typer.Option(
        default="data",
        envvar="COAT_BUCKET_URL",
        help="Destination bucket URL (local path or s3://...)",
    ),
    api_key: str = typer.Option(
        ...,
        envvar="COAT_PLAUSIBLE_API_KEY",
        help="Plausible Stats API key",
    ),
    site_id: str = typer.Option(
        default=PLAUSIBLE_SITE_ID,
        envvar="COAT_PLAUSIBLE_SITE_ID",
        help="Plausible site ID (e.g. data.coat.no)",
    ),
):
    """Run the Plausible analytics pipeline for data.coat.no."""
    pipeline = dlt.pipeline(
        pipeline_name="coat_plausible",
        destination=dlt.destinations.filesystem(
            bucket_url=bucket_url, layout="{table_name}.{ext}"
        ),
        dataset_name="coat_plausible",
    )
    pipeline.run(
        plausible_source(api_key=api_key, site_id=site_id),
        loader_file_format="parquet",
    )
    log.info(f"Plausible pipeline complete. Data at: {bucket_url}/coat_plausible")


if __name__ == "__main__":
    app()
