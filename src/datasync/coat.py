from datetime import datetime
from urllib.parse import urlparse

import dlt
import typer
from dlt.destinations.impl.filesystem.factory import filesystem
from dlt.sources.credentials import AwsCredentials
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import OffsetPaginator

from .settings import log

app = typer.Typer(help="Export COAT data to parquet")

PAGE_SIZE = 100
BASE_URL = "https://data.coat.no"
ENDPOINT_URL = BASE_URL + "/api/3/action"

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

# Some CKAN resources duplicate metadata under differently-cased "extra"
# keys (e.g. "Size" alongside "size"). Maps the extra key to the native
# field it should fill in when the native field is missing.
_RESOURCE_FIELD_OVERRIDES = {
    "Size": "size",
    "Created": "created",
    "Media type": "mimetype",
}

PLAUSIBLE_API_URL = "https://plausible.io/api/v2/query"
PLAUSIBLE_METRICS = ["visitors", "visits", "pageviews", "bounce_rate", "visit_duration"]
PLAUSIBLE_SITE_ID = "data.coat.no"

_BREAKDOWN_DIMENSIONS = [
    ("page", "event:page"),
    ("source", "visit:source"),
    ("country", "visit:country_name"),
    ("device", "visit:device"),
    ("browser", "visit:browser"),
    ("os", "visit:os"),
]


def s3_filesystem_destination(
    endpoint_url: str,
    access_key: str,
    secret_key: str,
    bucket: str,
    prefix: str,
    region: str,
):
    """Build an S3 bucket URL and dlt filesystem destination from credentials."""
    bucket_url = f"s3://{bucket}/{prefix}"
    credentials = AwsCredentials(
        s3_url_style="path",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
    )
    destination = filesystem(
        bucket_url=bucket_url,
        credentials=credentials,
        layout="{table_name}.{ext}",
    )
    return bucket_url, destination


def normalize_record(record, domain: str = BASE_URL):
    """Normalize a package record in-place."""
    org = record.get("organization") or {}
    for field in SPLIT:
        value = record.get(field)
        record[field] = [v.strip() for v in value.split(",")] if value else []

    # Parse date fields
    for field in DATE_FIELDS:
        value = record.get(field)
        if value:
            record[field] = datetime.strptime(value, "%Y-%m-%d").date()

    # Extract organization info
    record["organization_name"] = org.get("name")
    record["organization_title"] = org.get("title")

    # Flatten extras to JSON object
    record["extras"] = {e["key"]: e["value"] for e in record.get("extras", [])}
    record["extras_base_name"] = record["extras"].get("base_name")

    # Extract tag names
    record["tags"] = [t["name"] for t in record.get("tags", [])]

    # List of resource IDs belonging to this package, e.g. "[id1, id2]"
    resource_ids = [r["id"] for r in record.get("resources", []) if r.get("id")]
    record["resources_ids"] = f"[{', '.join(resource_ids)}]"

    # Build URL
    name = record.get("name")
    if not name:
        log.warning(
            f"Record {record.get('id')!r} is missing 'name', URL will be incomplete"
        )
    record["url"] = f"{domain}/dataset/{name or ''}"

    return record


@dlt.resource(
    name="packages",
    primary_key="id",
    write_disposition="replace",
)
def packages(
    api_key: str = "",
    base_url: str = ENDPOINT_URL,
):
    """Fetch packages from CKAN API and normalize them."""
    log.info(f"Starting package extraction from {base_url}")

    parsed = urlparse(base_url)
    if not parsed.scheme or not parsed.netloc:
        log.error("Invalid base_url: %s", base_url)
        raise ValueError(f"Invalid base_url: {base_url!r}")
    domain = f"{parsed.scheme}://{parsed.netloc}"

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
            yield normalize_record(record, domain=domain)
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
        res = dict(res)
        res.pop("__force", None)

        # Some resources carry duplicate metadata under differently-cased
        # keys (e.g. "Size" alongside "size"). These would otherwise
        # collide once column names are normalized downstream, silently
        # dropping whichever value loses. Fill the native field from the
        # extra one only when the native field is missing.
        for extra_field, native_field in _RESOURCE_FIELD_OVERRIDES.items():
            value = res.pop(extra_field, None)
            if value not in (None, "None") and not res.get(native_field):
                res[native_field] = value

        yield {
            **res,
            "package_name": pkg.get("name"),
            "package_id": pkg.get("id"),
        }


@dlt.source(name="coat", max_table_nesting=0)
def coat_source(api_key: str = "", base_url: str = ENDPOINT_URL):
    """Define the COAT data source."""
    return packages(api_key=api_key, base_url=base_url), resources


@app.command()
def get_packages_and_resources(
    endpoint_url: str = typer.Option(
        ...,
        envvar="COAT_AWS_ENDPOINT",
        help="AWS S3 endpoint URL",
    ),
    access_key: str = typer.Option(
        ...,
        envvar="COAT_AWS_ACCESS_KEY",
        help="AWS S3 access key",
    ),
    secret_key: str = typer.Option(
        ...,
        envvar="COAT_AWS_SECRET_KEY",
        help="AWS S3 secret key",
    ),
    bucket: str = typer.Option(
        ...,
        envvar="COAT_AWS_BUCKET",
        help="AWS S3 bucket name",
    ),
    prefix: str = typer.Option(
        default="coat",
        envvar="COAT_S3_PREFIX",
        help="AWS S3 prefix (folder path) for storing data",
    ),
    region: str = typer.Option(
        default="us-east-1",
        envvar="COAT_S3_REGION",
        help="AWS S3 region",
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
    dataset_name: str = typer.Option(
        default="coat",
        envvar="COAT_DATASET_NAME",
        help="Local pipeline name (used for dlt's local working/state directory)",
    ),
):
    """Run the COAT extraction pipeline."""
    bucket_url, filesystem_destination = s3_filesystem_destination(
        endpoint_url, access_key, secret_key, bucket, prefix, region
    )

    pipeline = dlt.pipeline(
        pipeline_name=dataset_name,
        destination=filesystem_destination,
        dataset_name="coat_resources",
    )
    run = pipeline.run(
        coat_source(api_key=api_key, base_url=base_url), loader_file_format="parquet"
    )
    log.info(
        f"COAT pipeline output written to:\n"
        f"- {bucket_url}/coat_resources/resources.parquet\n"
        f"- {bucket_url}/coat_resources/packages.parquet"
    )
    log.info(f"Pipeline run completed. Load info: {run}")


def plausible_paginate(api_key: str, base_payload: dict, page_size: int = 10_000):
    """Yield all result rows from a Plausible v2 query using dlt pagination."""
    client = RESTClient(
        base_url=PLAUSIBLE_API_URL,
        headers={"Content-Type": "application/json"},
        auth=BearerTokenAuth(token=api_key),
        paginator=OffsetPaginator(
            limit=page_size,
            offset_body_path="pagination.offset",
            limit_body_path="pagination.limit",
            total_path="meta.total_rows",
        ),
        data_selector="results",
    )

    payload = {
        **base_payload,
        "include": {"total_rows": True},
    }
    for page in client.paginate(method="POST", json=payload, timeout=60):
        yield from page


@dlt.resource(
    name="plausible_timeseries",
    primary_key="date",
    write_disposition="replace",
)
def plausible_timeseries(api_key: str, site_id: str = PLAUSIBLE_SITE_ID):
    """Fetch daily timeseries stats from Plausible Analytics."""
    log.info(f"Fetching Plausible timeseries for {site_id}")
    for row in plausible_paginate(
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
        for row in plausible_paginate(
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
    endpoint_url: str = typer.Option(
        ...,
        envvar="COAT_AWS_ENDPOINT",
        help="AWS S3 endpoint URL",
    ),
    access_key: str = typer.Option(
        ...,
        envvar="COAT_AWS_ACCESS_KEY",
        help="AWS S3 access key",
    ),
    secret_key: str = typer.Option(
        ...,
        envvar="COAT_AWS_SECRET_KEY",
        help="AWS S3 secret key",
    ),
    bucket: str = typer.Option(
        ...,
        envvar="COAT_AWS_BUCKET",
        help="AWS S3 bucket name",
    ),
    prefix: str = typer.Option(
        default="coat",
        envvar="COAT_S3_PREFIX",
        help="AWS S3 prefix (folder path) for storing data",
    ),
    region: str = typer.Option(
        default="us-east-1",
        envvar="COAT_S3_REGION",
        help="AWS S3 region",
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
    bucket_url, filesystem_destination = s3_filesystem_destination(
        endpoint_url, access_key, secret_key, bucket, prefix, region
    )

    pipeline = dlt.pipeline(
        pipeline_name="coat_plausible",
        destination=filesystem_destination,
        dataset_name="coat_plausible",
    )

    load_info = pipeline.run(
        plausible_source(api_key=api_key, site_id=site_id),
        loader_file_format="parquet",
    )

    log.info(f"Pipeline run info: {load_info}")
    log.info(f"Plausible pipeline complete. Data at: {bucket_url}/coat_plausible")


if __name__ == "__main__":
    app()
