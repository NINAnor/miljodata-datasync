import dlt
import typer
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import OffsetPaginator

from .settings import log

app = typer.Typer(help="Export COAT data to parquet")

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
