"""Sync Biodiversa+ CRIS database sections to parquet on S3.

The public SPA at databases.biodiversa.eu has no documented API, but its
"Download" button calls an unauthenticated CSV/XLSX/ODS export endpoint
backed by Elasticsearch:

    GET /databases/{section}/download?query={elasticsearch query}&format=csv

Passing `{"match_all": {}}` as the query returns the full section. This
module fetches all five sections (projects, organisations, funding agencies,
funding programmes, research infrastructures) this way.
"""

import csv
import io

import dlt
import requests
import typer
from dlt.sources.helpers.requests import Client

from ..settings import log

app = typer.Typer(help="Sync Biodiversa+ CRIS database data to parquet on S3")

BIODIVERSA_REQUEST_TIMEOUT = 120
_MATCH_ALL_QUERY = '{"match_all":{}}'

_SECTIONS = (
    ("research_projects", "research-projects"),
    ("research_organisations", "research-organisations"),
    ("funding_agencies", "funding-agencies"),
    ("funding_programmes", "funding-programmes"),
    ("research_infrastructures", "research-infrastructures"),
)


def _fetch_section_rows(base_url: str, path: str, session: requests.Session):
    """Download and parse one section's CSV export."""
    url = f"{base_url.rstrip('/')}/{path}/download"
    response = session.get(
        url,
        params={"query": _MATCH_ALL_QUERY, "format": "csv"},
        timeout=BIODIVERSA_REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    text = response.content.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    yield from reader


@dlt.resource(name="biodiversa", write_disposition="replace", primary_key="id")
def biodiversa_tables(base_url: str):
    """Fetch every Biodiversa+ database section, one dlt table per section."""
    session = Client(
        request_timeout=BIODIVERSA_REQUEST_TIMEOUT, raise_for_status=False
    ).session
    for table_name, path in _SECTIONS:
        count = 0
        for row in _fetch_section_rows(base_url, path, session):
            count += 1
            yield dlt.mark.with_table_name(row, table_name)
        log.info(f"Fetched {count} rows for Biodiversa+ section '{path}'")


@dlt.source(name="biodiversa", max_table_nesting=0)
def biodiversa_source(base_url: str):
    """Define the Biodiversa+ data source."""
    return biodiversa_tables(base_url)


@app.command()
def run(
    ctx: typer.Context,
    base_url: str = typer.Option(
        default="https://databases.biodiversa.eu/databases",
        envvar="BIODIVERSA_BASE_URL",
        help="Biodiversa+ CRIS database base URL",
    ),
    dataset_name: str = typer.Option(
        default="biodiversa",
        envvar="BIODIVERSA_DATASET_NAME",
        help="Local pipeline name (used for dlt's local working/state directory)",
    ),
):
    """Fetch all Biodiversa+ database sections and write parquet files to S3."""
    pipeline = dlt.pipeline(
        pipeline_name=dataset_name,
        destination=ctx.obj["filesystem_destination"],
        dataset_name="biodiversa",
        progress="tqdm",
    )
    load_info = pipeline.run(
        biodiversa_source(base_url=base_url),
        loader_file_format="parquet",
    )
    log.info(
        f"Biodiversa+ pipeline output written to {ctx.obj['bucket_url']}/biodiversa/"
    )
    log.info(f"Pipeline run completed. Load info: {load_info}")


if __name__ == "__main__":
    app()
