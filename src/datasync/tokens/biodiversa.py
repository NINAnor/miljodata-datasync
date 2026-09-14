"""Sync Biodiversa+ CRIS database sections to parquet on S3.

The public SPA at databases.biodiversa.eu has no documented API. Two
undocumented endpoints back it:

- A CSV/XLSX/ODS bulk export, used here for the four reference sections
  (organisations, funding agencies, funding programmes, research
  infrastructures) which are not linked to individual projects:

    GET /databases/{section}/download?query={elasticsearch query}&format=csv

- A proxied Elasticsearch `_msearch` endpoint backing the "projects" index,
  used here for `research_projects`:

    POST /es/projects/_msearch

  This is preferred over the CSV export for projects because the CSV export
  flattens the project's partner institutions ("orgunits") and funding
  sources ("fundings") into a fixed 5-column window, silently dropping any
  additional partners/funders beyond that. The raw ES documents carry the
  full, unbounded lists, plus a `url` and `keywords` field that the CSV
  export doesn't include at all. Confirmed: no other section (organisations,
  funding-agencies, funding-programmes, research-infrastructures) is
  exposed via `/es/{section}/_msearch` -- only `projects` is.

Passing `{"match_all": {}}` as the query returns the full section/index.
"""

import csv
import io
from urllib.parse import urlparse

import dlt
import requests
import typer
from dlt.sources.helpers.requests import Client

from ..settings import log
from ._pipeline import run_pipeline

app = typer.Typer(help="Sync Biodiversa+ CRIS database data to parquet on S3")

BIODIVERSA_REQUEST_TIMEOUT = 120
BIODIVERSA_ES_PAGE_SIZE = 500
_MATCH_ALL_QUERY = '{"match_all":{}}'

_CSV_SECTIONS = (
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


def _flatten_org_list(field: str, items: list[dict] | None) -> list[dict]:
    """Flatten a list of {enddate, startdate, classification, <field>: {id, name}}."""
    if not items:
        return []
    flattened = []
    for item in items:
        ref = item.get(field) or {}
        flattened.append(
            {
                f"{field}_id": ref.get("id"),
                f"{field}_name": ref.get("name"),
                "classification": item.get("classification"),
                "startdate": item.get("startdate"),
                "enddate": item.get("enddate"),
            }
        )
    return flattened


def _es_root(base_url: str) -> str:
    """Return the site root for `base_url` (the ES proxy lives at `/es/...`,
    not under the `/databases` path used by the CSV export endpoints).
    """
    parsed = urlparse(base_url)
    return f"{parsed.scheme}://{parsed.netloc}"


def _fetch_project_pages(base_url: str, session: requests.Session):
    """Page through the Biodiversa+ `projects` Elasticsearch index.

    Uses `from`/`size` pagination (verified to work well past the default
    10,000-document Elasticsearch result window on this cluster) and stops
    once a page returns fewer hits than requested.
    """
    url = f"{_es_root(base_url)}/es/projects/_msearch"
    offset = 0
    while True:
        body = (
            "{}\n"
            f'{{"query":{{"match_all":{{}}}},"size":{BIODIVERSA_ES_PAGE_SIZE},'
            f'"from":{offset}}}\n'
        )
        response = session.post(
            url,
            data=body,
            headers={"Content-Type": "application/x-ndjson"},
            timeout=BIODIVERSA_REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        result = response.json()["responses"][0]
        if "error" in result:
            raise RuntimeError(f"Biodiversa+ ES query failed: {result['error']}")
        hits = result["hits"]["hits"]
        if not hits:
            break
        for hit in hits:
            source = hit["_source"]
            funder = source.get("source") or {}
            country = funder.get("country") or {}
            yield {
                "id": hit["_id"],
                "title": source.get("title"),
                "acronym": source.get("acronym"),
                "url": source.get("url"),
                "abstract": source.get("abstract"),
                "keywords": source.get("keywords"),
                "startdate": source.get("startdate"),
                "enddate": source.get("enddate"),
                "amount": source.get("amount"),
                "currency": source.get("currency"),
                "source_name": funder.get("name"),
                "source_acronym": funder.get("acronym"),
                "source_country_code": country.get("code"),
                "source_country_name": country.get("name"),
                "orgunits": _flatten_org_list("orgunit", source.get("orgunits")),
                "fundings": _flatten_org_list("funding", source.get("fundings")),
            }
        offset += len(hits)
        log.info(f"Fetched {offset} Biodiversa+ projects so far")
        if len(hits) < BIODIVERSA_ES_PAGE_SIZE:
            break


@dlt.resource(
    name="research_projects",
    write_disposition="replace",
    primary_key="id",
    columns={"keywords": {"data_type": "text"}},
)
def biodiversa_research_projects(base_url: str):
    """Fetch every Biodiversa+ project via the live Elasticsearch index."""
    session = Client(
        request_timeout=BIODIVERSA_REQUEST_TIMEOUT, raise_for_status=False
    ).session
    count = 0
    for row in _fetch_project_pages(base_url, session):
        count += 1
        yield row
    log.info(f"Fetched {count} Biodiversa+ research projects (all sources)")


@dlt.resource(name="biodiversa", write_disposition="replace", primary_key="id")
def biodiversa_reference_tables(base_url: str):
    """Fetch the four Biodiversa+ reference sections (not linked to projects)."""
    session = Client(
        request_timeout=BIODIVERSA_REQUEST_TIMEOUT, raise_for_status=False
    ).session
    for table_name, path in _CSV_SECTIONS:
        count = 0
        for row in _fetch_section_rows(base_url, path, session):
            count += 1
            yield dlt.mark.with_table_name(row, table_name)
        log.info(f"Fetched {count} rows for Biodiversa+ section '{path}'")


@dlt.source(name="biodiversa", max_table_nesting=1)
def biodiversa_source(base_url: str):
    """Define the Biodiversa+ data source.

    `max_table_nesting=1` lets the `orgunits`/`fundings` lists on each
    research project explode into child tables
    (`research_projects__orgunits`, `research_projects__fundings`); the
    CSV-derived reference tables are flat and unaffected by this setting.
    """
    return (
        biodiversa_research_projects(base_url),
        biodiversa_reference_tables(base_url),
    )


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
    run_pipeline(
        ctx,
        pipeline_name=dataset_name,
        dataset_name="biodiversa",
        source=biodiversa_source(base_url=base_url),
        source_name="Biodiversa+",
    )


if __name__ == "__main__":
    app()
