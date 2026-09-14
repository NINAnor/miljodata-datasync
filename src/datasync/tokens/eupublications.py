"""Sync EU Open Publications report metadata from the CELLAR RDF store."""

import dlt
import requests
import typer

from ..settings import log
from ._pipeline import run_pipeline

app = typer.Typer(help="Sync EU Open Publications report metadata to parquet on S3")

DEFAULT_ENDPOINT_URL = "https://publications.europa.eu/webapi/rdf/sparql"
_CDM = "http://publications.europa.eu/ontology/cdm#"
_RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
_OWL = "http://www.w3.org/2002/07/owl#"
DEFAULT_QUERY = f"""
PREFIX cdm: <{_CDM}>
PREFIX rdf: <{_RDF}>
PREFIX owl: <{_OWL}>
SELECT ?work ?identifier ?date ?access_right ?same_as
WHERE {{
  ?work rdf:type cdm:report ;
        cdm:work_id_document ?identifier .
  OPTIONAL {{ ?work cdm:work_date_document ?date }}
  OPTIONAL {{ ?work cdm:work_restricted-by_access-right ?access_right }}
  OPTIONAL {{ ?work owl:sameAs ?same_as }}
}}
ORDER BY DESC(?date) STR(?identifier)
""".strip()


def _value(binding: dict, name: str) -> str | None:
    item = binding.get(name)
    return item["value"] if item else None


def _request_rows(endpoint_url: str, query: str, page_size: int):
    response = requests.get(
        endpoint_url,
        params={
            "query": f"{query}\nLIMIT {page_size}",
            "format": "application/sparql-results+json",
        },
        headers={"Accept": "application/sparql-results+json"},
        timeout=120,
    )
    response.raise_for_status()
    yield from response.json()["results"]["bindings"]


@dlt.resource(name="reports", write_disposition="replace", primary_key="work")
def eu_publication_reports(endpoint_url: str, query: str, page_size: int):
    """Fetch report metadata; CELLAR does not guarantee a document file per report."""
    count = 0
    for binding in _request_rows(endpoint_url, query, page_size):
        count += 1
        yield {
            "work": _value(binding, "work"),
            "identifier": _value(binding, "identifier"),
            "date": _value(binding, "date"),
            "access_right": _value(binding, "access_right"),
            "same_as": _value(binding, "same_as"),
        }
    log.info(f"Fetched {count} EU Open Publications reports")


@dlt.source(name="eupublications", max_table_nesting=0)
def eu_publications_source(endpoint_url: str, query: str, page_size: int):
    return eu_publication_reports(endpoint_url, query, page_size)


@app.command()
def run(
    ctx: typer.Context,
    endpoint_url: str = typer.Option(
        default=DEFAULT_ENDPOINT_URL,
        envvar="EU_PUBLICATIONS_SPARQL_URL",
        help="EU Open Publications CELLAR SPARQL endpoint",
    ),
    query: str = typer.Option(
        default=DEFAULT_QUERY,
        envvar="EU_PUBLICATIONS_SPARQL_QUERY",
        help="SPARQL query for report metadata",
    ),
    page_size: int = typer.Option(
        default=10000,
        envvar="EU_PUBLICATIONS_PAGE_SIZE",
        min=1,
        max=100000,
        help="Maximum number of report records to request",
    ),
    dataset_name: str = typer.Option(
        default="eupublications",
        envvar="EU_PUBLICATIONS_DATASET_NAME",
        help="Local pipeline name used for dlt state",
    ),
):
    """Fetch EU Open Publications report metadata and write parquet files to S3."""
    run_pipeline(
        ctx,
        pipeline_name=dataset_name,
        dataset_name="eupublications",
        source=eu_publications_source(endpoint_url, query, page_size),
        source_name="EU Open Publications",
    )


if __name__ == "__main__":
    app()
