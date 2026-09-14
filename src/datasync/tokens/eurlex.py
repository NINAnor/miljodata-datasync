"""Sync the environmental EUR-Lex policy documents used by TOKENS."""

import io
import re
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor

import dlt
import requests
import typer
from lxml import etree
from requests.adapters import HTTPAdapter

from ..settings import log
from ._pipeline import run_pipeline

app = typer.Typer(help="Sync EUR-Lex environmental policy documents to parquet on S3")

EURLEX_REQUEST_TIMEOUT = 120
EURLEX_MAX_WORKERS = 16
EURLEX_MAX_RETRIES = 4
EURLEX_RETRY_BASE_DELAY = 1.0
CELLAR_BASE_URL = "http://publications.europa.eu/resource"
EUROVOC_CATEGORY_IDS = ("2442", "2470")
_CDM = "http://publications.europa.eu/ontology/cdm#"
_RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
_OWL = "http://www.w3.org/2002/07/owl#"
_EUROVOC_CATEGORY_VALUES = "\n".join(
    f"    <http://eurovoc.europa.eu/{identifier}>"
    for identifier in EUROVOC_CATEGORY_IDS
)


def _make_session() -> requests.Session:
    """Build a `requests.Session` with a connection pool sized for
    `EURLEX_MAX_WORKERS` concurrent threads sharing it.
    """
    session = requests.Session()
    adapter = HTTPAdapter(
        pool_connections=EURLEX_MAX_WORKERS, pool_maxsize=EURLEX_MAX_WORKERS
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def _request_with_retry(
    session: requests.Session, method: str, url: str, **kwargs
) -> requests.Response:
    """GET/POST with exponential-backoff retries on transient failures
    (connection errors, timeouts, and 429/5xx responses).
    """
    last_error: Exception | None = None
    for attempt in range(1, EURLEX_MAX_RETRIES + 1):
        try:
            response = session.request(
                method, url, timeout=EURLEX_REQUEST_TIMEOUT, **kwargs
            )
            if response.status_code == 429 or response.status_code >= 500:
                last_error = requests.HTTPError(
                    f"{response.status_code} for {url}", response=response
                )
            else:
                response.raise_for_status()
                return response
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as exc:
            last_error = exc
        if attempt < EURLEX_MAX_RETRIES:
            delay = EURLEX_RETRY_BASE_DELAY * (2 ** (attempt - 1))
            log.warning(
                f"EUR-Lex request failed (attempt {attempt}/{EURLEX_MAX_RETRIES}), "
                f"retrying in {delay:.1f}s: {url}"
            )
            time.sleep(delay)
    raise last_error  # noqa: RSE102


def _parse_xml(content: bytes):
    parser = etree.XMLParser(resolve_entities=False, no_network=True, load_dtd=False)
    return etree.fromstring(content, parser=parser)  # noqa: S320


DEFAULT_SPARQL_QUERY = f"""
PREFIX cdm: <{_CDM}>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
SELECT ?work ?celex ?dateforce
       (GROUP_CONCAT(DISTINCT ?eurovoc_label; separator=";") AS ?eurovoc)
WHERE {{
  ?work cdm:resource_legal_id_celex ?celex ;
        cdm:resource_legal_in-force "1"^^xsd:boolean ;
        cdm:resource_legal_date_entry-into-force ?dateforce ;
        cdm:resource_legal_is_about_concept_directory-code ?directory ;
        cdm:work_is_about_concept_eurovoc ?eurovoc_uri .
  FILTER(
    xsd:integer(REPLACE(STR(?directory), ".*/", "")) < 15200000
    && STR(?directory) != "{CELLAR_BASE_URL}/authority/dir-eu-legal-act/15070000"
  )
    VALUES ?category {{
{_EUROVOC_CATEGORY_VALUES}
  }}
    {{ BIND(?category AS ?eurovoc_uri) }}
    UNION
    {{ ?eurovoc_uri <http://www.w3.org/2004/02/skos/core#broader> ?category }}
  ?eurovoc_uri <http://www.w3.org/2004/02/skos/core#prefLabel> ?eurovoc_label .
  FILTER(lang(?eurovoc_label) = "en")
}}
GROUP BY ?work ?celex ?dateforce
ORDER BY STR(?celex)
""".strip()


def _value(binding: dict, name: str) -> str | None:
    item = binding.get(name)
    return item["value"] if item else None


def _sparql_page(
    endpoint_url: str,
    query: str,
    page_size: int,
    session: requests.Session,
    last_celex: str | None = None,
):
    if last_celex is not None:
        order_by = re.search(r"\bORDER\s+BY\b", query, flags=re.IGNORECASE)
        if order_by is None:
            raise ValueError("Keyset pagination requires an ORDER BY clause")
        where_end = query.rfind("}", 0, order_by.start())
        if where_end == -1:
            raise ValueError("Keyset pagination requires an ORDER BY and WHERE block")
        query = (
            f'{query[:where_end]}FILTER(STR(?celex) > "{last_celex}")\n'
            f"{query[where_end : order_by.start()]}{query[order_by.start() :]}"
        )
    response = _request_with_retry(
        session,
        "GET",
        endpoint_url,
        params={
            "query": f"{query}\nLIMIT {page_size}",
            "format": "application/sparql-results+json",
        },
        headers={"Accept": "application/sparql-results+json"},
    )
    return response.json()["results"]["bindings"]


def _english_expression(work: str, session: requests.Session):
    response = _request_with_retry(
        session, "GET", work, headers={"Accept": "application/rdf+xml"}
    )
    root = _parse_xml(response.content)
    expressions = root.findall(f".//{{{_CDM}}}work_has_expression")
    for expression in expressions:
        uri = expression.attrib.get(f"{{{_RDF}}}resource", "")
        if not uri.endswith(".ENG"):
            continue
        expression_response = _request_with_retry(
            session, "GET", uri, headers={"Accept": "application/rdf+xml"}
        )
        expression_root = _parse_xml(expression_response.content)
        title = expression_root.find(f".//{{{_CDM}}}expression_title")
        manifestations = expression_root.findall(
            f".//{{{_CDM}}}expression_manifested_by_manifestation"
        )
        for manifestation in manifestations:
            resource = manifestation.attrib.get(f"{{{_RDF}}}resource", "")
            if resource.endswith(".fmx4"):
                return title.text if title is not None else None, resource
    return None, None


def _fetch_text(manifestation: str, session: requests.Session) -> str | None:
    response = _request_with_retry(
        session, "GET", manifestation, headers={"Accept": "application/rdf+xml"}
    )
    root = _parse_xml(response.content)
    same_as_urls = [
        item.attrib[f"{{{_RDF}}}resource"]
        for item in root.findall(f".//{{{_OWL}}}sameAs")
        if item.attrib.get(f"{{{_RDF}}}resource")
    ]
    xml_url = next(
        (
            url
            for url in same_as_urls
            if url.endswith(".xml") and not url.endswith((".doc.xml", ".toc.xml"))
        ),
        None,
    )
    if xml_url is None:
        xml_url = next((url for url in same_as_urls if url.endswith(".doc.xml")), None)
    if xml_url:
        document_response = _request_with_retry(session, "GET", xml_url)
        document = _parse_xml(document_response.content)
        return " ".join(text.strip() for text in document.itertext() if text.strip())

    zip_url = next((url for url in same_as_urls if url.endswith(".zip")), None)
    if not zip_url:
        return None
    package = _request_with_retry(session, "GET", zip_url)
    with zipfile.ZipFile(io.BytesIO(package.content)) as archive:
        names = archive.namelist()
        xml_name = next((name for name in names if name.endswith(".doc.fmx.xml")), None)
        if xml_name is None:
            xml_name = next(
                (
                    name
                    for name in names
                    if name.endswith(".xml") and not name.endswith(".toc.fmx.xml")
                ),
                None,
            )
        if xml_name is None:
            log.warning("No XML document found in EUR-Lex package", url=zip_url)
            return None
        document = _parse_xml(archive.read(xml_name))
    return " ".join(text.strip() for text in document.itertext() if text.strip())


def _fetch_one_document(binding: dict, session: requests.Session) -> dict | None:
    """Fetch the English title + full text for a single SPARQL binding.

    Returns `None` (and logs a warning) if the document can't be fetched
    after retries, so one bad document doesn't abort the whole sync.
    """
    work = _value(binding, "work")
    if work is None:
        return None
    try:
        title, manifestation = _english_expression(work, session)
        content = _fetch_text(manifestation, session) if manifestation else None
    except Exception as exc:  # noqa: BLE001
        log.warning(f"Skipping EUR-Lex document {work} after repeated failures: {exc}")
        return None
    return {
        "work": work,
        "celex": _value(binding, "celex"),
        "dateforce": _value(binding, "dateforce"),
        "eurovoc": _value(binding, "eurovoc"),
        "title": title,
        "content": content,
    }


@dlt.resource(name="documents", write_disposition="replace", primary_key="celex")
def eurlex_documents(endpoint_url: str, query: str, page_size: int):
    """Fetch filtered EUR-Lex metadata and English policy document text.

    Metadata pages are fetched sequentially via keyset (CELEX) pagination,
    but the (up to) `page_size` documents within each page are fetched
    concurrently -- each document requires 2-3 sequential CELLAR requests
    (work -> English expression -> manifestation/text), so parallelizing
    across documents is what makes a full sync of the ~2,500-3,000 matching
    documents tractable in minutes rather than hours.
    """
    session = _make_session()
    last_celex = None
    total = 0
    with ThreadPoolExecutor(max_workers=EURLEX_MAX_WORKERS) as pool:
        while True:
            bindings = _sparql_page(endpoint_url, query, page_size, session, last_celex)
            if not bindings:
                break
            results = pool.map(
                lambda binding: _fetch_one_document(binding, session), bindings
            )
            fetched = 0
            for row in results:
                if row is not None:
                    fetched += 1
                    yield row
            total += fetched
            last_celex = _value(bindings[-1], "celex")
            log.info(
                f"Fetched {fetched}/{len(bindings)} EUR-Lex policy records after "
                f"CELEX {last_celex} ({total} total so far)"
            )
            if len(bindings) < page_size:
                break


@dlt.source(name="eurlex", max_table_nesting=0)
def eurlex_source(endpoint_url: str, query: str, page_size: int):
    """Define the filtered EUR-Lex policy source."""
    return eurlex_documents(endpoint_url=endpoint_url, query=query, page_size=page_size)


@app.command()
def run(
    ctx: typer.Context,
    endpoint_url: str = typer.Option(
        default="https://publications.europa.eu/webapi/rdf/sparql",
        envvar="EURLEX_SPARQL_URL",
        help="Publications Office CELLAR SPARQL endpoint",
    ),
    query: str = typer.Option(
        default=DEFAULT_SPARQL_QUERY,
        envvar="EURLEX_SPARQL_QUERY",
        help="SPARQL query for filtered EUR-Lex policy metadata",
    ),
    page_size: int = typer.Option(
        default=100,
        envvar="EURLEX_PAGE_SIZE",
        min=1,
        max=1000,
        help="Number of metadata records to request per page",
    ),
    dataset_name: str = typer.Option(
        default="eurlex",
        envvar="EURLEX_DATASET_NAME",
        help="Local pipeline name used for dlt state",
    ),
):
    """Fetch the EUR-Lex policy dataset used by the TOKENS app."""
    run_pipeline(
        ctx,
        pipeline_name=dataset_name,
        dataset_name="eurlex",
        source=eurlex_source(
            endpoint_url=endpoint_url, query=query, page_size=page_size
        ),
        source_name="EUR-Lex",
    )


if __name__ == "__main__":
    app()
