"""Sync LIFE Public Database projects and reference data to S3."""

import dlt
import requests
import typer
from dlt.sources.rest_api import rest_api_source
from tqdm import tqdm

from ..settings import log

app = typer.Typer(help="Sync LIFE Public Database data to parquet on S3")

_REFERENCE_RESOURCES = (
    ("priority_areas", "priorityArea/list"),
    ("countries", "country/list"),
    ("themes", "theme/list"),
    ("keywords", "keyword/list"),
    ("legislation", "legislative/list"),
    ("beneficiary_types", "beneficiaryType/list"),
)


def life_reference_source(base_url: str):
    """Define the stable public LIFE reference-data endpoints."""
    return rest_api_source(
        {
            "client": {
                "base_url": base_url,
                "headers": {"Accept": "application/json"},
            },
            "resources": [
                {
                    "name": name,
                    "write_disposition": "replace",
                    "endpoint": {"path": path, "paginator": "single_page"},
                }
                for name, path in _REFERENCE_RESOURCES
            ],
        }
    )


def _project_search_payload(page: int, page_size: int) -> dict[str, str | int]:
    """Build the form data sent by LIFE's own advanced-search page."""
    return {
        "basicSearchText": "",
        "freeTextSearchType": "",
        "years": "",
        "priorityAreas": "",
        "submittingCountries": "",
        "nutsCodes": "",
        "beneficiaryTypes": "",
        "themes": "",
        "keywords": "",
        "legislatives": "",
        "habitats": "",
        "species": "",
        "nat2kSites": "",
        "isSelectedProject": "",
        "isBestProject": "",
        "isBestOfTheBestProject": "",
        "page": page,
        "pageSize": page_size,
        "skip": (page - 1) * page_size,
        "take": page_size,
        "sort[0][field]": "id",
        "sort[0][dir]": "desc",
    }


@dlt.resource(
    name="projects",
    primary_key="projectId",
    write_disposition="replace",
    columns={"location": {"data_type": "text"}},
)
def life_projects(base_url: str, project_search_path: str, page_size: int):
    """Fetch all LIFE projects through the public advanced-search endpoint."""
    search_url = f"{base_url.rstrip('/')}/{project_search_path}"
    page = 1
    total = None

    with tqdm(desc="Fetching LIFE projects", unit="project") as progress:
        while total is None or (page - 1) * page_size < total:
            response = requests.post(
                search_url,
                data=_project_search_payload(page, page_size),
                headers={"Accept": "application/json"},
                timeout=60,
            )
            response.raise_for_status()
            result = response.json()
            records = result["data"]
            total = result["total"]
            progress.total = total
            progress.update(len(records))
            progress.refresh()

            if not records:
                break
            yield from records
            page += 1


@dlt.source(name="life", max_table_nesting=0)
def life_source(base_url: str, project_search_path: str, page_size: int):
    """Define the LIFE projects and public reference-data source."""
    reference_resources = life_reference_source(base_url=base_url).resources.values()
    return (
        life_projects(
            base_url=base_url,
            project_search_path=project_search_path,
            page_size=page_size,
        ),
        *reference_resources,
    )


@app.command()
def run(
    ctx: typer.Context,
    base_url: str = typer.Option(
        ...,
        envvar="LIFE_API_URL",
        help="LIFE Public Database API base URL",
    ),
    project_search_path: str = typer.Option(
        ...,
        envvar="LIFE_PROJECT_SEARCH_PATH",
        help="LIFE project search endpoint path",
    ),
    project_page_size: int = typer.Option(
        ...,
        envvar="LIFE_PROJECT_PAGE_SIZE",
        help="Number of projects to request per page",
    ),
    dataset_name: str = typer.Option(
        default="life",
        envvar="LIFE_DATASET_NAME",
        help="Local pipeline name (used for dlt's local working/state directory)",
    ),
):
    """Fetch LIFE project and reference data and write parquet files to S3."""
    pipeline = dlt.pipeline(
        pipeline_name=dataset_name,
        destination=ctx.obj["filesystem_destination"],
        dataset_name="life",
    )
    load_info = pipeline.run(
        life_source(
            base_url=base_url,
            project_search_path=project_search_path,
            page_size=project_page_size,
        ),
        loader_file_format="parquet",
    )
    log.info(f"LIFE pipeline output written to {ctx.obj['bucket_url']}/life/")
    log.info(f"Pipeline run completed. Load info: {load_info}")


if __name__ == "__main__":
    app()
