"""Sync LIFE Public Database projects and reference data to S3.

Project search uses the same undocumented `dissemination/search/excel`
endpoint the LIFE public website's own "Export to Excel" button calls
(confirmed via direct testing: identical total count to the plainer
`dissemination/search` endpoint, but with a much richer per-project record:
`themes`, `keywords`, `legislatives`, `habitats`, `species`, `country`,
`startDate`/`endDate`, `totalBudget`/`ecContribution`,
`beneficiaryName`/`beneficiaryAddress`, and `participantNames` +
`participantTypes` giving the full coordinator/partner organisation list).
There is still no free-text abstract/description field available anywhere
in the public API -- `themes` + `keywords` are the closest substitute.

`country` comes back as the EC's own multi-lingual country label (e.g.
"Ellas", "Österreich", "Deutschland") rather than a single normalized name.
`_LIFE_COUNTRY_NAMES` maps every label from the `/country/list` reference
endpoint to an ISO 3166-1 alpha-2 code and English name so downstream
consumers don't have to do this by hand.
"""

import dlt
import requests
import typer
from dlt.sources.rest_api import rest_api_source
from tqdm import tqdm

from ._pipeline import run_pipeline

app = typer.Typer(help="Sync LIFE Public Database data to parquet on S3")
LIFE_REQUEST_TIMEOUT = 60

_REFERENCE_RESOURCES = (
    ("priority_areas", "priorityArea/list"),
    ("countries", "country/list"),
    ("themes", "theme/list"),
    ("keywords", "keyword/list"),
    ("legislation", "legislative/list"),
    ("beneficiary_types", "beneficiaryType/list"),
)

# Every label returned by GET /country/list as of 2026, mapped to
# (english_name, iso_alpha_2). Entries with no ISO code (regional groupings,
# "International Organisations", etc.) map to (english_name, None).
_LIFE_COUNTRY_NAMES: dict[str, tuple[str, str | None]] = {
    "Albania Shqipëria": ("Albania", "AL"),
    "Algerie": ("Algeria", "DZ"),
    "Andorra": ("Andorra", "AD"),
    "Belarus Belorussija": ("Belarus", "BY"),
    "België - Belgique": ("Belgium", "BE"),
    "Bosnia Herzegovina": ("Bosnia and Herzegovina", "BA"),
    "Bulgaria Balgarija": ("Bulgaria", "BG"),
    "Città del Vaticano": ("Vatican City", "VA"),
    "Croatia Hrvatska": ("Croatia", "HR"),
    "Cyprus": ("Cyprus", "CY"),
    "Czech Cesko": ("Czechia", "CZ"),
    "Danmark": ("Denmark", "DK"),
    "Deutschland": ("Germany", "DE"),
    "Egypt": ("Egypt", "EG"),
    "Ellas": ("Greece", "GR"),
    "España": ("Spain", "ES"),
    "Estonia Eesti": ("Estonia", "EE"),
    "Finland Suomi": ("Finland", "FI"),
    "France": ("France", "FR"),
    "French Polynesia": ("French Polynesia", "PF"),
    "Gambia": ("Gambia", "GM"),
    "Gaza strip & West Bank": ("Palestine", "PS"),
    "Group Associated Countries": ("Associated Countries (group)", None),
    "Group Baltic Countries": ("Baltic Countries (group)", None),
    "Group Mediterian": ("Mediterranean Countries (group)", None),
    "Group Other Third Countries": ("Other Third Countries (group)", None),
    "Hungary Magyarország": ("Hungary", "HU"),
    "Iceland": ("Iceland", "IS"),
    "International Organisations": ("International Organisations", None),
    "Ireland": ("Ireland", "IE"),
    "Israel": ("Israel", "IL"),
    "Italia": ("Italy", "IT"),
    "Jordan": ("Jordan", "JO"),
    "Kosovo": ("Kosovo", "XK"),
    "Latvia Latvija": ("Latvia", "LV"),
    "Lebanon": ("Lebanon", "LB"),
    "Liechtenstein": ("Liechtenstein", "LI"),
    "Lithuania Lietuva": ("Lithuania", "LT"),
    "Luxembourg": ("Luxembourg", "LU"),
    "Lybia": ("Libya", "LY"),
    "Malta": ("Malta", "MT"),
    "Maroc": ("Morocco", "MA"),
    "Moldavia Moldavija": ("Moldova", "MD"),
    "Monaco": ("Monaco", "MC"),
    "Montenegro": ("Montenegro", "ME"),
    "Nederland": ("Netherlands", "NL"),
    "New Caledonia": ("New Caledonia", "NC"),
    "North Macedonia": ("North Macedonia", "MK"),
    "Norway Norge": ("Norway", "NO"),
    "Poland Polska": ("Poland", "PL"),
    "Portugal": ("Portugal", "PT"),
    "Romania": ("Romania", "RO"),
    "Russia Rossija": ("Russia", "RU"),
    "San Marino": ("San Marino", "SM"),
    "Schweiz Suisse Svizzera": ("Switzerland", "CH"),
    "Serbia": ("Serbia", "RS"),
    "Slovakia Slovensko": ("Slovakia", "SK"),
    "Slovenia Slovenija": ("Slovenia", "SI"),
    "South Korea": ("South Korea", "KR"),
    "Sverige": ("Sweden", "SE"),
    "Syria": ("Syria", "SY"),
    "Tunisie": ("Tunisia", "TN"),
    "Turkey Türkiye": ("Türkiye", "TR"),
    "Ukraine": ("Ukraine", "UA"),
    "United Kingdom": ("United Kingdom", "GB"),
    "United States of America": ("United States", "US"),
    "Österreich": ("Austria", "AT"),
}


def _normalize_country(raw: str | None) -> tuple[str | None, str | None]:
    """Return (english_name, iso_alpha_2) for a raw LIFE `country` label.

    Falls back to `(raw, None)` for any label not seen in the reference
    list above (e.g. if the EC adds a new country/grouping).
    """
    if not raw:
        return None, None
    english_name, iso_code = _LIFE_COUNTRY_NAMES.get(raw, (raw, None))
    return english_name, iso_code


def _split_participants(names: str | None, types: str | None) -> dict:
    """Split the parallel `participantNames`/`participantTypes` strings into
    a coordinator name and a semicolon-joined list of partner names.
    """
    if not names or not types:
        return {"coordinator": None, "partners": None}
    name_list = [n.strip() for n in names.split(",")]
    type_list = [t.strip() for t in types.split(",")]
    coordinator = None
    partners = []
    for name, kind in zip(name_list, type_list, strict=False):
        if kind.lower() == "coordinator":
            coordinator = name
        else:
            partners.append(name)
    return {"coordinator": coordinator, "partners": "; ".join(partners) or None}


def _parse_amount(value: str | None) -> float | None:
    """Parse a comma-thousands-separated amount string (e.g. "951,230")."""
    if not value:
        return None
    try:
        return float(value.replace(",", ""))
    except ValueError:
        return None


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
    """Fetch all LIFE projects through the public "export to Excel" search
    endpoint, which -- unlike the plainer search endpoint -- includes
    themes/keywords/legislatives/habitats/species, country, budget, dates,
    and the full coordinator/partner participant list per project.
    """
    search_url = f"{base_url.rstrip('/')}/{project_search_path}"
    page = 1
    total = None

    with tqdm(desc="Fetching LIFE projects", unit="project") as progress:
        while total is None or (page - 1) * page_size < total:
            response = requests.post(
                search_url,
                data=_project_search_payload(page, page_size),
                headers={"Accept": "application/json"},
                timeout=LIFE_REQUEST_TIMEOUT,
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
            for record in records:
                country_en, country_code = _normalize_country(record.get("country"))
                yield {
                    **record,
                    "country_original": record.get("country"),
                    "country": country_en,
                    "country_code": country_code,
                    "total_budget": _parse_amount(record.get("totalBudget")),
                    "ec_contribution": _parse_amount(record.get("ecContribution")),
                    **_split_participants(
                        record.get("participantNames"), record.get("participantTypes")
                    ),
                }
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
        default="https://webgate.ec.europa.eu/life/publicWebsite/api/rest",
        envvar="LIFE_API_URL",
        help="LIFE Public Database API base URL",
    ),
    project_search_path: str = typer.Option(
        default="dissemination/search/excel",
        envvar="LIFE_PROJECT_SEARCH_PATH",
        help="LIFE project search endpoint path",
    ),
    project_page_size: int = typer.Option(
        default=100,
        envvar="LIFE_PROJECT_PAGE_SIZE",
        min=1,
        help="Number of projects to request per page",
    ),
    dataset_name: str = typer.Option(
        default="life",
        envvar="LIFE_DATASET_NAME",
        help="Local pipeline name (used for dlt's local working/state directory)",
    ),
):
    """Fetch LIFE project and reference data and write parquet files to S3."""
    run_pipeline(
        ctx,
        pipeline_name=dataset_name,
        dataset_name="life",
        source=life_source(
            base_url=base_url,
            project_search_path=project_search_path,
            page_size=project_page_size,
        ),
        source_name="LIFE",
    )


if __name__ == "__main__":
    app()
