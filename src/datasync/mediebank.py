"""Sync Mediebank employee portraits to S3."""

from datetime import datetime

import duckdb
import requests
import s3fs
import typer
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import OffsetPaginator

from .settings import log

PAGE_SIZE = 100

app = typer.Typer(help="Commands to handle Mediebank employee portraits")


def get_token(
    client_id: str,
    client_secret: str,
    token_url: str,
    audience: str,
) -> str:
    """Request a Mediebank access token using OAuth2 client credentials."""
    response = requests.post(
        token_url,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "audience": audience,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()["access_token"]


def browse_assets(token: str, api_url: str, collection: int) -> list[dict]:
    """List all assets in the employee portraits collection."""
    client = RESTClient(
        base_url=f"{api_url}/",
        auth=BearerTokenAuth(token),
        paginator=OffsetPaginator(
            limit=PAGE_SIZE,
            offset_param="offset",
            limit_param="limit",
            total_path="_page.total",
        ),
    )
    assets: list[dict] = [
        item
        for page in client.paginate("/", params={"collections": collection})
        for item in page
    ]
    log.info("browsed assets", collection=collection, count=len(assets))
    return assets


def download_asset(token: str, api_url: str, asset_id: str) -> bytes | None:
    """Download an asset's original file."""
    response = requests.get(
        f"{api_url}/download/{asset_id}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=120,
    )
    if response.status_code != 200:
        log.warning(
            "failed to download asset",
            asset_id=asset_id,
            status=response.status_code,
        )
        return None
    return response.content


def load_employees(employees_parquet: str) -> dict[str, dict]:
    """Load employees with a portrait url from the employees parquet."""
    with duckdb.connect() as connection:
        rows = connection.execute(
            "SELECT employee_id, firstname, lastname, picture_url FROM read_parquet(?)",
            [employees_parquet],
        ).fetchall()
    return {
        str(employee_id): {"name": f"{first} {last}"}
        for employee_id, first, last, url in rows
        if url and "ansattbilder/" in url
    }


def match_assets_to_employees(
    assets: list[dict], employees: dict[str, dict]
) -> dict[str, dict]:
    """Map each employee to their newest portrait, matched by headline."""
    by_name = {
        employee["name"]: employee_id for employee_id, employee in employees.items()
    }

    candidates: dict[str, list[dict]] = {}
    for asset in assets:
        employee_id = by_name.get(asset["headline"])
        if employee_id:
            candidates.setdefault(employee_id, []).append(asset)

    matched: dict[str, dict] = {}
    for employee_id, portraits in candidates.items():
        matched[employee_id] = max(portraits, key=lambda asset: asset["dateArchived"])
    return matched


@app.command(help="Sync employee portraits from Mediebank to S3")
def employees_portraits(
    client_id: str = typer.Option(
        ..., envvar="MEDIEBANK_CLIENT_ID", help="Mediebank API client id"
    ),
    client_secret: str = typer.Option(
        ..., envvar="MEDIEBANK_CLIENT_SECRET", help="Mediebank API client secret"
    ),
    token_url: str = typer.Option(
        "https://login.sdl.no/oauth/token",
        envvar="MEDIEBANK_TOKEN_URL",
        help="OAuth2 token URL",
    ),
    audience: str = typer.Option(
        "https://api.ntb.no", envvar="MEDIEBANK_AUDIENCE", help="OAuth2 audience"
    ),
    api_url: str = typer.Option(
        "https://api.ntb.no/media/v1/mb",
        envvar="MEDIEBANK_API_URL",
        help="Mediebank API base URL",
    ),
    collection: int = typer.Option(
        67480, envvar="MEDIEBANK_COLLECTION", help="Portraits collection id"
    ),
    employees_parquet: str = typer.Option(
        "https://s3-ext-1.nina.no/dms/nina/employees.parquet",
        envvar="NINA_EMPLOYEES_PARQUET",
        help="Path to the employees parquet file",
    ),
    s3_endpoint_url: str = typer.Option(
        ..., envvar="MEDIEBANK_S3_ENDPOINT_URL", help="S3 endpoint URL"
    ),
    s3_access_key: str = typer.Option(
        ..., envvar="MEDIEBANK_S3_ACCESS_KEY", help="S3 access key"
    ),
    s3_secret_key: str = typer.Option(
        ..., envvar="MEDIEBANK_S3_SECRET_KEY", help="S3 secret key"
    ),
    s3_bucket: str = typer.Option(..., envvar="MEDIEBANK_S3_BUCKET", help="S3 bucket"),
    s3_prefix: str = typer.Option(
        "nina.no/ansattbilder",
        envvar="MEDIEBANK_S3_PREFIX",
        help="S3 prefix for the portraits",
    ),
) -> None:
    """Copy employee portraits from the Mediebank to the S3 bucket."""
    log.info("Starting Mediebank employee portraits sync")
    token = get_token(client_id, client_secret, token_url, audience)
    employees = load_employees(employees_parquet)
    assets = browse_assets(token, api_url, collection)
    matched = match_assets_to_employees(assets, employees)

    log.info(
        "plan",
        employees=len(employees),
        matched=len(matched),
        missing=len(employees) - len(matched),
    )

    fs = s3fs.S3FileSystem(
        endpoint_url=s3_endpoint_url,
        key=s3_access_key,
        secret=s3_secret_key,
    )
    for employee_id, asset in sorted(matched.items(), key=lambda item: int(item[0])):
        path = f"s3://{s3_bucket}/{s3_prefix.rstrip('/')}/{employee_id}.jpg"

        if fs.exists(path):
            last_modified = fs.info(path)["LastModified"]
            archived = datetime.fromisoformat(
                asset["dateArchived"].replace("Z", "+00:00")
            )
            if archived <= last_modified:
                log.info("up to date", employee_id=employee_id, path=path)
                continue
            log.info("newer portrait", employee_id=employee_id, path=path)
        else:
            log.info("new portrait", employee_id=employee_id, path=path)

        data = download_asset(token, api_url, asset["id"])
        if data is None:
            log.warning("download failed", employee_id=employee_id)
            continue
        fs.write_bytes(path, data)
        log.info("copied", employee_id=employee_id, path=path)

    for employee_id in sorted(set(employees) - set(matched), key=int):
        log.warning(
            "no portrait found",
            employee_id=employee_id,
            name=employees[employee_id]["name"],
        )

    log.info("Mediebank employee portraits sync completed")


if __name__ == "__main__":
    app()
