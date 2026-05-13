import typer

from ..settings import (
    log,
)
from .nva_search_resources import search_resources_api
from .utils import (
    setup_duckdb_s3_connection,
    write_timestamp,
)

app = typer.Typer(help="Fetching publications from researchers at NINA")


def _fetch_publications_for_researcher(
    nva_id: str,
    storage_s3_path: str,
    storage_access_key: str,
    storage_secret_key: str,
    storage_bucket: str,
    storage_prefix: str,
    storage_region: str,
    base_url: str = "https://api.nva.unit.no/",
) -> str:
    log.debug("Fetching publications for researcher", nva_id=nva_id)
    search_resources_api(
        resource_name=f"publications_{nva_id}",
        base_url=base_url,
        filters=[f"contributor=https://api.nva.unit.no/cristin/person/{nva_id}"],
        apply_filter=False,
        storage_endpoint_url=storage_s3_path,
        storage_access_key=storage_access_key,
        storage_secret_key=storage_secret_key,
        storage_bucket=storage_bucket,
        storage_prefix=f"{storage_prefix}/{nva_id}/",
        storage_region=storage_region,
        add_timestamp=False,
    )
    return nva_id


@app.command()
def get_pubs_per_researcher(
    employees_parquet: str = typer.Option(
        ...,
        envvar="NINA_EMPLOYEES_PARQUET",
        help="Path to employees parquet file",
    ),
    storage_s3_path: str = typer.Option(
        ...,
        envvar="NVA_S3_ENDPOINT_URL",
        help="S3 endpoint URL for storage",
    ),
    storage_access_key: str = typer.Option(
        ..., envvar="NVA_S3_ACCESS_KEY", help="S3 access key for storage"
    ),
    storage_secret_key: str = typer.Option(
        ..., envvar="NVA_S3_SECRET_KEY", help="S3 secret key for storage"
    ),
    storage_bucket: str = typer.Option(
        ..., envvar="NVA_S3_BUCKET", help="S3 bucket for storage"
    ),
    storage_prefix: str = typer.Option(
        ..., envvar="NVA_S3_PREFIX", help="S3 prefix for storage"
    ),
    storage_region: str = typer.Option(
        "us-east-1", envvar="NVA_S3_REGION", help="S3 region for storage"
    ),
    test: bool = typer.Option(
        False,
        help="If set, will only fetch publications for the first 3 researchers",
    ),
):
    log.debug("Fetching ALL publications from researchers at NINA")

    con = setup_duckdb_s3_connection(
        endpoint_url=storage_s3_path,
        access_key=storage_access_key,
        secret_key=storage_secret_key,
        region=storage_region,
    )

    employees = con.read_parquet(employees_parquet)  # noqa: F841

    # get all employees with missing nva_id and log their names for debugging
    summary = con.execute("""
        SELECT
            COUNT(*) FILTER (WHERE nva_id IS NULL)                              AS missing_count,
            COUNT(*)                                                             AS total,
            array_agg(firstname || ' ' || lastname) FILTER (WHERE nva_id IS NULL) AS missing_names,
            array_agg(DISTINCT nva_id::VARCHAR) FILTER (WHERE nva_id IS NOT NULL)  AS nva_ids
        FROM employees
    """).fetchone()  # noqa: E501
    missing_count, total, missing_names, nva_ids = summary

    if missing_count > 0:
        log.warning(
            "Employees missing NVA IDs",
            missing_count=missing_count,
            total_employees=total,
            missing_names=missing_names,
        )
    con.close()

    if test:
        nva_ids = nva_ids[:3]
        log.warning(
            "Test mode enabled, only fetching publications for first 3 researchers",
            nva_ids=nva_ids,
        )

    failed = []
    for nva_id in nva_ids:
        try:
            _fetch_publications_for_researcher(
                nva_id,
                storage_s3_path,
                storage_access_key,
                storage_secret_key,
                storage_bucket,
                storage_prefix,
                storage_region,
            )
        except Exception as e:
            log.error(
                "Failed to fetch publications for researcher",
                nva_id=nva_id,
                error=str(e),
            )
            failed.append(nva_id)

    if failed:
        log.warning(
            "Some researchers failed to fetch", failed=failed, count=len(failed)
        )

    log.info(
        "Fetched all publications for researchers",
        total_researchers=len(nva_ids),
        failed=len(failed),
        succeeded=len(nva_ids) - len(failed),
    )

    con = setup_duckdb_s3_connection(
        endpoint_url=storage_s3_path,
        access_key=storage_access_key,
        secret_key=storage_secret_key,
        region=storage_region,
    )
    write_timestamp(con, storage_bucket, storage_prefix)
    con.close()
