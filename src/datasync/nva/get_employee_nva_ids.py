"""Find the NVA (Cristin) person id of every NINA employee.

The employees come from the UBW export. NVA persons are fetched for the
organization and matched by exact, case- and whitespace-sensitive full first
and last names.
"""

import duckdb
import pyarrow as pa
import typer

from ..settings import log
from .utils import nva_client, setup_duckdb_s3_connection

NVA_BASE_URL = "https://api.nva.unit.no/"
# the person search only accepts the short organization code, not "7511.0.0.0"
NVA_ORGANIZATION = "7511"
PAGE_SIZE = 100

MATCHED = "matched"
NOT_FOUND = "not_found"


def _person_names(person: dict) -> dict[str, str]:
    return {n["type"]: n["value"] for n in person["names"]}


def _cristin_id(person: dict) -> str:
    identifiers = {i["type"]: i["value"] for i in person["identifiers"]}
    return str(identifiers["CristinIdentifier"])


def load_persons() -> list[dict]:
    """Fetch all NVA persons affiliated with NINA."""
    client = nva_client(NVA_BASE_URL)
    return list(
        client.paginate(
            "cristin/person",
            method="GET",
            params={"organization": NVA_ORGANIZATION, "results": PAGE_SIZE},
        )
    )


def match_employees(
    employees: list[tuple[str, str, str]], persons: list[dict]
) -> list[dict]:
    """Match employee tuples to NVA ids using exact first and last names."""
    by_name: dict[tuple[str, str], set[str]] = {}
    for person in persons:
        names = _person_names(person)
        firstname = names["FirstName"]
        lastname = names["LastName"]
        cristin_id = _cristin_id(person)
        by_name.setdefault((firstname, lastname), set()).add(cristin_id)

    results = []
    for employee_id, firstname, lastname in employees:
        ids = by_name.get((firstname, lastname), set())
        if len(ids) == 1:
            status = MATCHED
        else:
            status = NOT_FOUND
        results.append(
            {
                "employee_id": str(employee_id),
                "nva_id": next(iter(ids)) if status == MATCHED else None,
                "match_status": status,
            }
        )
    return results


def load_employees(
    employees_parquet: str, resource_types: list[str]
) -> list[tuple[str, str, str]]:
    """Read (ansattid, firstname, lastname) from the UBW employees export."""
    with duckdb.connect() as con:
        con.execute("INSTALL httpfs; LOAD httpfs;")
        return con.execute(
            """
            SELECT ansattid, firstname, lastname
            FROM read_parquet(?)
            WHERE ressurstype IN (SELECT unnest(?))
            ORDER BY ansattid
            """,
            [employees_parquet, resource_types],
        ).fetchall()


def match_employees_to_s3(
    employees_parquet: str = typer.Option(
        "https://s3-int-1.nina.no/dms/ubw/ubw/employees.parquet",
        envvar="UBW_EMPLOYEES_PARQUET",
        help="UBW employees export (ansattid, firstname, lastname, ressurstype)",
    ),
    resource_types: str = typer.Option(
        "F,P,R",
        envvar="UBW_EMPLOYEE_RESOURCE_TYPES",
        help="Comma separated UBW resource types that count as employees",
    ),
    s3_endpoint_url: str = typer.Option(
        ..., envvar="NVA_IDS_S3_ENDPOINT_URL", help="S3 endpoint URL"
    ),
    s3_access_key: str = typer.Option(
        ..., envvar="NVA_IDS_S3_ACCESS_KEY", help="S3 access key"
    ),
    s3_secret_key: str = typer.Option(
        ..., envvar="NVA_IDS_S3_SECRET_KEY", help="S3 secret key"
    ),
    s3_bucket: str = typer.Option(..., envvar="NVA_IDS_S3_BUCKET", help="S3 bucket"),
    s3_path: str = typer.Option(
        "nina/employees_nva_ids.parquet",
        envvar="NVA_IDS_S3_PATH",
        help="Object path of the mapping parquet in the bucket",
    ),
    s3_region: str = typer.Option(
        "us-east-1", envvar="NVA_IDS_S3_REGION", help="S3 region"
    ),
) -> None:
    types = [t.strip() for t in resource_types.split(",")]
    employees = load_employees(employees_parquet, types)
    log.info("Matching employees to NVA", employees=len(employees))

    persons = load_persons()
    results = match_employees(employees, persons)
    matched = [r for r in results if r["match_status"] == MATCHED]
    names = {str(eid): f"{first} {last}" for eid, first, last in employees}
    unmatched = [
        names[r["employee_id"]] for r in results if r["match_status"] == NOT_FOUND
    ]
    if unmatched:
        log.warning(
            "Employees without NVA id",
            status=NOT_FOUND,
            count=len(unmatched),
            names=unmatched,
        )
    log.info("Matched", matched=len(matched), total=len(results))

    table = pa.Table.from_pylist(
        results,
        schema=pa.schema(
            [
                ("employee_id", pa.string()),
                ("nva_id", pa.string()),
                ("match_status", pa.string()),
            ]
        ),
    )
    con = setup_duckdb_s3_connection(
        s3_endpoint_url, s3_access_key, s3_secret_key, s3_region
    )
    con.register("mapping", table)
    # bucket and path are operator-supplied configuration, not user input
    con.execute(
        f"COPY (FROM mapping ORDER BY employee_id) "  # noqa: S608
        f"TO 's3://{s3_bucket}/{s3_path}' (FORMAT PARQUET)"
    )
    con.close()
    log.info("Wrote NVA id mapping", path=f"s3://{s3_bucket}/{s3_path}")
