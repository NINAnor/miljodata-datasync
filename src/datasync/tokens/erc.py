"""Sync the CORDIS ERC Principal Investigators list to parquet on S3.

ERC-funded projects are a subset of the CORDIS H2020/Horizon Europe project
data already ingested by ``cordis.py`` (identifiable via the
``fundingScheme`` column, e.g. ERC-STG, ERC-ADG, ERC-COG, ERC-SyG). The one
piece of ERC-specific data not in the bulk project ZIPs is the dedicated
Principal Investigators list, published as a single XLSX file:

    https://cordis.europa.eu/data/cordis-h2020-erc-pi.xlsx
"""

import io

import dlt
import openpyxl
import requests
import typer

from ..settings import log
from ._pipeline import run_pipeline

app = typer.Typer(help="Sync CORDIS ERC Principal Investigators data to parquet on S3")

ERC_REQUEST_TIMEOUT = 120


def _fetch_pi_rows(url: str):
    """Download the ERC PI XLSX file and yield each row as a dict."""
    response = requests.get(url, timeout=ERC_REQUEST_TIMEOUT)
    response.raise_for_status()

    workbook = openpyxl.load_workbook(io.BytesIO(response.content), read_only=True)
    try:
        worksheet = workbook.worksheets[0]
        rows = worksheet.iter_rows(values_only=True)
        header_row = next(rows, None)
        if header_row is None:
            raise ValueError("ERC PI workbook does not contain a header row")

        header_length = len(header_row)
        while header_length and header_row[header_length - 1] is None:
            header_length -= 1
        header = [str(cell).strip() for cell in header_row[:header_length]]
        if not header or any(not name for name in header):
            raise ValueError("ERC PI workbook contains an invalid header row")

        for row_number, row in enumerate(rows, start=2):
            extra_values = row[header_length:]
            if any(value is not None and str(value).strip() for value in extra_values):
                raise ValueError(
                    f"ERC PI workbook row {row_number} has values beyond the header"
                )
            values = (*row[:header_length],) + (None,) * max(
                0, header_length - len(row)
            )
            yield dict(zip(header, values, strict=True))
    finally:
        workbook.close()


@dlt.resource(name="principal_investigators", write_disposition="replace")
def erc_principal_investigators(url: str):
    """Fetch the CORDIS ERC Principal Investigators list."""
    count = 0
    for row in _fetch_pi_rows(url):
        count += 1
        yield row
    log.info(f"Fetched {count} ERC principal investigator rows")


@dlt.source(name="erc", max_table_nesting=0)
def erc_source(pi_list_url: str):
    """Define the CORDIS ERC data source."""
    return erc_principal_investigators(pi_list_url)


@app.command()
def run(
    ctx: typer.Context,
    pi_list_url: str = typer.Option(
        default="https://cordis.europa.eu/data/cordis-h2020-erc-pi.xlsx",
        envvar="ERC_PI_LIST_URL",
        help="CORDIS ERC Principal Investigators XLSX URL",
    ),
    dataset_name: str = typer.Option(
        default="erc",
        envvar="ERC_DATASET_NAME",
        help="Local pipeline name (used for dlt's local working/state directory)",
    ),
):
    """Fetch the CORDIS ERC Principal Investigators list and write parquet to S3.

    ERC-funded projects themselves are already covered by the CORDIS bulk
    project data (see `datasync tokens cordis run`); filter that data's
    `funding_scheme` column for values starting with "ERC-" to isolate them.
    """
    run_pipeline(
        ctx,
        pipeline_name=dataset_name,
        dataset_name="erc",
        source=erc_source(pi_list_url=pi_list_url),
        source_name="ERC",
    )


if __name__ == "__main__":
    app()
