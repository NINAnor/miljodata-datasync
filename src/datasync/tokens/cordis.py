"""Sync CORDIS bulk project data (H2020 + Horizon Europe) to parquet on S3.

CORDIS has no filtered REST API, so this downloads the official bulk CSV
export ZIPs and loads every CSV file found inside as its own table
(project, organization, topics, webItem, webLink, legalBasis, euroSciVoc,
and any programme-specific extras such as Horizon's policyPriorities).

ERC-funded projects are not a separate resource: they're a subset of the
CORDIS "project" table, identifiable via the `fundingScheme` column
(e.g. ERC-STG, ERC-ADG, ERC-COG, ERC-SyG).
"""

import csv
import io
import re

import dlt
import fsspec
import typer

from ..settings import log

app = typer.Typer(help="Sync CORDIS bulk project data to parquet on S3")


_CAMEL_TO_SNAKE = re.compile(r"(?<!^)(?=[A-Z])")


def _to_table_name(csv_path: str) -> str:
    """Convert a CORDIS CSV member name (e.g. 'euroSciVoc.csv') to snake_case."""
    stem = csv_path.removesuffix(".csv")
    return _CAMEL_TO_SNAKE.sub("_", stem).lower()


def _iter_csv_tables(programme: str, url: str):
    """Yield (table_name, row) for every top-level CSV file in a CORDIS export ZIP."""
    log.info(f"Downloading CORDIS {programme} export from {url}")
    archive = fsspec.filesystem("zip", fo=url, mode="r")
    csv_paths = sorted(archive.glob("*.csv"))
    log.info(f"Found {len(csv_paths)} CSV tables for programme '{programme}'")

    for path in csv_paths:
        table_name = _to_table_name(path)
        count = 0
        with archive.open(path, mode="rb") as raw:
            text = io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")
            reader = csv.DictReader(text, delimiter=";")
            for row in reader:
                row["programme"] = programme
                count += 1
                yield table_name, row
        log.info(f"Parsed {count} rows from '{path}' as table '{table_name}'")


@dlt.resource(name="cordis", write_disposition="replace")
def cordis_tables(programmes: dict[str, str]):
    """Fetch every CSV table from the CORDIS bulk export ZIPs, one table per file."""
    for programme, url in programmes.items():
        for table_name, row in _iter_csv_tables(programme, url):
            yield dlt.mark.with_table_name(row, table_name)


@dlt.source(name="cordis", max_table_nesting=0)
def cordis_source(programmes: dict[str, str]):
    """Define the CORDIS data source."""
    return cordis_tables(programmes)


@app.command()
def run(
    ctx: typer.Context,
    h2020_url: str = typer.Option(
        envvar="CORDIS_H2020_URL",
        help="CORDIS H2020 bulk CSV export ZIP URL",
    ),
    horizon_url: str = typer.Option(
        envvar="CORDIS_HORIZON_URL",
        help="CORDIS Horizon Europe bulk CSV export ZIP URL",
    ),
    dataset_name: str = typer.Option(
        default="cordis",
        envvar="CORDIS_DATASET_NAME",
        help="Local pipeline name (used for dlt's local working/state directory)",
    ),
):
    """Download the CORDIS bulk exports and load every table to parquet on S3."""
    bucket_url = ctx.obj["bucket_url"]
    filesystem_destination = ctx.obj["filesystem_destination"]

    pipeline = dlt.pipeline(
        pipeline_name=dataset_name,
        destination=filesystem_destination,
        dataset_name="cordis",
    )
    run = pipeline.run(
        cordis_source({"h2020": h2020_url, "horizon": horizon_url}),
        loader_file_format="parquet",
    )
    log.info(f"CORDIS pipeline output written to {bucket_url}/cordis/")
    log.info(f"Pipeline run completed. Load info: {run}")


if __name__ == "__main__":
    app()
