import typer

from ..settings import log
from .csw import eml_to_record, write_eml_record
from .dms import create_dms_dataset
from .geoapi import to_pygeoapi_resource, write_pygeoapi_resources
from .ipt import get_dataset_metadata, get_datasets
from .parquet import version_to_parquet
from .settings import (
    duckdb_install_extensions,
    duckdb_load_extensions,
    duckdb_load_s3_credentials,
)

app = typer.Typer()


@app.command()
def main(
    skip_data: bool = typer.Option(
        default=False, help="Ignore data conversion step, perform only metadata"
    ),
):
    duckdb_install_extensions()
    duckdb_load_extensions()
    duckdb_load_s3_credentials()
    eml_records = []
    geoapi_records = []
    for resource in get_datasets():
        if not skip_data:
            if resource.get("ipt_dwca") and resource["version"]:
                parquet_url = version_to_parquet(resource["id"], resource["version"])
            else:
                log.info(f"skipping {resource['id']} no dwca available")
                parquet_url = None

            create_dms_dataset(resource, parquet_url)

        text = get_dataset_metadata(resource_id=resource["id"])
        eml_records.append(eml_to_record(resource, text))
        geoapi_records.append(to_pygeoapi_resource(resource, text))

    write_eml_record(eml_records)
    write_pygeoapi_resources(geoapi_records)
