import typer

from ..settings import log
from .csw import eml_write_record
from .dms import create_dms_dataset
from .geoapi import to_pygeoapi_resource
from .ipt import get_dataset_metadata, get_datasets
from .parquet import version_to_parquet

app = typer.Typer(
    help="Provide commands to deal with IPT", pretty_exceptions_enable=False
)


@app.command(
    help="Convert IPT resources to geoparquet, register them in the DMS, publish metadata and configurations"  # noqa: E501
)
def run(
    skip_data: bool = typer.Option(
        default=False, help="Ignore data conversion step, perform only metadata"
    ),
    skip_dms: bool = typer.Option(default=False, help="Skip publishing to DMS"),
    limit: int | None = typer.Option(help="Only import a certain amount of records"),
):
    index = 1
    for resource in get_datasets():
        if not skip_data:
            if resource.get("ipt_dwca") and resource["version"]:
                parquet_url = version_to_parquet(resource["id"], resource["version"])
            else:
                log.info(f"skipping {resource['id']} no dwca available")
                parquet_url = None

            if not skip_dms:
                create_dms_dataset(resource, parquet_url)

        text = get_dataset_metadata(resource_id=resource["id"])
        eml_write_record(resource, text)
        to_pygeoapi_resource(resource, text)
        index += 1
        if limit and limit < index:
            break
