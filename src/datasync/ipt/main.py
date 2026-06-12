from xml.parsers.expat import ExpatError

import fsspec
import typer
import xmlschema

from ..settings import log
from .csw import eml_write_record
from .dms import create_dms_dataset
from .geoapi import to_pygeoapi_resource
from .ipt import get_dataset_metadata, get_datasets
from .parquet import version_to_parquet
from .settings import AWS_ENDPOINT_URL

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
    skip_csw: bool = typer.Option(default=False, help="Skip publishing to CSW"),
    skip_geoapi: bool = typer.Option(default=False, help="Skip publishing to pygeoapi"),
    limit: int | None = typer.Option(
        default=None, help="Only import a certain amount of records"
    ),
    search: str | None = typer.Option(
        default=None, help="execute only on resources which contains that string"
    ),
):
    if not AWS_ENDPOINT_URL:
        raise typer.BadParameter("IPT_AWS_ENDPOINT_URL is required")
    index = 1
    for resource in get_datasets():
        if search and search not in resource["id"]:
            continue
        log.info("processing resource", resource=resource)
        parquet_url = None
        if not skip_data:
            if resource.get("ipt_dwca") and resource["version"]:
                parquet_url = version_to_parquet(resource["id"], resource["version"])
            else:
                log.info(f"skipping {resource['id']} no dwca available")
                parquet_url = None

        text = get_dataset_metadata(resource_id=resource["id"])
        try:
            xml_url = eml_write_record(resource, text, skip=skip_csw)
            log.debug("xml generated", url=xml_url)

            if parquet_url and not skip_dms:
                create_dms_dataset(resource, parquet_url, xml_url)
        except ExpatError:
            log.error(
                "It was not possible to parse the metadata XML", resource=resource
            )

        if not skip_geoapi:
            to_pygeoapi_resource(resource, text)
        index += 1
        if limit and limit < index:
            break


def validate_iso(file: str):
    """Validate XML against ISO 19115 schema with both gmd and gmx namespaces."""
    with fsspec.open(file, "r") as f:
        content = f.read()
        log.debug("validating xml", file=file, content=content)
        # Load schemas from URLs
        log.debug("loading ISO 19115 gmd and gmx schemas")
        schemas = [
            "http://www.isotc211.org/2005/gmd/gmd.xsd",
            "http://www.isotc211.org/2005/gmx/gmx.xsd",
        ]
        schema = xmlschema.XMLSchema(schemas)

        # Validate the XML document
        if schema.is_valid(content):
            log.info("XML validation successful", file=file)
        else:
            for e in schema.iter_errors(content):
                log.error(
                    str(e),
                    file=file,
                )
            raise ValueError("XML validation failed")


app.command(help="Validate an XML file against ISO 19115 schema")(validate_iso)
