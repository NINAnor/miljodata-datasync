import httpx
from lxml import etree
from pygeometa.schemas.gbif_eml import GBIF_EMLOutputSchema
from pygeometa.schemas.iso19139 import ISO19139OutputSchema

from ..settings import log
from .settings import (
    AWS_ENDPOINT_URL,
    GEOAPI_PUBLISH_URL,
    METADATA_PREFIX,
    OGC_RECORDS_PUBLISH_URL,
    RESOURCES_PREFIX,
    S3_BUCKET,
    s3,
)

PARSER = etree.XMLParser(resolve_entities=False)
eml = GBIF_EMLOutputSchema()
iso = ISO19139OutputSchema()


def publish_csw_record(base_url, data, identifier):
    # see: https://docs.pycsw.org/en/latest/transactions.html#id2
    try:
        response = httpx.delete(
            f"{base_url}/{identifier}",
            headers={"Content-Type": "application/xml"},
        ).raise_for_status()
        log.info(
            "removed collection",
            response=response.text,
            status=response.status_code,
        )
    except httpx.HTTPStatusError as e:
        log.debug(
            "failed deletion",
            response=e.response.text,
            status=e.response.status_code,
        )

    try:
        response = httpx.post(
            base_url,
            content=data,
            headers={"Content-Type": "application/xml"},
        ).raise_for_status()
        log.info("created record", response=response.text, status=response.status_code)
    except httpx.HTTPStatusError as e:
        log.warn(
            "create failed",
            response=e.response.text,
            status=e.response.status_code,
            request=e.request.url,
        )


def eml_write_record(ds, text):
    identifier = f"ipt__{ds['id']}"
    metadata: dict = eml.import_(text)

    metadata["metadata"]["identifier"] = identifier

    metadata["distribution"]["geoparquet"] = {
        "name": "GeoParquet",
        "description": "GeoParquet",
        "type": "FILE:GEO",
        "url": f"{AWS_ENDPOINT_URL}/{S3_BUCKET}{RESOURCES_PREFIX}{ds['id']}.parquet",  # noqa: E501
    }

    if GEOAPI_PUBLISH_URL:
        metadata["distribution"]["pygeoapi"] = {
            "name": "OGC API Feature",
            "description": "OGC REST API",
            "type": "OGCFeat",
            "url": f"{GEOAPI_PUBLISH_URL}/collections/{identifier}/items?f=json",  # noqa: E501
        }

    xml = iso.write(metadata)

    with s3.open(
        f"{S3_BUCKET}{METADATA_PREFIX}{ds['id']}.xml", mode="w"
    ) as metadata_file:
        metadata_file.write(xml)

    publish_csw_record(OGC_RECORDS_PUBLISH_URL, xml, identifier=identifier)
