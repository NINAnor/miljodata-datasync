from urllib.parse import quote

import httpx
from lxml import etree
from pygeometa.schemas.gbif_eml import GBIF_EMLOutputSchema

from ..settings import log
from .settings import (
    AWS_ENDPOINT_URL,
    GEOAPI_PUBLISH_URL,
    RESOURCES_PREFIX,
    S3_BUCKET,
)

PARSER = etree.XMLParser(resolve_entities=False)
eml = GBIF_EMLOutputSchema()


def publish_pygeoapi_resource(base_url, data):
    log.debug("publishing configuration", data=data)
    try:
        response = httpx.post(
            f"{base_url}/admin/config/resources",
            json={
                data["id"]: data,
            },
        ).raise_for_status()
        log.info(
            "created collection", response=response.text, status=response.status_code
        )
    except httpx.HTTPStatusError as e:
        log.debug(
            "failed creation, expect resource exists",
            response=e.response.text,
            status=e.response.status_code,
            request=e.request.url,
        )
        try:
            response = httpx.put(
                f"{base_url}/admin/config/resources/{quote(data['id'], safe=[])}",
                json=data,
            ).raise_for_status()
            log.info(
                "updated collection",
                response=response.text,
                status=response.status_code,
            )
        except httpx.HTTPStatusError as e:
            log.warn(
                "checking admin config",
                response=e.response.text,
                status=e.response.status_code,
                request=e.request.url,
            )


def to_pygeoapi_resource(ds, eml_text):
    metadata = eml.import_(eml_text)

    idf = metadata["identification"]
    spatial = idf["extents"]["spatial"][0]

    contribs = []
    for role, contact in metadata["contact"].items():
        role = role.split("_")[0]
        contribs.append(contact["individualname"])

    keywords = []
    for _k, v in idf["keywords"].items():
        keywords += v["keywords"]

    identifier = f"ipt__{ds['id']}"

    config = {
        "id": identifier,
        "type": "collection",
        "visibility": "default",
        "title": ds["title"],
        "extents": {"spatial": spatial},
        "keywords": list(set(keywords)),
        "description": metadata["identification"]["abstract"],
        "providers": [
            {
                "type": "feature",
                "name": "OGR",
                "default": True,
                "id_field": "fid",
                "editable": False,
                "storage_crs": "http://www.opengis.net/def/crs/OGC/1.3/CRS84",
                "crs": [
                    "http://www.opengis.net/def/crs/OGC/1.3/CRS84",
                    "http://www.opengis.net/def/crs/EPSG/0/4326",
                ],
                "data": {
                    "source_type": "Parquet",
                    "source": f"/vsicurl/{AWS_ENDPOINT_URL}/{S3_BUCKET}{RESOURCES_PREFIX}{ds['id']}.parquet",  # noqa: E501
                },
                "layer": ds["id"],
            }
        ],
    }

    publish_pygeoapi_resource(GEOAPI_PUBLISH_URL, config)
