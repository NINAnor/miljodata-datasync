from copy import deepcopy

from lxml import etree
from pygeometa.schemas.gbif_eml import GBIF_EMLOutputSchema

from ..libs.geoapi import publish_pygeoapi_resource
from .settings import (
    AWS_ENDPOINT_URL,
    GEOAPI_PUBLISH_URL,
    RESOURCES_PREFIX,
    S3_BUCKET,
)

PARSER = etree.XMLParser(resolve_entities=False)
eml = GBIF_EMLOutputSchema()


def to_pygeoapi_resource(ds, eml_text):
    metadata = eml.import_(eml_text)

    idf = metadata["identification"]
    spatial = deepcopy(idf["extents"]["spatial"][0])
    spatial["crs"] = str(spatial["crs"])

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
