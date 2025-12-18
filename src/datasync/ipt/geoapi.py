import pyarrow as pa
from lxml import etree
from pygeometa.schemas.gbif_eml import GBIF_EMLOutputSchema

from ..settings import env as logger
from .settings import AWS_ENDPOINT_URL, GEOAPI_PATH, RESOURCES_PREFIX, S3_BUCKET, conn

PARSER = etree.XMLParser(resolve_entities=False)
eml = GBIF_EMLOutputSchema()


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

    return {
        "id": f"ipt__{ds['id']}",
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


def write_pygeoapi_resources(rows):
    logger.info("converting to arrow")
    records = pa.Table.from_pylist(rows)  # noqa: F841
    logger.info("write to S3")
    conn.sql(f"""
        COPY records to 's3://{S3_BUCKET}{GEOAPI_PATH}' (FORMAT json, ARRAY true)
    """)  # noqa: E501
