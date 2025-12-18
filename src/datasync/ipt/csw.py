import json

import pyarrow as pa
from lxml import etree
from pygeometa.schemas.gbif_eml import GBIF_EMLOutputSchema
from pygeometa.schemas.iso19139 import ISO19139OutputSchema
from shapely.geometry import box

from ..settings import env as logger
from .settings import (
    AWS_ENDPOINT_URL,
    CSW_PATH,
    GEOAPI_PUBLISH_URL,
    IPT_URL,
    RESOURCES_PREFIX,
    S3_BUCKET,
    conn,
)

PARSER = etree.XMLParser(resolve_entities=False)
eml = GBIF_EMLOutputSchema()
iso = ISO19139OutputSchema()


def get_anytext(bag):
    """
    generate bag of text for free text searches
    accepts list of words, string of XML, or etree.Element
    """

    if isinstance(bag, list):  # list of words
        return " ".join([_f for _f in bag if _f]).strip()
    else:  # xml
        if isinstance(bag, bytes) or isinstance(bag, str):
            # serialize to lxml
            bag = etree.fromstring(bag, PARSER)  # noqa: S320
        # get all XML element content
        return " ".join([value.strip() for value in bag.xpath("//text()")])


def eml_to_record(ds, text):
    metadata = eml.import_(text)

    metadata["metadata"]["identifier"] = f"ipt__{ds['id']}"

    xml = iso.write(metadata)
    fts = get_anytext(xml)
    idf = metadata["identification"]
    bbox = idf["extents"]["spatial"][0]["bbox"]

    contribs = []
    for role, contact in metadata["contact"].items():
        role = role.split("_")[0]
        contribs.append(contact["individualname"])

    keywords = []
    for _k, v in idf["keywords"].items():
        keywords += v["keywords"]

    if ds.get("ipt_dwca"):
        links = [
            {
                "name": "Parquet",
                "description": "The resource as (geo)parquet file",
                "protocol": "FILE:GEO",
                "url": f"{AWS_ENDPOINT_URL}/{S3_BUCKET}{RESOURCES_PREFIX}{ds['id']}.parquet",  # noqa: E501
            },
            {
                "name": "DWCA",
                "description": "The resource as Darwin Core Archive",
                "protocol": "file",
                "url": f"{IPT_URL}/archive.do?r={ds['id']}",  # noqa: E501
            },
        ]

        if GEOAPI_PUBLISH_URL:
            links.append(
                {
                    "name": "OGC API Feature",
                    "description": "OGC REST API to the resource",
                    "protocol": "OGCFeat",
                    "url": f"{GEOAPI_PUBLISH_URL}/collections/ipt__{ds['id']}/items?f=json",  # noqa: E501
                },
            )
    else:
        links = []

    return {
        "identifier": metadata["metadata"]["identifier"],
        "typename": "gmd:MD_Metadata",
        "schema": "http://www.isotc211.org/2005/gmd",
        "mdsource": "local",
        "insert_date": idf["dates"]["publication"],
        "title": ds["title"],
        "date_modified": idf["dates"]["publication"],
        "type": "dataset",
        "format": None,
        "wkt_geometry": box(*bbox).wkt,
        "metadata": xml,
        "xml": xml,
        "keywords": ", ".join(set(keywords)),
        "metadata_type": "application/xml",
        "anytext": fts,
        "abstract": metadata["identification"]["abstract"],
        "date": idf["dates"]["publication"],
        "creator": "Norsk institutt for naturforskning (NINA)",
        "publisher": "Norsk institutt for naturforskning (NINA)",
        "contributor": "; ".join(set(contribs)),
        "links": json.dumps(links),
    }


def write_eml_record(rows):
    logger.info("converting to arrow")
    records = pa.Table.from_pylist(rows)  # noqa: F841
    logger.info("write to S3")
    conn.sql("from records").write_parquet(
        f"s3://{S3_BUCKET}{CSW_PATH}",
        compression="zstd",
        overwrite=True,
    )
