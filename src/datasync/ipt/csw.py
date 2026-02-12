from lxml import etree
from pygeometa.schemas.gbif_eml import GBIF_EMLOutputSchema
from pygeometa.schemas.iso19139 import ISO19139OutputSchema

from ..libs.csw import publish_csw_record
from ..settings import log
from .settings import (
    AWS_ENDPOINT_URL,
    GEOAPI_PUBLISH_URL,
    IPT_URL,
    OGC_RECORDS_PUBLISH_URL,
    RESOURCES_PREFIX,
    S3_BUCKET,
    s3,
)

PARSER = etree.XMLParser(resolve_entities=False)
eml = GBIF_EMLOutputSchema()
iso = ISO19139OutputSchema()


def eml_write_record(ds, text, skip):
    identifier = f"ipt__{ds['id']}"
    metadata: dict = eml.import_(text)

    metadata["metadata"]["identifier"] = identifier

    metadata["distribution"]["geoparquet"] = {
        "name": "GeoParquet",
        "description": "GeoParquet",
        "type": "FILE:GEO",
        "format": "Parquet",
        "url": f"{AWS_ENDPOINT_URL}/{S3_BUCKET}{RESOURCES_PREFIX}{ds['id']}.parquet",  # noqa: E501
    }

    metadata["distribution"]["file"] = {
        "name": "DarwinCore Archive",
        "description": "DarwinCore archive",
        "type": "WWW:LINK",
        "function": "information",
        "format": "Zip",
        "url": f"{IPT_URL}/archive.do?r={ds['id']}",  # noqa: E501
    }

    # make sure that point of contact has some basic info required by GeoNorge
    if "pointOfContact" in metadata["contact"]:
        metadata["contact"]["pointOfContact"] = metadata["contact"][
            "pointOfContact"
        ] | {
            "organization": "Norsk institutt for naturforskning",
            "url": "https://www.nina.no",
            "email": "firmapost@nina.no",
        }

    if GEOAPI_PUBLISH_URL:
        metadata["distribution"]["pygeoapi"] = {
            "name": "OGC API Feature",
            "description": "OGC REST API",
            "type": "OGCFeat",
            "format": "GeoJSON",
            "url": f"{GEOAPI_PUBLISH_URL}/collections/{identifier}/items?f=json",  # noqa: E501
        }

    log.debug("using mcf", mcf=metadata)

    xml = iso.write(metadata)

    # # in order to publish on GeoNorge we need to have the pointOfContact with role owner  # noqa: E501
    # # this can only be achieved by xml manipulation

    # parser = etree.XMLParser(remove_blank_text=True)

    # root = etree.fromstring(xml, parser)  # noqa: S320
    # tree = etree.ElementTree(root)
    # ns = {k: v for k, v in root.nsmap.items() if k is not None}

    # role_nodes = root.xpath(
    #     "//gmd:pointOfContact//gmd:CI_RoleCode",
    #     namespaces=ns,
    # )
    # for role in role_nodes:
    #     log.debug("found role", role=role.text)
    #     role.text = "owner"
    #     role.set("codeListValue", "owner")

    # xml_bytes = etree.tostring(
    #     tree, encoding="UTF-8", xml_declaration=True, pretty_print=True
    # )

    with s3.open(
        f"{S3_BUCKET}{RESOURCES_PREFIX}{ds['id']}.xml", mode="w"
    ) as metadata_file:
        metadata_file.write(xml)

    if not skip:
        publish_csw_record(OGC_RECORDS_PUBLISH_URL, xml, identifier=identifier)

    return f"{AWS_ENDPOINT_URL}/{S3_BUCKET}{RESOURCES_PREFIX}{ds['id']}.xml"
