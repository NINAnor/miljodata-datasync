from copy import deepcopy
from time import sleep

import backoff
import httpx
from deepdiff import DeepDiff
from lxml import etree
from pygeometa.schemas.gbif_eml import GBIF_EMLOutputSchema

from ..settings import log
from .settings import (
    AWS_ENDPOINT_URL,
    GEOAPI_PUBLISH_URL,
    PUBLISH_PASSWORD,
    PUBLISH_USER,
    RESOURCES_PREFIX,
    S3_BUCKET,
)

PARSER = etree.XMLParser(resolve_entities=False)
eml = GBIF_EMLOutputSchema()


class NotFoundError(httpx.HTTPStatusError):
    pass


@backoff.on_exception(
    backoff.expo,
    (httpx.ConnectError, httpx.HTTPStatusError),
    max_tries=5,
    jitter=backoff.full_jitter,
)
def check_url_exists(url, auth, log) -> httpx.Response:
    response = httpx.get(
        url,
        auth=auth,
    )
    if response.status_code // 100 == 4:
        raise NotFoundError(
            "Resource not found", request=response.request, response=response
        )

    response.raise_for_status()

    log.debug("found", response=response.text, status=response.status_code)
    return response


@backoff.on_exception(
    backoff.expo,
    (httpx.ConnectError, httpx.HTTPStatusError),
    max_tries=10,
    jitter=backoff.full_jitter,
)
def req(*args, log, **kwargs):
    response = httpx.request(*args, **kwargs)

    if response.status_code // 100 == 5:
        raise httpx.HTTPStatusError(
            "response over 500", request=response.request, response=response
        )

    return response


def publish_pygeoapi_resource(base_url, data):
    log.debug("publishing configuration", data=data)
    auth = httpx.BasicAuth(username=PUBLISH_USER, password=PUBLISH_PASSWORD)

    try:
        resource = check_url_exists(
            f"{base_url}/admin/config/resources/{data['id']}",
            log=log,
            auth=auth,
        )
        old_data = resource.json()

        diff = DeepDiff(old_data, data, ignore_order=True)

        log.debug("compared", diff=diff)

        if diff:
            response = req(
                method="put",
                url=f"{base_url}/admin/config/resources/{data['id']}",
                json=data,
                auth=auth,
                log=log,
            )
            log.info(
                "updated collection",
                response=response.text,
                status=response.status_code,
            )
        else:
            log.info("skip, already updated")
    except NotFoundError:
        response = req(
            method="post",
            url=f"{base_url}/admin/config/resources",
            json={
                data["id"]: data,
            },
            auth=auth,
            log=log,
        )
        log.info(
            "created collection", response=response.text, status=response.status_code
        )

    sleep(5)

    check_url_exists(
        f"{base_url}/admin/config/resources/{data['id']}",
        auth=auth,
        log=log,
    )


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
