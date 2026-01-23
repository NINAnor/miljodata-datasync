from time import sleep

import httpx
from deepdiff import DeepDiff

from ..settings import env, log
from .helpers import ClientError, backoff_request


def publish_pygeoapi_resource(base_url, data):
    log.debug("publishing configuration", data=data)
    PUBLISH_USER = env.str("PUBLISH_USER", default=None)
    PUBLISH_PASSWORD = env.str("PUBLISH_PASSWORD", default=None)

    if PUBLISH_USER and PUBLISH_PASSWORD:
        auth = httpx.BasicAuth(username=PUBLISH_USER, password=PUBLISH_PASSWORD)
    else:
        auth = None

    try:
        resource = backoff_request(
            method="get",
            url=f"{base_url}/admin/config/resources/{data['id']}",
            auth=auth,
        )
        old_data = resource.json()

        diff = DeepDiff(old_data, data, ignore_order=True)

        log.debug("compared", diff=diff)

        if diff:
            response = backoff_request(
                method="put",
                url=f"{base_url}/admin/config/resources/{data['id']}",
                json=data,
                auth=auth,
            )
            log.info(
                "updated collection",
                response=response.text,
                status=response.status_code,
            )
        else:
            log.info("skip, already updated")
    except ClientError as e:
        log.debug("not found, creating")
        response_data = e.response.json()
        if response_data["code"] == "ResourceNotFound":
            response = backoff_request(
                method="post",
                url=f"{base_url}/admin/config/resources",
                json={
                    data["id"]: data,
                },
                auth=auth,
            )
            log.info(
                "created collection",
                response=response.text,
                status=response.status_code,
            )
        else:
            log.error("Unexpected response", data=response_data)
            raise Exception("Client error") from e

    sleep(5)

    backoff_request(
        method="get",
        url=f"{base_url}/admin/config/resources/{data['id']}",
        auth=auth,
        log=log,
        polling=True,
    )
