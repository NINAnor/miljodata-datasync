from xmldiff.main import diff_texts

from ..settings import log
from .helpers import NotFoundError, backoff_request


def publish_csw_record(base_url, data, identifier):
    # see: https://docs.pycsw.org/en/latest/transactions.html#id2

    try:
        response = backoff_request(
            method="get",
            url=f"{base_url}/{identifier}?f=xml",
        )
        diff = diff_texts(data, response.text)
        log.debug("xml diff", diff=diff)

        if diff:
            # PUT doesn't work: https://github.com/geopython/pycsw/issues/1194
            backoff_request(
                method="delete",
                url=f"{base_url}/{identifier}",
            )
            backoff_request(
                method="post",
                url=base_url,
                content=data,
                headers={"Content-Type": "application/xml"},
            )
        else:
            log.info("CSW Resource is already updated, skipping")
    except NotFoundError:
        backoff_request(
            method="post",
            url=base_url,
            content=data,
            headers={"Content-Type": "application/xml"},
        )
