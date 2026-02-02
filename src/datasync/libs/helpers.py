import re

import backoff
import httpx
from duckdb import DuckDBPyConnection
from lxml import etree
from lxml.etree import _Element

from ..settings import log

PARSER: etree.XMLParser = etree.XMLParser(resolve_entities=False)


def get_anytext(bag: str | _Element | list[str]) -> str:
    """
    generate bag of text for free text searches
    accepts list of words, string of XML, or etree.Element
    """

    if isinstance(bag, list):  # list of words
        return " ".join([_f for _f in bag if _f and isinstance(_f, str)]).strip()
    else:  # xml
        if isinstance(bag, bytes) or isinstance(bag, str):
            # serialize to lxml
            bag = etree.fromstring(bag, PARSER)  # noqa: S320
        # get all XML element content
        all_text = bag.xpath("//text()")
        if isinstance(all_text, list):
            return re.sub(
                r"\s+",
                " ",
                " ".join([str(value).strip() for value in all_text]).strip(),
            )
        # NOTE: this should never happen as the xpath evaluation always returns a list
        # but the type annotation is generic as xpath might return any type
        raise TypeError("xpath result was not a list of strings")


class DuckDBAtomicTransaction:
    def __init__(self, conn: DuckDBPyConnection):
        self.conn = conn

    def __enter__(self):
        self.conn.begin()
        return self.conn

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            self.conn.commit()
        else:
            self.conn.rollback()


class ClientError(httpx.HTTPStatusError):
    pass


class NotFoundError(httpx.HTTPStatusError):
    pass


class PollingError(NotFoundError):
    pass


class ServerError(httpx.HTTPStatusError):
    pass


@backoff.on_exception(
    backoff.expo,
    (httpx.ConnectError, ServerError, PollingError, httpx.TimeoutException),
    max_tries=5,
    jitter=backoff.full_jitter,
)
def backoff_request(*args, log=log, polling=False, **kwargs):
    response = httpx.request(*args, **kwargs)
    log.debug(
        "request",
        request=response.request.url,
        response=response.text,
        status=response.status_code,
    )

    if response.status_code // 100 == 4:
        if response.status_code == 404:
            if polling:
                raise PollingError(
                    "Polling: not found", request=response.request, response=response
                )
            else:
                raise NotFoundError(
                    "Not found error", request=response.request, response=response
                )
        else:
            raise ClientError(
                "Client error", request=response.request, response=response
            )

    if response.status_code // 100 == 5:
        raise ServerError(
            "response over 500", request=response.request, response=response
        )

    return response
