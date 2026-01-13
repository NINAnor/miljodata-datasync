import httpx
import xmltodict
from bs4 import BeautifulSoup

from .settings import IPT_URL, logging


def get_datasets():
    res = httpx.get(f"{IPT_URL}/rss")
    soup = BeautifulSoup(res.text, features="lxml-xml")
    for item in soup.find_all("item"):
        content = {
            k.replace(":", "_"): v
            for k, v in xmltodict.parse(item.prettify())["item"].items()
        }
        resource_id = content["link"].split("=")[1]
        if content.get("guid"):
            yield {
                **content,
                "id": resource_id,
                "version": content["guid"]["#text"].split("/")[1].replace("v", ""),
            }
        else:
            logging.warning(
                "Content does not have a guid, skipping",
                content=content,
                resource_id=resource_id,
            )


def get_dataset_metadata(resource_id: str):
    url = IPT_URL + "/eml.do?r=" + resource_id
    res = httpx.get(url)
    return res.text
