import re

from lxml import etree
from lxml.etree import _Element

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
