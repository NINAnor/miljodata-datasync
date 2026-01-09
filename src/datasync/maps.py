import json
from pathlib import Path
from urllib import parse

import typer
from owslib.wms import WebMapService

from .settings import log

app = typer.Typer()


@app.command()
def wms_to_map(get_capabilities_url: str, output: str = "output.json"):
    wms = WebMapService(get_capabilities_url)
    log.info(wms.contents)

    parsed = parse.urlparse(get_capabilities_url)
    qs = parse.parse_qs(parsed.query)
    map_param = qs.get("map", [None])[0]

    items = {}

    cn = wms.__class__.__name__

    for key, value in wms.contents.items():
        log.info(key, value=value)

        req = getattr(wms, f"_{cn}__build_getmap_request")(
            layers=[key],
            styles=[""],
            srs="EPSG:3857",
            format="image/png",
            size=(256, 256),
            transparent=True,
            bbox=(0, 0, 0, 0),
        )
        if map_param:
            req["map"] = map_param

        del req["bbox"]

        legend = {
            "service": "WMS",
            "version": wms.version,
            "request": "GetLegendGraphic",
            "layer": key,
            "style": "",
            "format": "image/png",
            "transparent": "true",
        }

        if map_param:
            legend["map"] = map_param

        items[key] = {
            "name": value.title,
            "type": "layer",
            "layer": {
                "type": "raster",
                "tiles": [
                    parse.urlunsplit(
                        (
                            parsed.scheme,
                            parsed.netloc,
                            parsed.path,
                            parse.urlencode(req, doseq=True) + "&bbox={bbox-epsg-3857}",
                            "",
                        )
                    )
                ],
                "legend": {
                    "type": "image",
                    "url": parse.urlunsplit(
                        (
                            parsed.scheme,
                            parsed.netloc,
                            parsed.path,
                            parse.urlencode(
                                legend,
                                doseq=True,
                            ),
                            "",
                        )
                    ),
                },
            },
        }

    conf = {
        "id": parsed.hostname,
        "title": wms.identification.title,
        "subtitle": "",
        "description": wms.identification.abstract,
        "icon": "https://s3-ext-1.nina.no/dms/maps/logosmall-BFfoJdyr.png",
        "baseMap": "positron",
        "layerOrder": [],
        "viewState": {"longitude": 17.8, "latitude": 65.6, "zoom": 2},
        "expandedItems": [],
        "items": {
            "root": {
                "name": "Layers",
                "type": "folder",
                "children": list(items.keys()),
            },
            **items,
        },
        "config": {
            "titiler_api_url": "/titiler",
            "theme": "nina",
            "language": "en",
            "footer": {
                "items": [
                    "![](https://s3-ext-1.nina.no/dms/maps/logowhite-6HSLYLYZ.png)",
                    "Norsk institutt for naturforskning - [www.nina.no](https://www.nina.no)",
                ],
                "justify": "justify-between",
                "align": "items-baseline",
            },
        },
    }

    with Path(output).open("w") as f:
        json.dump(conf, f, indent=4)
