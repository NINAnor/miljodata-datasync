import datetime
import functools
import json
import math
import shutil
import subprocess
import tempfile
from pathlib import Path
from urllib.parse import urlparse

import httpx
import s3fs
import typer
from owslib.wps import WebProcessingService, monitorExecution
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles
from typer import Typer

from .libs import dms
from .settings import env, log

app = Typer()

GEONODE_PAGE_SIZE = 100

# Output format and file extension per subtype
_WPS_OUTPUT_FORMAT = {
    "raster": "image/tiff",
    "vector": "application/geopackage+sqlite3",
}
_WPS_FILE_EXT = {
    "raster": ".tiff",
    "vector": ".gpkg",
}
_WPS_POLL_INTERVAL = 5  # seconds between status checks

GEONODE_S3_BUCKET: str = env.str("GEONODE_S3_BUCKET", default="")
GEONODE_S3_ENDPOINT: str = env.str("GEONODE_S3_ENDPOINT", default="")
GEONODE_S3_ACCESS_KEY: str = env.str("GEONODE_S3_ACCESS_KEY", default="")
GEONODE_S3_SECRET_KEY: str = env.str("GEONODE_S3_SECRET_KEY", default="")
GEONODE_S3_PREFIX: str = env.str("GEONODE_S3_PREFIX", default="/geonode")
GEONODE_TEMP_DIR: str = env.str("GEONODE_TEMP_DIR", default=".tmp")
GEONODE_TITILER_URL: str = env.str("GEONODE_TITILER_URL", default="/titiler")

_MAP_CONFIG_SCHEMA_URL = (
    "https://raw.githubusercontent.com/NINAnor/map-editor"
    "/refs/heads/main/schemas/map-config.schema.json"
)


def _get_wps_download_url(base: str, layer_alternate: str, subtype: str) -> str | None:
    """
    Submit a WPS gs:Download job via OWSLib and block until complete.
    Returns the result download URL, or None if the job fails.
    """
    output_format = _WPS_OUTPUT_FORMAT.get(subtype, "application/zip")
    wps = WebProcessingService(
        f"{base}/geoserver/ows",
        version="1.0.0",
        skip_caps=True,
    )

    execution = wps.execute(
        "gs:Download",
        inputs=[
            ("layerName", layer_alternate),
            ("outputFormat", output_format),
            ("cropToROI", "false"),
        ],
        output=[("result", True, output_format)],
        mode="async",
    )

    log.info(
        "WPS job submitted",
        layer=layer_alternate,
        status_url=execution.statusLocation,
    )

    monitorExecution(execution, sleepSecs=_WPS_POLL_INTERVAL, download=False)

    if not execution.isSucceded():
        errors = [(e.code, e.text) for e in execution.errors]
        log.error("WPS job failed", layer=layer_alternate, errors=errors)
        return None

    for output in execution.processOutputs:
        if output.identifier == "result" and output.reference:
            log.info(
                "WPS job completed",
                layer=layer_alternate,
                href=output.reference,
            )
            return output.reference

    log.warning(
        "WPS job succeeded but no reference output found",
        layer=layer_alternate,
    )
    return None


def _download_to_tempfile(
    download_url: str, http_client: httpx.Client, suffix: str, tmp_dir: str = ""
) -> Path:
    """Stream *download_url* into a named temporary file and return its path."""
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix, dir=tmp_dir or None)
    try:
        with http_client.stream("GET", download_url) as response:
            response.raise_for_status()
            for chunk in response.iter_bytes(chunk_size=8 * 1024 * 1024):
                tmp.write(chunk)
    finally:
        tmp.close()
    return Path(tmp.name)


def _convert_to_cog(src: Path, tmp_dir: str = "") -> Path:
    """
    Convert *src* GeoTIFF to a Cloud Optimised GeoTiff in a sibling temp file.

    Settings:
    - Compression : ZSTD
    - BigTIFF     : IF_SAFER
    - Overviews   : AUTO (rio-cogeo picks the levels)
    - Resampling  : nearest  (no data or CRS alteration)
    """
    dst = Path(
        tempfile.mktemp(suffix="_cog.tiff", dir=tmp_dir or None)  # noqa: S306
    )
    profile = cog_profiles.get("deflate")  # base profile, overridden below
    profile.update({"compress": "ZSTD", "bigtiff": "IF_SAFER"})
    cog_translate(
        source=src,
        dst_path=dst,
        dst_kwargs=profile,
        overview_level=None,  # AUTO
        overview_resampling="nearest",
        resampling="nearest",
        quiet=True,
    )
    log.info("COG conversion done", src=str(src), dst=str(dst))
    return dst


def _upload_file_to_s3(
    local_path: Path,
    s3_fs: s3fs.S3FileSystem,
    s3_path: str,
    s3_endpoint: str,
) -> str:
    """
    Upload *local_path* to *s3_path* and return the HTTPS URL.
    """
    bucket_path = s3_path.lstrip("/")
    https_uri = f"{s3_endpoint.rstrip('/')}/{bucket_path}"
    log.info("uploading to S3", src=str(local_path), dst=s3_path)
    with local_path.open("rb") as f_in, s3_fs.open(s3_path, "wb") as f_out:
        while chunk := f_in.read(8 * 1024 * 1024):
            f_out.write(chunk)
    log.info("upload complete", s3_path=s3_path)
    return https_uri


def _upload_to_s3(
    download_url: str,
    s3_fs: s3fs.S3FileSystem,
    s3_path: str,
    http_client: httpx.Client,
    s3_endpoint: str,
) -> str:
    """
    Stream-download a file from *download_url* and write it to *s3_path*.
    Skips the download if the file already exists in S3.
    Returns the HTTPS URL of the uploaded file (using *s3_endpoint*).
    """
    bucket_path = s3_path.lstrip("/")
    https_uri = f"{s3_endpoint.rstrip('/')}/{bucket_path}"

    if s3_fs.exists(s3_path):
        log.info("S3 file already exists, skipping upload", s3_path=s3_path)
        return https_uri

    log.info("uploading to S3", src=download_url, dst=s3_path)
    with http_client.stream("GET", download_url) as response:
        response.raise_for_status()
        with s3_fs.open(s3_path, "wb") as s3_file:
            for chunk in response.iter_bytes(chunk_size=8 * 1024 * 1024):
                s3_file.write(chunk)
    log.info("upload complete", s3_path=s3_path)
    return https_uri


# GeoNode uses ISO 639-2 codes; map to the two values the DMS schema accepts
_LANG_MAP = {
    "eng": "en",
    "en": "en",
    "nor": "no",
    "nob": "no",
    "nno": "no",
    "no": "no",
}

# ISO 19115 maintenance frequency values accepted by both GeoNode and the schema
_MAINTENANCE_FREQ = {
    "continual",
    "daily",
    "weekly",
    "fortnightly",
    "monthly",
    "quarterly",
    "biannually",
    "annually",
    "asNeeded",
    "irregular",
    "notPlanned",
    "unknown",
}

# ISO 19115 topic categories accepted by the schema
_TOPIC_CATEGORIES = {
    "geoscientificInformation",
    "farming",
    "elevation",
    "utilitiesCommunication",
    "oceans",
    "boundaries",
    "inlandWaters",
    "intelligenceMilitary",
    "environment",
    "location",
    "economy",
    "planningCadastre",
    "biota",
    "health",
    "imageryBaseMapsEarthCover",
    "transportation",
    "society",
    "structure",
    "climatologyMeteorologyAtmosphere",
}


def _build_metadata(resource: dict) -> dict:
    """
    Map fields from a GeoNode v2 API resource object to the DMS metadata schema
    (DataCite-based, see /api/v1/datasets/metadata-schema/).

    Only the ``language`` field is required; everything else is included only
    when the GeoNode resource provides a value.
    """
    raw_lang = resource.get("language") or "eng"
    language = _LANG_MAP.get(raw_lang, "en")

    metadata: dict = {"language": language}

    # descriptions
    abstract = (resource.get("abstract") or "").strip()
    if abstract:
        metadata["descriptions"] = [
            {
                "description": abstract,
                "descriptionType": "Abstract",
                "lang": language,
            }
        ]

    # dates – GeoNode exposes `date` (created/published) and optionally
    # `last_modified` or `csw_wkt_geometry` update timestamps
    dates = []
    if resource.get("date"):
        dates.append({"date": resource["date"], "dateType": "Created"})
    last_modified = resource.get("last_updated")
    if last_modified and last_modified != resource.get("date"):
        dates.append({"date": last_modified, "dateType": "Updated"})
    if dates:
        metadata["dates"] = dates

    # publicationYear
    date_str = resource.get("date") or ""
    if date_str:
        try:
            metadata["publicationYear"] = int(date_str[:4])
        except (ValueError, TypeError):
            pass

    # subjects (keywords)
    keywords = resource.get("keywords") or []
    subjects = []
    for kw in keywords:
        if not kw:
            continue
        name = kw.get("name") or kw if isinstance(kw, str) else None
        if not name:
            continue
        entry: dict = {"subject": name}
        thesaurus = kw.get("thesaurus") if isinstance(kw, dict) else None
        if isinstance(thesaurus, dict):
            if thesaurus.get("title"):
                entry["subjectScheme"] = thesaurus["title"]
            if thesaurus.get("about"):
                entry["schemeURI"] = thesaurus["about"]
        subjects.append(entry)
    if subjects:
        metadata["subjects"] = subjects

    # topiccategory (ISO 19115)
    category = resource.get("category") or {}
    if isinstance(category, dict):
        cat_id = category.get("identifier") or ""
    else:
        cat_id = str(category)
    if cat_id in _TOPIC_CATEGORIES:
        metadata["topiccategory"] = [cat_id]

    # status – GeoNode values don't always match the schema; keep only valid ones
    _STATUS_MAP = {
        "completed": "completed",
        "onGoing": "onGoing",
        "planned": "planned",
        "historicalArchive": "historicalArchive",
        "underDevelopment": "underDevelopment",
        "required": "required",
        "obsolete": "obsolete",
    }
    status = resource.get("purpose") or resource.get("status") or "completed"
    metadata["status"] = _STATUS_MAP.get(status, "completed")

    # maintenancefrequency
    freq = resource.get("maintenance_frequency") or "unknown"
    metadata["maintenancefrequency"] = freq if freq in _MAINTENANCE_FREQ else "unknown"

    # rightsList
    license_info = resource.get("license") or {}
    if isinstance(license_info, dict) and license_info.get("identifier"):
        rights_entry: dict = {
            "rights": license_info.get("name") or license_info["identifier"],
            "lang": language,
        }
        if license_info.get("url"):
            rights_entry["rightsURI"] = license_info["url"]
        if license_info.get("identifier"):
            rights_entry["rightsIdentifier"] = license_info["identifier"]
        metadata["rightsList"] = [rights_entry]

    # browsegraphic (thumbnail)
    thumbnail = resource.get("thumbnail_url") or ""
    if thumbnail:
        metadata["browsegraphic"] = thumbnail

    # alternateIdentifiers – store the GeoNode UUID
    uuid = resource.get("uuid") or resource.get("alternate") or ""
    if uuid:
        metadata["alternateIdentifiers"] = [
            {"alternateIdentifier": uuid, "alternateIdentifierType": "GeoNodeUUID"}
        ]

    return metadata


def _get_geometry_type(source: str | Path) -> str:
    """
    Return a simplified geometry type string ('polygon', 'line', 'point')
    for the first layer in *source* (local path or /vsicurl/ URL).
    Defaults to 'polygon' if detection fails.
    """
    try:
        import pyogrio

        layers = pyogrio.list_layers(str(source))
        if len(layers):
            geom = (layers[0][1] or "").lower()
            if "point" in geom:
                return "point"
            if "line" in geom or "string" in geom:
                return "line"
        return "polygon"
    except Exception as exc:
        log.debug("geometry type detection failed", error=exc)
    return "polygon"


def _gpkg_layer_name(source: str | Path) -> str:
    """
    Return the first layer name inside *source* (local path or /vsicurl/ URL).
    Falls back to the path/URL stem.
    """
    try:
        import pyogrio

        layers = pyogrio.list_layers(str(source))
        if len(layers):
            return layers[0][0]
    except Exception as exc:
        log.debug("layer name detection failed", error=exc)
    return Path(str(source)).stem


def _run_gpkg_to_pmtiles(source: str, layer_name: str, tmp_dir: str = "") -> Path:
    """
    Convert a GeoPackage at *source* (local path or /vsicurl/ HTTP URL) to
    PMTiles by running gpkg_to_pmtiles.sh.  The script writes its output to
    ``$(pwd)/{layer_name}.pmtiles`` so we set cwd to *work_dir*.
    Returns the path to the resulting .pmtiles file.
    """
    script = Path(__file__).parent / "gpkg_to_pmtiles.sh"
    if tmp_dir:
        work_dir = Path(tmp_dir)
        work_dir.mkdir(parents=True, exist_ok=True)
    else:
        work_dir = Path(tempfile.mkdtemp())

    bash = shutil.which("bash")
    if not bash:
        raise RuntimeError("bash not found in PATH")

    result = subprocess.run(  # noqa: S603
        [bash, str(script), source],
        capture_output=True,
        text=True,
        timeout=600,
        cwd=str(work_dir),
    )
    if result.returncode != 0:
        raise RuntimeError(f"gpkg_to_pmtiles.sh failed: {result.stderr.strip()}")
    output = work_dir / f"{layer_name}.pmtiles"
    if not output.exists():
        raise RuntimeError(f"Expected PMTiles output not found: {output}")
    log.info("PMTiles conversion done", src=source, dst=str(output))
    return output


@functools.lru_cache(maxsize=1)
def _get_map_config_schema() -> dict:
    """Fetch and cache the map-config JSON Schema from GitHub."""
    with httpx.Client(timeout=30) as client:
        response = client.get(_MAP_CONFIG_SCHEMA_URL)
        response.raise_for_status()
    return response.json()


def _validate_map_config(config: dict) -> None:
    """Validate *config* against the map-config schema; raises on failure."""
    import jsonschema

    schema = _get_map_config_schema()
    validator = jsonschema.Draft202012Validator(schema)
    errors = list(validator.iter_errors(config))
    if errors:
        summary = "; ".join(
            f"{'.'.join(str(p) for p in e.absolute_path) or '<root>'}: {e.message}"
            for e in errors[:5]
        )
        raise ValueError(f"map config validation failed: {summary}")


def _union_bbox(
    bboxes: list[tuple[float, float, float, float]],
) -> tuple[float, float, float, float] | None:
    """Return the union of a list of (minx, miny, maxx, maxy) tuples."""
    if not bboxes:
        return None
    minx = min(b[0] for b in bboxes)
    miny = min(b[1] for b in bboxes)
    maxx = max(b[2] for b in bboxes)
    maxy = max(b[3] for b in bboxes)
    return minx, miny, maxx, maxy


def _bbox_from_polygon(
    polygon: dict | None,
) -> tuple[float, float, float, float] | None:
    """Extract (minx, miny, maxx, maxy) from a GeoJSON Polygon dict."""
    try:
        coords = polygon["coordinates"][0]
        lons = [c[0] for c in coords]
        lats = [c[1] for c in coords]
        return min(lons), min(lats), max(lons), max(lats)
    except Exception:
        return None


def _bbox_center_zoom(ll_bbox: dict | None) -> tuple[float, float, int]:
    """
    Compute (longitude, latitude, zoom) from a GeoJSON Polygon bbox.
    Falls back to (15, 63, 5) (Norway overview) if bbox is unavailable.
    """
    try:
        coords = ll_bbox["coordinates"][0]  # exterior ring
        lons = [c[0] for c in coords]
        lats = [c[1] for c in coords]
        lon = (min(lons) + max(lons)) / 2
        lat = (min(lats) + max(lats)) / 2
        span = max(max(lons) - min(lons), max(lats) - min(lats))
        # Rough zoom: span 360° → zoom 0, halves every zoom level
        zoom = max(0, min(18, round(math.log2(360 / max(span, 0.001)))))
        return lon, lat, zoom
    except Exception:
        return 15.0, 63.0, 5


def _build_map_config(
    geonode_map: dict,
    layer_infos: list[dict],
    titiler_url: str,
    origin: str,
) -> dict:
    """
    Build a map-config.schema.json-compliant dict from a GeoNode map and
    a list of resolved layer info dicts (see loop below for the structure).
    """
    pk = geonode_map["pk"]
    map_id = f"{origin}__map__{pk}"

    items: dict = {}
    layer_order: list[str] = []

    for info in layer_infos:
        layer_id: str = info["id"]
        layer_order.append(layer_id)

        if info["subtype"] == "raster":
            layer_def = {
                "type": "titiler",
                "titiler": {
                    "url": info["https_uri"],
                    "bidx": "single",
                },
                "legend": {
                    "type": "linear",
                    "colormap_name": "viridis",
                    "min": "0",
                    "max": "1",
                },
            }
        elif info["subtype"] == "vector" and info.get("pmtiles_uri"):
            source_layer = info.get("source_layer", info["layer_name"])
            geom_type = info.get("geom_type", "polygon")
            child_type = (
                "circle"
                if geom_type == "point"
                else "line"
                if geom_type == "line"
                else "fill"
            )
            layer_def = {
                "type": "pmtiles",
                "pmtiles": {"url": info["pmtiles_uri"]},
                "children": {
                    "type": child_type,
                    "source-layer": source_layer,
                },
            }
        else:
            log.warning(
                "skipping layer in map config: no S3 resource available",
                layer=info["alternate"],
                subtype=info["subtype"],
                info=info,
            )
            layer_order.remove(layer_id)
            continue

        items[layer_id] = {
            "type": "layer",
            "name": info["title"],
            "description": info.get("abstract") or "",
            "download_url": info["https_uri"],
            "layer": layer_def,
        }

    # Compute view center/zoom from the union of all visible layer bboxes,
    # falling back to the map-level bbox.
    layer_bboxes = [
        b
        for info in layer_infos
        if info["id"] in items
        for b in [_bbox_from_polygon(info.get("ll_bbox_polygon"))]
        if b is not None
    ]
    union = _union_bbox(layer_bboxes) or _bbox_from_polygon(
        geonode_map.get("ll_bbox_polygon")
    )
    if union:
        minx, miny, maxx, maxy = union
        lon = (minx + maxx) / 2
        lat = (miny + maxy) / 2
        span = max(maxx - minx, maxy - miny)
        zoom: int = max(0, min(18, round(math.log2(360 / max(span, 0.001)))))
    else:
        lon, lat, zoom = _bbox_center_zoom(None)

    root_id = "root"
    items[root_id] = {
        "type": "folder",
        "name": "Layers",
        "description": (geonode_map.get("abstract") or "").strip(),
        "children": layer_order,
    }

    lang_raw = geonode_map.get("language") or "eng"
    language = _LANG_MAP.get(lang_raw, "en")

    return {
        "title": geonode_map.get("title") or map_id,
        "subtitle": geonode_map.get("abstract") or "",
        "id": map_id,
        "baseMap": "positron",
        "layerOrder": layer_order,
        "viewState": {
            "longitude": lon,
            "latitude": lat,
            "zoom": zoom,
        },
        "items": items,
        "expandedItems": [],
        "config": {
            "titiler_api_url": titiler_url or "",
            "theme": "nina",
            "language": language,
        },
    }


def _fetch_all_maps(url: str) -> list[dict]:
    """Paginate through all maps exposed by a GeoNode v4 instance."""
    return _paginate(url, "/api/v2/maps/", "maps")


def _fetch_all_datasets(url: str) -> list[dict]:
    """Paginate through all datasets exposed by a GeoNode v4 instance."""
    return _paginate(url, "/api/v2/datasets/", "datasets")


def _fetch_all_documents(url: str) -> list[dict]:
    """Paginate through all documents exposed by a GeoNode v4 instance."""
    return _paginate(url, "/api/v2/documents/", "documents")


def _paginate(url: str, endpoint: str, resource_key: str) -> list[dict]:
    """Generic paginator for GeoNode v4 list endpoints."""
    results: list[dict] = []
    page = 1
    base = url.rstrip("/")

    with httpx.Client(timeout=60) as client:
        while True:
            response = client.get(
                f"{base}{endpoint}",
                params={"page": page, "page_size": GEONODE_PAGE_SIZE},
            )
            response.raise_for_status()
            data = response.json()

            batch: list[dict] = data.get(resource_key, [])
            results.extend(batch)

            total: int = data.get("total", 0)
            log.info(
                "fetched page",
                endpoint=endpoint,
                page=page,
                fetched=len(results),
                total=total,
            )

            if not batch or len(results) >= total:
                break
            page += 1

    return results


@app.command()
def v4_to_dms(
    url: str,
    project_id: str,
    bucket: str = typer.Option(default=GEONODE_S3_BUCKET),
    endpoint: str = typer.Option(default=GEONODE_S3_ENDPOINT),
    access_key: str = typer.Option(default=GEONODE_S3_ACCESS_KEY),
    secret_key: str = typer.Option(default=GEONODE_S3_SECRET_KEY),
    prefix: str = typer.Option(
        default=GEONODE_S3_PREFIX,
        help="S3 key prefix for uploaded files (e.g. /geonode)",
    ),
    tmp_dir: str = typer.Option(
        default=GEONODE_TEMP_DIR,
        help="Temp directory for COG conversion (default: system tmp)",
    ),
    titiler_url: str = typer.Option(
        default=GEONODE_TITILER_URL,
        help="Base URL of the TiTiler service used for raster layers in map configs",
    ),
):
    """Fetch all datasets from a GeoNode v4 instance and sync them to a DMS project."""
    datasets = _fetch_all_datasets(url)
    documents = _fetch_all_documents(url)
    maps = _fetch_all_maps(url)
    log.info(
        "resources fetched",
        datasets=len(datasets),
        documents=len(documents),
        maps=len(maps),
    )

    base = url.rstrip("/")
    version = datetime.datetime.now().strftime("%Y%m%d")
    # Normalise: bucket has no trailing slash, prefix has no leading/trailing slash
    bucket = bucket.rstrip("/")
    prefix = prefix.strip("/")

    parsed = urlparse(url)
    # Replace dots/colons so the origin is safe to use inside a DMS ID
    origin = parsed.netloc.replace(".", "-").replace(":", "-")

    s3 = s3fs.S3FileSystem(
        endpoint_url=endpoint or None,
        key=access_key or None,
        secret=secret_key or None,
    )

    # Pre-compute which dataset alternates appear in at least one map so we
    # know to generate PMTiles during the datasets loop.
    alternates_in_maps: set[str] = {
        ml.get("name", "")
        for m in maps
        for ml in m.get("maplayers", [])
        if ml.get("name")
    }

    # Populated during the datasets loop; consumed by the maps loop.
    # alternate → {"pmtiles_uri": str, "geom_type": str, "source_layer": str}
    pmtiles_info: dict[str, dict] = {}

    with httpx.Client(timeout=300) as client:
        for dataset in datasets:
            try:
                pk: int = dataset["pk"]
                ds_id = f"{origin}__{pk}"
                title: str = dataset.get("title") or dataset.get("name") or str(pk)
                subtype: str = dataset.get("subtype", "")
                # GeoNode v2 API exposes the workspace:layername as `alternate`
                layer_alternate: str = dataset.get("alternate", "")

                # Build metadata conforming to the DMS schema
                metadata = _build_metadata(dataset)

                # Upsert dataset
                dms.upsert_dms_element(
                    "datasets",
                    ds_id,
                    {
                        "id": ds_id,
                        "title": title,
                        "metadata": metadata,
                        "version": version,
                        "project_id": project_id,
                    },
                    {"title": title, "version": version},
                )

                # Resolve download URL via WPS gs:Download, then upload to S3.
                # Fall back to the GeoNode direct download_url if WPS fails.
                # Rasters are converted to COG before upload.
                resource_uri: str | None = None
                if layer_alternate:
                    ext = _WPS_FILE_EXT.get(subtype, ".bin")
                    layer_name = layer_alternate.split(":")[-1]
                    s3_path = (
                        f"{bucket}/{prefix}/{project_id}/{ds_id}/{layer_name}{ext}"
                    )
                    bucket_path = s3_path.lstrip("/")
                    https_uri = f"{endpoint.rstrip('/')}/{bucket_path}"

                    if s3.exists(s3_path):
                        log.info(
                            "S3 file already exists, skipping WPS",
                            s3_path=s3_path,
                        )
                        resource_uri = https_uri
                    else:
                        try:
                            wps_url = _get_wps_download_url(
                                base, layer_alternate, subtype
                            )
                            if wps_url:
                                if subtype == "raster":
                                    raw = _download_to_tempfile(
                                        wps_url, client, ".tiff", tmp_dir
                                    )
                                    cog: Path | None = None
                                    try:
                                        cog = _convert_to_cog(raw, tmp_dir)
                                        resource_uri = _upload_file_to_s3(
                                            cog, s3, s3_path, endpoint
                                        )
                                    finally:
                                        raw.unlink(missing_ok=True)
                                        if cog is not None:
                                            cog.unlink(missing_ok=True)
                                else:
                                    resource_uri = _upload_to_s3(
                                        wps_url, s3, s3_path, client, endpoint
                                    )
                        except Exception as exc:
                            log.warning(
                                "WPS request failed",
                                layer=layer_alternate,
                                error=exc,
                            )

                if not resource_uri:
                    direct_url: str = dataset.get("download_url") or ""
                    if direct_url:
                        ext = _WPS_FILE_EXT.get(subtype, ".bin")
                        alt = layer_alternate or str(pk)
                        layer_name = alt.split(":")[-1]
                        s3_path = (
                            f"{bucket}/{prefix}/{project_id}/{ds_id}/{layer_name}{ext}"
                        )
                        bucket_path = s3_path.lstrip("/")
                        https_uri = f"{endpoint.rstrip('/')}/{bucket_path}"

                        if s3.exists(s3_path):
                            log.info(
                                "S3 file already exists, skipping download",
                                s3_path=s3_path,
                            )
                            resource_uri = https_uri
                        else:
                            try:
                                if subtype == "raster":
                                    raw = _download_to_tempfile(
                                        direct_url, client, ".tiff", tmp_dir
                                    )
                                    cog: Path | None = None
                                    try:
                                        cog = _convert_to_cog(raw, tmp_dir)
                                        resource_uri = _upload_file_to_s3(
                                            cog, s3, s3_path, endpoint
                                        )
                                    finally:
                                        raw.unlink(missing_ok=True)
                                        if cog is not None:
                                            cog.unlink(missing_ok=True)
                                else:
                                    resource_uri = _upload_to_s3(
                                        direct_url, s3, s3_path, client, endpoint
                                    )
                            except Exception as exc:
                                log.warning(
                                    "direct download failed",
                                    ds_id=ds_id,
                                    error=exc,
                                )

                if not resource_uri:
                    log.warning(
                        "no download URL, skipping resource upsert",
                        ds_id=ds_id,
                    )
                else:
                    # Choose the correct DMS resource endpoint based on GeoNode subtype
                    resource_endpoint = (
                        "rasterresources" if subtype == "raster" else "tabularresources"
                    )
                    resource_id = f"{ds_id}_resource"

                    dms.upsert_dms_element(
                        resource_endpoint,
                        resource_id,
                        {
                            "id": resource_id,
                            "dataset_id": ds_id,
                            "title": title,
                            "uri": resource_uri,
                            "access_type": "public",
                            "role": "data",
                        },
                        {"title": title, "uri": resource_uri},
                    )

                # PMTiles: convert vector datasets that are used in at least one map.
                # We pass the S3 HTTPS URL directly (via /vsicurl/) so GDAL reads it
                # without a local copy.
                if (
                    resource_uri
                    and subtype == "vector"
                    and layer_alternate in alternates_in_maps
                ):
                    _layer_name = (layer_alternate or str(pk)).split(":")[-1]
                    pmtiles_s3_path = (
                        f"{bucket}/{prefix}/{project_id}/{ds_id}/{_layer_name}.pmtiles"
                    )
                    pmtiles_bucket_path = pmtiles_s3_path.lstrip("/")
                    pmtiles_https = f"{endpoint.rstrip('/')}/{pmtiles_bucket_path}"
                    vsicurl_uri = f"/vsicurl/{resource_uri}"

                    try:
                        if s3.exists(pmtiles_s3_path):
                            log.info(
                                "PMTiles already exists, skipping conversion",
                                s3_path=pmtiles_s3_path,
                            )
                            _geom_type = "polygon"
                            _source_layer = _layer_name
                        else:
                            pmtiles_tmp: Path | None = None
                            try:
                                _geom_type = _get_geometry_type(vsicurl_uri)
                                _source_layer = _gpkg_layer_name(vsicurl_uri)
                                pmtiles_tmp = _run_gpkg_to_pmtiles(
                                    vsicurl_uri, _layer_name, tmp_dir
                                )
                                pmtiles_https = _upload_file_to_s3(
                                    pmtiles_tmp, s3, pmtiles_s3_path, endpoint
                                )
                            finally:
                                if pmtiles_tmp is not None:
                                    pmtiles_tmp.unlink(missing_ok=True)

                            pmtiles_resource_id = f"{ds_id}_pmtiles"
                            dms.upsert_dms_element(
                                "tabularresources",
                                pmtiles_resource_id,
                                {
                                    "id": pmtiles_resource_id,
                                    "dataset_id": ds_id,
                                    "title": f"{title} PMTiles",
                                    "uri": pmtiles_https,
                                    "access_type": "public",
                                    "role": "data",
                                },
                                {
                                    "title": f"{title} PMTiles",
                                    "uri": pmtiles_https,
                                },
                            )

                        pmtiles_info[layer_alternate] = {
                            "pmtiles_uri": pmtiles_https,
                            "geom_type": _geom_type,
                            "source_layer": _source_layer,
                        }
                        log.info(
                            "PMTiles registered",
                            alternate=layer_alternate,
                            uri=pmtiles_https,
                        )
                    except Exception as exc:
                        log.error(
                            "PMTiles conversion failed",
                            alternate=layer_alternate,
                            error=exc,
                            exc_info=True,
                        )

                log.info("synced dataset", ds_id=ds_id, title=title, subtype=subtype)
            except Exception as exc:
                log.error(
                    "failed to sync dataset",
                    pk=dataset.get("pk"),
                    error=exc,
                    exc_info=True,
                )

        for document in documents:
            try:
                pk: int = document["pk"]
                resource_type: str = document.get("resource_type", "document")

                if resource_type == "map":
                    log.debug("skipping map", pk=pk)
                    continue

                ds_id = f"{origin}_DOC__{pk}"
                title: str = document.get("title") or document.get("name") or str(pk)
                extension: str = document.get("extension") or ""

                metadata = _build_metadata(document)

                dms.upsert_dms_element(
                    "datasets",
                    ds_id,
                    {
                        "id": ds_id,
                        "title": title,
                        "metadata": metadata,
                        "version": version,
                        "project_id": project_id,
                    },
                    {"title": title, "version": version},
                )

                # Documents are downloaded directly from GeoNode
                doc_download_url = (
                    document.get("href") or f"{base}/documents/{pk}/download"
                )
                file_name = f"{pk}"
                if extension:
                    file_name += f".{extension.lstrip('.')}"
                s3_path = f"{bucket}/{prefix}/{project_id}/{ds_id}/{file_name}"

                resource_uri = None
                try:
                    resource_uri = _upload_to_s3(
                        doc_download_url, s3, s3_path, client, endpoint
                    )
                except httpx.HTTPError as exc:
                    log.warning("document download failed", pk=pk, error=exc)
                    resource_uri = doc_download_url

                resource_id = f"{ds_id}_resource"
                dms.upsert_dms_element(
                    "resources",
                    resource_id,
                    {
                        "id": resource_id,
                        "dataset_id": ds_id,
                        "title": title,
                        "uri": resource_uri,
                        "access_type": "public",
                        "role": "data",
                    },
                    {"title": title, "uri": resource_uri},
                )

                log.info("synced document", ds_id=ds_id, title=title)
            except Exception as exc:
                log.error(
                    "failed to sync document",
                    pk=document.get("pk"),
                    error=exc,
                    exc_info=True,
                )

        # Build a lookup from dataset alternate → (ds_id, subtype) for maps
        ds_by_alternate: dict[str, dict] = {
            d["alternate"]: d for d in datasets if d.get("alternate")
        }

        for geonode_map in maps:
            try:
                map_pk: int = geonode_map["pk"]
                map_id = f"{origin}__map__{map_pk}"
                map_title = (
                    geonode_map.get("title") or geonode_map.get("name") or str(map_pk)
                )
                log.info("processing map", map_id=map_id, title=map_title)

                layer_infos: list[dict] = []

                for maplayer in geonode_map.get("maplayers", []):
                    ml_alternate: str = maplayer.get("name", "")
                    dataset = ds_by_alternate.get(ml_alternate)
                    if not dataset:
                        log.warning(
                            "map layer dataset not found, skipping",
                            alternate=ml_alternate,
                        )
                        continue

                    ds_pk = dataset["pk"]
                    ds_subtype: str = dataset.get("subtype", "")
                    layer_name = ml_alternate.split(":")[-1]
                    layer_id = f"{origin}__{ds_pk}"

                    # PMTiles info was produced during the datasets loop
                    pt = pmtiles_info.get(ml_alternate, {})
                    pmtiles_uri: str | None = pt.get("pmtiles_uri")
                    source_layer: str = pt.get("source_layer", layer_name)
                    geom_type: str = pt.get("geom_type", "polygon")

                    # HTTPS URI of the primary data file (COG/gpkg)
                    ext = _WPS_FILE_EXT.get(ds_subtype, ".bin")
                    s3_data_path = (
                        f"{bucket}/{prefix}/{project_id}/{layer_id}/{layer_name}{ext}"
                    )
                    bucket_path = s3_data_path.lstrip("/")
                    data_https_uri = f"{endpoint.rstrip('/')}/{bucket_path}"

                    layer_infos.append(
                        {
                            "id": layer_id,
                            "alternate": ml_alternate,
                            "layer_name": layer_name,
                            "title": dataset.get("title") or layer_name,
                            "abstract": (dataset.get("abstract") or "").strip(),
                            "ll_bbox_polygon": dataset.get("ll_bbox_polygon"),
                            "subtype": ds_subtype,
                            "https_uri": data_https_uri,
                            "pmtiles_uri": pmtiles_uri,
                            "source_layer": source_layer,
                            "geom_type": geom_type,
                        }
                    )

                # Build the map config JSON
                map_config = _build_map_config(
                    geonode_map, layer_infos, titiler_url, origin
                )
                _validate_map_config(map_config)
                map_json = json.dumps(map_config, ensure_ascii=False, indent=2)

                # Upload to S3
                map_s3_path = f"{bucket}/{prefix}/{project_id}/maps/{map_id}.json"
                map_bucket_path = map_s3_path.lstrip("/")
                map_https_uri = f"{endpoint.rstrip('/')}/{map_bucket_path}"

                map_bytes = map_json.encode("utf-8")
                with s3.open(
                    map_s3_path,
                    "wb",
                    ContentType="application/json",
                ) as f:
                    f.write(map_bytes)
                log.info("map config uploaded", s3_path=map_s3_path)

                # Upsert DMS dataset + map resource
                map_metadata = _build_metadata(geonode_map)
                dms.upsert_dms_element(
                    "datasets",
                    map_id,
                    {
                        "id": map_id,
                        "title": map_title,
                        "metadata": map_metadata,
                        "version": version,
                        "project_id": project_id,
                    },
                    {"title": map_title, "version": version},
                )
                map_resource_id = f"{map_id}_config"
                dms.upsert_dms_element(
                    "mapresources",
                    map_resource_id,
                    {
                        "id": map_resource_id,
                        "dataset_id": map_id,
                        "title": map_title + " Map Config",
                        "uri": map_https_uri,
                        "access_type": "public",
                        "role": "data",
                        "map_type": "nina",
                    },
                    {
                        "title": map_title + " Map Config",
                        "uri": map_https_uri,
                        "map_type": "nina",
                    },
                )

                log.info("synced map", map_id=map_id, title=map_title)
            except Exception as exc:
                log.error(
                    "failed to sync map",
                    pk=geonode_map.get("pk"),
                    error=exc,
                    exc_info=True,
                )
