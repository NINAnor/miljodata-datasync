import datetime
from urllib.parse import urlparse

import httpx
import s3fs
import typer
from owslib.wps import WebProcessingService, monitorExecution
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
):
    """Fetch all datasets from a GeoNode v4 instance and sync them to a DMS project."""
    datasets = _fetch_all_datasets(url)
    documents = _fetch_all_documents(url)
    log.info(
        "resources fetched",
        datasets=len(datasets),
        documents=len(documents),
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
                # The DMS resource URI is always the HTTPS source URL.
                resource_uri: str | None = None
                if layer_alternate:
                    try:
                        wps_url = _get_wps_download_url(base, layer_alternate, subtype)
                        if wps_url:
                            ext = _WPS_FILE_EXT.get(subtype, ".bin")
                            layer_name = layer_alternate.split(":")[-1]
                            s3_path = (
                                f"{bucket}/{prefix}/{project_id}"
                                f"/{ds_id}/{layer_name}{ext}"
                            )
                            resource_uri = _upload_to_s3(
                                wps_url, s3, s3_path, client, endpoint
                            )
                    except Exception as exc:
                        log.warning(
                            "WPS request failed", layer=layer_alternate, error=exc
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
                        try:
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
