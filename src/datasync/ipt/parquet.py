import pathlib
import traceback

import fsspec

from ..settings import env as logger
from .dwca import get_context_from_metafile
from .settings import (
    AWS_ENDPOINT_URL,
    CACHE_PATH,
    IPT_URL,
    RESOURCES_PREFIX,
    S3_BUCKET,
    conn,
    templates,
)


def version_to_parquet(resource_id: str, version_id: str):
    logger.info(f"starting {resource_id}@{version_id}")

    s3_path_latest = f"s3://{S3_BUCKET}{RESOURCES_PREFIX}{resource_id}.parquet"
    cache = pathlib.Path(CACHE_PATH) / f"{resource_id}-v{version_id}.zip"

    # Check that the version exists, otherwise create it and overwrite the latest one
    try:
        # create a temporary cache to allow duckdb to read it
        # httpfs + zipfs does not work greatly together
        logger.info("downloading locally")
        with fsspec.open(
            f"{IPT_URL}/archive.do?r={resource_id}&v={version_id}"
        ) as source:
            with cache.open("wb") as dest:
                dest.write(source.read())

        cursor = conn.cursor()
        ctx = get_context_from_metafile(resource_path=cache)
        query = templates.get_template("query.sql").render(**ctx, trim_blocks=True)
        logger.info("write to parquet")
        cursor.sql(query).write_parquet(
            s3_path_latest, compression="zstd", overwrite=True
        )
    except FileNotFoundError:
        logger.error(f"resource {resource_id}@{version_id} not found")
    except Exception:
        logger.error(traceback.format_exc())
    finally:
        logger.info("done")
        cache.unlink(missing_ok=True)
    # else:
    #     logger.info("already available")

    return f"{AWS_ENDPOINT_URL}/{S3_BUCKET}{RESOURCES_PREFIX}{resource_id}.parquet"
