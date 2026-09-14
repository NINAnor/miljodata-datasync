from typing import Any

import dlt
import typer

from ..settings import log


def run_pipeline(
    ctx: typer.Context,
    *,
    pipeline_name: str,
    dataset_name: str,
    source: Any,
    source_name: str,
):
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=ctx.obj["filesystem_destination"],
        dataset_name=dataset_name,
        progress="tqdm",
    )
    load_info = pipeline.run(source, loader_file_format="parquet")
    log.info(
        f"{source_name} pipeline output written to "
        f"{ctx.obj['bucket_url']}/{dataset_name}/"
    )
    log.info(f"Pipeline run completed. Load info: {load_info}")
