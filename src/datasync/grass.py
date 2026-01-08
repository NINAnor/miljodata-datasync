import datetime
import json

import duckdb
from typer import Typer

from .libs import dms
from .settings import log

app = Typer()


def trim_color_table(metadata: dict) -> dict:
    """
    Remove colorTable entries from gdal json output
    """
    bands = metadata.get("bands", [])

    for idx, band in enumerate(bands[:]):
        if "colorTable" in band:
            metadata["bands"][idx]["colorTable"]["entries"] = []
            log.debug("trimming...")

    return metadata


@app.command()
def register_layers(parquet_file_path: str, project_number: str, gisbase: str):
    log.debug("using file", file=parquet_file_path)
    con = duckdb.connect()
    layers = con.read_parquet(parquet_file_path)

    resources = (
        # 1. Remove the GISBASE from the file path, trim also eventual " or / from the file string  # noqa: E501
        layers.select(
            *[
                duckdb.StarExpression(),
                duckdb.FunctionExpression(
                    "trim",
                    duckdb.FunctionExpression(
                        "replace",
                        duckdb.FunctionExpression(
                            "trim",
                            duckdb.ColumnExpression("file"),
                            duckdb.ConstantExpression('"'),
                        ),
                        duckdb.ConstantExpression(gisbase),
                        duckdb.ConstantExpression(""),
                    ),
                    duckdb.ConstantExpression("/"),
                ).alias("cleaned_file"),
            ]
        )
        # 2. extract location, mapset, type and resource from the cleaned filepath
        .select(
            *[
                duckdb.StarExpression(),
                duckdb.FunctionExpression(
                    "split_part",
                    duckdb.ColumnExpression("cleaned_file"),
                    duckdb.ConstantExpression("/"),
                    duckdb.ConstantExpression(1),
                ).alias("location"),
                duckdb.FunctionExpression(
                    "split_part",
                    duckdb.ColumnExpression("cleaned_file"),
                    duckdb.ConstantExpression("/"),
                    duckdb.ConstantExpression(2),
                ).alias("mapset"),
                duckdb.FunctionExpression(
                    "split_part",
                    duckdb.ColumnExpression("cleaned_file"),
                    duckdb.ConstantExpression("/"),
                    duckdb.ConstantExpression(3),
                ).alias("type"),
                duckdb.FunctionExpression(
                    "split_part",
                    duckdb.ColumnExpression("cleaned_file"),
                    duckdb.ConstantExpression("/"),
                    duckdb.ConstantExpression(4),
                ).alias("resource"),
            ]
        )
        # 3. produce something that can be used as dataset_id
        .select(
            *[
                duckdb.StarExpression(),
                duckdb.FunctionExpression(
                    "concat",
                    duckdb.ConstantExpression("grass-"),
                    duckdb.ColumnExpression("mapset"),
                ).alias("dataset_id"),
            ]
        )
    )

    log.info(resources)

    datasets = resources.aggregate("mapset, dataset_id, count(*) as count").order(
        "count"
    )
    log.info(datasets)

    # version will be based on the date of execution
    # since this script cannot be executed in a cron job
    version = datetime.datetime.now().strftime("%Y%m%d")

    for d in datasets.to_arrow_table().to_pylist():
        dms.upsert_dms_element(
            "datasets",
            d.get("dataset_id"),
            {
                "id": d.get("dataset_id"),
                "title": d["mapset"],
                # TODO: provide some metadata
                "metadata": {},
                "version": version,
                "project_id": project_number,
            },
            {"title": d["mapset"], "version": version},
        )

    for d in resources.to_arrow_table().to_pylist():
        # DMS requires a resource URI, but Grass GIS doesn't provide anything like that
        # the only valid reference is Grass is mapname@mapset
        # the prefix grass: is not a standard
        # the query parameters GISBASE and LOCATION_NAME are actually valid env variables for grass gis  # noqa: E501
        # type is an additional query param to add differentiate between raster and vectors  # noqa: E501
        uri = f"grass://{d.get('resource')}@{gisbase}/{d.get('location')}/{d.get('mapset')}?type={d.get('type')}"  # noqa: E501
        # NOTE: it's necessary to add a type, some resources otherwise have the same name between rasters and vectors  # noqa: E501
        resource_id = f"{d.get('dataset_id')}-{d.get('resource')}-{d.get('type')}"
        metadata = trim_color_table(json.loads(d.get("metadata")))
        dms.upsert_dms_element(
            "tabularresources" if d.get("type") == "vector" else "rasterresources",
            resource_id,
            {
                "id": resource_id,
                "dataset_id": d.get("dataset_id"),
                "title": d["resource"],
                "metadata": metadata,
                "is_metadata_manual": True,
                "access_type": "permission_required",
                "role": "data",
                "uri": uri,
            },
            {
                "dataset_id": d.get("dataset_id"),
                "title": d["resource"],
                "metadata": metadata,
                "uri": uri,
                "is_metadata_manual": True,
            },
        )
