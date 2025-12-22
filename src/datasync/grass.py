import datetime
import json

import duckdb
from typer import Typer

from .libs import dms
from .settings import log

app = Typer()


@app.command()
def register_layers(parquet_file_path: str, project_number: str, gisbase: str):
    log.debug("using file", file=parquet_file_path)
    con = duckdb.connect()
    layers = con.read_parquet(parquet_file_path)

    resources = (
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
        uri = f"grass:{d.get('resource')}@{d.get('mapset')}?GISBASE={gisbase}&LOCATION_NAME={d.get('location')}&type={d.get('type')}"  # noqa: E501
        resource_id = f"{d.get('dataset_id')}-{d.get('resource')}-{d.get('type')}"
        metadata = json.loads(d.get("metadata"))
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
