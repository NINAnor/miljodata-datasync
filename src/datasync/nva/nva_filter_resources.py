from collections import defaultdict
from importlib.resources import files

import typer
from duckdb import ColumnExpression, ConstantExpression, FunctionExpression

from ..settings import (
    log,
)
from .utils import (
    setup_duckdb_s3_connection,
    write_timestamp,
)

app = typer.Typer(
    help="Create tables from NVA parquet file and export to parquet on a S3 Bucket"
)


@app.command()
def filter_data(
    resource_name: str = typer.Option(..., help="Name for the filtered resource"),
    data_s3_path: str = typer.Option(
        ..., envvar="NVA_RESOURCES_DATA_S3_PATH", help="S3 path for input data"
    ),
    storage_s3_endpoint: str = typer.Option(
        ..., envvar="NVA_S3_ENDPOINT_URL", help="S3 endpoint URL for storage"
    ),
    storage_access_key: str = typer.Option(
        ..., envvar="NVA_S3_ACCESS_KEY", help="S3 access key for storage"
    ),
    storage_secret_key: str = typer.Option(
        ..., envvar="NVA_S3_SECRET_KEY", help="S3 secret key for storage"
    ),
    storage_bucket: str = typer.Option(
        ..., envvar="NVA_S3_BUCKET", help="S3 bucket for storage"
    ),
    storage_prefix: str = typer.Option(
        ..., envvar="NVA_S3_PREFIX", help="S3 prefix for storage"
    ),
    filter_by: list[str] | None = typer.Option(  # noqa: B008
        None,
        "--filter",
        help="Filter by column value (format: column=value). "
        "Can specify multiple filters.",
    ),
    show_columns: bool = typer.Option(
        False,
        "--show-columns",
        help="Display available column names and exit",
    ),
):
    """
    Filter NVA resources by custom criteria and export to parquet on S3.

    Examples:
        # Show available columns
        datasync nva filter-data --resource-name test --show-columns

        # Filter by publication type and ISSN
        datasync nva filter-data \
            --resource-name latest-publications \
            --filter pub_instance_type=academicarticle \
            --filter pub_instance_type=academicmonograph \
            --filter online_issn=1504-3312 \
            --filter ctx_print_issn=2703-9447
    """
    if not data_s3_path:
        log.error(
            "S3 path for input data must be provided.\n"
            "Either set it in the environment "
            "variable NVA_RESOURCES_DATA_S3_PATH or pass it as a parameter."
        )
        raise typer.Exit(code=1)

    log.debug("Starting NVA data filtering with parameters", filter_by=filter_by)

    con = setup_duckdb_s3_connection(
        endpoint_url=storage_s3_endpoint,
        access_key=storage_access_key,
        secret_key=storage_secret_key,
        region="eu-west-1",
    )

    additional_identifiers = con.read_parquet(  # noqa: F841
        f"{data_s3_path}/resources__additional_identifiers.parquet"
    )
    resources = con.read_parquet(f"{data_s3_path}/resources.parquet")
    filter_query = files("datasync.nva.queries").joinpath("extract_useful_info.sql")
    filter_resources_raw = con.sql(filter_query.read_text())  # noqa: F841

    if show_columns:
        log.info("Available columns for filtering:")
        for col in filter_resources_raw.columns:
            print(f"  - {col}")
        con.close()
        return

    log.debug("Available columns for filtering", columns=filter_resources_raw.columns)
    filter_resources = (
        filter_resources_raw.filter(
            ColumnExpression("publication_date") <= FunctionExpression("CURRENT_DATE")
        )
        .order("publication_date DESC, entity_description__main_title ASC")
        .distinct()
    )
    log.info("Resources count", all=len(resources), filtered=len(filter_resources))

    filters_by_column = defaultdict(list)
    if filter_by:
        for filter_str in filter_by:
            if "=" in filter_str:
                column, value = filter_str.split("=", 1)
                filters_by_column[column.strip()].append(value.strip())
            else:
                log.warning(
                    "Ignoring invalid filter format (expected column=value): "
                    f"{filter_str}"
                )

    conditions = []
    for column, values in filters_by_column.items():
        if column not in filter_resources_raw.columns:
            log.warning(
                f"Column '{column}' not found in dataset. Use --show-columns to see "
                "available columns."
            )
            continue

        if column == "pub_instance_type":
            lower_column = FunctionExpression("lower", ColumnExpression(column))
            for value in values:
                conditions.append(lower_column == ConstantExpression(value.lower()))
        else:
            # TODO: maybe be less strict and allow partial matches for other columns as
            # well? For example for names, titles and so on
            for value in values:
                conditions.append(ColumnExpression(column) == ConstantExpression(value))
    log.debug("Constructed filter conditions", conditions=conditions)
    if conditions:
        combined_condition = conditions[0]
        for condition in conditions[1:]:
            combined_condition = combined_condition | condition
        filter_resources = filter_resources.filter(combined_condition)

    log.debug("Filtered resources count", count=len(filter_resources))
    output_path = f"s3://{storage_bucket}/{storage_prefix}/{resource_name}.parquet"

    log.info(f"Writing {resource_name}.parquet to: {output_path}")
    filter_resources.write_parquet(
        output_path,
        compression="zstd",
        overwrite=True,
    )

    log.info(
        "Filtered data written to parquet",
        resource_name=resource_name,
        count=len(filter_resources),
        where=f"{storage_s3_endpoint}/{storage_bucket}/{storage_prefix}/{resource_name}.parquet",
    )

    # write timestamp
    write_timestamp(con, storage_bucket, storage_prefix)
    log.debug("NVA data filtering completed", resource_name=resource_name)
    con.close()


if __name__ == "__main__":
    app()
