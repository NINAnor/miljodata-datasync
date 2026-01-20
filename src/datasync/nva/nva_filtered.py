from importlib.resources import files

import duckdb
import typer

from ..settings import (
    env,
    log,
)

NVA_RESOURCES_DATA_S3_PATH = env("NVA_RESOURCES_DATA_S3_PATH", default="")
NVA_FILTER_STORAGE_S3_PATH = env("NVA_FILTER_STORAGE_S3_PATH", default="")
NVA_FILTER_STORAGE_ACCESS_KEY = env("NVA_FILTER_STORAGE_ACCESS_KEY", default="")
NVA_FILTER_STORAGE_SECRET_KEY = env("NVA_FILTER_STORAGE_SECRET_KEY", default="")
NVA_FILTER_STORAGE_BUCKET = env("NVA_FILTER_STORAGE_BUCKET", default="")

NVA_FILTER_STORAGE_PREFIX = env("NVA_FILTER_STORAGE_PREFIX", default="nva-filtered")
NVA_FILTER_STORAGE_REGION = env("NVA_FILTER_STORAGE_REGION", default="us-east-1")
NVA_FILTER_PATH_PARQUET_OUTPUT = env("NVA_FILTER_PATH_PARQUET_OUTPUT", default="")

app = typer.Typer(
    help="Create NINA specific tables from NVA data and export to parquet on a "
    "S3 Bucket"
)


def create_parquet_views(
    con: duckdb.DuckDBPyConnection,
    file_name: str,
    query: str,
    storage_s3_bucket: str,
    storage_s3_prefix: str,
    data_s3_path: str,
):
    """
    Create a view for latest publications from NVA data.

    Args:
        query_file: Path to the .sql file
        output_file: Optional path to save results
        data_s3_path: S3 path to the source data
    """
    # execute the query with parameters and write directly to parquet
    log.info(
        f"Creating and writing parquet file to s3://{storage_s3_bucket}/{storage_s3_prefix}/{file_name}"
    )
    con.sql(query, params={"data_s3_path": data_s3_path}).write_parquet(
        f"s3://{storage_s3_bucket}/{storage_s3_prefix}/{file_name}",
        compression="zstd",
        overwrite=True,
    )


@app.command()
def filter_data(
    data_s3_path: str = NVA_RESOURCES_DATA_S3_PATH,
    storage_s3_path: str = NVA_FILTER_STORAGE_S3_PATH,
    storage_access_key: str = NVA_FILTER_STORAGE_ACCESS_KEY,
    storage_secret_key: str = NVA_FILTER_STORAGE_SECRET_KEY,
    storage_bucket: str = NVA_FILTER_STORAGE_BUCKET,
    storage_prefix: str = NVA_FILTER_STORAGE_PREFIX,
    storage_url_style: str = "path",
):
    log.info("Filtering NVA data and exporting to parquet files on S3 Bucket")
    log.info(
        f"Data will be available at: {storage_s3_path}/{storage_bucket}/"
        f"{storage_prefix}"
    )

    con = duckdb.connect()
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")

    con.execute(f"""
        CREATE OR REPLACE SECRET secret (
            TYPE s3,
            REGION 'eu-west-1',
            KEY_ID '{storage_access_key}',
            SECRET '{storage_secret_key}',
            ENDPOINT '{storage_s3_path.replace(r"https://", "")}',
            URL_STYLE '{storage_url_style}'
        );
    """).fetchall()

    for query_file in files("datasync.nva.queries").iterdir():
        if query_file.is_file() and query_file.name.endswith(".sql"):
            file_name = f"{query_file.name[:-4]}.parquet"
            create_parquet_views(
                con,
                file_name=file_name,
                query=query_file.read_text(),
                storage_s3_bucket=storage_bucket,
                storage_s3_prefix=storage_prefix,
                data_s3_path=data_s3_path,
            )

    con.close()

    log.info("NVA data sync completed")
    log.info(f"Data available at: {storage_s3_path}/{storage_bucket}/{storage_prefix}")


if __name__ == "__main__":
    app()
