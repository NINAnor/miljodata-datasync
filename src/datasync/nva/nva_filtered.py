import datetime
from importlib.resources import files

import duckdb
import typer

from ..settings import (
    env,
    log,
)

NVA_RESOURCES_DATA_S3_PATH = env("NVA_RESOURCES_DATA_S3_PATH", default="")
NVA_FILTER_STORAGE_S3_PATH = env("NVA_FILTER_STORAGE_ENDPOINT_URL", default="")
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
    additional_identifiers = con.read_parquet(  # noqa: F841
        f"{data_s3_path}/resources__additional_identifiers.parquet"
    )
    resources = con.read_parquet(f"{data_s3_path}/resources.parquet")  # noqa: F841

    con.sql(query).write_parquet(
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
    """
    The parquet files are being used to display publications on the NINA website.
    """
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

    additional_identifiers = con.read_parquet(  # noqa: F841
        f"{data_s3_path}/resources__additional_identifiers.parquet"
    )
    resources = con.read_parquet(f"{data_s3_path}/resources.parquet")  # noqa: F841
    filter_query = files("datasync.nva.queries").joinpath("filter_cols.sql")

    # fix duplication of publication
    filter_resources_raw = con.sql(filter_query.read_text())  # noqa: F841
    filter_resources = con.sql("""
        SELECT * FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY identifier
                    ORDER BY modified_date DESC
                ) as rn
            FROM filter_resources_raw
        )
        WHERE rn = 1
    """)  # noqa: F841

    # NINA Datarapport
    data_reports = con.sql("""
    SELECT *
    FROM filter_resources
    WHERE ctx_print_issn LIKE '%2703-9447%'
    ORDER BY publication_year DESC
    """)
    data_reports_count = (
        result[0]
        if (result := con.sql("SELECT COUNT(*) FROM data_reports").fetchone())
        else 0
    )

    data_reports.write_parquet(
        f"s3://{storage_bucket}/{storage_prefix}/data_reports.parquet",
        compression="zstd",
        overwrite=True,
    )

    log.info(
        "NINA Datarapport filtered and written to parquet",
        count=data_reports_count,
        where=f"s3://{storage_bucket}/{storage_prefix}/data_reports.parquet",
    )

    # NINA Temahefte
    special_reports = con.sql("""
    SELECT *
    FROM filter_resources
    WHERE
        LOWER(ctx_print_issn) LIKE '%2535-6526%' OR
        LOWER(ctx_print_issn) LIKE '%0804-421x%'
    ORDER BY publication_year DESC
    """)
    special_reports_count = (
        result[0]
        if (result := con.sql("SELECT COUNT(*) FROM special_reports").fetchone())
        else 0
    )
    special_reports.write_parquet(
        f"s3://{storage_bucket}/{storage_prefix}/special_reports.parquet",
        compression="zstd",
        overwrite=True,
    )

    log.info(
        "NINA Temahefte filtered and written to parquet",
        count=special_reports_count,
        where=f"s3://{storage_bucket}/{storage_prefix}/special_reports.parquet",
    )

    # NINA Rapporter
    reports = con.sql("""
    SELECT *
    FROM filter_resources
    WHERE online_issn LIKE '%1504-3312%'
    ORDER BY publication_year DESC""")
    reports_count = (
        result[0]
        if (result := con.sql("SELECT COUNT(*) FROM reports").fetchone())
        else 0
    )
    reports.write_parquet(
        f"s3://{storage_bucket}/{storage_prefix}/reports.parquet",
        compression="zstd",
        overwrite=True,
    )

    log.info(
        "NINA Rapporter filtered and written to parquet",
        count=reports_count,
        where=f"s3://{storage_bucket}/{storage_prefix}/reports.parquet",
    )

    # NINA Årsmelding
    yearly_reports = con.sql("""
    SELECT *
    FROM filter_resources
    WHERE ctx_print_issn LIKE '%0809-8794%'
    ORDER BY publication_year DESC""")
    yearly_reports_count = (
        result[0]
        if (result := con.sql("SELECT COUNT(*) FROM yearly_reports").fetchone())
        else 0
    )
    yearly_reports.write_parquet(
        f"s3://{storage_bucket}/{storage_prefix}/yearly_reports.parquet",
        compression="zstd",
        overwrite=True,
    )

    log.info(
        "NINA Årsmelding filtered and written to parquet",
        count=yearly_reports_count,
        where=f"s3://{storage_bucket}/{storage_prefix}/yearly_reports.parquet",
    )

    # Nylige publikasjoner inkludert NINA-rapporter
    latest_publications = con.sql("""
    SELECT *
    FROM filter_resources
    WHERE
        publication_date <= CURRENT_DATE
        AND (
            (
            LOWER(pub_instance_type) LIKE '%academicarticle%'
            )

            OR (
            LOWER(pub_instance_type) LIKE '%academicliteraturereview%'
            )
            OR (
            LOWER(pub_instance_type) LIKE '%academicmonograph%'
            )
            OR (
            LOWER(pub_instance_type) LIKE '%reportresearch%'
            )
            OR (
            LOWER(pub_instance_type) LIKE '%academicchapter%'
            )
        )
    ORDER BY
    publication_date DESC,
    entity_description__main_title ASC
    """)
    latest_publications_count = (
        result[0]
        if (result := con.sql("SELECT COUNT(*) FROM latest_publications").fetchone())
        else 0
    )
    latest_publications.write_parquet(
        f"s3://{storage_bucket}/{storage_prefix}/latest_publications.parquet",
        compression="zstd",
        overwrite=True,
    )

    log.info(
        "Nylige publikasjoner inkludert NINA-rapporter filtered and written to parquet",
        count=latest_publications_count,
        where=f"s3://{storage_bucket}/{storage_prefix}/latest_publications.parquet",
    )

    # write last successful run timestamp to S3
    timestamp = datetime.datetime.now().isoformat()
    con.execute(f"""
        COPY (SELECT '{timestamp}' as last_successful_run)
        TO 's3://{storage_bucket}/{storage_prefix}/last_successful_run.parquet'
        (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    log.info(f"Last successful run timestamp written: {timestamp}")

    con.close()

    log.info("NVA data sync completed")
    log.info(f"Data available at: {storage_s3_path}/{storage_bucket}/{storage_prefix}")


if __name__ == "__main__":
    app()
