# used for https://www.vitenskapsradet.no/Rapporter


import datetime
from importlib.resources import files

import dlt
import duckdb
import typer
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator

from ..settings import (
    env,
    log,
)

NVA_BASE_URL = env("NVA_BASE_URL", default="https://api.nva.unit.no/")

NVA_RESOURCES_DATA_S3_PATH = env("NVA_RESOURCES_DATA_S3_PATH", default="")


NVA_ATLANTIC_SALMON_STORAGE_ENDPOINT_URL = env(
    "NVA_ATLANTIC_SALMON_STORAGE_ENDPOINT_URL", default=""
)
NVA_ATLANTIC_SALMON_STORAGE_ACCESS_KEY = env(
    "NVA_ATLANTIC_SALMON_STORAGE_ACCESS_KEY", default=""
)
NVA_ATLANTIC_SALMON_STORAGE_SECRET_KEY = env(
    "NVA_ATLANTIC_SALMON_STORAGE_SECRET_KEY", default=""
)
NVA_ATLANTIC_SALMON_STORAGE_BUCKET = env(
    "NVA_ATLANTIC_SALMON_STORAGE_BUCKET", default=""
)
NVA_ATLANTIC_SALMON_STORAGE_PREFIX = env(
    "NVA_ATLANTIC_SALMON_STORAGE_PREFIX", default="nva-atlantic-salmon"
)
NVA_ATLANTIC_SALMON_STORAGE_REGION = env(
    "NVA_ATLANTIC_SALMON_STORAGE_REGION", default="us-east-1"
)

app = typer.Typer(
    help="Create parquets for reports from vitenskapelig råd for lakseforvaltning "
    "specific tables from NVA data and export to parquet on a S3 Bucket"
)


def get_atlantic_salmon_resources(client: RESTClient):
    """Fetch resources from Vitenskapelig råd for lakseforvaltning publisher."""
    log.debug("Fetching Atlantic Salmon advisory resources")
    yield from client.paginate(
        "search/resources",
        method="GET",
        params={
            "publisher": "Vitenskapelig råd for lakseforvaltning",
        },
    )


@dlt.source()
def atlantic_salmon_source(base_url: str = NVA_BASE_URL):
    """DLT source for Atlantic Salmon advisory resources."""
    client = RESTClient(
        base_url=base_url,
        paginator=JSONLinkPaginator(next_url_path="nextResults"),
        data_selector="hits",
    )

    yield dlt.resource(
        get_atlantic_salmon_resources(client),
        name="resources_advisory_for_atlantic_salmon",
        write_disposition="replace",
        primary_key="identifier",
        max_table_nesting=1,
    )


@app.command()
def atlantic_salmon_filter_data(
    base_url: str = NVA_BASE_URL,
    data_s3_path: str = NVA_RESOURCES_DATA_S3_PATH,
    storage_s3_path: str = NVA_ATLANTIC_SALMON_STORAGE_ENDPOINT_URL,
    storage_access_key: str = NVA_ATLANTIC_SALMON_STORAGE_ACCESS_KEY,
    storage_secret_key: str = NVA_ATLANTIC_SALMON_STORAGE_SECRET_KEY,
    storage_bucket: str = NVA_ATLANTIC_SALMON_STORAGE_BUCKET,
    storage_prefix: str = NVA_ATLANTIC_SALMON_STORAGE_PREFIX,
    storage_region: str = NVA_ATLANTIC_SALMON_STORAGE_REGION,
    storage_url_style: str = "path",
):
    """
    The parquet files are being used to display publications on the
    vitenskapelig råd for lakseforvaltning website.
    """
    log.info("Fetching Atlantic Salmon advisory data from NVA API")

    log.info(
        f"Data will be available at: {storage_s3_path}/{storage_bucket}/"
        f"{storage_prefix}"
    )

    con = duckdb.connect()

    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    additional_identifiers = con.read_parquet(  # noqa: F841
        f"{data_s3_path}/resources__additional_identifiers.parquet"
    )
    resources = con.read_parquet(f"{data_s3_path}/resources.parquet")  # noqa: F841
    filter_query = files("datasync.nva.queries").joinpath("filter_cols.sql")

    # fix duplication of publication
    filter_resources = con.sql(filter_query.read_text())  # noqa: F841

    # select unique publiacations by identifier
    filter_resources_unique = con.sql("""
    SELECT DISTINCT ON (identifier) *
    FROM filter_resources
    ORDER BY entity_description__main_title ASC
    """)  # noqa: F841

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

    report_types = [
        {
            "name": "Reports",
            "description": "Rapport fra Vitenskapelig råd for lakseforvaltning",
            "issn": "1891-442X",
            "output_file": "reports.parquet",
        },
        {
            "name": "Special reports",
            "description": "Temarapport fra Vitenskapelig råd for lakseforvaltning",
            "issn": "1891-5302",
            "output_file": "special_reports.parquet",
        },
        {
            "name": "Method reports",
            "description": "Metoderapport fra Vitenskapelig råd for lakseforvaltning",
            "issn": "2704-212X",
            "output_file": "method_reports.parquet",
        },
    ]

    # Load and transform data once for all report types
    log.info("Loading and transforming NVA data")

    for report in report_types:
        result_set = con.sql(f"""
        FROM filter_resources_unique
        WHERE ctx_print_issn = '{report["issn"]}'
        ORDER BY publication_year DESC
        """)  # noqa: S608

        count = (
            result[0]
            if (result := con.sql("SELECT COUNT(*) FROM result_set").fetchone())
            else 0
        )

        s3_path = f"s3://{storage_bucket}/{storage_prefix}/{report['output_file']}"
        result_set.write_parquet(
            s3_path,
            compression="zstd",
            overwrite=True,
        )

        log.info(
            f"{report['name']} are filtered and written to parquet",
            count=count,
            where=s3_path,
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
