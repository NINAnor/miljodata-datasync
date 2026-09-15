"""Sync KEEP.eu Interreg programme, project, and partnership data to S3."""

import dlt
import typer
from dlt.sources.helpers.requests import Client
from dlt.sources.rest_api import rest_api_source

from ..settings import log

app = typer.Typer(help="Sync KEEP.eu Interreg data to parquet on S3")

KEEP_REQUEST_TIMEOUT = 300
KEEP_PERIODS = {
    "1": "2000-2006",
    "2": "2007-2013",
    "3": "2014-2020",
    "4": "2021-2027",
}


def keep_source(
    api_key: str,
    base_url: str,
    period: str | None = None,
    programme_ids: str | None = None,
    only_programme: bool = False,
    calls_status: str | None = None,
):
    """Define the KEEP.eu open-data REST source.

    dlt normalizes the nested programme, project, and partnership objects into
    related tables automatically.
    """
    if period is not None:
        period = KEEP_PERIODS.get(period, period)
    params = {
        "key": api_key,
        "period": period,
        "ids": programme_ids,
        "onlyprogramme": str(only_programme).lower() if only_programme else None,
        "callsstatus": calls_status,
    }
    if not (period or programme_ids or only_programme):
        log.warning(
            "KEEP.eu request has no period/ids/onlyprogramme filter; "
            "this fetches the full historical dataset and can take several "
            "minutes to respond"
        )
    return rest_api_source(
        {
            "client": {
                "base_url": base_url,
                "headers": {"Accept": "application/json"},
                "session": Client(
                    request_timeout=KEEP_REQUEST_TIMEOUT, raise_for_status=False
                ).session,
            },
            "resources": [
                {
                    "name": "open_data",
                    "write_disposition": "replace",
                    "endpoint": {
                        "path": "open-data",
                        "params": {
                            key: value for key, value in params.items() if value
                        },
                        "paginator": "single_page",
                    },
                }
            ],
        }
    )


@app.command()
def run(
    ctx: typer.Context,
    api_key: str = typer.Option(
        ...,
        envvar="KEEP_API_KEY",
        help="KEEP.eu open-data API key",
    ),
    period: str | None = typer.Option(
        default=None,
        envvar="KEEP_PERIOD",
        help="Programme period: 1=2000-2006, 2=2007-2013, 3=2014-2020, 4=2021-2027",
    ),
    programme_ids: str | None = typer.Option(
        default=None,
        envvar="KEEP_PROGRAMME_IDS",
        help="Comma-separated KEEP.eu programme IDs",
    ),
    only_programme: bool = typer.Option(
        default=False,
        envvar="KEEP_ONLY_PROGRAMME",
        help="Load programme data only, excluding projects and partnerships",
    ),
    calls_status: str | None = typer.Option(
        default=None,
        envvar="KEEP_CALLS_STATUS",
        help="Call status: ongoing, future, or both",
    ),
    base_url: str = typer.Option(
        envvar="KEEP_API_URL",
        help="KEEP.eu API base URL",
    ),
    dataset_name: str = typer.Option(
        default="keep",
        envvar="KEEP_DATASET_NAME",
        help="Local pipeline name (used for dlt's local working/state directory)",
    ),
):
    """Fetch the KEEP.eu open-data export and write it as parquet files to S3."""
    pipeline = dlt.pipeline(
        pipeline_name=dataset_name,
        destination=ctx.obj["filesystem_destination"],
        dataset_name="keep",
        progress="tqdm",
    )
    load_info = pipeline.run(
        keep_source(
            api_key=api_key,
            period=period,
            programme_ids=programme_ids,
            only_programme=only_programme,
            calls_status=calls_status,
            base_url=base_url,
        ),
        loader_file_format="parquet",
    )
    log.info(f"KEEP.eu pipeline output written to {ctx.obj['bucket_url']}/keep/")
    log.info(f"Pipeline run completed. Load info: {load_info}")


if __name__ == "__main__":
    app()
