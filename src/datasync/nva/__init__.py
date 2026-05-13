# load the files containg the scripts
# this allows to attach the scripts to the app
import typer

from . import (
    nva,
    nva_filter_resources,
    nva_get_publications_per_researcher,
    nva_search_resources,
)

app = typer.Typer(help="Commands to handle NVA tasks")

app.command(help="Sync NVA data from REST API to target")(nva.run)

app.command(help="Filter NVA parquet files and export to S3")(
    nva_filter_resources.filter_data
)

app.command(help="Fetch and filter NVA data with flexible search parameters")(
    nva_search_resources.search_resources_api
)

app.command(help="Fetch publications for researchers at NINA")(
    nva_get_publications_per_researcher.get_pubs_per_researcher
)
