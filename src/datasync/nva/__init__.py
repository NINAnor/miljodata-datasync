# load the files containg the scripts
# this allows to attach the scripts to the app
import typer

from . import nva, nva_atlantic_salmon, nva_filtered

app = typer.Typer(help="Commands to handle NVA tasks")

app.command(help="Sync NVA data from REST API to target")(nva.run)

app.command(help="Filter and create the tables from NVA data to parquet")(
    nva_filtered.filter_data  # TODO: rename to nina_publications_filter_data
)

app.command(help="From NVA resources save atlantic salmon advisory data to parquet")(
    nva_atlantic_salmon.atlantic_salmon_filter_data
)
