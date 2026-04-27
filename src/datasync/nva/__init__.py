# load the files containg the scripts
# this allows to attach the scripts to the app
import typer

from . import nva, nva_filtered, nva_renew_hydro

app = typer.Typer(help="Commands to handle NVA tasks")

app.command(help="Sync NVA data from REST API to target")(nva.run)

app.command(help="Filter and create the tables from NVA data to parquet")(
    nva_filtered.filter_data
)

app.command(help="Filter NVA data to parquet for Renew Hydro and Hydrocen")(
    nva_renew_hydro.renew_hydro_filter_data
)
