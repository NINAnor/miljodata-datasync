# load the files containg the scripts
# this allows to attach the scripts to the app
import typer

from . import snp_analysis, snp_database  # noqa: F401

app = typer.Typer(help="Commands to handle NINAGEN tasks")


app.command(help="Convert SNP excel sheet to parquet")(
    snp_database.snp_database_normalize
)
app.command(help="Convert SNP csv of an analysis to a parquet file")(
    snp_analysis.snp_analysis_to_parquet
)
