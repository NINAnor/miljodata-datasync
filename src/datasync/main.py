#!/usr/bin/env python3

"""Main script."""

import typer

from . import (
    coat,
    dms,
    gbif_backbone,
    grass,
    mediebank,
    ninagen,
    nva,
    pit_registering_salmon,
    services,
    ubw,
)
from .ipt.main import app as ipt_app

app = typer.Typer(
    help="Provide subcommands for synchronizing different resources, see subcommands"
)
app.add_typer(nva.app, name="nva")
app.add_typer(ubw.app, name="ubw")
app.add_typer(dms.app, name="dms")
app.add_typer(ninagen.app, name="ninagen")
app.add_typer(pit_registering_salmon.app, name="pit-registering-salmon")
app.add_typer(grass.app, name="grass-gis")
app.add_typer(services.app, name="services")
app.add_typer(gbif_backbone.app, name="gbif-backbone")
app.add_typer(ipt_app, name="ipt")
app.add_typer(mediebank.app, name="mediebank")
app.add_typer(coat.app, name="coat")

if __name__ == "__main__":
    app()
