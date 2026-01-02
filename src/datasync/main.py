#!/usr/bin/env python3

"""Main script."""

import typer

from . import dms, ninagen, nva, pit_registering_salmon, ubw

app = typer.Typer(
    help="Provide subcommands for synchronizing different resources, see subcommands"
)
app.add_typer(nva.app, name="nva")
app.add_typer(ubw.app, name="ubw")
app.add_typer(dms.app, name="dms")
app.add_typer(ninagen.app, name="ninagen")
app.add_typer(pit_registering_salmon.app, name="pit-registering-salmon")

if __name__ == "__main__":
    app()
