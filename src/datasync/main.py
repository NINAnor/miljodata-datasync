#!/usr/bin/env python3

"""Main script."""

import typer

from . import dms, ninagen, nva, ubw

app = typer.Typer()
app.add_typer(nva.app, name="nva")
app.add_typer(ubw.app, name="ubw")
app.add_typer(dms.app, name="dms")
app.add_typer(ninagen.app, name="ninagen")

if __name__ == "__main__":
    app()
