from importlib.resources import files

import duckdb
import fsspec
import typer

from .libs.helpers import pa_read_tsv
from .settings import (
    env,
    log,
)

resources = files(__package__).joinpath("gbif_backbone")
taxon_sql = resources.joinpath("taxon.sql").read_text()
vernacularname_sql = resources.joinpath("vernacularname.sql").read_text()


log.debug("Importing GBIF Backbone settings")


GBIF_BACKBONE_DUCKDB_NAME = env.path(
    "GBIF_BACKBONE_DUCKDB_FILE_NAME", default="gbif_backbone.duckdb"
).root
GBIF_BACKBONE_URL = env.url(
    "GBIF_BACKBONE_URL",
    default="https://hosted-datasets.gbif.org/datasets/backbone/current/backbone.zip",
)

app = typer.Typer(help="export GBIF Backbone data to DuckDB database")


def import_taxon(conn, archive):
    log.debug("Importing Taxon.tsv")
    pa_taxon = pa_read_tsv(archive, "Taxon.tsv")  # noqa: F841
    conn.execute(taxon_sql)


def import_vernacular_name(conn, archive):
    log.debug("Importing VernacularName.tsv")
    pa_vernacular_names = pa_read_tsv(archive, "VernacularName.tsv")  # noqa: F841
    conn.execute(vernacularname_sql)


@app.command()
def import_all():
    """Import GBIF Backbone data into a DuckDB database."""
    archive = fsspec.filesystem("zip", fo=GBIF_BACKBONE_URL.geturl(), mode="r")
    with duckdb.connect(GBIF_BACKBONE_DUCKDB_NAME) as conn:
        import_taxon(conn, archive)
        import_vernacular_name(conn, archive)
    log.info("GBIF Backbone data imported successfully")
