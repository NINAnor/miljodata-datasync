import fsspec
import typer
from duckdb import (
    DuckDBPyConnection,
    connect,
)

from .libs.helpers import DuckDBAtomicTransaction
from .settings import (
    env,
    log,
)

log.debug("Importing GBIF Backbone settings")


GBIF_BACKBONE_DUCKDB_NAME = env.path(
    "GBIF_BACKBONE_DUCKDB_FILE_NAME", default="gbif_backbone.duckdb"
).root
GBIF_BACKBONE_URL = env.url(
    "GBIF_BACKBONE_URL",
    default="https://hosted-datasets.gbif.org/datasets/backbone/current/backbone.zip",
)

app = typer.Typer(help="export GBIF Backbone data to DuckDB database")


def import_taxon(conn: DuckDBPyConnection, archive):
    log.debug("Importing Taxon.tsv")
    conn.sql("DROP TABLE IF EXISTS taxon")
    conn.from_csv_auto(archive.open("Taxon.tsv")).to_table("taxon")
    conn.sql("CREATE INDEX taxon_taxonid ON taxon (taxonid)")
    conn.sql("""
        PRAGMA create_fts_index(
            "taxon", "taxonID", "canonicalName", overwrite=TRUE
        )
    """)


def import_vernacular_name(conn: DuckDBPyConnection, archive):
    log.debug("Importing VernacularName.tsv")
    conn.sql("DROP TABLE IF EXISTS vernacular_name")
    vernacular_names = conn.from_csv_auto(archive.open("VernacularName.tsv"))  # noqa: F841
    conn.sql("""
        SELECT *, CONCAT_WS('|', taxonID, language, vernacularName) AS vernacularID
        FROM vernacular_names
    """).to_table("vernacular_name")
    conn.sql("CREATE INDEX vernacular_name_taxonid ON vernacular_name (taxonid)")
    conn.sql("CREATE INDEX vernacular_name_language ON vernacular_name (language)")
    conn.sql("""
        PRAGMA create_fts_index(
            "vernacular_name", "vernacularID", "vernacularName", overwrite=TRUE
        )
    """)


@app.command()
def import_all():
    """Import GBIF Backbone data into a DuckDB database."""
    archive = fsspec.filesystem("zip", fo=GBIF_BACKBONE_URL.geturl(), mode="r")
    with connect(GBIF_BACKBONE_DUCKDB_NAME) as conn:
        with DuckDBAtomicTransaction(conn):
            import_taxon(conn, archive)
            import_vernacular_name(conn, archive)
    log.info("GBIF Backbone data imported successfully")
