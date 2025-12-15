# load the files containg the scripts
# this allows to attach the scripts to the app
from . import snp_analysis, snp_database  # noqa: F401

# then export the app
from .app import app

__all__ = ["app"]
