import pathlib

import environ

from .logger import configure_logger, logging

env = environ.Env()
BASE_DIR = pathlib.Path(__file__).parent.parent.parent

environ.Env.read_env(str(BASE_DIR / ".env"))

log = configure_logger(
    logging.DEBUG if env.bool("DEBUG", default=False) else logging.INFO
)
