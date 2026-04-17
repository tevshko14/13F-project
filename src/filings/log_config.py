"""Shared logging setup for CLI workers and sync jobs.

Replaces the duplicated ``_setup_logging()`` function in 7+ worker modules:
  * options_sync.py
  * insider_sync.py
  * short_interest_sync.py
  * sync_worker.py
  * digest_worker.py
  * youtube_sync.py
  * migrate_cold_storage.py

Each had a near-identical 10-line Railway-JSON-vs-local-text format switch
with slightly different lists of quiet libraries.  This module unifies it.

Note: ``web.py`` keeps its own ``_setup_logging()`` for now because it uses
em-dash separators in the local-dev format (pre-existing log-parser contract).
"""

from __future__ import annotations

import logging
import os

# Libraries that are noisy at INFO but useful at WARNING+.
# Individual workers can extend via the ``extra_quiet`` kwarg.
_DEFAULT_QUIET_LOGGERS = (
    "httpx",
    "httpcore",
    "yfinance",
    "peewee",
    "urllib3",
)


def setup_worker_logging(*extra_quiet: str) -> None:
    """Configure JSON logs on Railway, plain text locally.

    Args:
        *extra_quiet: Additional logger names to silence to WARNING.
                      The default set already covers httpx, httpcore,
                      yfinance, peewee, and urllib3.

    Example:
        >>> from filings.log_config import setup_worker_logging
        >>> setup_worker_logging()                    # use defaults
        >>> setup_worker_logging("asyncio", "aiohttp") # add more
    """
    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    if os.environ.get("RAILWAY_ENVIRONMENT"):
        fmt = '{"time":"%(asctime)s","level":"%(levelname)s","name":"%(name)s","msg":"%(message)s"}'
    else:
        fmt = "%(asctime)s %(levelname)-8s %(name)s -- %(message)s"
    logging.basicConfig(level=log_level, format=fmt, force=True)
    for name in _DEFAULT_QUIET_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)
    for name in extra_quiet:
        logging.getLogger(name).setLevel(logging.WARNING)
