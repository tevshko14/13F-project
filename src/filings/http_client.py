"""Process-wide async HTTP client.

Why this module exists
----------------------
Most external-API helpers in the codebase use sync ``httpx.get(...)`` or
``urllib.request``.  Each call opens a fresh TCP/TLS connection and holds
a thread-pool slot for the entire round trip.  An ``httpx.AsyncClient``
with connection pooling avoids both costs:

* Connections are reused across calls (saves ~50-100ms per request after
  the first to a given host).
* Awaiting an async client request yields the event loop, so the worker
  can serve other requests during network I/O — no thread slot held.

Usage
-----
::

    from filings.http_client import get_async_client

    async def fetch_thing():
        client = get_async_client()
        r = await client.get("https://api.example.com/...")
        r.raise_for_status()
        return r.json()

The client is lazily created on first use and lives for the lifetime of
the process.  ``shutdown_async_client()`` is called from the FastAPI
lifespan shutdown to drain in-flight connections cleanly.
"""

from __future__ import annotations

import logging

import httpx

logger = logging.getLogger(__name__)


_BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121 Safari/537.36"
)

# Pool sized for typical Railway worker — enough headroom for a fan-out
# burst from the warmer or a busy /_v2/home request without exhausting
# socket descriptors.
_LIMITS = httpx.Limits(
    max_connections=64,
    max_keepalive_connections=32,
    keepalive_expiry=30.0,
)

_DEFAULT_TIMEOUT = httpx.Timeout(connect=5.0, read=15.0, write=15.0, pool=30.0)


_client: httpx.AsyncClient | None = None


def get_async_client() -> httpx.AsyncClient:
    """Return the shared async HTTP client, creating it on first call.

    Lazy-init race is benign under the GIL: two concurrent first calls
    can each construct a client, but only one assignment wins and the
    orphan is GC'd within the same tick.  Not worth a lock for a one-off
    cost on the very first request.
    """
    global _client
    if _client is None:
        _client = httpx.AsyncClient(
            timeout=_DEFAULT_TIMEOUT,
            limits=_LIMITS,
            headers={"User-Agent": _BROWSER_UA},
            follow_redirects=True,
        )
    return _client


async def shutdown_async_client() -> None:
    """Close the shared client.  Called from FastAPI lifespan shutdown."""
    global _client
    if _client is not None:
        try:
            await _client.aclose()
        except Exception as exc:
            logger.debug("shutdown_async_client: close raised: %s", exc)
        _client = None
