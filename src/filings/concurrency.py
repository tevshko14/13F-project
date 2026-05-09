"""Process-wide thread-pool + semaphore management for blocking I/O work.

Why this module exists
----------------------
FastAPI is async-first, but most of our data-access libraries are sync
(yfinance, supabase-py, requests, sync-mode httpx).  Wrapping each call
in ``asyncio.to_thread()`` works, but every active call holds a thread
slot until the network round-trip completes.  On a small Railway
container the default thread pool is ~8-32 slots — easy to drown when a
single page handler fans out a dozen blocking calls in parallel.

Two pools, two helpers
----------------------
* ``to_heavy(fn, *args)`` — slow upstream APIs (yfinance, Finnhub, SEC
  EDGAR, ApeWisdom, FRED).  Runs on the heavy pool, gated by a global
  semaphore so a single fan-out request can't drown the worker.
* ``to_light(fn, *args)`` — fast bounded ops (Supabase reads/writes,
  cache hits, small JSON parses).  Runs on the default pool with **no
  semaphore** — these aren't the calls we need to throttle.

Routers / non-web modules call these instead of ``asyncio.to_thread(...)``
so they automatically participate in the right budget.  The module is
dependency-light (stdlib only) so it can be imported anywhere without
circular-import risk.
"""

from __future__ import annotations

import asyncio
import logging
import os
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)


_default_pool: ThreadPoolExecutor | None = None
_heavy_pool: ThreadPoolExecutor | None = None
_heavy_sem: asyncio.Semaphore | None = None

# Config values cached at init time so heavy_pool_status() doesn't have
# to read CPython implementation-private attributes off the executors.
_default_workers_cfg = 0
_heavy_workers_cfg = 0
_heavy_concurrency_cfg = 0


# Defaults sized for a Railway container with ~1-2 vCPU and ~512MB-2GB RAM.
# Each thread carries a stack (web.py sets it to 2MB) so over-sizing the
# pools translates directly into RSS.  16 / 12 / 8 is plenty for the
# fan-out patterns observed in production; bump via env vars if any
# specific page handler queues consistently on the default pool.
_DEFAULT_WORKER_THREADS = 16
_DEFAULT_HEAVY_THREADS = 12
_DEFAULT_HEAVY_CONCURRENCY = 8


def init_pools() -> None:
    """Create the default + heavy pools and the heavy-pool semaphore.

    Called once from the FastAPI lifespan startup.  Idempotent — a
    second call shuts down the previous pools first.

    Reads ``WORKER_THREADS`` / ``HEAVY_THREADS`` / ``HEAVY_CONCURRENCY``
    from the environment if set.
    """
    global _default_pool, _heavy_pool, _heavy_sem
    global _default_workers_cfg, _heavy_workers_cfg, _heavy_concurrency_cfg

    if _default_pool is not None:
        shutdown_pools()

    _default_workers_cfg = int(os.environ.get("WORKER_THREADS", _DEFAULT_WORKER_THREADS))
    _heavy_workers_cfg = int(os.environ.get("HEAVY_THREADS", _DEFAULT_HEAVY_THREADS))
    _heavy_concurrency_cfg = int(os.environ.get("HEAVY_CONCURRENCY", _DEFAULT_HEAVY_CONCURRENCY))

    _default_pool = ThreadPoolExecutor(
        max_workers=_default_workers_cfg,
        thread_name_prefix="default",
    )
    asyncio.get_running_loop().set_default_executor(_default_pool)

    _heavy_pool = ThreadPoolExecutor(
        max_workers=_heavy_workers_cfg,
        thread_name_prefix="heavy",
    )
    _heavy_sem = asyncio.Semaphore(_heavy_concurrency_cfg)

    logger.info(
        "concurrency: pools initialised — default=%d heavy=%d sem=%d",
        _default_workers_cfg, _heavy_workers_cfg, _heavy_concurrency_cfg,
    )


def shutdown_pools() -> None:
    """Tear down the pools.  Called from FastAPI lifespan shutdown.

    Resets the loop's default executor so subsequent ``asyncio.to_thread``
    calls don't queue against a dead pool (matters for tests + reloads).
    """
    global _default_pool, _heavy_pool, _heavy_sem
    if _heavy_pool is not None:
        _heavy_pool.shutdown(wait=False)
        _heavy_pool = None
    if _default_pool is not None:
        _default_pool.shutdown(wait=False)
        _default_pool = None
        try:
            asyncio.get_running_loop().set_default_executor(None)
        except RuntimeError:
            pass  # No running loop (e.g. reloader teardown).
    _heavy_sem = None


async def to_heavy(fn, *args):
    """Run a slow blocking call on the heavy pool, gated by the semaphore.

    Use for yfinance / Finnhub / SEC EDGAR / ApeWisdom / FRED — anything
    where a single round trip can take 5-30s.  The semaphore is the actual
    fan-out protection: even if one handler fires 50 ``to_heavy`` calls,
    only N run at once.

    Falls back to ``asyncio.to_thread`` when pools aren't initialised so
    unit tests + ad-hoc scripts work without lifespan setup.
    """
    pool = _heavy_pool
    sem = _heavy_sem
    if pool is None or sem is None:
        return await asyncio.to_thread(fn, *args)
    async with sem:
        return await asyncio.get_running_loop().run_in_executor(pool, fn, *args)


async def to_light(fn, *args):
    """Run a fast blocking call on the default pool — no semaphore gate.

    Use for Supabase reads/writes, in-memory cache lookups that need a
    thread for thread-safe library reasons, small JSON parses.  These
    don't deserve a heavy-pool slot and shouldn't throttle other heavy
    work, so they bypass the semaphore.
    """
    return await asyncio.to_thread(fn, *args)


def heavy_pool_status() -> dict:
    """Expose pool state for /health or debug endpoints."""
    if _default_pool is None:
        return {"initialised": False}
    return {
        "initialised": True,
        "default_workers": _default_workers_cfg,
        "heavy_workers": _heavy_workers_cfg,
        "heavy_semaphore_size": _heavy_concurrency_cfg,
    }
