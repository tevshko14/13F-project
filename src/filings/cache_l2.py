"""Standardised Supabase L2 read-through cache wrapper.

The async-friendly companion to ``supabase_cache.fetch_with_cache``.
Adds **stale-while-revalidate**: a stale row is returned to the caller
immediately and a background task refreshes it, so the user-facing
request never waits on the upstream API.

Routing
-------
* L2 reads + writes run on the *default* thread pool via ``to_supabase``
  — Supabase round trips are 10-100ms network ops and don't deserve a
  heavy-pool slot.  Gated by the Supabase semaphore so a Supabase
  slowdown can't saturate the entire default pool (root cause of the
  2026-05-10 outage).
* The ``compute_sync`` callable runs on the *heavy* pool via ``to_heavy``
  because that's where slow upstream APIs live (yfinance, Finnhub, …).
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable

from filings import supabase_cache
from filings.concurrency import to_heavy, to_supabase

logger = logging.getLogger(__name__)


async def l2_cached(
    key: str,
    ttl_seconds: int,
    compute: Callable[[], Any],
    *,
    category: str,
) -> Any:
    """Read-through Supabase L2 cache with stale-while-revalidate.

    Args:
        key:          Supabase cache key, e.g. ``"redesign:home:cnn_fg"``.
        ttl_seconds:  How long the cached row is considered fresh.
        compute:      Either a zero-arg sync callable OR an async
                      coroutine function.  Async compute paths skip the
                      heavy thread pool entirely — async httpx clients
                      yield the event loop during network I/O instead of
                      holding a thread slot.
        category:     Cache row category — kept stable per key family
                      so ops queries can target rows safely.

    Returns:
        Cached or freshly-computed payload.  Returns ``None`` only when
        both L2 and the upstream fail.
    """
    try:
        result = await to_supabase(supabase_cache.get_cached_with_stale, key)
        # `allow_drop` defaults to False for reads, so result is always
        # the (cached, is_fresh) tuple unless the call raised.
        cached, is_fresh = result if result is not None else (None, False)
    except Exception as exc:
        logger.debug("l2_cached: read failed for %s: %s", key, exc)
        cached, is_fresh = None, False

    if cached is not None and is_fresh:
        return cached

    if cached is not None:
        # Stale hit — return it immediately, refresh in the background so
        # the next caller gets fresh data without waiting.
        asyncio.create_task(_refresh(key, ttl_seconds, compute, category))
        return cached

    # Cold miss — compute, then fire-and-forget the writeback so the
    # caller doesn't pay the L2 set round trip.
    try:
        payload = await _run_compute(compute)
    except Exception as exc:
        logger.warning("l2_cached: compute raised for %s: %s", key, exc)
        return None
    if payload:
        asyncio.create_task(_writeback(key, category, payload, ttl_seconds))
    return payload


async def _run_compute(compute: Callable[[], Any]) -> Any:
    """Dispatch to async-await or heavy-pool execution based on shape.

    Async coroutine functions are awaited directly so they can yield the
    event loop during network I/O (no thread-pool slot held).  Sync
    callables go through ``to_heavy`` so they participate in the global
    semaphore budget.
    """
    if asyncio.iscoroutinefunction(compute):
        return await compute()
    return await to_heavy(compute)


async def _refresh(
    key: str, ttl_seconds: int, compute: Callable[[], Any], category: str,
) -> None:
    """Background task: re-run the upstream fetch and write it back to L2."""
    try:
        payload = await _run_compute(compute)
    except Exception as exc:
        logger.debug("l2_cached: bg refresh compute failed for %s: %s", key, exc)
        return
    if payload:
        await _writeback(key, category, payload, ttl_seconds)


async def _writeback(key: str, category: str, payload: Any, ttl_seconds: int) -> None:
    """Background task: write a payload to L2.  Errors are swallowed.

    Uses ``allow_drop=True`` so that under Supabase saturation we
    skip the writeback entirely instead of piling up behind already-
    timed-out calls.  Losing an L2 write is recoverable (next request
    will recompute and try again); saturating the default pool with
    background writes is what brings the site down.
    """
    try:
        await to_supabase(
            supabase_cache.set_cached,
            key, category, payload, ttl_seconds,
            allow_drop=True,
        )
    except Exception as exc:
        logger.debug("l2_cached: writeback failed for %s: %s", key, exc)
