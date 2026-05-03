"""Standardised Supabase L2 read-through cache wrapper.

The async-friendly companion to ``supabase_cache.fetch_with_cache``.
Adds **stale-while-revalidate**: a stale row is returned to the caller
immediately and a background task refreshes it, so the user-facing
request never waits on the upstream API.

Routing
-------
* L2 reads + writes run on the *default* thread pool via ``to_light`` —
  Supabase round trips are 10-100ms network ops and don't deserve a
  heavy-pool slot.
* The ``compute_sync`` callable runs on the *heavy* pool via ``to_heavy``
  because that's where slow upstream APIs live (yfinance, Finnhub, …).
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable

from filings import supabase_cache
from filings.concurrency import to_heavy, to_light

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
        cached, is_fresh = await to_light(supabase_cache.get_cached_with_stale, key)
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
    """Background task: write a payload to L2.  Errors are swallowed."""
    try:
        await to_light(supabase_cache.set_cached, key, category, payload, ttl_seconds)
    except Exception as exc:
        logger.debug("l2_cached: writeback failed for %s: %s", key, exc)
