"""Standardised Supabase L2 read-through cache with LKG fallback.

Adds two layers on top of ``supabase_cache``:

  1. **Stale-while-revalidate** — a stale row is returned immediately
     and the upstream is refreshed in the background, so the user-facing
     request never waits on yfinance / Finnhub / etc.
  2. **Last-known-good (LKG) sidecar** — every successful write also
     persists a ``lkg:{key}`` row with no TTL.  When the primary L2 row
     is missing AND the caller has opted out of synchronous upstream
     fetching (``block_on_miss=False, lkg_fallback=True``), the request
     falls back to the LKG snapshot instead of None / design-time mocks.

The combination lets the request path read exclusively from Supabase
even during a full upstream outage: fresh hit → stale hit → LKG hit →
caller's bounded() fallback.  Upstream API failures stop affecting
rendered pages once an LKG row exists for the key.

L2 reads/writes run on the event loop via the async Supabase client
(gated by ``gate_supabase_async`` for backpressure -- no thread
held).  The ``compute_sync`` callable runs on the heavy pool via
``to_heavy`` because the upstream libraries (yfinance, Finnhub, …)
are sync.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, TypedDict

from filings import supabase_cache
from filings.concurrency import gate_supabase_async, to_heavy

logger = logging.getLogger(__name__)


# ── LKG sidecar key naming ───────────────────────────────────────────


def _lkg_key(key: str) -> str:
    """LKG rows live under a parallel ``lkg:`` namespace so they coexist
    with the primary TTL'd row.  Overwritten on every successful refresh,
    so reading ``lkg:{key}`` always returns the most recent successful
    upstream snapshot."""
    return f"lkg:{key}"


# ── Cache metadata returned alongside data ──────────────────────────


class CacheMeta(TypedDict, total=False):
    """Freshness metadata for L2 reads.

    Returned alongside data via ``l2_cached_with_meta``; discarded by
    the plain ``l2_cached`` wrapper.  Fields:

    * ``source``      — one of "l2_fresh" / "l2_stale" / "lkg" /
                        "compute" / "miss".  Lets callers render
                        provenance badges or log degraded-mode renders.
    * ``as_of_ts``    — ISO-8601 UTC timestamp of when the data was
                        actually fetched from upstream.  None for
                        compute-now paths that haven't written back yet.
    * ``age_seconds`` — wall-clock age, integer seconds.  Negative or
                        zero on fresh writes.
    """
    source:      str
    as_of_ts:    str | None
    age_seconds: int | None


# ── Public API ───────────────────────────────────────────────────────


async def l2_cached(
    key: str,
    ttl_seconds: int,
    compute: Callable[[], Any],
    *,
    category: str,
    block_on_miss: bool = True,
    stale_budget_seconds: int | None = None,
    lkg_fallback: bool = False,
) -> Any:
    """Read-through Supabase L2 cache with SWR + optional LKG fallback.

    Args:
        key:                  Supabase cache key, e.g. ``"redesign:home:cnn_fg"``.
        ttl_seconds:          How long the cached row is considered fresh.
        compute:              Zero-arg sync callable OR async coroutine
                              function.  Async compute paths skip the
                              heavy thread pool entirely.
        category:             Cache row category — kept stable per key
                              family so ops queries can target rows safely.
        block_on_miss:        ``True`` (default) — cold miss synchronously
                              runs ``compute`` on the request path.
                              ``False`` — cold miss returns LKG (if
                              ``lkg_fallback``) or None; ``compute`` is
                              still kicked off in the background so the
                              next caller sees a fresh row.  Request-path
                              callers that have warmer coverage should
                              pass ``False`` to guarantee the request
                              never blocks on upstream.
        stale_budget_seconds: Beyond this age (since last successful
                              upstream fetch), a stale row is treated as
                              missing.  Default ``None`` = serve stale
                              indefinitely.  Set on data that must not
                              be served as live beyond a horizon.
        lkg_fallback:         When ``block_on_miss=False`` and the
                              primary row is missing/exceeded budget,
                              read ``lkg:{key}`` instead of returning
                              None.  No effect when ``block_on_miss=True``.

    Returns:
        Cached, LKG, or freshly-computed payload.  Returns ``None`` only
        when every source failed.

    Behavior matrix (block_on_miss / fresh / stale-in-budget / stale-over-budget / missing):

      | block_on_miss | fresh L2 | stale L2 (in budget) | stale over budget | L2 missing |
      |---------------|----------|----------------------|-------------------|------------|
      | True          | return   | return + bg refresh  | compute + write   | compute + write |
      | False         | return   | return + bg refresh  | LKG / None + bg   | LKG / None + bg |
    """
    data, meta = await _l2_cached_impl(
        key, ttl_seconds, compute,
        category=category, block_on_miss=block_on_miss,
        stale_budget_seconds=stale_budget_seconds, lkg_fallback=lkg_fallback,
    )
    return data


async def l2_cached_with_meta(
    key: str,
    ttl_seconds: int,
    compute: Callable[[], Any],
    *,
    category: str,
    block_on_miss: bool = True,
    stale_budget_seconds: int | None = None,
    lkg_fallback: bool = False,
) -> tuple[Any, CacheMeta]:
    """Sibling of ``l2_cached`` that returns ``(data, meta)``.

    ``meta`` carries freshness info (source, as_of_ts, age_seconds) so
    callers can render a "Cached · 2m ago" badge or log degraded-mode
    renders without an extra Supabase round-trip.
    """
    return await _l2_cached_impl(
        key, ttl_seconds, compute,
        category=category, block_on_miss=block_on_miss,
        stale_budget_seconds=stale_budget_seconds, lkg_fallback=lkg_fallback,
    )


# ── Internals ────────────────────────────────────────────────────────


async def _l2_cached_impl(
    key: str,
    ttl_seconds: int,
    compute: Callable[[], Any],
    *,
    category: str,
    block_on_miss: bool,
    stale_budget_seconds: int | None,
    lkg_fallback: bool,
) -> tuple[Any, CacheMeta]:
    """Core read-through logic shared by ``l2_cached`` and ``l2_cached_with_meta``."""
    row = await _read_l2_row(key)

    if row is not None:
        is_fresh    = row.get("is_fresh", False)
        age_seconds = row.get("age_seconds")
        as_of_ts    = row.get("as_of_ts")
        data        = row.get("data")

        if is_fresh:
            return data, _meta("l2_fresh", as_of_ts, age_seconds)

        # Stale hit — check budget.  Stale-but-within-budget serves
        # immediately + kicks a bg refresh.  Stale-over-budget falls
        # through to the cold-miss path.
        within_budget = (
            stale_budget_seconds is None
            or age_seconds is None
            or age_seconds <= stale_budget_seconds
        )
        if within_budget:
            asyncio.create_task(_refresh(key, ttl_seconds, compute, category))
            return data, _meta("l2_stale", as_of_ts, age_seconds)

    # Cold miss (row absent or exceeded stale budget).
    if block_on_miss:
        try:
            payload = await _run_compute(compute)
        except Exception as exc:
            logger.warning("l2_cached: compute raised for %s: %s", key, exc)
            return None, _meta("miss", None, None)
        if payload:
            asyncio.create_task(_writeback(key, category, payload, ttl_seconds))
        return payload, _meta("compute", None, 0)

    # Non-blocking miss: kick off the compute in the background, return
    # LKG / None to the caller right now so the request path doesn't
    # wait on the upstream.
    asyncio.create_task(_refresh(key, ttl_seconds, compute, category))
    if lkg_fallback:
        lkg = await _read_l2_row(_lkg_key(key))
        if lkg is not None and lkg.get("data") is not None:
            return lkg["data"], _meta("lkg", lkg.get("as_of_ts"), lkg.get("age_seconds"))
    return None, _meta("miss", None, None)


def _meta(source: str, as_of_ts: str | None, age_seconds: int | None) -> CacheMeta:
    return {"source": source, "as_of_ts": as_of_ts, "age_seconds": age_seconds}


async def _read_l2_row(key: str) -> dict | None:
    """Read a row via ``get_cached_full_row_async``, gated by the
    Supabase backpressure semaphore.  Returns the full meta dict or
    ``None`` on miss / read failure."""
    try:
        return await gate_supabase_async(
            supabase_cache.get_cached_full_row_async(key),
        )
    except Exception as exc:
        logger.debug("l2_cached: read failed for %s: %s", key, exc)
        return None


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
    """Background task: write a payload to L2 + LKG.  Errors are swallowed.

    Uses ``allow_drop=True`` so that under Supabase saturation we
    skip the writeback entirely instead of piling up behind already-
    timed-out calls.  Losing an L2 write is recoverable (next request
    will recompute and try again); saturating the default pool with
    background writes is what brings the site down.

    LKG sidecar (``lkg:{key}`` with ``ttl_seconds=None``) is written
    after the primary so a primary write failure doesn't poison the
    LKG snapshot.  Both writes go through the same backpressure gate.
    """
    try:
        await gate_supabase_async(
            supabase_cache.set_cached_async(key, category, payload, ttl_seconds),
            allow_drop=True,
        )
    except Exception as exc:
        logger.debug("l2_cached: writeback failed for %s: %s", key, exc)
        return

    # LKG sidecar — overwrites on every successful refresh, ttl=None so
    # the row never expires.  Category prefix keeps ops/admin queries
    # able to target LKG rows distinctly.
    try:
        await gate_supabase_async(
            supabase_cache.set_cached_async(
                _lkg_key(key), f"lkg_{category}", payload, None,
            ),
            allow_drop=True,
        )
    except Exception as exc:
        logger.debug("l2_cached: LKG writeback failed for %s: %s", key, exc)
