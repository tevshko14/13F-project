"""Supabase L2 read-through cache with SWR + LKG sidecar.

Two layers on top of ``supabase_cache``:

  1. **Stale-while-revalidate** — a stale row is returned immediately
     and the upstream is refreshed in the background.
  2. **Last-known-good (LKG) sidecar** — every successful write also
     persists a ``lkg:{key}`` row with no TTL.  When ``block_on_miss=False
     lkg_fallback=True``, an L2 miss reads the LKG snapshot before
     falling through to None.

Read order: fresh hit → stale hit → LKG hit → None / compute.  Reads/
writes run on the event loop via the async Supabase client (gated by
``gate_supabase_async`` for backpressure).  Sync compute callables
go through ``to_heavy`` for thread-pool budget.

**Single-flight compute** — concurrent callers for the same cold/stale
key coalesce onto one upstream fetch.  Without this, N concurrent users
hitting the same expired key fire N upstream requests AND N writebacks,
amplifying load on a degraded upstream day; with it, all callers share
one in-flight compute.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, TypedDict

from filings import supabase_cache
from filings.concurrency import fire_and_forget, gate_supabase_async, to_heavy

logger = logging.getLogger(__name__)


# Single-flight registry — one entry per in-flight compute keyed by cache
# key.  Cleared in the `finally` of `_compute_singleflight`.  Both the
# blocking-miss path and the background `_refresh` path go through this,
# so a user request and the warmer can't race-fire two computes for the
# same key.
_inflight: dict[str, asyncio.Future[Any]] = {}


# ── LKG sidecar key naming ───────────────────────────────────────────


def lkg_key(key: str) -> str:
    """LKG rows live under a parallel ``lkg:`` namespace so they coexist
    with the primary TTL'd row.  Overwritten on every successful refresh,
    so reading ``lkg:{key}`` always returns the most recent successful
    upstream snapshot."""
    return f"lkg:{key}"


# Back-compat private alias — old call sites used `_lkg_key`.  Will be
# removed once all callers migrate to the public name.
_lkg_key = lkg_key


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


# Sentinel distinguishing "caller didn't pass prefetched_row" from
# "caller passed prefetched_row=None to signal a known cache miss" (the
# warmer uses the latter after a batch read finds the key absent).
_NO_PREFETCH: Any = object()


# ── Public API ───────────────────────────────────────────────────────


async def l2_cached_with_meta(
    key: str,
    ttl_seconds: int,
    compute: Callable[[], Any],
    *,
    category: str,
    block_on_miss: bool = True,
    stale_budget_seconds: int | None = None,
    lkg_fallback: bool = False,
    prefetched_row: dict | None = _NO_PREFETCH,  # type: ignore[assignment]
) -> tuple[Any, CacheMeta]:
    """Read-through Supabase L2 cache with SWR + optional LKG fallback.

    Returns ``(data, meta)``.  ``meta`` carries freshness info
    (source, as_of_ts, age_seconds) so callers can render a "Cached ·
    2m ago" badge or log degraded-mode renders without an extra
    Supabase round-trip.  The thin ``l2_cached(...)`` wrapper drops
    the meta for callers that don't need it.

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
                              next caller sees a fresh row.
        stale_budget_seconds: Beyond this age, a stale row is treated as
                              missing.  Default ``None`` = serve stale
                              indefinitely.
        lkg_fallback:         When ``block_on_miss=False`` and the
                              primary row is missing/exceeded budget,
                              read ``lkg:{key}`` instead of returning
                              None.  No effect when ``block_on_miss=True``.
        prefetched_row:       Optional full-meta row (as returned by
                              ``supabase_cache.get_cached_full_row_async``)
                              from a prior batch read — skips the per-key
                              Supabase round-trip.  Pass an explicit
                              ``None`` to skip the read AND treat as a
                              cache miss (used by the warmer after a
                              batched ``get_cached_full_rows_async``).
                              Omit (default) to read inline as before.

    Behavior matrix:

      | block_on_miss | fresh L2 | stale L2 (in budget) | stale over budget | L2 missing |
      |---------------|----------|----------------------|-------------------|------------|
      | True          | return   | return + bg refresh  | compute + write   | compute + write |
      | False         | return   | return + bg refresh  | LKG / None + bg   | LKG / None + bg |
    """
    return await _l2_cached_impl(
        key, ttl_seconds, compute,
        category=category, block_on_miss=block_on_miss,
        stale_budget_seconds=stale_budget_seconds, lkg_fallback=lkg_fallback,
        prefetched_row=prefetched_row,
    )


async def l2_cached(
    key: str,
    ttl_seconds: int,
    compute: Callable[[], Any],
    *,
    category: str,
    block_on_miss: bool = True,
    stale_budget_seconds: int | None = None,
    lkg_fallback: bool = False,
    prefetched_row: dict | None = _NO_PREFETCH,  # type: ignore[assignment]
) -> Any:
    """Data-only wrapper around ``l2_cached_with_meta`` — same behavior,
    discards the freshness metadata for callers that don't need it."""
    kwargs: dict[str, Any] = {
        "category": category, "block_on_miss": block_on_miss,
        "stale_budget_seconds": stale_budget_seconds, "lkg_fallback": lkg_fallback,
    }
    if prefetched_row is not _NO_PREFETCH:
        kwargs["prefetched_row"] = prefetched_row
    data, _meta = await l2_cached_with_meta(key, ttl_seconds, compute, **kwargs)
    return data


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
    prefetched_row: dict | None = _NO_PREFETCH,  # type: ignore[assignment]
) -> tuple[Any, CacheMeta]:
    """Core read-through logic shared by ``l2_cached`` and ``l2_cached_with_meta``.

    When ``prefetched_row`` is supplied (anything other than the
    ``_NO_PREFETCH`` sentinel), the inline Supabase read is skipped and
    the passed row is used directly — letting the warmer batch-read N
    keys in one query and then dispatch per-key compute without N
    additional reads.
    """
    if prefetched_row is _NO_PREFETCH:
        row = await _read_l2_row(key)
    else:
        row = prefetched_row

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
            fire_and_forget(
                _refresh(key, ttl_seconds, compute, category),
                name=f"l2_refresh:{key}",
            )
            return data, _meta("l2_stale", as_of_ts, age_seconds)

    # Cold miss (row absent or exceeded stale budget).
    if block_on_miss:
        try:
            payload = await _compute_singleflight(key, compute, category, ttl_seconds)
        except Exception as exc:
            logger.warning("l2_cached: compute raised for %s: %s", key, exc)
            return None, _meta("miss", None, None)
        return payload, _meta("compute", None, 0)

    # Non-blocking miss: kick off the compute in the background, return
    # LKG / None to the caller right now so the request path doesn't
    # wait on the upstream.
    fire_and_forget(
        _refresh(key, ttl_seconds, compute, category),
        name=f"l2_refresh:{key}",
    )
    if lkg_fallback:
        lkg = await _read_l2_row(_lkg_key(key))
        if lkg is not None and lkg.get("data") is not None:
            return lkg["data"], _meta("lkg", lkg.get("as_of_ts"), lkg.get("age_seconds"))
    return None, _meta("miss", None, None)


async def _compute_singleflight(
    key: str,
    compute: Callable[[], Any],
    category: str,
    ttl_seconds: int,
) -> Any:
    """Run ``compute`` for ``key`` with single-flight coalescing.

    The first caller for a given key kicks off the compute + writeback
    chain and registers the in-flight Future.  Concurrent callers (user
    requests or the bg refresh task) for the same key await that Future
    instead of starting their own compute — collapsing N upstream calls
    into one, and N writebacks into one.

    On a degraded-upstream day where compute is slow, this is the
    difference between "fan out 10× to a slow yfinance because 10 users
    hit the same expired key" vs "one fetch, nine fast awaits."
    """
    fut = _inflight.get(key)
    if fut is not None and not fut.done():
        return await fut

    loop = asyncio.get_event_loop()
    fut = loop.create_future()
    _inflight[key] = fut
    try:
        payload = await _run_compute(compute)
        # Schedule writeback BEFORE resolving the future so concurrent
        # awaiters know the writeback is queued (preserves the existing
        # "writeback follows successful compute" invariant).  Use
        # fire_and_forget so the task gets a strong reference and can't
        # be GC'd mid-flight under load — plain create_task only weakly
        # tracks pending tasks.
        if payload:
            fire_and_forget(
                _writeback(key, category, payload, ttl_seconds),
                name=f"l2_writeback:{key}",
            )
        fut.set_result(payload)
        return payload
    except Exception as exc:
        fut.set_exception(exc)
        raise
    finally:
        # Always remove from the registry so a transient failure doesn't
        # poison subsequent attempts.
        _inflight.pop(key, None)


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
    """Background task: re-run the upstream fetch and write it back to L2.

    Routes through ``_compute_singleflight`` so a stale-triggered bg
    refresh AND a concurrent cold-miss user request for the same key
    share one upstream fetch + one writeback.  Without this coalescing,
    a stale hit on the warmer's tick + a stale hit on a user request
    fire two computes one tick apart.
    """
    try:
        await _compute_singleflight(key, compute, category, ttl_seconds)
    except Exception as exc:
        logger.debug("l2_cached: bg refresh compute failed for %s: %s", key, exc)


async def _writeback(key: str, category: str, payload: Any, ttl_seconds: int) -> None:
    """Write a payload to L2 + LKG.  Errors swallowed; ``allow_drop=True``
    so Supabase saturation skips the writeback instead of piling up.
    LKG is written second so a primary failure doesn't poison the snapshot."""
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
