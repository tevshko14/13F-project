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
import collections
import logging
import os
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)


_default_pool: ThreadPoolExecutor | None = None
_heavy_pool: ThreadPoolExecutor | None = None
_heavy_sem: asyncio.Semaphore | None = None
_supabase_sem: asyncio.Semaphore | None = None

# Config values cached at init time so heavy_pool_status() doesn't have
# to read CPython implementation-private attributes off the executors.
_default_workers_cfg = 0
_heavy_workers_cfg = 0
_heavy_concurrency_cfg = 0
_supabase_concurrency_cfg = 0

# Live counter of in-flight `to_light` / `to_supabase` calls,
# *including* coroutines queued waiting on the Supabase semaphore
# (so the watchdog sees queue saturation, not just execution
# saturation -- a bug an earlier draft of this file had).  Mutated
# only from coroutines on the asyncio loop, so no cross-thread
# writes -- the loop's cooperative scheduling makes the +=/-=
# pair effectively atomic for monitoring purposes.  Read by the
# saturation watchdog: when this hovers near the default-pool size
# for sustained intervals, the pool is starved (most likely a
# downstream Supabase slowdown) and the worker should self-recycle
# before the health check starts 503ing.
_to_light_active = 0

# Per-callsite breakdown of thread-holding work currently in the
# default pool.  Diagnostic-only: lets us see WHICH function names
# are holding default-pool slots when the pool saturates.  Excludes
# `gate_supabase_async` (those run on the loop, no thread held).
# Mutated only from coroutines on the asyncio loop -- the loop's
# cooperative scheduling makes the +=/-= pair effectively atomic.
_default_pool_active_by_label: collections.Counter[str] = collections.Counter()


# Defaults sized for a Railway container with ~1-2 vCPU and ~512MB-2GB RAM.
# Each thread carries a 2MB stack (web.py sets it via threading.stack_size)
# so over-sizing the pools translates directly into RSS.
#
# HEAVY_THREADS = 20: bumped from 12 to give headroom against thread-leak
# pressure when an upstream times out.  CPython can't kill threads, so
# each `to_heavy` timeout leaves the underlying thread running until the
# upstream finally returns.  With aggressive yfinance per-request timeouts
# (`_YF_TIMEOUT=6` in market_data.py) threads typically free within ~24s
# even on rate-limited paths; +8 thread headroom (~16MB extra RSS) is
# the cheap belt-and-suspenders.
_DEFAULT_WORKER_THREADS = 16
_DEFAULT_HEAVY_THREADS = 20
_DEFAULT_HEAVY_CONCURRENCY = 8

# Cap concurrent Supabase calls at half the default-pool size so a
# Supabase slowdown can't saturate the pool and starve the rest of
# the request path (health check, static assets, non-Supabase work).
# Treat as a backpressure budget, not a performance ceiling.
_DEFAULT_SUPABASE_CONCURRENCY = 8


# Per-call timeout ceilings.  Sized to be SHORTER than the longest
# expected upstream HTTP timeout so the underlying pool thread reliably
# returns *before* the awaiting coroutine gives up.  This is the
# structural fix for the thread-leak deadlock we observed in prod:
# CPython can't kill threads, so a coroutine timeout that fires before
# the underlying HTTP call returns leaks the pool slot until the call
# eventually completes.  After enough such leaks (HEAVY_THREADS), the
# pool is empty and every new request queues forever -- the worker
# silently deadlocks.  Observed twice (uptime 53min, 62min) before
# this tightening.
#
# Pairing requirements:
#   - Every reachable upstream HTTP timeout must be <= these values.
#     yfinance per-request is capped at 4s in market_data.py so a
#     4-call .info sequence fits inside 15s.  External httpx.get()
#     calls in request-path code are at <= 10s.
#   - Page-handler-level `bounded()` wrappers stay below these
#     (typically 4-12s) so users see fallback data on slow paths
#     before the thread-leak protection layer fires.
#
# Memory note: shorter timeouts mean coroutines + their closures
# release faster -- strictly memory-positive.
_DEFAULT_HEAVY_TIMEOUT = 15.0
_DEFAULT_LIGHT_TIMEOUT = 8.0


def init_pools() -> None:
    """Create the default + heavy pools and the heavy-pool semaphore.

    Called once from the FastAPI lifespan startup.  Idempotent — a
    second call shuts down the previous pools first.

    Reads ``WORKER_THREADS`` / ``HEAVY_THREADS`` / ``HEAVY_CONCURRENCY`` /
    ``SUPABASE_CONCURRENCY`` from the environment if set.
    """
    global _default_pool, _heavy_pool, _heavy_sem, _supabase_sem
    global _default_workers_cfg, _heavy_workers_cfg, _heavy_concurrency_cfg
    global _supabase_concurrency_cfg

    if _default_pool is not None:
        shutdown_pools()

    _default_workers_cfg = int(os.environ.get("WORKER_THREADS", _DEFAULT_WORKER_THREADS))
    _heavy_workers_cfg = int(os.environ.get("HEAVY_THREADS", _DEFAULT_HEAVY_THREADS))
    _heavy_concurrency_cfg = int(os.environ.get("HEAVY_CONCURRENCY", _DEFAULT_HEAVY_CONCURRENCY))
    _supabase_concurrency_cfg = int(os.environ.get("SUPABASE_CONCURRENCY", _DEFAULT_SUPABASE_CONCURRENCY))

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
    _supabase_sem = asyncio.Semaphore(_supabase_concurrency_cfg)

    logger.info(
        "concurrency: pools initialised — default=%d heavy=%d heavy_sem=%d supabase_sem=%d",
        _default_workers_cfg, _heavy_workers_cfg,
        _heavy_concurrency_cfg, _supabase_concurrency_cfg,
    )


def shutdown_pools() -> None:
    """Tear down the pools.  Called from FastAPI lifespan shutdown.

    Resets the loop's default executor so subsequent ``asyncio.to_thread``
    calls don't queue against a dead pool (matters for tests + reloads).
    """
    global _default_pool, _heavy_pool, _heavy_sem, _supabase_sem
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
        except TypeError:
            # Python 3.12 rejects None here; harmless since the process
            # is shutting down anyway.  The pool itself is already
            # closed via shutdown(wait=False) above.
            pass
    _heavy_sem = None
    _supabase_sem = None
    # Drop per-source breakers/sems too -- they recreate lazily on the
    # next call.  Matters for test reloads where lingering circuits
    # could mask post-restart upstream issues.
    _upstream_sems.clear()
    _upstream_failures.clear()
    _upstream_open_until.clear()


def _fn_label(fn) -> str:
    """``module.qualname`` for log lines; falls back gracefully."""
    return f"{getattr(fn, '__module__', '?')}.{getattr(fn, '__qualname__', repr(fn))}"


# ── Per-upstream concurrency + circuit breaker ────────────────────────
#
# Each external API (ApeWisdom, Finnhub, FRED, SEC, yfinance, …) gets
# its own concurrency budget + failure tracker.  Why: ``to_heavy`` only
# has a single shared semaphore (8 slots).  If ApeWisdom slows to a
# crawl, the 8 slots fill with stuck ApeWisdom calls and every Finnhub
# / SEC / yfinance call queues behind them.  One slow upstream =
# everyone slow.  Per-source budgets make the budgets independent: a
# stuck ApeWisdom can only consume its own 2 slots; Finnhub etc. keep
# their own.
#
# Circuit breaker: rolling failure window per source.  If `threshold`
# failures land within `window_s`, the circuit opens for `open_for_s`
# -- subsequent calls return None immediately without touching the
# upstream.  Restores the request-path to a fast path while the
# upstream recovers.  Bundle-section fallback handles the None for
# the user (graceful degradation).
#
# Sizes chosen so the sum of slots stays under heavy_pool capacity
# (20) on the happy path; in practice not all sources max simultaneously.

_UPSTREAM_CFG: dict[str, dict] = {
    "apewisdom":     {"sem": 2, "threshold": 3, "window_s": 60, "open_for_s": 300},
    "finnhub":       {"sem": 4, "threshold": 5, "window_s": 60, "open_for_s": 180},
    "fred":          {"sem": 2, "threshold": 3, "window_s": 60, "open_for_s": 600},
    "yfinance":      {"sem": 4, "threshold": 5, "window_s": 60, "open_for_s": 180},
    "sec_edgar":     {"sem": 3, "threshold": 3, "window_s": 60, "open_for_s": 300},
    "tiingo":        {"sem": 4, "threshold": 5, "window_s": 60, "open_for_s": 180},
    "cboe":          {"sem": 2, "threshold": 3, "window_s": 60, "open_for_s": 300},
    "google_trends": {"sem": 2, "threshold": 3, "window_s": 60, "open_for_s": 300},
}
_DEFAULT_UPSTREAM_CFG = {"sem": 3, "threshold": 3, "window_s": 60, "open_for_s": 180}

_upstream_sems: dict[str, asyncio.Semaphore] = {}
_upstream_failures: dict[str, list[float]] = {}
_upstream_open_until: dict[str, float] = {}


def _upstream_cfg(name: str) -> dict:
    return _UPSTREAM_CFG.get(name, _DEFAULT_UPSTREAM_CFG)


def _ensure_upstream_sem(name: str) -> asyncio.Semaphore:
    sem = _upstream_sems.get(name)
    if sem is None:
        sem = asyncio.Semaphore(_upstream_cfg(name)["sem"])
        _upstream_sems[name] = sem
    return sem


def is_circuit_open(name: str) -> bool:
    """True when the circuit breaker for ``name`` is currently open."""
    import time as _time
    return _upstream_open_until.get(name, 0) > _time.time()


def _record_upstream_failure(name: str) -> None:
    """Record a failure; open the circuit if threshold reached in window."""
    import time as _time
    cfg = _upstream_cfg(name)
    now = _time.time()
    failures = _upstream_failures.setdefault(name, [])
    failures.append(now)
    # Drop entries outside the rolling window.
    cutoff = now - cfg["window_s"]
    while failures and failures[0] < cutoff:
        failures.pop(0)
    if len(failures) >= cfg["threshold"]:
        _upstream_open_until[name] = now + cfg["open_for_s"]
        logger.warning(
            "upstream %s: circuit OPEN for %ds after %d failures in %ds",
            name, cfg["open_for_s"], len(failures), cfg["window_s"],
        )
        failures.clear()  # avoid double-trigger right after reopen


def _record_upstream_success(name: str) -> None:
    """Reset the failure counter; a success closes the rolling window."""
    if name in _upstream_failures:
        _upstream_failures[name].clear()


async def to_upstream(
    source: str, fn, *args, timeout: float = _DEFAULT_HEAVY_TIMEOUT,
):
    """Run a slow external-API call gated by a per-source semaphore +
    a per-source circuit breaker.

    Three layers of protection:
      1. **Per-source semaphore** -- bounded concurrent calls to this
         source (e.g. 2 for ApeWisdom, 4 for Finnhub).  A slow source
         can't starve calls to other sources.
      2. **Circuit breaker** -- after N timeouts in a rolling window,
         calls return None immediately without touching the upstream.
         Page handlers see a fast "no data" instead of a 15s wait.
      3. **Per-call timeout** -- existing ``asyncio.wait_for`` ceiling
         (same shape as ``to_heavy``).

    Returns the call's result, or ``None`` if the circuit is open.
    Bundle-section ``_bounded`` fallbacks handle the None gracefully.

    Use this for every slow external API.  Generic ``to_heavy`` stays
    for sync work that isn't a single upstream (CPU-bound transforms,
    in-process aggregation, etc.) -- those don't need the breaker.
    """
    if is_circuit_open(source):
        logger.debug(
            "upstream %s: circuit open, returning None for %s",
            source, _fn_label(fn),
        )
        return None
    sem = _ensure_upstream_sem(source)
    async with sem:
        loop = asyncio.get_running_loop()
        pool = _heavy_pool  # may be None pre-init -- fall through to to_thread
        fut = (
            loop.run_in_executor(pool, fn, *args)
            if pool is not None else asyncio.to_thread(fn, *args)
        )
        try:
            result = await asyncio.wait_for(fut, timeout=timeout)
            _record_upstream_success(source)
            return result
        except asyncio.TimeoutError:
            _record_upstream_failure(source)
            logger.warning(
                "upstream %s: timeout after %.1fs in %s",
                source, timeout, _fn_label(fn),
            )
            raise
        except Exception:
            _record_upstream_failure(source)
            raise


def upstream_status() -> dict:
    """Snapshot of per-source circuit-breaker state for diagnostics."""
    import time as _time
    now = _time.time()
    out: dict = {}
    for name, cfg in _UPSTREAM_CFG.items():
        open_until = _upstream_open_until.get(name, 0)
        out[name] = {
            "sem_size":        cfg["sem"],
            "circuit_open":    open_until > now,
            "circuit_reopens_in": max(0, int(open_until - now)),
            "recent_failures": len(_upstream_failures.get(name, [])),
            "threshold":       cfg["threshold"],
            "window_s":        cfg["window_s"],
        }
    return out


async def to_heavy(fn, *args, timeout: float = _DEFAULT_HEAVY_TIMEOUT):
    """Run a slow blocking call on the heavy pool, gated by the semaphore.

    Use for yfinance / Finnhub / SEC EDGAR / ApeWisdom / FRED — anything
    where a single round trip can take 5-30s.  The semaphore is the actual
    fan-out protection: even if one handler fires 50 ``to_heavy`` calls,
    only N run at once.

    Bounded by ``timeout`` (default ``_DEFAULT_HEAVY_TIMEOUT``) so a
    hung upstream can't pin the coroutine forever.  On timeout we raise
    ``asyncio.TimeoutError``; the underlying thread is leaked (CPython
    can't kill threads) but the semaphore + closures held by the
    awaiting coroutine are released.  Pass ``timeout=N`` for paths
    that legitimately take longer (e.g. bulk SEC sync).

    Falls back to ``asyncio.to_thread`` when pools aren't initialised so
    unit tests + ad-hoc scripts work without lifespan setup.
    """
    pool = _heavy_pool
    sem = _heavy_sem
    if pool is None or sem is None:
        try:
            return await asyncio.wait_for(asyncio.to_thread(fn, *args), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning(
                "to_heavy: upstream timeout after %.1fs in %s (no pool)",
                timeout, _fn_label(fn),
            )
            raise
    async with sem:
        loop = asyncio.get_running_loop()
        fut = loop.run_in_executor(pool, fn, *args)
        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning(
                "to_heavy: upstream timeout after %.1fs in %s -- "
                "thread slot leaked, semaphore released",
                timeout, _fn_label(fn),
            )
            raise


async def to_light(fn, *args, timeout: float = _DEFAULT_LIGHT_TIMEOUT):
    """Run a fast blocking call on the default pool — no semaphore gate.

    Use for in-memory cache lookups that need a thread for thread-safe
    library reasons, small JSON parses.  These don't deserve a
    heavy-pool slot and shouldn't throttle other heavy work, so they
    bypass the heavy semaphore.

    For Supabase calls specifically, use ``to_supabase`` instead --
    that variant gates concurrency so a Supabase slowdown can't
    saturate the entire default pool.

    Bounded by ``timeout`` (default ``_DEFAULT_LIGHT_TIMEOUT``).  Same
    semantics as ``to_heavy`` on timeout: the thread leaks, the
    coroutine releases.

    Tracks ``_to_light_active`` so the saturation watchdog can spot
    pool starvation in real time.
    """
    label = _fn_label(fn)
    global _to_light_active
    _to_light_active += 1
    _default_pool_active_by_label[label] += 1
    try:
        return await asyncio.wait_for(asyncio.to_thread(fn, *args), timeout=timeout)
    except asyncio.TimeoutError:
        logger.warning(
            "to_light: upstream timeout after %.1fs in %s",
            timeout, label,
        )
        raise
    finally:
        _to_light_active -= 1
        if _default_pool_active_by_label[label] <= 1:
            del _default_pool_active_by_label[label]
        else:
            _default_pool_active_by_label[label] -= 1


async def _run_under_supabase_gate(
    awaitable,
    *,
    timeout: float,
    allow_drop: bool,
    label: str,
    holds_thread: bool,
):
    """Shared body for ``to_supabase`` (sync fn) and ``gate_supabase_async``
    (already-async coro).

    Both wrappers do identical semaphore + active-counter + drop
    bookkeeping; the only difference is what the awaitable was built
    from.  Keeping the bookkeeping in one place means a future
    watchdog/metric tweak only has to land here.

    The counter increments BEFORE acquiring the semaphore so coroutines
    queued waiting on a saturated semaphore are visible to the watchdog
    -- otherwise it would see only the ~8 executing slots and miss the
    case where 30+ coroutines are piled up behind them.

    ``holds_thread`` is True for the sync path (``to_supabase``) so
    we record it in the per-label diagnostic counter; False for the
    async path (``gate_supabase_async``) since those run on the loop
    and don't hold a default-pool thread -- they shouldn't show up
    when we're diagnosing pool saturation.
    """
    sem = _supabase_sem
    if sem is None:
        return await asyncio.wait_for(awaitable, timeout=timeout)

    if allow_drop and sem.locked():
        # Dispose of the awaitable so the runtime doesn't warn about
        # a never-awaited coroutine.  No-op for non-coroutine types.
        if hasattr(awaitable, "close"):
            awaitable.close()
        logger.debug("supabase: dropping %s (semaphore saturated)", label)
        return None

    global _to_light_active
    _to_light_active += 1
    if holds_thread:
        _default_pool_active_by_label[label] += 1
    try:
        async with sem:
            try:
                return await asyncio.wait_for(awaitable, timeout=timeout)
            except asyncio.TimeoutError:
                logger.warning(
                    "supabase: upstream timeout after %.1fs in %s",
                    timeout, label,
                )
                raise
    finally:
        _to_light_active -= 1
        if holds_thread:
            if _default_pool_active_by_label[label] <= 1:
                del _default_pool_active_by_label[label]
            else:
                _default_pool_active_by_label[label] -= 1


async def to_supabase(
    fn,
    *args,
    timeout: float = _DEFAULT_LIGHT_TIMEOUT,
    allow_drop: bool = False,
):
    """Run a sync Supabase blocking call on the default pool, gated by
    the Supabase semaphore so a slow Supabase can't saturate the pool.

    For already-async callables (e.g. ``supabase_cache.X_async``) use
    ``gate_supabase_async`` instead -- it skips the threadpool entirely.

    Args:
        fn:         Sync Supabase callable (typically from ``supabase_cache``).
        timeout:    Per-call timeout ceiling.
        allow_drop: Return ``None`` immediately without queueing if the
                    semaphore is full.  For fire-and-forget writebacks.
    """
    return await _run_under_supabase_gate(
        asyncio.to_thread(fn, *args),
        timeout=timeout,
        allow_drop=allow_drop,
        label=_fn_label(fn),
        holds_thread=True,
    )


def heavy_pool_status() -> dict:
    """Expose pool state for /health or debug endpoints."""
    if _default_pool is None:
        return {"initialised": False}
    return {
        "initialised": True,
        "default_workers": _default_workers_cfg,
        "default_active": _to_light_active,
        "heavy_workers": _heavy_workers_cfg,
        "heavy_semaphore_size": _heavy_concurrency_cfg,
        "supabase_semaphore_size": _supabase_concurrency_cfg,
        "supabase_saturated": is_supabase_saturated(),
    }


def default_pool_thread_traces(max_frames_per_thread: int = 3) -> list[dict]:
    """Stack-trace each default-pool worker so we can see WHAT it's doing
    without requiring callers to go through our wrappers.

    Catches the bulk of pool saturators we can't see otherwise -- 163
    raw ``asyncio.to_thread`` callsites + library code + framework
    internals all dispatch through the default pool but skip our
    counter.  ``sys._current_frames()`` gives us each live thread's
    current frame; we walk up through callers (skipping the
    concurrent.futures worker plumbing) and capture the top user/lib
    frames.  An idle pool worker blocks in ``queue.get``; after
    skipping plumbing its trace is empty -- that's how we distinguish
    busy from idle.

    Cost: ~10ms for 16-32 threads, only called on saturation events
    or admin polling -- never on the request hot path.
    """
    import sys
    import threading as _threading

    frames = sys._current_frames()
    threads_by_id = {t.ident: t for t in _threading.enumerate()}

    # Skip the outer worker plumbing so the trace shows the caller's
    # actual work, not concurrent.futures._worker -> queue.get bookkeeping.
    skip_modules = (
        "concurrent.futures",
        "queue",
        "threading",
        "_thread",
    )

    traces: list[dict] = []
    for tid, frame in frames.items():
        thread = threads_by_id.get(tid)
        if thread is None:
            continue
        # Default ThreadPoolExecutor names threads `<prefix>_<n>`.
        if not thread.name.startswith("default_"):
            continue

        chain: list[str] = []
        f = frame
        # Walk from deepest (currently executing) up through callers.
        while f is not None and len(chain) < max_frames_per_thread:
            mod = f.f_globals.get("__name__", "?")
            if any(mod.startswith(s) for s in skip_modules):
                f = f.f_back
                continue
            try:
                qual = f.f_code.co_qualname  # 3.11+
            except AttributeError:
                qual = f.f_code.co_name
            chain.append(f"{mod}.{qual}:{f.f_lineno}")
            f = f.f_back

        traces.append({
            "thread": thread.name,
            "busy": bool(chain),  # empty chain = idle worker waiting on queue.get
            "trace": chain,       # deepest frame first
        })

    # Stable order by thread name so logs are diff-friendly.
    traces.sort(key=lambda t: t["thread"])
    return traces


def default_pool_diagnostic(top_n: int = 5) -> dict:
    """Snapshot of default-pool work for diagnosing saturation events.

    Includes:
      * ``tracked_active`` -- sum of in-flight `to_light` + sync
        `to_supabase` calls (work we know is holding pool threads)
      * ``top_callers`` -- top-N callsites by current in-flight count
      * ``queue_depth`` -- best-effort read of the executor's pending
        work queue (private API; falls back to None)
      * ``thread_traces`` -- stack trace for each default-pool worker
        so saturators that bypass our wrappers are visible too
      * ``busy_threads`` / ``idle_threads`` -- derived from traces so
        we can spot the gap: if `busy_threads > tracked_active`, the
        difference is untracked work (raw asyncio.to_thread, libs,
        framework internals)

    Called from saturation-detection paths (health-check 503,
    watchdog SIGTERM) so the next event lands an actionable trace
    in the logs instead of just "pool starved -- bye".
    """
    queue_depth = None
    if _default_pool is not None:
        try:
            queue_depth = _default_pool._work_queue.qsize()
        except Exception:
            queue_depth = None

    tracked = sum(_default_pool_active_by_label.values())
    traces = default_pool_thread_traces()
    busy = sum(1 for t in traces if t["busy"])

    return {
        "tracked_active": tracked,
        "max_workers": _default_workers_cfg,
        "queue_depth": queue_depth,
        "to_light_active_total": _to_light_active,
        "busy_threads": busy,
        "idle_threads": len(traces) - busy,
        "untracked_estimate": max(0, busy - tracked),
        "top_callers": _default_pool_active_by_label.most_common(top_n),
        "thread_traces": traces,
    }


async def gate_supabase_async(
    coro,
    *,
    timeout: float = _DEFAULT_LIGHT_TIMEOUT,
    allow_drop: bool = False,
):
    """Run an async Supabase coroutine under the Supabase semaphore.

    Async equivalent of ``to_supabase`` for callers that already have
    an awaitable (typically a ``X_async`` helper from ``supabase_cache``).
    Skips the threadpool entirely so a Supabase slowdown can't hold
    default-pool slots at all.
    """
    return await _run_under_supabase_gate(
        coro, timeout=timeout, allow_drop=allow_drop,
        label="async", holds_thread=False,
    )


def is_heavy_saturated() -> bool:
    """True when the heavy-pool semaphore has zero free slots.

    Used by request handlers to apply backpressure -- if the pool is
    already full, queueing additional cold-path work means an
    indefinitely-long wait for the user (and continued upstream load
    while we're being crawler-spammed).  Better to short-circuit and
    serve stale or 503 immediately.
    """
    if _heavy_sem is None:
        return False
    return _heavy_sem.locked()


def is_supabase_saturated() -> bool:
    """True when the Supabase semaphore has zero free slots.

    Mirrors ``is_heavy_saturated`` for the Supabase gate.  Useful for
    callers that want to short-circuit a Supabase read entirely (e.g.
    skip the L1 -> L2 fall-through and serve the in-memory layer only)
    when the pool is under pressure.
    """
    if _supabase_sem is None:
        return False
    return _supabase_sem.locked()


def to_light_active() -> int:
    """Number of in-flight ``to_light`` / ``to_supabase`` calls.

    Used by the default-pool saturation watchdog -- when this hovers
    near ``_default_workers_cfg`` for sustained intervals, the pool is
    starved and the worker should self-recycle.  Read-only int, safe
    to call from any thread.
    """
    return _to_light_active


def default_pool_capacity() -> int:
    """Default-pool max worker count, or 0 if not initialised."""
    return _default_workers_cfg
