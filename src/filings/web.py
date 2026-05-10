"""13F Filing Viewer — FastAPI web application.

Production-ready with: security headers, request logging, exception
handlers, rate limiting, health check, structured logging, Sentry,
and Supabase-backed persistent cache.
"""

import os as _os

# ── EDGAR rate limit (must be set before edgartools is imported) ──────
# Default is 9 req/sec; 5 is conservative and avoids 429 errors.
_os.environ.setdefault("EDGAR_RATE_LIMIT_PER_SEC", "5")

# ── Smaller thread stacks (must run before any thread is spawned) ──
# Default is 8MB per thread on Linux glibc.  With ~70-100 worker +
# ad-hoc threads in flight, that's ~600-800MB of RSS just in stacks.
# Most of our threads do shallow I/O wait (httpx, supabase, yfinance);
# 2MB is plenty.  Saves ~70 * 6MB ≈ 420MB at steady state.
import threading as _threading
try:
    _threading.stack_size(2 * 1024 * 1024)  # 2 MB
except (ValueError, RuntimeError) as _exc:
    # Some platforms reject sub-default stack sizes; fall through with
    # the platform default rather than failing startup.
    pass

import asyncio
from concurrent.futures import ThreadPoolExecutor
import functools
import json as json_module
import logging
import os
import re as _re
import time as time_module
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from pathlib import Path

import uvicorn
from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.responses import (
    HTMLResponse,
    JSONResponse,
    PlainTextResponse,
    RedirectResponse,
    Response,
)
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.gzip import GZipMiddleware
import httpx

from filings import (
    client,
    cache,
    analysts,
    market_data,
    notifications,
    sentiment,
    vitals,
    company_filings,
    insider_trading,
    insider_insights,
    congress_trading,
    supabase_cache,
    auth,
    youtube,
    aum_data,
    web_traffic,
    google_trends,
)
from filings.app_state import (
    real_ip as _real_ip,
    templates,
    valid_cik as _valid_cik,
    valid_cusip as _valid_cusip,
    valid_member_id as _valid_member_id,
    valid_ticker as _valid_ticker,
)
from filings.concurrency import to_heavy as _to_heavy
from filings.models import SuperinvestorSummary, StockInfo
from filings.routers import auth_admin as _auth_admin_router
from filings.routers import redesign_preview as _redesign_preview_router
from filings.routers import static_pages as _static_pages_router
from filings.routers import watchlist as _watchlist_router
from filings.superinvestors import SUPERINVESTORS, SUPERINVESTORS_BY_CIK


# ═══════════════════════════════════════════════════════════════════════
# Logging
# ═══════════════════════════════════════════════════════════════════════


def _setup_logging() -> None:
    """Configure structured JSON logging for production (Railway)."""
    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()

    if os.environ.get("RAILWAY_ENVIRONMENT"):
        # JSON format for Railway log aggregation
        fmt = '{"time":"%(asctime)s","level":"%(levelname)s","name":"%(name)s","msg":"%(message)s"}'
    else:
        fmt = "%(asctime)s %(levelname)-8s %(name)s — %(message)s"

    logging.basicConfig(level=log_level, format=fmt, force=True)
    # Quiet noisy libraries
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("yfinance").setLevel(logging.WARNING)
    logging.getLogger("peewee").setLevel(logging.WARNING)


_setup_logging()
logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════
# Sentry (optional — only if SENTRY_DSN is set)
# ═══════════════════════════════════════════════════════════════════════

_sentry_dsn = os.environ.get("SENTRY_DSN", "")
if _sentry_dsn:
    try:
        import sentry_sdk

        sentry_sdk.init(
            dsn=_sentry_dsn,
            traces_sample_rate=0.1,
            environment=os.environ.get("RAILWAY_ENVIRONMENT", "development"),
        )
        logger.info("Sentry initialized (10%% trace sampling)")
    except ImportError:
        logger.warning("SENTRY_DSN set but sentry-sdk not installed — skipping")

# ── Analytics (optional) ─────────────────────────────────────────────
# NOTE: Set POSTHOG_KEY in production (Railway) to enable analytics.
_POSTHOG_KEY = os.environ.get("POSTHOG_KEY", "")
_CLERK_PUBLISHABLE_KEY = os.environ.get("CLERK_PUBLISHABLE_KEY", "")
# CLERK_WEBHOOK_SECRET is read in filings.routers.auth_admin — no longer needed here.


# ═══════════════════════════════════════════════════════════════════════
# Rate limiting (optional — graceful fallback if slowapi not installed)
#
# The ``limiter`` instance lives in ``filings.app_state`` so routers can
# attach ``@limiter.limit(...)`` decorators without a circular import
# back through this module.  See app_state.py for the actual definition.
# ═══════════════════════════════════════════════════════════════════════

from filings.app_state import limiter, has_limiter as _has_limiter
try:
    from slowapi import _rate_limit_exceeded_handler
    from slowapi.errors import RateLimitExceeded
except ImportError:
    _rate_limit_exceeded_handler = None
    RateLimitExceeded = None
if not _has_limiter:
    logger.info("slowapi not installed — rate limiting disabled")


# ═══════════════════════════════════════════════════════════════════════
# App startup time (for /health uptime)
# ═══════════════════════════════════════════════════════════════════════

_app_start_time = time_module.time()

# ── Background refresh configuration ─────────────────────────────────
# Self-healing: the web app can refresh stale 13F data in the background,
# reducing dependency on the Railway cron job.  Disable via env var if needed.
_ENABLE_BACKGROUND_REFRESH = (
    os.environ.get("ENABLE_BACKGROUND_REFRESH", "true").lower() == "true"
)

# WORKER_MODE controls which periodic tasks run in this process:
#   "combined" (default) -- all tasks in this web process.  Legacy
#                            behaviour, kept for backwards-compat.
#   "web"                -- skip the heavy/external-API periodic tasks
#                            that have a sibling worker process running them.
#                            Web tier still runs its own watchdog, memprof
#                            poller, gc trim -- those are web-specific.
#   "worker"             -- not used by web.py (worker.py is the entry point)
# When migrating, set both: the new worker service runs `filings-worker`,
# and the web service gets WORKER_MODE=web so periodic tasks don't double up.
_WORKER_MODE = os.environ.get("WORKER_MODE", "combined").strip().lower()
_RUN_WORKER_TASKS = _WORKER_MODE != "web"
# Per-CIK locks so the background sweep and request-triggered refreshes
# can run concurrently for *different* funds without blocking each other.
# Bounded: the 85 SUPERINVESTOR CIKs plus any ad-hoc /holdings/{cik} lookups
# a user tries.  Without a cap, arbitrary CIK URLs would leak one Lock per
# unique CIK until the next background sweep (which only clears CIKs not in
# the active fund list).  Insertion-order eviction caps the footprint at
# ~200 × ~300 bytes/Lock = ~60 KB.  Python 3.7+ dicts preserve insertion
# order so `next(iter(...))` is the oldest key; no OrderedDict needed.
_REFRESH_LOCKS_MAX = 200
_refresh_locks: dict[str, asyncio.Lock] = {}
_refresh_locks_mu = asyncio.Lock()          # guards the dict itself
_refresh_in_progress: set[str] = set()


async def _get_refresh_lock(cik: str) -> asyncio.Lock:
    """Return (creating if needed) the per-CIK refresh lock.

    Insertion-order eviction once the dict reaches ``_REFRESH_LOCKS_MAX``
    so arbitrary ``/holdings/{cik}`` traffic can't leak Lock objects.  A
    concurrent refresh for an evicted CIK just creates a new Lock on its
    next request — worst case is two refreshes racing for the same fund,
    which is the same semantics as before the lock existed.
    """
    async with _refresh_locks_mu:
        lock = _refresh_locks.get(cik)
        if lock is None:
            lock = asyncio.Lock()
            _refresh_locks[cik] = lock
            while len(_refresh_locks) > _REFRESH_LOCKS_MAX:
                # Evict oldest by insertion order
                oldest_key = next(iter(_refresh_locks))
                _refresh_locks.pop(oldest_key, None)
        return lock


# ── Ticker logo cache ─────────────────────────────────────────────
# Only the *set* of known IDs is loaded at startup (negligible memory).
# Actual image bytes are fetched on-demand and held in a bounded LRU dict.
import base64 as _b64
from collections import OrderedDict

_LOGO_LRU_MAX = 200
_HEADSHOT_LRU_MAX = 50
_ANALYST_PHOTO_LRU_MAX = 50


def _lru_put(cache: OrderedDict, key: str, value: bytes, maxsize: int) -> None:
    """Insert into bounded OrderedDict, evicting oldest if over maxsize."""
    cache[key] = value
    cache.move_to_end(key)
    while len(cache) > maxsize:
        cache.popitem(last=False)


_logo_cache: OrderedDict[str, bytes] = OrderedDict()
_logo_set: set[str] = set()  # exposed to templates as Jinja global

_headshot_cache: OrderedDict[str, bytes] = OrderedDict()
_headshot_set: set[str] = set()  # exposed to templates as Jinja global

_http_pool: httpx.AsyncClient | None = None

_analyst_photo_cache: OrderedDict[str, bytes] = OrderedDict()
_analyst_photo_set: set[str] = set()  # exposed to templates as Jinja global


async def _safe_fetch(coro, label: str, timeout: int = 10, default=None):
    """Await *coro* with a timeout; return *default* on any failure.

    Prevents a single slow/broken data source from blocking the page
    render.  Failures are logged but never bubble up.
    """
    try:
        return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError:
        logger.warning("%s timed out (%ds)", label, timeout)
        return default
    except Exception:
        logger.warning("%s failed", label, exc_info=True)
        return default


_retail_data_cache: tuple[float, tuple[list, dict | None]] | None = None
_RETAIL_DATA_TTL = 120  # 2 minutes — ApeWisdom updates every ~5 min

_SIGNALS_TIMEOUT = 8  # seconds per source in /api/signals/{ticker}


_HTML_CACHE_MAX = 300  # max entries per _HtmlCache instance (LRU eviction)


class _HtmlCache:
    """Lightweight TTL cache for rendered HTML responses.

    Avoids re-fetching + re-rendering when the same page is requested
    within the TTL window.  Keyed by an optional string (e.g. view name);
    single-entry caches use key="".  Bounded to _HTML_CACHE_MAX entries
    with oldest-first eviction to prevent unbounded memory growth.
    """

    __slots__ = ("_ttl", "_store", "_max")

    def __init__(self, ttl: int, maxsize: int = _HTML_CACHE_MAX):
        self._ttl = ttl
        self._store: dict[str, tuple[float, bytes]] = {}
        self._max = maxsize

    def get(self, key: str = "") -> Response | None:
        entry = self._store.get(key)
        if entry and time_module.time() - entry[0] < self._ttl:
            return Response(content=entry[1], media_type="text/html")
        return None

    def get_stale(self, key: str = "") -> Response | None:
        """Return cached content even if TTL has expired (for stampede fallback)."""
        entry = self._store.get(key)
        if entry:
            return Response(content=entry[1], media_type="text/html")
        return None

    def set(self, resp, key: str = "") -> None:  # noqa: ANN001
        try:
            self._store[key] = (time_module.time(), resp.body)
            # Evict oldest entries if over limit
            if len(self._store) > self._max:
                sorted_keys = sorted(
                    self._store, key=lambda k: self._store[k][0]
                )
                for k in sorted_keys[: len(self._store) - self._max]:
                    del self._store[k]
        except Exception:
            logger.warning("HTML cache write failed (key=%s)", key, exc_info=True)


def _l2_get_html(cache_key: str) -> str | None:
    """Load stale HTML from Supabase L2 cache (returns raw HTML string or None)."""
    try:
        data, _fresh = supabase_cache.get_cached_with_stale(cache_key)
        return data.get("html") if isinstance(data, dict) else None
    except Exception:
        return None


def _l2_set_html(cache_key: str, category: str, html: bytes, ttl: int) -> None:
    """Persist rendered HTML to Supabase L2 cache."""
    try:
        supabase_cache.set_cached(
            cache_key, category, {"html": html.decode()}, ttl
        )
    except Exception:
        logger.debug("L2 HTML cache write failed: %s", cache_key, exc_info=True)


# ---------------------------------------------------------------------------
# L2 stale-fallback decorator — wraps HTML endpoints with 3-tier caching
# ---------------------------------------------------------------------------
#   L1 (in-memory, fast) → live fetch + render → L2 fallback (Supabase)
#
# Usage:
#   @app.get("/api/foo/{ticker}", response_class=HTMLResponse)
#   @_with_l2_fallback("stock:foo:{ticker}", category="stock", l1_ttl=300, l2_ttl=7200)
#   async def foo_endpoint(request, ticker):
#       ...  # normal handler, returns TemplateResponse
# ---------------------------------------------------------------------------

_l2_caches: dict[str, _HtmlCache] = {}  # auto-created per category

# Stampede protection: tracks keys currently being refreshed.
# While a key is in this set, other requests serve stale L1 or L2 instead
# of all firing the handler simultaneously.
_l2_refreshing: set[str] = set()

# Registry: each decorated endpoint stores its key_template so the 429
# handler can reconstruct cache keys without a separate hand-coded map.
_l2_key_registry: list[tuple[str, str]] = []  # [(key_template, func_name), ...]


def _with_l2_fallback(
    key_template: str,
    *,
    category: str = "page",
    l1_ttl: int = 300,
    l2_ttl: int = 7200,
):
    """Decorator: L1 in-memory → handler → L2 Supabase stale-fallback.

    *key_template* may contain ``{ticker}`` or other kwargs that will be
    substituted from the decorated function's arguments at call-time.
    """

    def decorator(fn):  # noqa: ANN001
        # Lazily create one _HtmlCache per (category, l1_ttl) pair
        cache_id = f"{category}:{l1_ttl}"
        if cache_id not in _l2_caches:
            _l2_caches[cache_id] = _HtmlCache(l1_ttl)
        l1 = _l2_caches[cache_id]

        @functools.wraps(fn)
        async def wrapper(*args, **kwargs):  # noqa: ANN002, ANN003
            # Build cache key from route params
            cache_key = key_template.format(**kwargs)

            # ── L1 hit ──
            cached = l1.get(cache_key)
            if cached is not None:
                return cached

            # ── Stampede guard: if another request is already refreshing
            #    this key, serve stale L1 or L2 instead of piling on ──
            if cache_key in _l2_refreshing:
                # Try stale L1 (expired but still in store)
                stale = l1.get_stale(cache_key)
                if stale:
                    return stale
                # Try L2
                try:
                    stale_html = await asyncio.to_thread(_l2_get_html, cache_key)
                    if stale_html:
                        return Response(content=stale_html.encode(), media_type="text/html")
                except Exception:
                    pass
                # Fall through to handler as last resort
                # (better to double-fetch than show nothing)

            # ── Live fetch + render ──
            _l2_refreshing.add(cache_key)
            try:
                resp = await fn(*args, **kwargs)
                # Save to L1
                l1.set(resp, cache_key)
                # Async save to L2 (non-blocking)
                if hasattr(resp, "body") and resp.body:
                    asyncio.create_task(
                        asyncio.to_thread(
                            _l2_set_html, cache_key, category, resp.body, l2_ttl
                        )
                    )
                return resp
            except Exception:
                logger.warning("L2 fallback triggered for %s", cache_key, exc_info=True)
            finally:
                _l2_refreshing.discard(cache_key)

            # ── L2 stale fallback ──
            try:
                stale_html = await asyncio.to_thread(_l2_get_html, cache_key)
                if stale_html:
                    logger.info("Serving stale L2 for %s", cache_key)
                    body = stale_html.encode()
                    l1.set(Response(content=body, media_type="text/html"), cache_key)
                    return Response(content=body, media_type="text/html")
            except Exception:
                pass

            # ── Final fallback ──
            return HTMLResponse(
                '<p class="text-muted" style="text-align:center;padding:2em 0;">'
                "Data temporarily unavailable. Please try again shortly.</p>"
            )

        # Register so the 429 handler can reconstruct cache keys dynamically
        wrapper._l2_key_template = key_template  # type: ignore[attr-defined]
        _l2_key_registry.append((key_template, fn.__name__))
        return wrapper

    return decorator


async def _fetch_retail_data() -> tuple[list[dict], dict | None]:
    """Fetch ApeWisdom + CNN Fear&Greed for leaderboard endpoints.

    Returns (all_data, fear_greed) — both degrade gracefully to empty/None.
    Cached in-memory for 2 minutes to avoid hammering external APIs.
    """
    global _retail_data_cache
    now = time_module.time()
    if _retail_data_cache and now - _retail_data_cache[0] < _RETAIL_DATA_TTL:
        return _retail_data_cache[1]
    try:
        all_data, fear_greed = await asyncio.wait_for(
            asyncio.gather(
                _to_heavy(sentiment._get_apewisdom_all),
                _to_heavy(sentiment._get_cnn_fear_greed),
            ),
            timeout=10,
        )
    except Exception:
        logger.warning("_fetch_retail_data: data fetch failed", exc_info=True)
        all_data, fear_greed = [], None
    result = (all_data or [], fear_greed)
    _retail_data_cache = (now, result)
    return result


# ═══════════════════════════════════════════════════════════════════════
# Lifespan
# ═══════════════════════════════════════════════════════════════════════


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Load cache from Supabase on startup, then self-heal stale data.

    Data is primarily kept fresh by the standalone sync worker (Railway
    Cron Job).  As a safety net, the web process can also refresh stale
    funds in the background so users never see outdated data even if the
    cron job fails.
    """
    global _startup_ts
    _startup_ts = time_module.time()

    # ── Thread pool architecture ─────────────────────────────────────────
    # Owned by `filings.concurrency.init_pools()` so routers can also call
    # `to_heavy(...)` without circular-importing web.py.  Defaults sized
    # for a Railway container (≈1-2 vCPU, 512MB-2GB RAM):
    #   default pool = 32 (was 8) — fast/light work, prevents queueing
    #                                on health checks + hot paths
    #   heavy  pool  = 16 (was 8) — yfinance / Finnhub / SEC / ApeWisdom
    #   semaphore   = 12 (was 10) — global cap on concurrent heavy ops;
    #                                THE protection against fan-out blowups
    # Override via WORKER_THREADS / HEAVY_THREADS / HEAVY_CONCURRENCY env vars.
    from filings.concurrency import init_pools as _init_pools
    _init_pools()

    # Eagerly construct the async Supabase client so the first request
    # path doesn't pay the init latency.  Best-effort: failures are
    # logged inside the helper and the caller falls back to a "miss"
    # response identical to the sync path's behaviour.
    try:
        from filings import supabase_cache as _supabase_cache
        await _supabase_cache.init_async_client()
    except Exception as exc:
        logger.warning("Async Supabase client eager-init failed: %s", exc)

    global _http_pool
    _http_pool = httpx.AsyncClient(
        timeout=10.0,
        follow_redirects=True,
        limits=httpx.Limits(max_connections=30, max_keepalive_connections=15),
        headers={"User-Agent": "PaperPanda/1.0 (+https://paperpanda.io)"},
    )

    # ── Load all startup data concurrently ──────────────────────────
    # Previously 5 sequential Supabase loads; now runs them in parallel
    # for ~3-5× faster cold-start.

    async def _load_funds():
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(cache.load_cache_from_supabase), timeout=30
            )
        except (asyncio.TimeoutError, Exception) as exc:
            logger.warning(
                "Supabase startup cache load failed (%s), falling back to disk",
                exc,
            )
            return None

    async def _load_deploy():
        try:
            return await asyncio.to_thread(aum_data.load_all_deployment_data)
        except Exception:
            return {}

    async def _load_logo_set():
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(supabase_cache.get_existing_logo_tickers),
                timeout=30,
            )
        except Exception as exc:
            logger.warning("Logo set load failed (%s), logos disabled", exc)
            return None

    async def _load_headshot_set():
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(supabase_cache.get_existing_headshot_members),
                timeout=30,
            )
        except Exception as exc:
            logger.warning(
                "Headshot set load failed (%s), headshots disabled", exc
            )
            return None

    async def _load_analyst_photo_set():
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(supabase_cache.get_existing_analyst_photo_ids),
                timeout=30,
            )
        except Exception as exc:
            logger.warning("Analyst photo set load failed (%s)", exc)
            return None

    (
        fund_result,
        deploy_result,
        logo_ids,
        headshot_ids,
        analyst_photo_ids,
    ) = await asyncio.gather(
        _load_funds(),
        _load_deploy(),
        _load_logo_set(),
        _load_headshot_set(),
        _load_analyst_photo_set(),
    )

    # ── Process results ──────────────────────────────────────────
    app.state.fund_cache = fund_result or cache.load_cache()
    app.state.refresh_status = "disabled"
    app.state.refresh_progress = {"total": 0, "done": 0, "failed": 0}
    app.state.deployment_cache = deploy_result or {}

    # If no deployment data cached, sync in background on first startup
    if not app.state.deployment_cache and app.state.fund_cache:

        async def _bg_deploy_sync():
            try:
                await asyncio.to_thread(
                    aum_data.sync_all_deployment_data,
                    SUPERINVESTORS,
                    app.state.fund_cache,
                    force=True,
                )
                app.state.deployment_cache = await asyncio.to_thread(
                    aum_data.load_all_deployment_data
                )
                logger.info(
                    "Background deployment sync populated %d entries",
                    len(app.state.deployment_cache),
                )
            except Exception as e:
                logger.warning("Background deployment sync failed: %s", e)

        asyncio.create_task(_bg_deploy_sync())

    if logo_ids:
        _logo_set.update(logo_ids)
        templates.env.globals["logo_tickers"] = _logo_set
        logger.info("Loaded %d ticker logo IDs (lazy-fetch on demand)", len(_logo_set))
    else:
        templates.env.globals["logo_tickers"] = set()

    if headshot_ids:
        _headshot_set.update(headshot_ids)
        templates.env.globals["headshot_members"] = _headshot_set
        logger.info(
            "Loaded %d headshot member IDs (lazy-fetch on demand)",
            len(_headshot_set),
        )
    else:
        templates.env.globals["headshot_members"] = set()

    if analyst_photo_ids:
        _analyst_photo_set.update(analyst_photo_ids)
    templates.env.globals["analyst_photo_set"] = _analyst_photo_set
    logger.info(
        "Loaded %d analyst photo IDs (lazy-fetch on demand)",
        len(_analyst_photo_set),
    )

    # Track background tasks for clean shutdown
    _bg_tasks: set[asyncio.Task] = set()

    def _track(coro, name: str):
        task = asyncio.create_task(coro, name=name)
        _bg_tasks.add(task)
        task.add_done_callback(_bg_tasks.discard)
        return task

    # ── Tasks below are gated by WORKER_MODE: when set to "web", a
    # ── sibling worker process owns these and the web tier skips them.

    if _RUN_WORKER_TASKS:
        # Prefetch S&P 500 market data in background (~30-60s on cold start)
        _track(_prefetch_market_data(app), "prefetch_market")

        # Run retention cleanup in background (keep DB small)
        _track(asyncio.to_thread(supabase_cache.run_retention_cleanup), "retention_cleanup")

        # Self-heal: refresh any stale funds in background
        if _ENABLE_BACKGROUND_REFRESH:
            app.state.refresh_status = "pending"
            _track(_delayed_refresh_sweep(app), "refresh_sweep")

        # Reddit velocity notification scanner (every 30 min)
        _track(_periodic_task(
            name="Reddit velocity scan", startup_delay=60,
            interval=_REDDIT_SCAN_INTERVAL, body=_run_reddit_velocity_scan,
        ), "reddit_scanner")

        # Feature announcement → notification scanner (every 10 min)
        _track(_periodic_task(
            name="Feature announcement scan", startup_delay=45,
            interval=_FEATURE_ANNOUNCE_INTERVAL, body=_run_feature_announcement_scan,
        ), "feature_announce_scanner")

        # Redesign /_v2/home L2 cache warmer (only when env-gated route is on)
        if _redesign_preview_router.is_enabled():
            _track(_periodic_task(
                name="Redesign L2 warmer", startup_delay=30,
                interval=_REDESIGN_L2_WARMER_INTERVAL, body=_run_redesign_l2_warm,
            ), "redesign_l2_warmer")
    else:
        logger.info(
            "lifespan: WORKER_MODE=web -- skipping periodic tasks owned "
            "by the worker process (prefetch_market, retention_cleanup, "
            "refresh_sweep, reddit_scanner, feature_announce_scanner, "
            "redesign_l2_warmer)"
        )

    # Memory profile poller — logs RSS / threads / cache sizes every 5 min
    # to stdout so Railway log search can chart leaks over time.  Zero
    # external deps; on-demand deep dive via /api/admin/memprof.
    _track(_periodic_task(
        name="memprof poller", startup_delay=20,
        interval=_MEMPROF_POLL_INTERVAL, body=_run_memprof_log,
    ), "memprof_poller")

    # gc.collect + glibc malloc_trim — every 5 min, force release of
    # fragmented heap memory back to the OS.  Counters Python's habit
    # of holding free arenas after request bursts.
    _track(_periodic_task(
        name="gc trim", startup_delay=120,
        interval=_GC_TRIM_INTERVAL, body=_run_gc_trim,
    ), "gc_trim")

    # Stuck-thread watchdog — recycles the worker before a slow-leak
    # path silently deadlocks the heavy pool.  See _run_thread_watchdog
    # docstring above for the failure mode this protects against.
    _track(_periodic_task(
        name="thread watchdog", startup_delay=60,
        interval=_WATCHDOG_INTERVAL_S, body=_run_thread_watchdog,
    ), "thread_watchdog")

    if _RUN_WORKER_TASKS:
        # Stock-bundle warmer — maintains the stock_overview_cache table so
        # the /stock/{ticker} request path can serve from a single Supabase
        # read instead of fanning out 10+ sync upstream calls.
        _track(_periodic_task(
            name="stock warmer", startup_delay=180,
            interval=_STOCK_WARMER_INTERVAL_S, body=_run_stock_warmer,
        ), "stock_warmer")

        # Seed the stock_overview_cache hot/warm tiers from the just-loaded
        # fund_cache.  Idempotent (no-op when rows already exist), so it's
        # safe to run on every worker startup.  Delay 60s so fund_cache is
        # populated first (it loads in another startup task).
        _track(_run_initial_tier_seed(), "stock_tier_seeder")
    _alert_dests = []
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        _alert_dests.append("telegram")
    if WATCHDOG_ALERT_URL:
        _alert_dests.append("webhook")
    if _alert_dests:
        logger.info("watchdog: alert destinations active = %s",
                    ", ".join(_alert_dests))
    else:
        logger.info(
            "watchdog: no alert destinations (set TELEGRAM_BOT_TOKEN+"
            "TELEGRAM_CHAT_ID and/or WATCHDOG_ALERT_URL to enable)"
        )

    yield

    # ── Shutdown cleanup ──────────────────────────────────────────────
    # Cancel all background tasks so they don't leak on worker recycle
    for task in _bg_tasks:
        task.cancel()
    if _bg_tasks:
        await asyncio.gather(*_bg_tasks, return_exceptions=True)

    if _http_pool is not None:
        await _http_pool.aclose()
        _http_pool = None

    # Drain the shared async HTTP client used by redesign fetchers.
    from filings.http_client import shutdown_async_client as _shutdown_async_client
    await _shutdown_async_client()

    # Tear down the shared concurrency pools owned by filings.concurrency.
    from filings.concurrency import shutdown_pools as _shutdown_pools
    _shutdown_pools()


# ═══════════════════════════════════════════════════════════════════════
# Inline CSS minification (defined before app so middleware can be registered)
# ═══════════════════════════════════════════════════════════════════════
_CSS_COMMENT_RE = _re.compile(r"/\*.*?\*/", _re.DOTALL)
_CSS_WHITESPACE_RE = _re.compile(r"\s+")
_CSS_PUNCT_RE = _re.compile(r"\s*([{}:;,>~])\s*")
_CSS_TRAILING_SEMI_RE = _re.compile(r";}")


def _minify_css_block(css: str) -> str:
    """Strip comments, collapse whitespace, remove unnecessary chars."""
    css = _CSS_COMMENT_RE.sub("", css)
    css = _CSS_WHITESPACE_RE.sub(" ", css)
    css = _CSS_PUNCT_RE.sub(r"\1", css)
    css = _CSS_TRAILING_SEMI_RE.sub("}", css)
    return css.strip()


_STYLE_BLOCK_RE = _re.compile(
    r"(<style[^>]*>)(.*?)(</style>)", _re.DOTALL | _re.IGNORECASE
)


def _minify_inline_styles(html: str) -> str:
    """Find all <style>...</style> blocks in HTML and minify their CSS."""
    def _repl(m):
        return m.group(1) + _minify_css_block(m.group(2)) + m.group(3)
    return _STYLE_BLOCK_RE.sub(_repl, html)


class CSSMinifyMiddleware(BaseHTTPMiddleware):
    """Minify inline <style> blocks in HTML responses."""

    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        ct = response.headers.get("content-type", "")
        if "text/html" not in ct:
            return response
        body_chunks = []
        async for chunk in response.body_iterator:
            if isinstance(chunk, str):
                body_chunks.append(chunk.encode("utf-8"))
            else:
                body_chunks.append(chunk)
        body = b"".join(body_chunks)
        if b"<style" in body:
            minified = _minify_inline_styles(body.decode("utf-8", errors="replace"))
            headers = dict(response.headers)
            headers.pop("content-length", None)
            return Response(
                content=minified,
                status_code=response.status_code,
                headers=headers,
                media_type=response.media_type,
            )
        return Response(
            content=body,
            status_code=response.status_code,
            headers=dict(response.headers),
            media_type=response.media_type,
        )


# ═══════════════════════════════════════════════════════════════════════
# App creation
# ═══════════════════════════════════════════════════════════════════════

app = FastAPI(title="PaperPanda", lifespan=lifespan)

# ── Middleware stack (LIFO: last added = outermost) ──
# Registration order determines nesting: first = innermost, last = outermost.
# Response flow: App → CSSMinify → Brotli → GZip(fallback) → client
app.add_middleware(CSSMinifyMiddleware)

try:
    import brotli as _brotli_mod

    class BrotliMiddleware(BaseHTTPMiddleware):
        """Compress responses with Brotli when the client supports it."""

        async def dispatch(self, request: Request, call_next):
            accept = request.headers.get("accept-encoding", "")
            if "br" not in accept:
                return await call_next(request)
            response = await call_next(request)
            # Skip if already compressed (e.g. by inner middleware)
            if response.headers.get("content-encoding"):
                return response
            # Only compress text-based responses above 1 KB
            ct = response.headers.get("content-type", "")
            if not any(t in ct for t in ("text/", "json", "javascript", "xml")):
                return response
            if hasattr(response, "body"):
                body = response.body
                if len(body) < 1000:
                    return response
                compressed = _brotli_mod.compress(body, quality=4)
                if len(compressed) < len(body):
                    headers = dict(response.headers)
                    headers["content-encoding"] = "br"
                    headers["content-length"] = str(len(compressed))
                    headers["vary"] = "Accept-Encoding"
                    return Response(
                        content=compressed,
                        status_code=response.status_code,
                        headers=headers,
                        media_type=response.media_type,
                    )
                return response
            return response

    app.add_middleware(BrotliMiddleware)
    logger.info("Brotli compression enabled")
except ImportError:
    logger.info("Brotli not available, using GZip only")

# GZip outermost — catches anything Brotli didn't handle (non-br clients)
app.add_middleware(GZipMiddleware, minimum_size=1000)

# ``templates`` is imported from filings.app_state (single shared Jinja2
# environment; routers in filings/routers/ reuse the same instance).

# Static files (logo, favicon, etc.) — immutable cache for hashed assets
_static_dir = Path(__file__).parent / "static"
app.mount(
    "/static",
    StaticFiles(directory=_static_dir),
    name="static",
)

# Template globals
templates.env.globals["current_year"] = datetime.now().year
# Rolling 12-month stale cutoff for fund filings
# Computed at startup; Railway restarts daily so drift is negligible.
_stale_cutoff = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
templates.env.globals["stale_cutoff"] = _stale_cutoff
templates.env.globals["supabase_url"] = auth.SUPABASE_URL
templates.env.globals["supabase_anon_key"] = auth.SUPABASE_ANON_KEY
templates.env.globals["auth_enabled"] = bool(auth.CLERK_DOMAIN or auth.SUPABASE_ANON_KEY)
templates.env.globals["clerk_domain"] = auth.CLERK_DOMAIN
templates.env.globals["posthog_key"] = _POSTHOG_KEY
templates.env.globals["clerk_publishable_key"] = _CLERK_PUBLISHABLE_KEY


# ═══════════════════════════════════════════════════════════════════════
# Router mounts — see filings/routers/ (audit-sprint-4)
# ═══════════════════════════════════════════════════════════════════════
app.include_router(_auth_admin_router.router)
app.include_router(_static_pages_router.router)
app.include_router(_watchlist_router.router)

# Redesign preview — env-gated.  Only mounted when PP_REDESIGN_PREVIEW=1
# is set (local dev only).  Routes live under /_v2/* and render the
# new design templates with mock data, while the production routes keep
# rendering the old templates unchanged.  Without the env var the router
# is never registered, so even an accidental Railway deploy is inert.
if _redesign_preview_router.is_enabled():
    app.include_router(_redesign_preview_router.router)
    logger.info("Redesign preview routes mounted at /_v2/*")


# ── Template filters ──────────────────────────────────────────────────
_SECTOR_ABBREV = {
    "Consumer Cyclical": "Cons. Cyc.",
    "Consumer Defensive": "Cons. Def.",
    "Communication Services": "Comm. Svcs.",
    "Financial Services": "Financial",
    "Basic Materials": "Materials",
    "Real Estate": "Real Est.",
    "Industrials": "Industrial",
    "Information Technology": "Info Tech",
}


def _format_short_date(value: str) -> str:
    """'2026-02-15' → 'Feb 15 2026'.

    Renamed semantically — this is the project's canonical "MMM DD YYYY"
    format.  Kept under the old name (and as a Jinja filter) so existing
    v1 templates pick up the new style without churn.
    """
    if not value:
        return "—"
    from filings.dates_format import format_date
    return format_date(value, fallback=value)


def _abbreviate_sector(value: str) -> str:
    """Shorten long GICS sector names for table display."""
    if not value:
        return "—"
    return _SECTOR_ABBREV.get(value, value)


def _format_pretty_date(value: str) -> str:
    """'2026-03-02' -> 'Mar 02 2026' (canonical MMM DD YYYY format).

    Used to emit ordinal-suffixed dates ("Mar 2nd, 2026") — switched to
    the same canonical product format for consistency.  Old name kept so
    Jinja filter callers don't need to change.
    """
    if not value:
        return "—"
    from filings.dates_format import format_date
    return format_date(value, fallback=value)


def _format_value(value, prefix: str = "$", precision: int = 1) -> str:
    """Format a dollar value with B/M/K suffixes for compact display.

    Examples: 6_500_000_000 → "$6.5B", 450_000_000 → "$450M", 75_000 → "$75K"
    """
    if not value or value == 0:
        return f"{prefix}0"
    v = float(value)
    if v >= 1_000_000_000:
        return f"{prefix}{v / 1_000_000_000:.{precision}f}B"
    if v >= 1_000_000:
        return f"{prefix}{v / 1_000_000:.0f}M"
    if v >= 1_000:
        return f"{prefix}{v / 1_000:.0f}K"
    return f"{prefix}{v:,.0f}"


def _fmt_num(value, precision: int = 0) -> str:
    """Format a number with thousands separators — ``{{ x|fmt_num }}``.

    Replaces the ~96 inline ``{{ "{:,.0f}".format(value) }}`` sites in
    templates with a shorter, consistent syntax.  Falls back to ``str()``
    on non-numeric input so Jinja doesn't raise during render.

    Examples: ``1234567|fmt_num`` → ``"1,234,567"``;
              ``3.14159|fmt_num(2)`` → ``"3.14"``.
    """
    try:
        return f"{float(value):,.{precision}f}"
    except (TypeError, ValueError):
        return str(value) if value is not None else ""


templates.env.filters["format_short_date"] = _format_short_date
templates.env.filters["format_pretty_date"] = _format_pretty_date
templates.env.filters["abbreviate_sector"] = _abbreviate_sector
templates.env.filters["format_value"] = _format_value
templates.env.filters["fmt_num"] = _fmt_num


def _top_tickers(cached: dict, n: int = 5) -> list[str]:
    """Extract up to *n* valid ticker symbols from a fund's cached top holdings.

    Reads the first *n* entries of ``all_holdings`` (already value-sorted
    desc, identical to the dropped ``top_holdings`` prefix).
    """
    return [h["ticker"] for h in cached.get("all_holdings", [])[:n] if h.get("ticker")]

# Attach rate limiter
if _has_limiter:
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


# ═══════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════


def _fund_cache() -> dict:
    """Return the current fund cache dict from app.state."""
    return getattr(app.state, "fund_cache", {})


def _current_quarter_bounds() -> tuple[str, str]:
    """Return (start, end) ISO dates for the current SEC filing quarter."""
    today = date.today()
    q = (today.month - 1) // 3
    starts = [f"{today.year}-01-01", f"{today.year}-04-01",
              f"{today.year}-07-01", f"{today.year}-10-01"]
    ends = [f"{today.year}-03-31", f"{today.year}-06-30",
            f"{today.year}-09-30", f"{today.year}-12-31"]
    return starts[q], ends[q]


# ═══════════════════════════════════════════════════════════════════════
# Cached ownership map (avoids O(funds × holdings) on every request)
# ═══════════════════════════════════════════════════════════════════════


def _get_ownership_map() -> dict[str, list[str]]:
    """Return ticker → [superinvestor names] map, cached per fund_cache version.

    Rebuilds only when the fund_cache dict object changes (new reference
    after background refresh). Race conditions are benign — worst case two
    concurrent requests both rebuild, same as the old per-request behavior.
    """
    cache_data = _fund_cache()
    if not cache_data:
        return {}

    cache_id = id(cache_data)
    cached = getattr(app.state, "_ownership_map", None)

    if cached is not None:
        prev_id, prev_map = cached
        if prev_id == cache_id:
            return prev_map

    ownership_map = client.build_ticker_ownership_map(cache_data, SUPERINVESTORS_BY_CIK)
    app.state._ownership_map = (cache_id, ownership_map)
    return ownership_map


# ═══════════════════════════════════════════════════════════════════════
# Analyst consensus cache (avoids 25 async thread spawns on warm lookups)
# ═══════════════════════════════════════════════════════════════════════

_consensus_cache: "OrderedDict[str, tuple[float, dict | None]]"
try:
    from collections import OrderedDict as _OrderedDict
    _consensus_cache = _OrderedDict()
except ImportError:
    _consensus_cache = {}  # type: ignore[assignment]
_CONSENSUS_TTL = 1800   # 30 minutes in seconds
_CONSENSUS_MAX = 500    # ~0.5 MB worst-case; evict oldest when exceeded


def _consensus_cache_set(key: str, value: tuple[float, dict | None]) -> None:
    """Insert/update *key* in the consensus LRU cache, evicting if needed."""
    _consensus_cache[key] = value
    _consensus_cache.move_to_end(key)      # mark as most-recently used
    while len(_consensus_cache) > _CONSENSUS_MAX:
        _consensus_cache.popitem(last=False)  # evict the oldest (LRU) entry


# ═══════════════════════════════════════════════════════════════════════
# Security helpers
# ═══════════════════════════════════════════════════════════════════════
# NOTE: _valid_ticker / _valid_cik / _valid_cusip / _valid_member_id are
# imported from filings.app_state at the top of this file so routers can
# reuse the same validators.

_ALLOWED_HOST: str = os.environ.get("ALLOWED_HOST", "")  # e.g. "paperpanda.io"


async def _get_fund_data(cik: str) -> dict | None:
    """L1/L2 fund data lookup with Supabase fallback and background refresh.

    Checks the in-memory fund cache (L1), falls back to Supabase (L2),
    promotes hits to L1, and triggers a background refresh if stale.
    """
    cik_normalized = cik.lstrip("0") or cik
    cache_data = _fund_cache()
    cached = (
        cache_data.get(cik_normalized)
        or cache_data.get(cik)
        or getattr(app.state, "fund_cache", {}).get(cik_normalized)
        or getattr(app.state, "fund_cache", {}).get(cik)
    )

    # L1 miss → try Supabase L2
    if not cached:
        data, _is_fresh = await asyncio.to_thread(
            supabase_cache.get_cached_with_stale, f"13f:{cik_normalized}"
        )
        if isinstance(data, dict):
            cached = data
            cache_data[cik_normalized] = cached
            if hasattr(app.state, "fund_cache"):
                app.state.fund_cache[cik_normalized] = cached

    # Trigger background refresh if stale
    if cached and _ENABLE_BACKGROUND_REFRESH:
        if cache.is_fund_stale(cached) and cik_normalized not in _refresh_in_progress:
            asyncio.create_task(_trigger_single_refresh(app, cik_normalized))

    return cached


def _check_csrf_origin(request: Request) -> JSONResponse | None:
    """Reject cross-origin POST requests.

    Returns a 403 JSONResponse if the Origin header doesn't match the
    app's host; returns ``None`` if the request is safe.
    """
    origin = request.headers.get("origin", "")
    if not origin:
        # Non-browser clients (curl, Postman) don't send Origin; allow.
        return None
    from urllib.parse import urlparse

    origin_host = urlparse(origin).hostname or ""
    # Accept localhost/127.0.0.1 for local development
    if origin_host in ("localhost", "127.0.0.1"):
        return None
    # Accept configured production host
    if _ALLOWED_HOST and origin_host == _ALLOWED_HOST:
        return None
    # Accept if the Host header matches the Origin (same-origin)
    request_host = (request.headers.get("host") or "").split(":")[0]
    if request_host and origin_host == request_host:
        return None
    return JSONResponse({"error": "cross-origin request blocked"}, status_code=403)


# ═══════════════════════════════════════════════════════════════════════
# Middleware
# ═══════════════════════════════════════════════════════════════════════


_HTMX_PARTIAL_CACHE: dict[str, int] = {
    "/api/ticker-tape": 120,
    "/api/market-overview": 120,
    "/api/market-news": 300,
    "/api/heatmap": 120,
    "/api/heatmap-data": 120,
    "/api/retail-sentiment": 120,
    "/api/trending-combined": 120,
    "/api/google-trends/trending": 300,
}
# Prefix-based cache for stock tab HTMX partials (e.g. /api/stock/AAPL/analysts)
_HTMX_PREFIX_CACHE: list[tuple[str, int]] = [
    ("/api/stock/", 120),       # all stock tabs: analysts, sentiment, vitals, etc.
    ("/api/signals/", 120),     # alt signals per-ticker
    ("/api/insider-trades/", 120),
]


class TrailingSlashMiddleware(BaseHTTPMiddleware):
    """301-redirect paths with trailing slashes to their canonical form.

    Prevents Google from indexing /stock/AAPL and /stock/AAPL/ as separate pages.
    """

    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        if path != "/" and path.endswith("/"):
            # Rebuild URL without trailing slash, preserving query string
            new_path = path.rstrip("/")
            query = str(request.url.query)
            new_url = new_path + ("?" + query if query else "")
            return RedirectResponse(url=new_url, status_code=301)
        return await call_next(request)


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        # Static asset cache: 1 year for images/fonts, 1 hour for other static
        if request.url.path.startswith("/static/"):
            ext = request.url.path.rsplit(".", 1)[-1].lower() if "." in request.url.path else ""
            if ext in ("png", "webp", "jpg", "jpeg", "svg", "ico", "woff2", "woff"):
                response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
            else:
                response.headers["Cache-Control"] = "public, max-age=3600"
        # HTMX partial cache: short-lived, stale-while-revalidate for snappy UX
        elif request.url.path in _HTMX_PARTIAL_CACHE:
            ttl = _HTMX_PARTIAL_CACHE[request.url.path]
            response.headers["Cache-Control"] = (
                f"public, max-age={ttl}, stale-while-revalidate={ttl * 2}"
            )
        else:
            for prefix, ttl in _HTMX_PREFIX_CACHE:
                if request.url.path.startswith(prefix):
                    response.headers["Cache-Control"] = (
                        f"public, max-age={ttl}, stale-while-revalidate={ttl * 2}"
                    )
                    break
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "strict-origin"
        response.headers["Permissions-Policy"] = (
            "camera=(), microphone=(), geolocation=()"
        )
        # HSTS unconditionally — Railway terminates TLS upstream
        response.headers["Strict-Transport-Security"] = (
            "max-age=31536000; includeSubDomains"
        )
        # Content-Security-Policy — allow known CDN sources
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline' blob: "
            "https://cdn.jsdelivr.net https://unpkg.com "
            "https://us.i.posthog.com https://us-assets.i.posthog.com "
            "https://js.stripe.com "
            "https://challenges.cloudflare.com "
            "https://*.clerk.accounts.dev https://*.clerk.com; "
            "style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net "
            "https://*.clerk.com; "
            "img-src 'self' data: https: blob:; "
            "font-src 'self' data: https://*.clerk.com; "
            "worker-src blob: 'self'; "
            "connect-src 'self' "
            "https://cdn.jsdelivr.net "
            "https://us.i.posthog.com https://us-assets.i.posthog.com "
            "https://*.supabase.co "
            "https://js.stripe.com https://api.tickertick.com "
            "https://*.clerk.accounts.dev https://*.clerk.com "
            "https://clerk.paperpanda.io https://*.paperpanda.io; "
            "frame-src https://js.stripe.com https://tally.so "
            "https://challenges.cloudflare.com "
            "https://*.clerk.accounts.dev https://*.clerk.com; "
            "object-src 'none'; "
            "base-uri 'self'"
        )
        return response


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start = time_module.time()
        response = await call_next(request)
        duration_ms = round((time_module.time() - start) * 1000)
        logger.info(
            "%s %s %s %dms",
            request.method,
            request.url.path,
            response.status_code,
            duration_ms,
        )
        return response


# Note: TrustedHostMiddleware was removed — allowed_hosts=["*"] did nothing
# useful, and Railway terminates TLS upstream. Saves ~0.5ms per request.
app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(TrailingSlashMiddleware)
app.add_middleware(RequestLoggingMiddleware)

# Auth middleware (Clerk JWKS) -- also runs when LOCAL_DEV_USER is set
# so the mock user can be injected for local dev without Clerk config.
if auth.CLERK_DOMAIN or auth.LOCAL_DEV_USER:
    AuthMiddleware = auth._build_auth_middleware()
    app.add_middleware(AuthMiddleware)
    if auth.CLERK_DOMAIN:
        logger.info("Auth middleware enabled (Clerk JWKS: %s)", auth.CLERK_DOMAIN)
    if auth.LOCAL_DEV_USER:
        logger.warning(
            "⚠️  LOCAL_DEV_USER active -- mock user injected for local dev. "
            "DO NOT SET IN PRODUCTION."
        )


# ═══════════════════════════════════════════════════════════════════════
# Rate-limit L2 fallback — serves stale cache instead of blank errors
# ═══════════════════════════════════════════════════════════════════════

# Built lazily on first 429 from the route table + _l2_key_registry.
_rate_limit_route_map: dict | None = None


def _build_rate_limit_route_map() -> dict[str, str]:
    """Build a map of {route_path: key_template} from FastAPI routes.

    Matches routes whose endpoint has a ``_l2_key_template`` attribute
    (set by ``_with_l2_fallback``).
    """
    result: dict[str, str] = {}
    for route in app.routes:
        if hasattr(route, "endpoint"):
            tpl = getattr(route.endpoint, "_l2_key_template", None)
            if tpl and hasattr(route, "path"):
                result[route.path] = tpl
    return result


def _match_cache_key(request: Request) -> str | None:
    """Try to resolve an L2 cache key for the given request URL."""
    global _rate_limit_route_map
    if _rate_limit_route_map is None:
        _rate_limit_route_map = _build_rate_limit_route_map()

    path = request.url.path
    # Sort by route path length descending so /api/financials/{ticker}/history
    # matches before /api/financials/{ticker}
    for route_path, key_template in sorted(
        _rate_limit_route_map.items(), key=lambda x: len(x[0]), reverse=True
    ):
        # Split route into segments: /api/analysts/{ticker} → ["api","analysts","{ticker}"]
        route_parts = route_path.strip("/").split("/")
        path_parts = path.strip("/").split("/")

        if len(route_parts) != len(path_parts):
            continue

        params: dict[str, str] = {}
        matched = True
        for rp, pp in zip(route_parts, path_parts):
            if rp.startswith("{") and rp.endswith("}"):
                params[rp[1:-1]] = pp
            elif rp != pp:
                matched = False
                break

        if not matched:
            continue

        # Include query params (for macro endpoints like scorecard, breadth)
        for k, v in request.query_params.items():
            if k not in params:
                params[k] = v

        try:
            return key_template.format(**params)
        except KeyError:
            continue

    return None


def _try_l2_for_rate_limited(cache_key: str) -> str | None:
    """Attempt to serve stale L2 HTML (sync, run via to_thread)."""
    return _l2_get_html(cache_key)


# ═══════════════════════════════════════════════════════════════════════
# Exception handlers
# ═══════════════════════════════════════════════════════════════════════


# Pick the redesign error template when the v2 router is mounted (prod
# default since the cutover); fall back to the v1 ``error.html`` only
# for deploys that explicitly disable PP_REDESIGN_PREVIEW.  Both
# templates share the same ctx contract (``message`` + ``status_code``).
_ERROR_TEMPLATE = (
    "_redesign/error.html"
    if _redesign_preview_router.is_enabled()
    else "error.html"
)


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    # HTMX partial requests (api/ endpoints) get inline error fragments
    is_htmx = request.headers.get("HX-Request") == "true"

    if exc.status_code == 429:
        # For API/HTMX requests, try to serve stale L2 cached content instead
        # of a blank error — 12-hour-old data is better than nothing.
        if is_htmx or request.url.path.startswith("/api/"):
            cache_key = _match_cache_key(request)
            if cache_key:
                try:
                    stale_html = await asyncio.to_thread(
                        _try_l2_for_rate_limited, cache_key
                    )
                    if stale_html:
                        logger.info("Serving stale L2 for rate-limited %s", request.url.path)
                        return Response(content=stale_html.encode(), media_type="text/html")
                except Exception:
                    pass
            return templates.TemplateResponse(
                "partials/data_error.html",
                {
                    "request": request,
                    "error_type": "rate_limit",
                },
                status_code=429,
            )
        message = "Too many requests. Please slow down and try again in a minute."
    elif exc.status_code == 404:
        message = "The page you're looking for doesn't exist."
    else:
        message = exc.detail or "An unexpected error occurred."

    return templates.TemplateResponse(
        _ERROR_TEMPLATE,
        {
            "request":     request,
            "message":     message,
            "status_code": exc.status_code,
        },
        status_code=exc.status_code,
    )


@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled exception on %s %s", request.method, request.url.path)

    # HTMX partial requests get inline error fragments with one auto-retry.
    # The retry handles transient errors during the startup cache-warming
    # window (fund cache, market data, etc. load in the background).
    is_htmx = request.headers.get("HX-Request") == "true"
    if is_htmx or request.url.path.startswith("/api/"):
        # Only auto-retry once (check if this is already a retry)
        retry_count = int(request.headers.get("X-PP-Retry", "0"))
        retry_div = ""
        if retry_count < 1:
            retry_url = request.url.path
            if request.url.query:
                retry_url += f"?{request.url.query}"
            retry_div = (
                f'<div hx-get="{retry_url}" hx-trigger="load delay:4s" '
                f'hx-swap="outerHTML" hx-headers=\'{{"X-PP-Retry": "1"}}\'></div>'
            )
        return HTMLResponse(
            '<div class="data-error" style="text-align:center;padding:2em 1em;color:var(--pp-text-muted);">'
            '<p aria-busy="true" style="font-size:0.92em;">Loading...</p>'
            '</div>' + retry_div,
            status_code=500,
        )

    return templates.TemplateResponse(
        _ERROR_TEMPLATE,
        {
            "request":     request,
            "message":     "Something went wrong on our end. Please try again later.",
            "status_code": 500,
        },
        status_code=500,
    )


# ═══════════════════════════════════════════════════════════════════════
# Background tasks
# ═══════════════════════════════════════════════════════════════════════


async def _prefetch_market_data(app: FastAPI):
    """Two-phase market data prefetch for fast cold starts.

    Phase 1 (~2-5s): Load from Supabase → set market_data_ready = True
    immediately so users see (slightly stale) charts right away.

    Phase 2 (~30s, background): Refresh from yfinance → update memory
    caches + write back to Supabase for the next redeploy.
    """
    app.state.market_data_ready = False

    # ── Phase 1: Supabase warm (fast) ──
    try:
        warmed = await _to_heavy(market_data.warm_from_supabase)
        if warmed:
            app.state.market_data_ready = True
            logger.info("Phase 1 complete: market data ready from Supabase")
    except Exception as e:
        logger.warning("Supabase warm-load failed: %s", e)

    # ── Phase 1b: Tiingo warm-check (instant) ──
    try:
        from filings import tiingo
        await asyncio.to_thread(tiingo.warm_from_supabase)
    except Exception:
        pass

    # ── Phase 2: yfinance refresh (slow, runs regardless) ──
    try:
        await asyncio.gather(
            _to_heavy(market_data.get_sp500_market_data),
            _to_heavy(market_data.get_index_market_data),
        )
        app.state.market_data_ready = True
        logger.info("Phase 2 complete: market data refreshed from yfinance")
    except Exception as e:
        logger.warning("yfinance prefetch failed: %s", e)
        if not app.state.market_data_ready:
            app.state.market_data_ready = False

    # ── Phase 3: Warm remaining homepage widgets (parallel, best-effort) ──
    # Prevents cold-cache stampede when first user hits homepage after deploy.
    try:
        await asyncio.gather(
            _to_heavy(sentiment.get_retail_sentiment_overview),
            asyncio.to_thread(
                supabase_cache.get_recent_notifications, 7
            ),
            return_exceptions=True,
        )
        logger.info("Phase 3 complete: homepage widgets pre-warmed")
    except Exception as e:
        logger.debug("Homepage widget prefetch (non-critical): %s", e)


# ── Background 13F Refresh ────────────────────────────────────────────


async def _refresh_single_fund_async(app: FastAPI, cik: str) -> bool:
    """Refresh a single fund from SEC EDGAR in a background thread.

    Updates app.state.fund_cache on success.
    Returns True on success, False on failure.
    """
    if cik in _refresh_in_progress:
        return False  # Already being refreshed

    _refresh_in_progress.add(cik)
    try:
        # SEC EDGAR (via edgartools) is heavy, rate-limited, and locks
        # an httpcore HTTP/2 connection internally -- 12 concurrent
        # refreshes serialize on that lock and saturate the default
        # pool (caught in 2026-05-10 stack-trace diagnostic).  Route
        # through `to_heavy` so the heavy semaphore caps concurrent
        # SEC work at 8 and the default pool stays free for requests.
        data = await _to_heavy(cache.refresh_single_fund, cik, timeout=60.0)
        if data is not None:
            app.state.fund_cache[cik] = data
            logger.info("Background refresh OK: CIK %s (%s)", cik, data.get("name", ""))
            return True
        else:
            logger.warning("Background refresh returned None: CIK %s", cik)
            return False
    except Exception:
        logger.exception("Background refresh failed: CIK %s", cik)
        return False
    finally:
        _refresh_in_progress.discard(cik)


async def _background_refresh_sweep(app: FastAPI) -> None:
    """Sweep all stale funds and refresh them in the background.

    Uses per-CIK locks so request-triggered refreshes for *other* funds
    can still proceed concurrently while the sweep is running.
    """
    try:
        cache_data = app.state.fund_cache
        all_ciks = [si.cik for si in SUPERINVESTORS]
        stale_ciks = await asyncio.to_thread(
            cache.get_stale_ciks, cache_data, all_ciks
        )

        if not stale_ciks:
            logger.info("Background sweep: all %d funds fresh", len(all_ciks))
            app.state.refresh_status = "idle"
            return

        logger.info(
            "Background sweep: refreshing %d/%d stale funds",
            len(stale_ciks),
            len(all_ciks),
        )
        app.state.refresh_status = "running"
        app.state.refresh_progress = {
            "total": len(stale_ciks),
            "done": 0,
            "failed": 0,
        }

        for idx, cik in enumerate(stale_ciks):
            if not cache._check_sec_rate_limit():
                logger.warning(
                    "Background sweep: SEC session limit reached at %d/%d",
                    idx,
                    len(stale_ciks),
                )
                break

            # Acquire the per-CIK lock — if an on-demand refresh is already
            # running for this fund, skip it rather than waiting.
            lock = await _get_refresh_lock(cik)
            if lock.locked():
                logger.debug("Sweep skipping CIK %s (on-demand refresh in progress)", cik)
                app.state.refresh_progress["done"] += 1
                continue

            async with lock:
                success = await _refresh_single_fund_async(app, cik)

            if success:
                app.state.refresh_progress["done"] += 1
            else:
                app.state.refresh_progress["failed"] += 1

            # Rate limiting: 2s between funds
            if idx < len(stale_ciks) - 1:
                await asyncio.sleep(2)

            # Batch pause every 10 funds
            if (idx + 1) % cache._SEC_BATCH_SIZE == 0 and idx < len(stale_ciks) - 1:
                logger.info(
                    "Background sweep: batch pause at %d/%d",
                    idx + 1,
                    len(stale_ciks),
                )
                await asyncio.sleep(cache._SEC_BATCH_PAUSE)

        done = app.state.refresh_progress["done"]
        failed = app.state.refresh_progress["failed"]
        logger.info(
            "Background sweep complete: %d refreshed, %d failed", done, failed
        )
        app.state.refresh_status = "idle"

        # Cleanup stale per-CIK locks and progress tracking
        active_ciks = {si.cik for si in SUPERINVESTORS}
        async with _refresh_locks_mu:
            stale_keys = [k for k in _refresh_locks if k not in active_ciks]
            for k in stale_keys:
                _refresh_locks.pop(k, None)
        _refresh_in_progress.difference_update(
            _refresh_in_progress - active_ciks
        )

    except Exception:
        logger.exception("Background sweep crashed")
        app.state.refresh_status = "error"


async def _delayed_refresh_sweep(app: FastAPI) -> None:
    """Wait for initial startup to settle, then begin background sweep."""
    await asyncio.sleep(30)
    await _background_refresh_sweep(app)


async def _trigger_single_refresh(app: FastAPI, cik: str) -> None:
    """Request-triggered refresh for a single stale fund.

    Uses a per-CIK lock so it only blocks if *this specific fund* is
    already being refreshed (by the sweep or another request), never
    blocking refreshes for different funds.
    """
    try:
        lock = await _get_refresh_lock(cik)
        if lock.locked():
            logger.debug("Skipping on-demand refresh for CIK %s (already in progress)", cik)
            return
        async with asyncio.timeout(5):
            async with lock:
                if not cache._check_sec_rate_limit():
                    return
                await _refresh_single_fund_async(app, cik)
    except TimeoutError:
        logger.debug("On-demand refresh timed out for CIK %s", cik)
    except Exception:
        logger.debug("On-demand refresh failed for CIK %s", cik)


# ── Reddit velocity notification scanner ──────────────────────────────

async def _periodic_task(
    *, name: str, startup_delay: int, interval: int, body,
) -> None:
    """Run *body* on a recurring schedule with an initial startup delay.

    Replaces the duplicated ``await sleep / while True / try / except /
    sleep`` skeleton each background scanner used to copy.  Exceptions
    from *body* are logged and swallowed so one bad iteration never
    kills the loop.
    """
    await asyncio.sleep(startup_delay)
    while True:
        try:
            await body()
        except Exception as exc:
            logger.error("%s iteration failed: %s", name, exc)
        await asyncio.sleep(interval)


# ── Reddit velocity → notification scanner ──────────────────────────

_REDDIT_SCAN_INTERVAL = 30 * 60  # 30 minutes


async def _run_reddit_velocity_scan() -> None:
    """One iteration of the Reddit velocity scanner."""
    apewisdom = await _to_heavy(sentiment._get_apewisdom_all)
    if not apewisdom:
        return
    notif_rows: list[dict] = []
    for item in apewisdom:
        ticker = (item.get("ticker") or "").upper()
        if not ticker:
            continue
        mentions = int(item.get("mentions") or 0)
        mentions_24h = int(item.get("mentions_24h_ago") or 0)
        velocity_pct = ((mentions - mentions_24h) / mentions_24h) * 100 if mentions_24h > 0 else 0.0
        notif = notifications.create_reddit_notification(
            ticker=ticker,
            name=item.get("name", ticker),
            velocity_pct=velocity_pct,
            mentions=mentions,
        )
        if notif is not None:
            notif_rows.append(notif)

    if notif_rows:
        # Dedup handled by deterministic IDs (reddit-{ticker}-{date})
        n = await asyncio.to_thread(supabase_cache.upsert_notifications, notif_rows)
        if n:
            logger.info("Created %d Reddit velocity notifications", n)


# ── Feature announcement → notification scanner ─────────────────────

_FEATURE_ANNOUNCE_INTERVAL = 10 * 60  # 10 minutes


async def _run_feature_announcement_scan() -> None:
    """One iteration of the feature announcement scanner."""
    announcements = await asyncio.to_thread(
        supabase_cache.get_recent_feature_announcements
    )
    if not announcements:
        return
    notif_rows: list[dict] = []
    for ann in announcements:
        notif = notifications.create_feature_release_notification(ann)
        if notif is not None:
            notif_rows.append(notif)
    if notif_rows:
        n = await asyncio.to_thread(supabase_cache.upsert_notifications, notif_rows)
        if n:
            logger.info("Synced %d feature release notifications", n)


# ── Memory profile background poller ─────────────────────────────────

_MEMPROF_POLL_INTERVAL = 5 * 60  # 5 minutes — cheap enough; won't churn logs


async def _run_memprof_log() -> None:
    """One iteration of the memory poller.  Logs a single grep-friendly
    line per interval so Railway log search can chart RSS over time
    without us standing up a separate metrics pipeline.
    """
    from filings import memprof as _memprof
    state = getattr(app, "state", None)
    try:
        logger.info(_memprof.log_line(state))
    except Exception as exc:
        logger.debug("memprof poller failed: %s", exc)


# ── Periodic GC + glibc heap trim ────────────────────────────────────
# Python's pymalloc + glibc allocator both keep fragments around after
# a burst (request fan-out, cache warm).  Forcing a `gc.collect()` and
# then `malloc_trim(0)` every 2 min asks the kernel to take back free
# arenas, dropping RSS without affecting correctness.  Earlier prod
# logs showed a single cycle reclaiming ~1.2GB on a fragmented worker;
# running it 2.5x more often keeps less time at peak RSS between bursts.
# Cost: ~50ms wall-time per cycle on the heavy pool.

_GC_TRIM_INTERVAL = 2 * 60


async def _run_gc_trim() -> None:
    """One iteration of the gc/malloc_trim task."""
    import gc
    rss_before = -1
    rss_after = -1
    try:
        import psutil
        rss_before = psutil.Process().memory_info().rss // (1024 * 1024)
    except Exception:
        pass
    collected = await asyncio.to_thread(gc.collect)
    trimmed = -1
    try:
        import ctypes
        libc = ctypes.CDLL("libc.so.6")
        trimmed = libc.malloc_trim(0)
    except (OSError, AttributeError, FileNotFoundError):
        pass  # non-glibc platform (macOS, alpine musl, etc.)
    try:
        import psutil
        rss_after = psutil.Process().memory_info().rss // (1024 * 1024)
    except Exception:
        pass
    logger.info(
        "gc_trim collected=%d malloc_trim=%d rss=%dMB->%dMB",
        collected, trimmed, rss_before, rss_after,
    )


# ── Saturation watchdog ─────────────────────────────────────────────
# Originally framed as a thread-LEAK watchdog (catch threads piling up
# past pool capacity).  In practice the failure mode under crawler
# load isn't leak -- it's the heavy pool sitting at 100% utilisation
# with every slot busy on a slow rate-limited upstream.  Active-thread
# count stays at ~43 (= default 16 + heavy 20 + framework ~7) which is
# normal-pool-saturation, not leak.  Old threshold of 65 never fired
# in that scenario; site stayed effectively hung until manual recycle.
#
# New threshold: 50 -- just over the ~46 normal-pool-saturated max,
# so we fire when the pool has stayed FULL for the sustained window.
# Recovery is now ~2 min (4 × 30s) instead of "until I notice".  This
# does NOT prevent saturation -- crawler-driven cold-path traffic
# can still queue work indefinitely.  Backpressure on cold-tier
# requests is the durable follow-up; this just makes the symptom
# self-heal faster while we work on that.
#
# Reads `threading.active_count()` -- a free, sync, microsecond call,
# so the watchdog itself can't get stuck on a slow upstream.
#
# 2026-05-10 update: added a SECOND trigger -- default-pool saturation.
# The thread-count trigger never fired during that outage because the
# failure mode was 16/16 default-pool slots stuck on slow Supabase
# calls (with `_to_light_active=16` for sustained intervals) while
# total threads stayed in the 60s -- below the 50 threshold by
# construction, since `_DANGER_THREADS` measures the wrong thing for
# that mode.  We now SIGTERM if EITHER trigger trips: thread-leak
# (heavy-pool stuck) OR default-pool saturation (Supabase stuck).

_WATCHDOG_INTERVAL_S = 30
_DANGER_THREADS = 50         # ~4 over the saturated-pool max
_SUSTAIN_CHECKS = 4          # 4 × 30s = 2 min of sustained saturation
# Default-pool saturation trigger: when 90%+ of `to_light` slots are
# in flight for sustained intervals, the pool is starved -- almost
# always means Supabase calls aren't returning.  Health check can't
# acquire a slot, requests queue, watchdog must recycle.
_DEFAULT_POOL_DANGER_FRAC = 0.875   # 14/16 slots active

# Optional alert destinations fired right before the watchdog SIGTERMs.
# Both are independent -- you can set either, both, or neither:
#
# (a) Telegram bot (best for phone push notifications):
#       TELEGRAM_BOT_TOKEN  — from @BotFather (/newbot)
#       TELEGRAM_CHAT_ID    — your numeric chat id (see _send_watchdog_alert
#                             docstring for the lookup recipe)
#
# (b) Discord/Slack webhook URL:
#       WATCHDOG_ALERT_URL  — Discord (https://discord.com/api/webhooks/...)
#                             or Slack  (https://hooks.slack.com/services/...)
#
# When all are unset, alerting is silent (logs still fire normally).
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
WATCHDOG_ALERT_URL = os.environ.get("WATCHDOG_ALERT_URL", "").strip()


async def _send_watchdog_alert(active: int) -> None:
    """Best-effort POST to alert destinations before forcing exit.

    Both destinations (Telegram + generic webhook) fire in parallel
    when configured.  Each has a hard 5s timeout so a slow service
    can't delay SIGTERM (recovery > alert delivery).  Failures are
    logged and swallowed.

    To set up Telegram:
      1. In Telegram, open @BotFather and send /newbot.  Follow prompts;
         BotFather returns a token like "1234567:ABC-...".  Set this as
         TELEGRAM_BOT_TOKEN.
      2. Open a chat with your new bot and send any message.
      3. Visit https://api.telegram.org/bot<TOKEN>/getUpdates -- find
         "chat":{"id": <NNNN>} in the response.  Set <NNNN> as
         TELEGRAM_CHAT_ID.  (Use a negative id for a group chat.)
      4. Both env vars set on Railway -> alerts go to your phone.
    """
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID) and not WATCHDOG_ALERT_URL:
        return

    try:
        import psutil
        proc = psutil.Process()
        rss_mb = proc.memory_info().rss // (1024 * 1024)
        uptime_min = (time_module.time() - proc.create_time()) / 60
    except Exception:
        rss_mb = -1
        uptime_min = -1.0

    sustained_min = _SUSTAIN_CHECKS * _WATCHDOG_INTERVAL_S // 60

    # Telegram HTML formatting (cleaner than MarkdownV2, which would
    # require escaping every . _ * [ ] ( ) etc.).
    tg_text = (
        "🚨 <b>paperpanda.io watchdog: thread leak detected</b>\n"
        f"• active threads: <b>{active}</b> (danger ≥ {_DANGER_THREADS})\n"
        f"• sustained: {sustained_min} min\n"
        f"• rss: {rss_mb} MB · uptime: {uptime_min:.1f} min · pid: {os.getpid()}\n"
        f"• action: <i>SIGTERM imminent — gunicorn will recycle this worker</i>\n"
        f"• log search: <code>watchdog: active_threads=</code>"
    )

    # Discord uses standard markdown (`**bold**`, `*italic*`).
    # Slack uses its own "mrkdwn" (`*bold*`, `_italic_`).  They're
    # incompatible: Slack renders `**bold**` as literal asterisks.
    # Detect the destination and pick the right syntax.
    is_slack = "hooks.slack.com" in WATCHDOG_ALERT_URL.lower()
    if is_slack:
        md_text = (
            "🚨 *paperpanda.io watchdog: thread leak detected*\n"
            f"• active threads: *{active}* (danger ≥ {_DANGER_THREADS})\n"
            f"• sustained: {sustained_min} min\n"
            f"• rss: {rss_mb} MB · uptime: {uptime_min:.1f} min · pid: {os.getpid()}\n"
            f"• action: _SIGTERM imminent — gunicorn will recycle this worker_\n"
            f"• log search: `watchdog: active_threads=`"
        )
        webhook_payload = {"text": md_text}
    else:
        md_text = (
            "🚨 **paperpanda.io watchdog: thread leak detected**\n"
            f"• active threads: **{active}** (danger ≥ {_DANGER_THREADS})\n"
            f"• sustained: {sustained_min} min\n"
            f"• rss: {rss_mb} MB · uptime: {uptime_min:.1f} min · pid: {os.getpid()}\n"
            f"• action: *SIGTERM imminent — gunicorn will recycle this worker*\n"
            f"• log search: `watchdog: active_threads=`"
        )
        # Discord webhooks read `content`; generic webhooks may read `text`.
        webhook_payload = {"content": md_text, "text": md_text}

    import httpx
    tasks = []
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        tg_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        tg_payload = {
            "chat_id":    TELEGRAM_CHAT_ID,
            "text":       tg_text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        tasks.append(("telegram", tg_url, tg_payload))
    if WATCHDOG_ALERT_URL:
        tasks.append(("webhook", WATCHDOG_ALERT_URL, webhook_payload))

    async def _post(name: str, url: str, payload: dict) -> None:
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.post(url, json=payload)
                if resp.status_code >= 400:
                    logger.warning("watchdog: %s alert returned %d: %s",
                                   name, resp.status_code, resp.text[:200])
                else:
                    logger.info("watchdog: %s alert delivered", name)
        except Exception as exc:
            logger.warning("watchdog: %s alert POST failed: %s", name, exc)

    await asyncio.gather(
        *(_post(n, u, p) for n, u, p in tasks),
        return_exceptions=True,
    )


async def _run_thread_watchdog() -> None:
    """One iteration of the saturation watchdog.

    Two independent triggers; either one trips the SIGTERM:

    1. **Thread-leak / heavy-pool stuck** -- active thread count over
       `_DANGER_THREADS`.  Catches the original failure mode where
       slow upstreams left ghost threads piling up past pool capacity.

    2. **Default-pool saturation** -- `to_light` in-flight count at
       >= 90% of pool capacity.  Catches the 2026-05-10 mode where
       Supabase slowness pinned all 16 default workers, health check
       starved, watchdog (1) never fired because total threads stayed
       in the 60s (below 50 by definition -- saturation, not leak).

    Each trigger has its own sustained-counter; both feed the same
    SIGTERM path so gunicorn recycles the worker once either trigger
    fires for `_SUSTAIN_CHECKS` consecutive intervals.
    """
    global _watchdog_over_count, _watchdog_pool_over_count
    import threading as _threading
    from filings import concurrency as _concurrency

    try:
        active = _threading.active_count()
    except Exception:
        active = 0

    # Trigger 1: thread-leak / heavy-pool stuck.
    leak_trip = active >= _DANGER_THREADS
    if leak_trip:
        _watchdog_over_count += 1
        logger.warning(
            "watchdog: active_threads=%d >= %d (sustained %d/%d)",
            active, _DANGER_THREADS,
            _watchdog_over_count, _SUSTAIN_CHECKS,
        )
    else:
        if _watchdog_over_count > 0:
            logger.info(
                "watchdog: active_threads=%d back below danger (was %d cycles)",
                active, _watchdog_over_count,
            )
        _watchdog_over_count = 0

    # Trigger 2: default-pool saturation (Supabase stuck).
    try:
        in_flight = _concurrency.to_light_active()
        capacity = _concurrency.default_pool_capacity()
    except Exception:
        in_flight, capacity = 0, 0
    pool_trip = (
        capacity > 0
        and in_flight >= int(capacity * _DEFAULT_POOL_DANGER_FRAC)
    )
    if pool_trip:
        _watchdog_pool_over_count += 1
        logger.warning(
            "watchdog: default_pool_active=%d/%d (sustained %d/%d)",
            in_flight, capacity,
            _watchdog_pool_over_count, _SUSTAIN_CHECKS,
        )
    else:
        if _watchdog_pool_over_count > 0:
            logger.info(
                "watchdog: default_pool_active=%d/%d back below danger "
                "(was %d cycles)",
                in_flight, capacity, _watchdog_pool_over_count,
            )
        _watchdog_pool_over_count = 0

    # Either trigger sustained -> SIGTERM.
    leak_fire = _watchdog_over_count >= _SUSTAIN_CHECKS
    pool_fire = _watchdog_pool_over_count >= _SUSTAIN_CHECKS
    if not (leak_fire or pool_fire):
        return

    if leak_fire and pool_fire:
        reason = "thread leak + pool saturation"
    elif leak_fire:
        reason = "thread leak"
    else:
        reason = "default-pool saturation"
    # Snapshot of who's holding pool slots, so post-mortem doesn't
    # require log archaeology to figure out the saturator.
    try:
        diag = _concurrency.default_pool_diagnostic()
    except Exception:
        diag = {}
    logger.error(
        "watchdog: %s sustained for %d min — "
        "forcing graceful exit so gunicorn recycles this worker "
        "(threads=%d default_pool=%d/%d) diag=%s",
        reason,
        _SUSTAIN_CHECKS * _WATCHDOG_INTERVAL_S // 60,
        active, in_flight, capacity, diag,
    )
    # Fire-and-forget alert before SIGTERM.  Capped at 5s so a slow
    # webhook can't delay recovery; if it fails we still SIGTERM.
    try:
        await _send_watchdog_alert(active)
    except Exception as exc:
        logger.warning("watchdog: alert path threw: %s", exc)
    try:
        import os as _os
        import signal as _signal
        _os.kill(_os.getpid(), _signal.SIGTERM)
    except Exception as exc:
        logger.error("watchdog: SIGTERM failed: %s", exc)
    # Reset so we don't re-fire immediately if SIGTERM was ignored.
    _watchdog_over_count = 0
    _watchdog_pool_over_count = 0


_watchdog_over_count = 0
_watchdog_pool_over_count = 0


# ── /_v2/home L2 cache warmer ────────────────────────────────────────

_REDESIGN_L2_WARMER_INTERVAL = 4 * 60  # 4 min — refreshes before any 5-min TTL expires


async def _run_redesign_l2_warm() -> None:
    """One iteration of the redesign L2 cache warmer.

    Without this, the first /_v2/home request after a worker restart
    fills every L2 entry by hitting upstreams synchronously — that's
    what jammed the thread pool the first time we shipped /_v2/home.
    """
    status = await _redesign_preview_router.warm_l2_caches()
    logger.info(
        "redesign L2 warmer: warmed %d/%d (failed=%s)",
        status.get("warmed", 0),
        status.get("total", 0),
        status.get("failed", []),
    )


# ── Stock-page bundle warmer ─────────────────────────────────────────
# Walks tickers in hot+warm tiers, calls the bundle builder, writes
# the result to ``stock_overview_cache``.  Page handler reads from
# that table on the request path so popular tickers don't fan out 10+
# parallel sync upstream calls per render.

# 5 min cycle vs 10 min hot TTL keeps hot tickers strictly within TTL
# (a 10-min cycle = 10-min TTL meant hot tickers spent ~30-60s past
# their TTL between refreshes -- users hitting in that gap fell to
# the cold path and saw 4-5s waits).  Each cycle takes ~127s on prod,
# so 5 min budget has plenty of headroom; warmer occupies the heavy
# pool ~42% of the time vs ~21% before.
_STOCK_WARMER_INTERVAL_S = 5 * 60
# Per-tier batch size.  Sized so the hot tier (~50 tickers in steady
# state) refreshes fully in one cycle; otherwise stragglers drift to
# 2× the TTL window before the next cycle picks them up.  Confirmed
# empirically via /admin/stock-cache-status -- max_s started exceeding
# the 600s hot TTL when batch=30 and hot_count=36.
_STOCK_WARMER_BATCH_PER_TIER = 60
_STOCK_WARMER_CONCURRENCY = 3        # parallel ticker fetches per cycle

_STOCK_SEED_STARTUP_DELAY_S = 60     # let fund_cache finish loading first


# Telemetry of the most recent warmer cycle, exposed via
# /admin/stock-cache-status so we can spot when the warmer falls
# behind without needing Railway log access.  Updated atomically at
# end of each cycle from a single coroutine, so no lock needed.
_LAST_WARMER_CYCLE: dict = {
    "ts":          None,    # ISO timestamp of last cycle completion
    "duration_s":  0.0,
    "batch":       0,
    "refreshed":   0,       # successful upserts
    "failed":      0,       # build or write failures
    "by_tier":     {},      # {"hot": {"refreshed": N, "failed": M}, ...}
}


def get_last_warmer_cycle() -> dict:
    """Return a copy of the most-recent cycle's telemetry."""
    return dict(_LAST_WARMER_CYCLE)


async def _run_initial_tier_seed() -> None:
    """One-shot startup seeder for ``stock_overview_cache`` tiers.

    Reads ``app.state.fund_cache`` and assigns a hot/warm tier to the
    most-held tickers across all 13F filings.  Idempotent -- existing
    rows are not re-seeded, so this can run on every worker startup
    without overwriting tier promotions/demotions.

    Wrapped in a delay so the cache hydration on startup completes
    first; without that the seeder would no-op on an empty fund_cache.
    """
    await asyncio.sleep(_STOCK_SEED_STARTUP_DELAY_S)
    from filings import stock_bundle as _bundle

    fund_cache = getattr(app.state, "fund_cache", {}) or {}
    if not fund_cache:
        logger.info("stock seed: skipping (fund_cache empty after %ds)",
                    _STOCK_SEED_STARTUP_DELAY_S)
        return
    try:
        await asyncio.to_thread(_bundle.populate_initial_tiers, fund_cache)
    except Exception as exc:
        logger.warning("stock seed: failed: %s", exc)


async def _run_stock_warmer() -> None:
    """One iteration of the stock-bundle warmer.

    Pulls the oldest-stale batch from every warmable tier, then
    refreshes them all in a single bounded-concurrency gather.
    Combining tiers (rather than processing hot then warm
    sequentially) keeps the cycle inside the 10-min interval --
    sequential would have run hot's batch fully before warm started,
    risking budget overrun under slow upstreams.

    Each ticker fans out ~10 heavy calls against the HEAVY_CONCURRENCY
    semaphore.  Concurrency=3 lets one ticker's post-fetch work
    (JSON-build + Supabase upsert) pipeline against the next ticker's
    fetches without piling extra contention onto the semaphore.

    Recently-failed tickers are excluded by ``get_stale_tickers``'s
    failure-backoff filter -- a broken upstream can't drive a retry
    loop here.
    """
    from filings import stock_bundle as _bundle
    from filings.routers.redesign_preview import build_stock_data_bundle

    fund_cache = getattr(app.state, "fund_cache", {}) or {}
    cycle_started = time_module.monotonic()

    targets: list[tuple[str, str]] = []
    for tier in _bundle.WARMABLE_TIERS:
        stale = await asyncio.to_thread(
            _bundle.get_stale_tickers, tier, _STOCK_WARMER_BATCH_PER_TIER,
        )
        targets.extend((t, tier) for t in stale)

    if not targets:
        _LAST_WARMER_CYCLE.update({
            "ts":         datetime.now(timezone.utc).isoformat(),
            "duration_s": round(time_module.monotonic() - cycle_started, 2),
            "batch":      0, "refreshed": 0, "failed": 0, "by_tier": {},
        })
        logger.info("stock warmer: cycle done batch=0 refreshed=0 failed=0")
        return

    sem = asyncio.Semaphore(_STOCK_WARMER_CONCURRENCY)

    async def _refresh_one(t: str, tier_local: str) -> bool:
        async with sem:
            try:
                bundle, source_status = await build_stock_data_bundle(
                    fund_cache, t,
                )
            except Exception as exc:
                logger.warning(
                    "stock warmer: build %s (%s) failed: %s",
                    t, tier_local, exc,
                )
                await asyncio.to_thread(
                    _bundle.record_attempt, t, status=_bundle.STATUS_FAILED,
                )
                return False
            return await asyncio.to_thread(
                _bundle.set_bundle,
                t, bundle,
                tier=tier_local,
                source_status=source_status,
            )

    results = await asyncio.gather(
        *(_refresh_one(t, tier) for t, tier in targets),
        return_exceptions=True,
    )

    # Per-tier breakdown for the telemetry + log line.
    by_tier: dict[str, dict[str, int]] = {}
    for (_t, tier), result in zip(targets, results):
        bucket = by_tier.setdefault(tier, {"refreshed": 0, "failed": 0})
        if result is True:
            bucket["refreshed"] += 1
        else:
            bucket["failed"] += 1

    refreshed_total = sum(b["refreshed"] for b in by_tier.values())
    failed_total    = sum(b["failed"]    for b in by_tier.values())
    duration_s = round(time_module.monotonic() - cycle_started, 2)

    _LAST_WARMER_CYCLE.update({
        "ts":         datetime.now(timezone.utc).isoformat(),
        "duration_s": duration_s,
        "batch":      len(targets),
        "refreshed":  refreshed_total,
        "failed":     failed_total,
        "by_tier":    by_tier,
    })

    tier_summary = " ".join(
        f"{tier}={b['refreshed']}/{b['refreshed'] + b['failed']}"
        for tier, b in sorted(by_tier.items())
    )
    logger.info(
        "stock warmer: cycle done batch=%d refreshed=%d failed=%d duration=%.1fs (%s)",
        len(targets), refreshed_total, failed_total, duration_s, tier_summary,
    )


# ═══════════════════════════════════════════════════════════════════════
# Pages
# ═══════════════════════════════════════════════════════════════════════

# --- Health check (for UptimeRobot / load balancers) ---


_health_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="health")
_startup_ts: float = 0.0  # set in lifespan
_STARTUP_GRACE = 300  # seconds: don't report unhealthy during cold start + sweep


@app.api_route("/health", methods=["GET", "HEAD"])
async def health_check():
    """Health check that detects thread pool starvation.

    During the startup grace period (90s), always returns OK so Railway
    doesn't restart us mid-warmup.  After that, probes the default pool
    with a retry to distinguish temporary load from true starvation.
    """
    import time as _time

    # During cold start, yfinance downloads saturate the pool temporarily.
    # Don't report unhealthy — Railway would restart us in a loop.
    if _startup_ts and (_time.time() - _startup_ts) < _STARTUP_GRACE:
        return JSONResponse({"status": "ok", "warming": True})

    try:
        # Probe the DEFAULT pool (the one user requests use)
        await asyncio.wait_for(
            asyncio.to_thread(lambda: True), timeout=3
        )
    except (asyncio.TimeoutError, Exception):
        # Default pool is saturated — retry with longer timeout
        try:
            await asyncio.wait_for(
                asyncio.to_thread(lambda: True), timeout=5
            )
        except (asyncio.TimeoutError, Exception):
            # Dump a per-callsite snapshot so we can see WHICH functions
            # are holding pool slots when the pool starves.  See
            # concurrency.default_pool_diagnostic() docstring for fields.
            try:
                from filings import concurrency as _concurrency
                diag = _concurrency.default_pool_diagnostic()
            except Exception:
                diag = {}
            logger.error(
                "health_check: default thread pool starved — returning 503; "
                "diag=%s", diag,
            )
            return JSONResponse(
                {"status": "unhealthy", "reason": "thread pool exhausted"},
                status_code=503,
            )
        # Recovered on retry — pool was temporarily busy but not stuck
        logger.warning("health_check: default pool slow but recovered on retry")

    return JSONResponse({"status": "ok"})


# --- Ticker logos (self-hosted, served from memory) ---


@app.get("/api/logo/{ticker}.png")
async def serve_logo(ticker: str):
    """Serve a company logo PNG.

    Checks bounded LRU first, then fetches single row from Supabase on
    cache miss.  Browser caches for 1 year (immutable).
    """
    t = ticker.upper()
    data = _logo_cache.get(t)
    if data:
        _logo_cache.move_to_end(t)  # touch for LRU
        return Response(
            content=data,
            media_type="image/png",
            headers={"Cache-Control": "public, max-age=31536000, immutable"},
        )
    # Not in LRU — check if it exists in DB at all
    if t not in _logo_set:
        return Response(status_code=404)
    # On-demand fetch from Supabase
    fetched = await asyncio.to_thread(supabase_cache.get_single_logo, t)
    if not fetched:
        return Response(status_code=404)
    _lru_put(_logo_cache, t, fetched, _LOGO_LRU_MAX)
    return Response(
        content=fetched,
        media_type="image/png",
        headers={"Cache-Control": "public, max-age=31536000, immutable"},
    )


_logo_populate_status: dict = {}  # shared progress dict for background task


# ── Well-known ticker → domain overrides ───────────────────────────────
# Covers top S&P 500 companies whose domains can't be guessed from issuer names.
_KNOWN_TICKER_DOMAINS: dict[str, str] = {
    "GOOGL": "google.com", "GOOG": "google.com",
    "META": "meta.com",
    "BRK.A": "berkshirehathaway.com", "BRK.B": "berkshirehathaway.com",
    "BRK": "berkshirehathaway.com",
    "UNH": "unitedhealthgroup.com",
    "PG": "pg.com",
    "HD": "homedepot.com",
    "CVX": "chevron.com",
    "KO": "coca-cola.com",
    "PEP": "pepsico.com",
    "MRK": "merck.com",
    "ABBV": "abbvie.com",
    "WMT": "walmart.com",
    "DIS": "disney.com",
    "CSCO": "cisco.com",
    "CRM": "salesforce.com",
    "NFLX": "netflix.com",
    "ADBE": "adobe.com",
    "CMCSA": "comcast.com",
    "INTC": "intel.com",
    "QCOM": "qualcomm.com",
    "TXN": "ti.com",
    "AVGO": "broadcom.com",
    "COST": "costco.com",
    "TMO": "thermofisher.com",
    "DHR": "danaher.com",
    "PM": "pmi.com",
    "RTX": "rtx.com",
    "NEE": "nexteraenergy.com",
    "LIN": "linde.com",
    "LOW": "lowes.com",
    "BMY": "bms.com",
    "SPGI": "spglobal.com",
    "ISRG": "intuitive.com",
    "GS": "goldmansachs.com",
    "BLK": "blackrock.com",
    "AXP": "americanexpress.com",
    "SYK": "stryker.com",
    "MDLZ": "mondelezinternational.com",
    "GILD": "gilead.com",
    "ADI": "analog.com",
    "BKNG": "booking.com",
    "VRTX": "vrtx.com",
    "REGN": "regeneron.com",
    "PANW": "paloaltonetworks.com",
    "LRCX": "lamresearch.com",
    "KLAC": "kla.com",
    "BSX": "bostonscientific.com",
    "CB": "chubb.com",
    "MMC": "marshmclennan.com",
    "CI": "cigna.com",
    "ZTS": "zoetis.com",
    "SNPS": "synopsys.com",
    "CDNS": "cadence.com",
    "PYPL": "paypal.com",
    "CME": "cmegroup.com",
    "HCA": "hcahealthcare.com",
    "MCK": "mckesson.com",
    "WM": "wm.com",
    "GEV": "gevernova.com",
    "MAR": "marriott.com",
    "APH": "amphenol.com",
    "MSI": "motorolasolutions.com",
    "USB": "usbank.com",
    "PLTR": "palantir.com",
    "F": "ford.com",
    "GM": "gm.com",
    "CAT": "caterpillar.com",
    "BA": "boeing.com",
    "DE": "deere.com",
    "MMM": "3m.com",
    "SLB": "slb.com",
    "EOG": "eogresources.com",
    "WFC": "wellsfargo.com",
    "BAC": "bankofamerica.com",
    "C": "citigroup.com",
    "T": "att.com",
    "VZ": "verizon.com",
    "TMUS": "t-mobile.com",
    "SO": "southerncompany.com",
    "DUK": "duke-energy.com",
    "COP": "conocophillips.com",
    "UBER": "uber.com",
    "SPOT": "spotify.com",
    "SQ": "squareup.com",
    "SHOP": "shopify.com",
    "SNOW": "snowflake.com",
    "ABNB": "airbnb.com",
    "COIN": "coinbase.com",
    "HOOD": "robinhood.com",
    "RIVN": "rivian.com",
    "LCID": "lucidmotors.com",
    "DDOG": "datadoghq.com",
    "NET": "cloudflare.com",
    "CRWD": "crowdstrike.com",
    "ZS": "zscaler.com",
    "TEAM": "atlassian.com",
    "MDB": "mongodb.com",
    "OKTA": "okta.com",
    "TTD": "thetradedesk.com",
    "RBLX": "roblox.com",
    "U": "unity.com",
    "DASH": "doordash.com",
    "LYFT": "lyft.com",
    "PINS": "pinterest.com",
    "SNAP": "snapchat.com",
    "TWLO": "twilio.com",
    "ROKU": "roku.com",
    "Z": "zillow.com",
    "FICO": "fico.com",
    "LLY": "lilly.com",
    "PFE": "pfizer.com",
    "ABT": "abbott.com",
    "JNJ": "jnj.com",
    "NKE": "nike.com",
    "SBUX": "starbucks.com",
    "MCD": "mcdonalds.com",
    "TGT": "target.com",
    "YUM": "yum.com",
    "CMG": "chipotle.com",
    "ADP": "adp.com",
    "FIS": "fisglobal.com",
    "ICE": "ice.com",
    "MRVL": "marvell.com",
    "ON": "onsemi.com",
    "AMAT": "appliedmaterials.com",
    "MU": "micron.com",
    "NOW": "servicenow.com",
    "ORCL": "oracle.com",
    "SAP": "sap.com",
    "INTU": "intuit.com",
    "WDAY": "workday.com",
    "VEEV": "veeva.com",
    "SPLK": "splunk.com",
    "FTNT": "fortinet.com",
    "DELL": "dell.com",
    "HPQ": "hp.com",
    "HPE": "hpe.com",
}


_ISSUER_SUFFIXES = (
    " INC", " CORP", " CORPORATION", " CO", " COMPANY", " LTD",
    " LLC", " PLC", " GROUP", " HOLDINGS", " ENTERPRISES",
    " INTERNATIONAL", " TECHNOLOGIES", " TECHNOLOGY",
    " THERAPEUTICS", " PHARMACEUTICALS", " PHARMA",
    " SOLUTIONS", " SYSTEMS", " PARTNERS", " CAPITAL", " FINANCIAL",
    " BANCORP", " BANCSHARES", " BRANDS", " INDUSTRIES",
    " CLASS A", " CLASS B", " CLASS C", " CL A", " CL B", " CL C",
    " COMMON", " NEW", " DEL", " COM", " SHS",
)


def _guess_domains(issuer_name: str, ticker: str = "") -> list[str]:
    """Guess company domains from 13F issuer name and ticker symbol.

    Uses endswith-based suffix stripping (not replace) to avoid
    mid-string matches like "AMAZON COM" → "AMAZONM".
    """
    # 1. Check well-known mapping first
    if ticker and ticker in _KNOWN_TICKER_DOMAINS:
        return [_KNOWN_TICKER_DOMAINS[ticker]]

    # 2. Strip corporate suffixes from END of name only (fixes the replace bug)
    name = issuer_name.upper().strip()
    changed = True
    while changed:
        changed = False
        for suffix in _ISSUER_SUFFIXES:
            if name.endswith(suffix):
                name = name[: -len(suffix)].strip()
                changed = True
                break

    # Remove punctuation
    name = _re.sub(r"[^A-Z0-9\s]", "", name).strip()
    words = name.lower().split()
    if not words:
        return [f"{ticker.lower()}.com"] if ticker else []

    guesses: list[str] = []
    # Try first-word.com (e.g., "APPLE" → "apple.com")
    guesses.append(f"{words[0]}.com")
    # Try joined-words.com (e.g., "HOME DEPOT" → "homedepot.com")
    if len(words) >= 2:
        guesses.append(f"{''.join(words[:2])}.com")
        # If 3+ words, also try all words joined
        if len(words) >= 3:
            guesses.append(f"{''.join(words[:3])}.com")

    return guesses


async def _populate_logos_task(limit: int = 200):
    """Background task: guess domains from issuer names + download favicons.

    Uses Google Favicons API.  No yfinance calls — lightweight HTTP only.
    Processes at most ``limit`` new tickers per invocation.
    Insert-only — existing rows in ticker_logos are NEVER modified.
    """
    if _http_pool is None:
        logger.warning("_populate_logos_task: _http_pool not initialized, skipping")
        return

    status = _logo_populate_status
    status.update({"phase": "collecting", "downloaded": 0, "failed": 0, "total": 0})

    _valid_ticker = _re.compile(r"^[A-Z]{1,6}$")

    try:
        # 1. Collect ticker -> issuer_name + count how many funds hold each
        ticker_names: dict[str, str] = {}
        ticker_fund_count: dict[str, int] = {}  # popularity score
        fc = _fund_cache()
        for fund_data in fc.values():
            seen_this_fund: set[str] = set()
            for h in fund_data.get("all_holdings", []):
                t = h.get("ticker")
                name = h.get("issuer")
                if t and name and _valid_ticker.match(t.upper()):
                    tu = t.upper()
                    ticker_names.setdefault(tu, name)
                    if tu not in seen_this_fund:
                        ticker_fund_count[tu] = ticker_fund_count.get(tu, 0) + 1
                        seen_this_fund.add(tu)

        # 2. Skip tickers already in Supabase, sort by popularity (most held first)
        existing = await asyncio.to_thread(supabase_cache.get_existing_logo_tickers)
        all_new = sorted(
            set(ticker_names.keys()) - existing,
            key=lambda t: ticker_fund_count.get(t, 0),
            reverse=True,
        )
        new_tickers = all_new[:limit]

        status.update({
            "phase": "downloading",
            "total": len(new_tickers),
            "already_in_db": len(existing),
            "remaining_after_this": max(0, len(all_new) - limit),
        })

        if not new_tickers:
            status.update({"phase": "done", "message": "All tickers already processed"})
            return

        # 3. Download logos via Google Favicons API
        _FAVICON_URL = "https://www.google.com/s2/favicons?domain={domain}&sz=128"
        # Google returns HTTP 404 for unknown domains.
        # For valid domains, even small favicons (Visa=338B, AMD=183B) are real.
        # Threshold of 50 bytes filters only truly empty/broken responses.
        _MIN_LOGO_BYTES = 50

        rows_to_insert: list[dict] = []
        downloaded = 0
        failed = 0

        for ticker in new_tickers:
            issuer = ticker_names.get(ticker, "")
            domains_to_try = _guess_domains(issuer, ticker=ticker)

            found = False
            for domain in domains_to_try:
                try:
                    resp = await _http_pool.get(_FAVICON_URL.format(domain=domain))
                    if resp.status_code == 200 and len(resp.content) > _MIN_LOGO_BYTES:
                        b64 = _b64.b64encode(resp.content).decode("ascii")
                        ct = resp.headers.get("content-type", "image/png")
                        rows_to_insert.append({
                            "ticker": ticker,
                            "logo_b64": b64,
                            "content_type": ct,
                            "logo_domain": domain,
                        })
                        _lru_put(_logo_cache, ticker, resp.content, _LOGO_LRU_MAX)
                        _logo_set.add(ticker)
                        downloaded += 1
                        found = True
                        break
                    elif downloaded == 0 and failed < 3:
                        # Log first few failures for debugging
                        logger.info(
                            "Logo miss: %s domain=%s status=%s size=%d",
                            ticker, domain, resp.status_code, len(resp.content),
                        )
                except Exception as exc:
                    if downloaded == 0 and failed < 3:
                        logger.warning("Logo fetch error: %s domain=%s: %s", ticker, domain, exc)
                await asyncio.sleep(0.05)

            if not found:
                failed += 1

            # Batch insert every 25 (insert-only, never overwrites)
            if len(rows_to_insert) >= 25:
                await asyncio.to_thread(supabase_cache.insert_logos, rows_to_insert)
                rows_to_insert.clear()

            status.update({"downloaded": downloaded, "failed": failed})
            await asyncio.sleep(0.05)

        if rows_to_insert:
            await asyncio.to_thread(supabase_cache.insert_logos, rows_to_insert)

        templates.env.globals["logo_tickers"] = _logo_set

        status.update({
            "phase": "done",
            "downloaded": downloaded,
            "failed": failed,
            "in_memory": len(_logo_cache),
        })
        logger.info("Logo populate done: %d downloaded, %d failed, %d in memory",
                    downloaded, failed, len(_logo_cache))

    except Exception as exc:
        status.update({"phase": "error", "error": str(exc)})
        logger.error("Logo populate failed: %s", exc)


_MEMPROF_TOKEN = os.environ.get("MEMPROF_TOKEN", "").strip()


def _require_admin_token(request: Request) -> Response | None:
    """Token-gate an /admin/* endpoint.

    Returns a 404 Response when the token is unset or doesn't match,
    or ``None`` when the request is authorised (caller proceeds).
    Centralises the gate so the same env-var + query-param check
    isn't repeated across every admin endpoint.
    """
    if not _MEMPROF_TOKEN:
        return PlainTextResponse("Not Found", status_code=404)
    supplied = (request.query_params.get("token") or "").strip()
    if supplied != _MEMPROF_TOKEN:
        return PlainTextResponse("Not Found", status_code=404)
    return None


@app.get("/admin/stock-cache-status")
async def admin_stock_cache_status(request: Request):
    """Aggregated health view of the stock_overview_cache pipeline.

    Returns request-path hit/miss counters, tier distribution,
    bundle-age stats per tier (spots warmer-falling-behind), the
    most recent warmer cycle's telemetry, and any tickers whose
    last warmer attempt failed.

    Token-gated -- safe to expose because it leaks only counts and
    aggregates, no payload data.
    """
    if (gate := _require_admin_token(request)) is not None:
        return gate

    from filings import stock_bundle as _bundle

    # Each helper hits Supabase; run them in parallel rather than
    # serially -- the sequential version was ~250-750ms wall time
    # for the admin endpoint (5 round-trips back-to-back).
    distribution, age_hot, age_warm, age_cold, failures = await asyncio.gather(
        asyncio.to_thread(_bundle.get_tier_distribution),
        asyncio.to_thread(_bundle.get_tier_age_stats, _bundle.HOT_TIER),
        asyncio.to_thread(_bundle.get_tier_age_stats, _bundle.WARM_TIER),
        asyncio.to_thread(_bundle.get_tier_age_stats, _bundle.COLD_TIER),
        asyncio.to_thread(_bundle.get_recently_failed, 20),
    )

    return JSONResponse({
        "request_metrics":    _bundle.get_request_metrics(),
        "tier_distribution":  distribution,
        "tier_age_stats": {
            _bundle.HOT_TIER:  age_hot,
            _bundle.WARM_TIER: age_warm,
            _bundle.COLD_TIER: age_cold,
        },
        "last_warmer_cycle":  get_last_warmer_cycle(),
        "recently_failed":    failures,
    })


@app.get("/admin/tier-seed")
async def admin_tier_seed(
    request: Request,
    hot: int = Query(50, ge=1, le=500),
    warm: int = Query(200, ge=0, le=2000),
):
    """Manually re-run the stock-bundle tier seeder.

    Token-gated; reads ``app.state.fund_cache`` and assigns hot/warm
    tier classifications to the most-held tickers.  Idempotent --
    existing rows are not overwritten, so promotion/demotion done by
    other paths (warmer, future tier-promoter) is preserved.

    Useful for tweaking the size of each tier without restarting the
    worker, e.g. ``?hot=100&warm=400`` to widen coverage.
    """
    if (gate := _require_admin_token(request)) is not None:
        return gate

    from filings import stock_bundle as _bundle
    fund_cache = getattr(app.state, "fund_cache", {}) or {}
    if not fund_cache:
        return PlainTextResponse(
            "fund_cache empty -- can't seed.  Wait for startup hydration.",
            status_code=503,
        )
    result = await asyncio.to_thread(
        _bundle.populate_initial_tiers, fund_cache,
        hot_count=hot, warm_count=warm,
    )
    return JSONResponse({
        "hot_seeded":  len(result.get("hot_seeded", [])),
        "warm_seeded": len(result.get("warm_seeded", [])),
        "skipped":     result.get("skipped", 0),
        "hot_sample":  result.get("hot_seeded", [])[:10],
        "warm_sample": result.get("warm_seeded", [])[:10],
    })


@app.get("/admin/watchdog-test")
async def admin_watchdog_test(request: Request):
    """Token-gated end-to-end test of the watchdog alert pipeline.

    Calls ``_send_watchdog_alert`` with a synthetic ``active`` value so
    the configured destinations (Telegram / Slack / Discord webhook)
    actually receive a message -- without triggering the SIGTERM that
    real watchdog firing does.  Use this to verify the alert chain
    (env vars set, webhook URL valid, app reachable from prod, etc.).
    """
    if (gate := _require_admin_token(request)) is not None:
        return gate

    import threading as _threading
    real_active = _threading.active_count()
    # Use a synthetic "active" that's clearly above threshold so the
    # alert payload doesn't read as a true incident.
    await _send_watchdog_alert(active=real_active + 1000)

    dests = []
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        dests.append("telegram")
    if WATCHDOG_ALERT_URL:
        dests.append("webhook")
    if not dests:
        return PlainTextResponse(
            "No alert destinations configured.  Set TELEGRAM_BOT_TOKEN+"
            "TELEGRAM_CHAT_ID and/or WATCHDOG_ALERT_URL.",
            status_code=500,
        )
    return PlainTextResponse(
        f"Alert dispatched to: {', '.join(dests)}.\n"
        f"(real active_threads={real_active}; payload reports +1000 "
        "as a synthetic test marker).\n"
        "Check the destination channel — message should land within "
        "a few seconds.\n"
        "If nothing arrives: check Railway logs for 'watchdog: ... "
        "alert POST failed' or 'returned 4xx/5xx'."
    )


@app.get("/admin/memprof-diag")
async def admin_memprof_diag(request: Request):
    """Diagnostic memprof — non-/api/ path so the global HTMX retry
    shim doesn't intercept errors.  Plain-text response, every step
    wrapped, returns whatever it got plus exception text per failure.
    """
    if (gate := _require_admin_token(request)) is not None:
        return gate

    import traceback as _tb
    lines: list[str] = ["=== memprof diagnostic ==="]

    def step(label, fn):
        try:
            r = fn()
            lines.append(f"[OK]   {label}: {repr(r)[:300]}")
        except Exception as exc:
            lines.append(f"[FAIL] {label}: {repr(exc)}")
            lines.append(_tb.format_exc())

    def _import_memprof():
        from filings import memprof as _m
        return _m

    step("import memprof", _import_memprof)
    try:
        from filings import memprof as _m
        step("process_metrics()", lambda: _m.process_metrics())
        step("type_counts(top_n=5)", lambda: _m.type_counts(5)[:3])
        step("cache_metrics() len", lambda: len(_m.cache_metrics()))
        step("app.state attributes", lambda: [k for k in _m._STATE_KEYS if hasattr(app.state, k)])
        step("fund_cache len", lambda: len(getattr(app.state, "fund_cache", {})))
        step("_approx_size(fund_cache)", lambda: _m._approx_size(getattr(app.state, "fund_cache", {})))
        step("snapshot(with_types=False, with_state_sizes=False)",
             lambda: list(_m.snapshot(app.state, with_types=False, with_state_sizes=False).keys()))
    except Exception as exc:
        lines.append(f"[FATAL] {repr(exc)}")
        lines.append(_tb.format_exc())

    return PlainTextResponse("\n".join(lines))


@app.get("/admin/pool-diag")
async def admin_pool_diag(request: Request):
    """Snapshot of default-pool work for diagnosing saturation.

    Returns JSON: tracked_active, queue_depth, top_callers (top 10 by
    in-flight count), max_workers, plus the standard heavy_pool_status.
    Token-gated.  Safe to expose -- only counts and function names,
    no payload data.

    Useful for:
      * polling during a load-test to watch what saturates first
      * comparing tracked_active vs queue_depth to spot untracked
        saturators (raw asyncio.to_thread, framework internals)
    """
    if (gate := _require_admin_token(request)) is not None:
        return gate

    from filings import concurrency as _concurrency
    return JSONResponse({
        "pool_status": _concurrency.heavy_pool_status(),
        "default_pool": _concurrency.default_pool_diagnostic(top_n=10),
    })


@app.get("/api/admin/memprof")
async def admin_memprof(request: Request):
    """JSON memory snapshot -- process / cache / app.state / type histogram.

    Token-gated via the ``MEMPROF_TOKEN`` env var.  Endpoint returns 404
    when the token is unset or doesn't match.

    Query knobs (all default to "include"):
      ``types=0``  -- skip the ``gc.get_objects()`` type histogram
                     (fastest savings; that walk dominates on large
                     processes)
      ``sizes=0``  -- skip the recursive ``_approx_size`` walks on
                     ``app.state`` objects (still report dict / seq
                     lengths)
      ``lite=1``   -- shorthand for ``types=0&sizes=0`` -- returns
                     process metrics + cache stats + app.state lengths
                     in <50ms

    Snapshot work runs in a thread so the event loop stays free for
    other requests.
    """
    if (gate := _require_admin_token(request)) is not None:
        return gate

    qp = request.query_params
    lite = (qp.get("lite") or "0").strip() == "1"
    with_types = (qp.get("types") or "1").strip() != "0" and not lite
    with_state_sizes = (qp.get("sizes") or "1").strip() != "0" and not lite

    # Try each component in isolation so a single broken piece can't
    # blow up the whole snapshot.  Returns whatever it could collect
    # plus an "errors" dict naming the components that failed.
    import traceback as _tb
    from filings import memprof as _memprof

    def _safe(name, fn, *args, **kwargs):
        try:
            return fn(*args, **kwargs), None
        except Exception as exc:
            return None, {"name": name, "error": repr(exc),
                          "traceback": _tb.format_exc().splitlines()[-6:]}

    def _collect():
        out: dict = {"ts": time_module.time(), "errors": []}
        proc, err = _safe("process", _memprof.process_metrics)
        if err: out["errors"].append(err)
        else:   out["process"] = proc
        caches, err = _safe("caches", _memprof.cache_metrics)
        if err: out["errors"].append(err)
        else:   out["caches"] = caches
        if with_state_sizes:
            state, err = _safe("app_state", _memprof.app_state_metrics, app.state)
            if err: out["errors"].append(err)
            else:   out["app_state"] = state
        else:
            # Cheap variant — already inlined in memprof.snapshot but
            # duplicated here for full isolation.
            rows = []
            try:
                for key in _memprof._STATE_KEYS:
                    if not hasattr(app.state, key):
                        continue
                    val = getattr(app.state, key)
                    entry: dict = {"name": f"app.state.{key}"}
                    if isinstance(val, dict):
                        entry["dict_len"] = len(val)
                    elif isinstance(val, (list, tuple)):
                        entry["seq_len"] = len(val)
                    rows.append(entry)
            except Exception as exc:
                out["errors"].append({"name": "app_state_lite", "error": repr(exc)})
            out["app_state"] = rows
        if with_types:
            tc, err = _safe("type_counts", _memprof.type_counts, 30)
            if err: out["errors"].append(err)
            else:   out["type_counts"] = tc
        return out

    try:
        payload = await asyncio.to_thread(_collect)
    except Exception as exc:
        # Last-resort: return JSON directly so the global /api/* HTML
        # error handler doesn't replace our error message.
        return Response(
            content=json_module.dumps({"error": repr(exc),
                                       "traceback": _tb.format_exc().splitlines()[-10:]}),
            media_type="application/json",
            status_code=500,
        )
    return JSONResponse(payload)


@app.get("/api/admin/populate-logos")
async def populate_logos(request: Request, limit: int = Query(200, ge=1, le=500)):
    """Kick off background logo population. Returns immediately with status.

    ?limit=N controls how many new tickers to process (default 200, max 500).
    Call repeatedly to process all tickers in chunks.
    Call /api/admin/logo-status to check progress.
    Existing logos are NEVER overwritten (insert-only).
    """
    if _logo_populate_status.get("phase") in ("collecting", "resolving_domains", "downloading"):
        return JSONResponse({
            "status": "already_running",
            **_logo_populate_status,
        })

    _logo_populate_status.clear()
    asyncio.create_task(_populate_logos_task(limit=limit), name="populate_logos")

    return JSONResponse({
        "status": "started",
        "message": "Logo population started in background. Check /api/admin/logo-status for progress.",
    })


@app.get("/api/admin/logo-status")
async def logo_status():
    """Check the progress of the background logo population task."""
    return JSONResponse({
        "in_memory": len(_logo_cache),
        **_logo_populate_status,
    })


# --- Congress headshots (self-hosted, served from memory) ----------


@app.get("/api/headshot/{member_id}.jpg")
async def serve_headshot(member_id: str):
    """Serve a congress member headshot JPEG.

    Checks bounded LRU first, then fetches from Supabase on miss.
    Browser caches for 1 year (immutable).
    """
    data = _headshot_cache.get(member_id)
    if data:
        _headshot_cache.move_to_end(member_id)
        return Response(
            content=data,
            media_type="image/jpeg",
            headers={"Cache-Control": "public, max-age=31536000, immutable"},
        )
    if member_id not in _headshot_set:
        return Response(status_code=404)
    fetched = await asyncio.to_thread(supabase_cache.get_single_headshot, member_id)
    if not fetched:
        return Response(status_code=404)
    _lru_put(_headshot_cache, member_id, fetched, _HEADSHOT_LRU_MAX)
    return Response(
        content=fetched,
        media_type="image/jpeg",
        headers={"Cache-Control": "public, max-age=31536000, immutable"},
    )


@app.get("/api/analyst-photo/{analyst_id}.jpg")
async def serve_analyst_photo(analyst_id: str):
    """Serve an analyst headshot JPEG.

    Checks bounded LRU → Supabase → TipRanks CDN fallback.
    Browser caches for 1 year (immutable).
    """
    data = _analyst_photo_cache.get(analyst_id)
    if data:
        _analyst_photo_cache.move_to_end(analyst_id)
        return Response(
            content=data,
            media_type="image/jpeg",
            headers={"Cache-Control": "public, max-age=31536000, immutable"},
        )

    # LRU miss — try Supabase first (fast)
    if analyst_id in _analyst_photo_set:
        fetched = await asyncio.to_thread(
            supabase_cache.get_single_analyst_photo, analyst_id
        )
        if fetched:
            _lru_put(_analyst_photo_cache, analyst_id, fetched, _ANALYST_PHOTO_LRU_MAX)
            return Response(
                content=fetched,
                media_type="image/jpeg",
                headers={"Cache-Control": "public, max-age=31536000, immutable"},
            )

    # Supabase miss — try to download from TipRanks CDN on-demand
    try:
        from filings import analyst_scraper as _as

        photo_bytes = await asyncio.to_thread(_as.download_analyst_photo, analyst_id)
        if photo_bytes:
            _lru_put(
                _analyst_photo_cache, analyst_id, photo_bytes, _ANALYST_PHOTO_LRU_MAX
            )
            _analyst_photo_set.add(analyst_id)
            # Persist to DB in background
            _sb = supabase_cache._get_client()
            if _sb:
                asyncio.create_task(
                    asyncio.to_thread(
                        _as.save_analyst_photos, {analyst_id: photo_bytes}, _sb
                    )
                )
            return Response(
                content=photo_bytes,
                media_type="image/jpeg",
                headers={"Cache-Control": "public, max-age=31536000, immutable"},
            )
    except Exception as exc:
        logger.debug("On-demand analyst photo fetch failed for %s: %s", analyst_id, exc)

    return Response(status_code=404)


_headshot_populate_status: dict = {}  # shared progress dict


async def _fetch_wikipedia_headshot(
    http, member_name: str, min_bytes: int = 500,
    chamber: str = "", state: str = "",
) -> bytes | None:
    """Try to fetch a headshot from Wikipedia by member name.

    Strategy (tries in order):
      1. Exact title: ``<Name> (politician)`` then ``<Name>``
      2. Search API: ``<Name> <state> <role>`` — resolves disambiguation
         and verifies the page is categorised as a politician.

    Returns raw image bytes, or None on failure.
    """
    import urllib.parse as _urlparse

    _TITLE_API = (
        "https://en.wikipedia.org/w/api.php"
        "?action=query&titles={title}&prop=pageimages"
        "&format=json&pithumbsize=300&redirects=1"
    )
    _SEARCH_API = (
        "https://en.wikipedia.org/w/api.php"
        "?action=query&list=search&srsearch={q}"
        "&srnamespace=0&srlimit=5&format=json"
    )
    _PAGE_API = (
        "https://en.wikipedia.org/w/api.php"
        "?action=query&pageids={pid}&prop=pageimages|categories"
        "&format=json&pithumbsize=300"
    )
    _POL_KEYWORDS = ("politician", "congress", "senator", "representative", "member")

    async def _download_thumb(thumb_url: str) -> bytes | None:
        try:
            img_resp = await http.get(thumb_url)
            if img_resp.status_code == 200 and len(img_resp.content) > min_bytes:
                return img_resp.content
        except Exception:
            pass
        return None

    try:
        # ── Strategy 1: exact title match ──
        for suffix in (" (politician)", ""):
            title = _urlparse.quote(member_name + suffix)
            resp = await http.get(_TITLE_API.format(title=title))
            if resp.status_code != 200:
                continue
            data = resp.json()
            pages = data.get("query", {}).get("pages", {})
            for page in pages.values():
                thumb_url = page.get("thumbnail", {}).get("source")
                if thumb_url:
                    img = await _download_thumb(thumb_url)
                    if img:
                        return img
            await asyncio.sleep(0.1)

        # ── Strategy 2: search API with state/role disambiguation ──
        role = "senator" if chamber == "Senate" else "representative"
        queries = []
        if state:
            queries.append(f"{member_name} {state} {role}")
            queries.append(f"{member_name} {state} politician")
        queries.append(f"{member_name} congressman")

        for q in queries:
            resp = await http.get(_SEARCH_API.format(q=_urlparse.quote(q)))
            if resp.status_code != 200:
                continue
            results = resp.json().get("query", {}).get("search", [])
            for r in results:
                pid = str(r["pageid"])
                resp2 = await http.get(_PAGE_API.format(pid=pid))
                if resp2.status_code != 200:
                    continue
                page = resp2.json().get("query", {}).get("pages", {}).get(pid, {})
                thumb_url = page.get("thumbnail", {}).get("source")
                if not thumb_url:
                    continue
                # Verify this is actually a politician page
                cats = [c.get("title", "").lower() for c in page.get("categories", [])]
                is_pol = any(kw in cat for cat in cats for kw in _POL_KEYWORDS)
                if is_pol:
                    img = await _download_thumb(thumb_url)
                    if img:
                        return img
            await asyncio.sleep(0.1)

    except Exception:
        pass
    return None


async def _populate_headshots_task(limit: int = 200):
    """Background task: download congress member headshots.

    Source chain (tries in order):
      1. GitHub Pages: unitedstates.github.io/images/congress/225x275/{id}.jpg
      2. GitHub Pages: unitedstates.github.io/images/congress/original/{id}.jpg
      3. Wikipedia API: pageimages thumbnail by member name

    Processes at most ``limit`` new members per invocation.
    Insert-only — existing rows in congress_headshots are NEVER modified.
    """
    if _http_pool is None:
        logger.warning("_populate_headshots_task: _http_pool not initialized, skipping")
        return

    status = _headshot_populate_status
    status.update({"phase": "collecting", "downloaded": 0, "failed": 0, "total": 0})

    _GH_225 = "https://unitedstates.github.io/images/congress/225x275/{member_id}.jpg"
    _GH_ORIG = "https://unitedstates.github.io/images/congress/original/{member_id}.jpg"
    _MIN_PHOTO_BYTES = 500  # real headshots are ~10-30KB

    try:
        # 1. Get all known member_ids from congress_members table
        all_members = await asyncio.to_thread(supabase_cache.get_all_congress_members)
        if not all_members:
            status.update({"phase": "done", "message": "No congress members found"})
            return

        member_ids = [m["member_id"] for m in all_members if m.get("member_id")]
        member_lookup = {m["member_id"]: m for m in all_members}

        # 2. Skip members already in Supabase, prioritise current members
        existing = await asyncio.to_thread(supabase_cache.get_existing_headshot_members)
        all_new = sorted(
            set(member_ids) - existing,
            key=lambda mid: (
                not member_lookup.get(mid, {}).get("is_current", False),
                -(member_lookup.get(mid, {}).get("total_trades", 0)),
            ),
        )
        new_members = all_new[:limit]

        status.update({
            "phase": "downloading",
            "total": len(new_members),
            "already_in_db": len(existing),
            "remaining_after_this": max(0, len(all_new) - limit),
        })

        if not new_members:
            status.update({"phase": "done", "message": "All members already processed"})
            return

        # 3. Download headshots — try GitHub Pages, then Wikipedia
        rows_to_insert: list[dict] = []
        downloaded = 0
        failed = 0
        wiki_hits = 0

        for member_id in new_members:
            photo_bytes: bytes | None = None
            source = ""

            # Try GitHub Pages 225x275
            try:
                resp = await _http_pool.get(_GH_225.format(member_id=member_id))
                if resp.status_code == 200 and len(resp.content) > _MIN_PHOTO_BYTES:
                    photo_bytes = resp.content
                    source = "github-225"
            except Exception:
                pass

            # Try GitHub Pages original
            if not photo_bytes:
                try:
                    resp = await _http_pool.get(_GH_ORIG.format(member_id=member_id))
                    if resp.status_code == 200 and len(resp.content) > _MIN_PHOTO_BYTES:
                        photo_bytes = resp.content
                        source = "github-orig"
                except Exception:
                    pass

            # Try Wikipedia API by full name + chamber/state
            if not photo_bytes:
                member_data = member_lookup.get(member_id, {})
                full_name = member_data.get("full_name", "")
                if full_name:
                    photo_bytes = await _fetch_wikipedia_headshot(
                        _http_pool, full_name, _MIN_PHOTO_BYTES,
                        chamber=member_data.get("chamber", ""),
                        state=member_data.get("state", ""),
                    )
                    if photo_bytes:
                        source = "wikipedia"
                        wiki_hits += 1

            if photo_bytes:
                b64 = _b64.b64encode(photo_bytes).decode("ascii")
                ct = "image/jpeg"
                rows_to_insert.append({
                    "member_id": member_id,
                    "photo_b64": b64,
                    "content_type": ct,
                })
                _lru_put(_headshot_cache, member_id, photo_bytes, _HEADSHOT_LRU_MAX)
                _headshot_set.add(member_id)
                downloaded += 1
                if downloaded <= 5 or source == "wikipedia":
                    logger.info("Headshot OK: %s source=%s", member_id, source)
            else:
                failed += 1
                if failed <= 5:
                    name = member_lookup.get(member_id, {}).get("full_name", "?")
                    logger.info("Headshot miss (all sources): %s (%s)", member_id, name)

            # Batch insert every 25
            if len(rows_to_insert) >= 25:
                await asyncio.to_thread(supabase_cache.insert_headshots, rows_to_insert)
                rows_to_insert.clear()

            status.update({"downloaded": downloaded, "failed": failed, "wiki_hits": wiki_hits})
            await asyncio.sleep(0.05)

        if rows_to_insert:
            await asyncio.to_thread(supabase_cache.insert_headshots, rows_to_insert)

        templates.env.globals["headshot_members"] = _headshot_set

        status.update({
            "phase": "done",
            "downloaded": downloaded,
            "failed": failed,
            "wiki_hits": wiki_hits,
            "in_memory": len(_headshot_cache),
        })
        logger.info(
            "Headshot populate done: %d downloaded (%d from wiki), %d failed, %d in memory",
            downloaded, wiki_hits, failed, len(_headshot_cache),
        )

    except Exception as exc:
        status.update({"phase": "error", "error": str(exc)})
        logger.error("Headshot populate failed: %s", exc)


@app.get("/api/admin/populate-headshots")
async def populate_headshots(request: Request, limit: int = Query(200, ge=1, le=600)):
    """Kick off background headshot population. Returns immediately with status.

    ?limit=N controls how many new members to process (default 200, max 600).
    Call repeatedly to process all members in chunks.
    Existing headshots are NEVER overwritten (insert-only).
    """
    if _headshot_populate_status.get("phase") in ("collecting", "downloading"):
        return JSONResponse({
            "status": "already_running",
            **_headshot_populate_status,
        })

    _headshot_populate_status.clear()
    asyncio.create_task(_populate_headshots_task(limit=limit), name="populate_headshots")

    return JSONResponse({
        "status": "started",
        "message": "Headshot population started in background. Check /api/admin/headshot-status for progress.",
    })


@app.get("/api/admin/headshot-status")
async def headshot_status():
    """Check the progress of the background headshot population task."""
    return JSONResponse({
        "in_memory": len(_headshot_cache),
        **_headshot_populate_status,
    })


@app.get("/api/admin/sync-announcements")
async def sync_announcements():
    """Manually trigger feature announcement → notification sync.

    Use after adding a row in the Supabase dashboard to push it
    immediately instead of waiting for the 10-minute scanner.
    """
    announcements = await asyncio.to_thread(
        supabase_cache.get_recent_feature_announcements
    )
    notif_rows: list[dict] = []
    for ann in announcements:
        notif = notifications.create_feature_release_notification(ann)
        if notif is not None:
            notif_rows.append(notif)
    n = 0
    if notif_rows:
        n = await asyncio.to_thread(
            supabase_cache.upsert_notifications, notif_rows
        )
    return JSONResponse({
        "status": "ok",
        "announcements_found": len(announcements),
        "notifications_synced": n,
    })


@app.post("/api/admin/backfill-revenue")
async def admin_backfill_revenue(request: Request, index: str = "sp500"):
    """Backfill revenue data for all index constituents.

    Iterates over tickers, calling Finnhub (primary) / FMP (fallback)
    per-symbol, and updates ``earnings_history`` rows that have NULL
    revenue columns.  Runs in the heavy thread pool.
    """
    from filings import earnings_scorecard

    result = await _to_heavy(earnings_scorecard.backfill_revenue, index)
    return JSONResponse(result)


# --- Homepage: dashboard with market data & widgets ---


# Panda Fund stats cache — homepage is the site's highest-traffic endpoint.
# Without this, every homepage hit re-queries Supabase `supporters` for the
# running monthly total.  Donation data changes at human pace (minutes
# between donations), so a 5-min TTL trades at most a few seconds of
# staleness on the progress bar for N× fewer Supabase reads.
# Keyed by (month) so the cache auto-invalidates at month boundaries —
# otherwise we'd serve last month's total for up to 5 min into the new month.
_panda_fund_cache: tuple[float, str, dict] | None = None  # (ts, YYYY-MM, data)
_PANDA_FUND_TTL = 300  # 5 minutes


async def _get_panda_fund_stats() -> dict:
    """Return Panda Fund donation stats for the current month.

    Shared by the homepage and /support page to avoid duplicate logic.
    Cached in-memory for 5 minutes, keyed by current YYYY-MM.
    """
    global _panda_fund_cache
    now = time_module.time()
    current_month = datetime.now().strftime("%Y-%m")
    if _panda_fund_cache is not None:
        ts, cached_month, data = _panda_fund_cache
        if cached_month == current_month and now - ts < _PANDA_FUND_TTL:
            return data

    monthly_goal = _PANDA_FUND_MONTHLY_GOAL
    raised_cents = await asyncio.to_thread(
        supabase_cache.get_monthly_raised_cents, current_month
    )
    if raised_cents > 0:
        raw_raised = raised_cents // 100
    else:
        raw_raised = int(os.environ.get("PANDA_FUND_RAISED", "0"))
    raised = min(raw_raised, monthly_goal)
    pct = min(100, round(raised / monthly_goal * 100)) if monthly_goal else 0

    result = {"raised": raised, "goal": monthly_goal, "pct": pct}
    _panda_fund_cache = (now, current_month, result)
    return result


# Legacy v1 home — UNREACHABLE in production.  The redesign router is
# registered before this handler (see `app.include_router` above), so
# v2's `preview_home` wins the `/` path conflict.  Kept in the repo for
# historical reference only; can be removed once v1 cleanup lands.
@app.api_route("/", methods=["GET", "HEAD"], response_class=HTMLResponse)
async def homepage(request: Request):
    pf = await _get_panda_fund_stats()
    return templates.TemplateResponse(
        "home.html",
        {
            "request": request,
            "panda_raised": pf["raised"],
            "panda_goal": pf["goal"],
            "panda_pct": pf["pct"],
            "stripe_publishable_key": _STRIPE_PUBLISHABLE_KEY,
            "stripe_configured": bool(_STRIPE_SECRET_KEY and _STRIPE_PUBLISHABLE_KEY),
            "price_onetime": _STRIPE_PRICE_ONETIME,
            "price_bamboo": _STRIPE_PRICE_BAMBOO,
            "price_panda": _STRIPE_PRICE_PANDA,
            "price_giant_panda": _STRIPE_PRICE_GIANT_PANDA,
        },
    )


# --- Superinvestors: portfolio list ---


_LEGACY_VIEW_ALIAS = {
    "funds":      "Funds",
    "holdings":   "Holdings",
    "activity":   "Activity",
    "deployment": "Capital Deployed",
}


@app.get("/_v2/{path:path}")
async def legacy_v2_prefix_redirect(request: Request, path: str):
    """Backward-compat: /_v2/X → /X (the redesign is now mounted at root)."""
    suffix = "?" + str(request.url.query) if request.url.query else ""
    target = f"/{path}{suffix}" if path else f"/{suffix}".rstrip("/") or "/"
    return RedirectResponse(url=target, status_code=301)


@app.get("/home")
async def home_redirect(request: Request):
    """Backward-compat: /home → / (v2 home is mounted at root)."""
    return RedirectResponse(url="/", status_code=301)


@app.get("/superinvestors", response_class=HTMLResponse)
async def superinvestors_page(request: Request):
    """Backward-compat redirect to /funds."""
    return RedirectResponse(url="/funds", status_code=301)


@app.get("/grand-portfolio", response_class=HTMLResponse)
async def grand_portfolio_redirect(request: Request):
    """Backward-compat redirect: /grand-portfolio → /funds."""
    view = _LEGACY_VIEW_ALIAS.get(request.query_params.get("view", "").lower(), "Funds")
    from urllib.parse import quote
    return RedirectResponse(url=f"/funds?view={quote(view)}", status_code=301)


# --- Lazy-load a single fund row (HTMX) ---


@app.get("/api/fund-row/{cik}", response_class=HTMLResponse)
async def fund_row(request: Request, cik: str):
    if not _valid_cik(cik):
        return PlainTextResponse("Invalid CIK", status_code=400)
    cik_normalized = cik.lstrip("0") or cik
    si = SUPERINVESTORS_BY_CIK.get(cik_normalized) or SUPERINVESTORS_BY_CIK.get(cik)

    cached = await _get_fund_data(cik)

    if cached:
        # Trigger background refresh if this fund is stale
        if (
            _ENABLE_BACKGROUND_REFRESH
            and cache.is_fund_stale(cached)
            and cik_normalized not in _refresh_in_progress
        ):
            asyncio.create_task(_trigger_single_refresh(app, cik_normalized))

        top_tickers = _top_tickers(cached)
        return templates.TemplateResponse(
            "partials/fund_row.html",
            {
                "request": request,
                "si": si,
                "data": cached,
                "top_tickers": top_tickers,
            },
        )

    # Cache miss: data not yet synced by the background worker
    return templates.TemplateResponse(
        "partials/fund_row_error.html",
        {
            "request": request,
            "si": si,
            "error": "Data not yet synced. It will be available after the next sync cycle.",
        },
    )


# --- Enhanced Holdings page ---


@app.get("/holdings/{cik}", response_class=HTMLResponse)
async def holdings_redirect(request: Request, cik: str):
    """Backward-compat redirect: /holdings/{cik} → /funds/{cik}."""
    return RedirectResponse(url=f"/funds/{cik}", status_code=301)


# Legacy v1 holdings handler — UNREACHABLE after the redirect above.
# Kept for historical reference only.
async def _legacy_v1_holdings_unused(request: Request, cik: str, top_n: int = Query(25, ge=1, le=200)):
    if not _valid_cik(cik):
        return PlainTextResponse("Invalid CIK", status_code=400)
    si = SUPERINVESTORS_BY_CIK.get(cik)
    cached = await _get_fund_data(cik)

    if cached:
        # ── Cache hit: build from stored data (zero SEC calls) ──
        fund, holdings_list = client.get_enriched_holdings_from_cache(
            cached, cik, top_n
        )
    else:
        # ── Cache miss: data not yet synced (no live SEC fallback) ──
        return templates.TemplateResponse(
            "error.html",
            {
                "request": request,
                "message": f"Data for this fund (CIK {cik}) has not been synced yet. "
                "It will be available after the next automatic sync cycle.",
            },
            status_code=404,
        )

    # Build quarterly changes with ticker enrichment
    # Merge hot quarters (from Supabase Postgres) with cold quarters
    # (from Supabase Storage archive) for full historical view.
    quarterly_changes = []
    if cached:
        raw_quarters = list(cached.get("quarterly_changes", []))

        # Load cold (archived) quarters from Supabase Storage
        try:
            cold_quarters = await asyncio.to_thread(cache.load_historical_quarters, cik)
            if cold_quarters:
                seen_periods = {q.get("period") for q in raw_quarters}
                for cq in cold_quarters:
                    if cq.get("period") not in seen_periods:
                        raw_quarters.append(cq)
                        seen_periods.add(cq.get("period"))
        except Exception:
            pass  # Cold storage unavailable -- show hot quarters only

        # Build cusip->ticker lookup from all_holdings
        cusip_to_ticker: dict[str, str | None] = {}
        for h in cached.get("all_holdings", []):
            if h.get("cusip"):
                cusip_to_ticker[h["cusip"]] = h.get("ticker")
        # Also check changes for additional cusips
        for q in raw_quarters:
            for c in q.get("changes", []):
                if c.get("cusip") and c["cusip"] not in cusip_to_ticker:
                    cusip_to_ticker[c["cusip"]] = None

        for q in raw_quarters:
            enriched_changes = []
            for c in q.get("changes", []):
                enriched = dict(c)
                enriched["ticker"] = cusip_to_ticker.get(c.get("cusip"))
                enriched_changes.append(enriched)
            quarterly_changes.append(
                {
                    "period": q.get("period", ""),
                    "report_period": q.get("report_period", ""),
                    "filing_date": q.get("filing_date", ""),
                    "changes": enriched_changes,
                }
            )

    # ── Build SEO summary for server-rendered content ──
    seo_summary: dict = {}
    if holdings_list:
        total_value = fund.total_value if fund else 0
        seo_summary["portfolio_value"] = total_value
        seo_summary["num_holdings"] = fund.total_holdings if fund else len(holdings_list)
        seo_summary["report_period"] = fund.report_period if fund else ""
        # Top 5 holdings text
        top5 = holdings_list[:5]
        seo_summary["top_holdings"] = [
            {
                "ticker": h.ticker,
                "issuer": h.issuer_name,
                "pct": h.pct_of_portfolio,
            }
            for h in top5
        ]
        # Activity counts
        new_buys = sum(1 for h in holdings_list if h.activity == "NEW BUY")
        sold = sum(1 for h in holdings_list if h.activity == "SOLD")
        adds = sum(1 for h in holdings_list if h.activity == "ADD")
        reduces = sum(1 for h in holdings_list if h.activity == "REDUCE")
        seo_summary["new_buys"] = new_buys
        seo_summary["sold"] = sold
        seo_summary["adds"] = adds
        seo_summary["reduces"] = reduces

    return templates.TemplateResponse(
        "investor.html",
        {
            "request": request,
            "fund": fund,
            "holdings": holdings_list,
            "top_n": top_n,
            "investor_name": si.display_name if si else None,
            "quarterly_changes": quarterly_changes,
            "seo_summary": seo_summary,
        },
    )


# --- Compare page (redirects to investor page) ---


@app.get("/compare/{cik}")
async def compare(request: Request, cik: str):
    if not _valid_cik(cik):
        return PlainTextResponse("Invalid CIK", status_code=400)
    return RedirectResponse(url=f"/holdings/{cik}", status_code=301)


# --- Compare API (lazy-loaded into investor page Compare tab) ---


@app.get("/api/compare/{cik}", response_class=HTMLResponse)
async def compare_api(request: Request, cik: str, top_n: int = Query(25, ge=1, le=200)):
    if not _valid_cik(cik):
        return PlainTextResponse("Invalid CIK", status_code=400)
    cached = await _get_fund_data(cik)

    if cached:
        # ── Cache hit: reconstruct comparison from stored data ──
        current, previous, changes = client.get_compare_from_cache(cached, cik, top_n)
        if previous is None:
            return templates.TemplateResponse(
                "partials/compare_content.html",
                {
                    "request": request,
                    "error": "Only one quarter available — nothing to compare yet.",
                },
            )
    else:
        # ── Cache miss: data not yet synced (no live SEC fallback) ──
        return templates.TemplateResponse(
            "partials/compare_content.html",
            {
                "request": request,
                "error": "Data not yet synced. Comparison will be available after the next sync cycle.",
            },
        )

    return templates.TemplateResponse(
        "partials/compare_content.html",
        {
            "request": request,
            "current": current,
            "previous": previous,
            "changes": changes,
        },
    )


# --- Portfolio Pie Chart Data (lazy-loaded into investor page) ---


@app.get("/api/portfolio-chart/{cik}")
async def portfolio_chart_data(request: Request, cik: str):
    """Return top-10 holdings with cross-investor ownership counts.

    Used by the ECharts donut on the investor profile page.
    """
    if not _valid_cik(cik):
        return JSONResponse({"error": "Invalid CIK"}, status_code=400)
    cached = await _get_fund_data(cik)

    if not cached:
        return JSONResponse(content=[])

    # Build ownership map (cached per fund_cache version)
    ownership_map = _get_ownership_map()

    # Get current investor's display name to exclude from "also held by"
    si = SUPERINVESTORS_BY_CIK.get(cik)
    current_name = si.display_name if si else None

    # Build changes lookup (cusip -> change dict)
    change_by_cusip: dict[str, dict] = {}
    for ch in cached.get("changes", []):
        change_by_cusip[ch["cusip"]] = ch

    # Period label from latest quarterly_changes
    period = ""
    qc = cached.get("quarterly_changes", [])
    if qc:
        period = qc[0].get("period", "")

    # Activity label mapping
    status_labels = {
        "NEW": "NEW BUY",
        "CLOSED": "SOLD",
        "INCREASED": "ADD",
        "DECREASED": "REDUCE",
    }

    # Top 10 holdings by value
    all_h = cached.get("all_holdings", [])
    top_10 = all_h[:10]
    result = []
    for h in top_10:
        ticker = h.get("ticker") or ""
        pct = h.get("pct", 0)
        cusip = h.get("cusip", "")

        # Activity from changes
        ch = change_by_cusip.get(cusip, {})
        raw_status = ch.get("status", "")
        activity = status_labels.get(raw_status, "Unchanged")

        # Cross-investor owners (exclude current investor)
        t_upper = ticker.upper() if ticker else ""
        owners = ownership_map.get(t_upper, [])
        other_owners = [n for n in owners if n != current_name]

        result.append(
            {
                "ticker": ticker or cusip[:6],
                "issuer": h.get("issuer", ""),
                "pct": round(pct, 2),
                "value": h.get("value", 0),
                "activity": activity,
                "quarter": period,
                "also_held_by": len(other_owners),
                "owner_names": other_owners[:5],
            }
        )

    return JSONResponse(content=result)


# --- Activity Feed ---


@app.get("/activity", response_class=HTMLResponse)
async def activity_feed(request: Request):
    """Backward-compat redirect: /activity → /funds?view=Activity."""
    return RedirectResponse(url="/funds?view=Activity", status_code=301)


# --- Top Funds (formerly Grand Portfolio) ---


@app.get("/funds", response_class=HTMLResponse)
async def funds_page(request: Request, view: str = "funds"):
    if view not in ("funds", "holdings", "activity", "deployment"):
        view = "funds"

    cache_data = _fund_cache()

    # ── Build superinvestor summaries (for the Superinvestors tab) ──
    si_summaries = []
    for si in SUPERINVESTORS:
        cached = cache_data.get(si.cik)
        if cached:
            top_tickers = _top_tickers(cached)
            si_summaries.append(
                SuperinvestorSummary(
                    cik=si.cik,
                    display_name=si.display_name,
                    fund_name=cached.get("name", si.fund_name),
                    portfolio_value=cached.get("total_value", 0),
                    num_holdings=cached.get("total_holdings", 0),
                    top_holdings=top_tickers,
                    report_period=cached.get("report_period", ""),
                    filing_date=cached.get("filing_date", ""),
                )
            )
        else:
            si_summaries.append(None)

    if not cache_data:
        return templates.TemplateResponse(
            "grand_portfolio.html",
            {
                "request": request,
                "entries": [],
                "empty": True,
                "consensus_json": "[]",
                "momentum_json": "[]",
                "view": view,
                "superinvestors": SUPERINVESTORS,
                "summaries": si_summaries,
                "cache_age": cache.get_cache_age_str(cache_data),
            },
        )

    # Run both CPU-bound computations concurrently via the heavy pool.
    # `build_most_added_table` triggers a yfinance fetch for 503 S&P
    # tickers when the close_df cache is cold; without `to_heavy`
    # routing it lands on the default pool and starves health checks
    # (2026-05-10 saturation pattern).
    entries, most_added = await asyncio.gather(
        _to_heavy(client.build_grand_portfolio, cache_data, SUPERINVESTORS_BY_CIK, timeout=30.0),
        _to_heavy(market_data.build_most_added_table, cache_data, SUPERINVESTORS_BY_CIK, timeout=60.0),
    )

    # ── Build Consensus Leaders data (top 10 by holder count) ──
    consensus_data = []
    for e in entries[:10]:
        top_holders = e.holders[:3]
        consensus_data.append(
            {
                "ticker": e.ticker or e.cusip[:6],
                "issuer": e.issuer_name,
                "holders": e.num_holders,
                "avg_weight": round(e.avg_weight, 2) if hasattr(e, "avg_weight") else 0,
                "top_holders": top_holders,
                "combined_value": e.combined_value,
                "link": f"/stock/{e.ticker}" if e.ticker else None,
            }
        )

    # Cross-reference: tickers in both consensus and momentum
    consensus_tickers = {d["ticker"].upper() for d in consensus_data if d["ticker"]}
    momentum_data = []
    for ma in most_added[:15]:
        ticker = ma.get("ticker") or (ma.get("cusip", "")[:6])
        is_trending = ticker.upper() in consensus_tickers if ticker else False
        momentum_data.append(
            {
                "ticker": ticker,
                "issuer": ma.get("issuer_name", ""),
                "add_count": ma.get("add_count", 0),
                "adders": ma.get("adders", []),
                "total_value": ma.get("total_value", 0),
                "is_trending": is_trending,
                "link": f"/stock/{ma['ticker']}" if ma.get("ticker") else None,
            }
        )

    # Activity feed is now lazy-loaded via HTMX → /api/activity-feed

    return templates.TemplateResponse(
        "grand_portfolio.html",
        {
            "request": request,
            "entries": entries[:100],
            "consensus_json": json_module.dumps(consensus_data),
            "momentum_json": json_module.dumps(momentum_data),
            "view": view,
            "superinvestors": SUPERINVESTORS,
            "summaries": si_summaries,
            "cache_age": cache.get_cache_age_str(cache_data),
        },
    )


# --- Stock Detail ---


@app.get("/stock/cusip/{cusip}", response_class=HTMLResponse)
async def stock_detail_by_cusip(request: Request, cusip: str):
    if not _valid_cusip(cusip):
        return PlainTextResponse("Invalid CUSIP", status_code=400)
    cache_data = _fund_cache()

    # Try to build superinvestor ownership data (may be None).
    # Parallel: build_stock_history only produces non-empty output when the
    # CUSIP is held by a superinvestor, same condition as build_stock_detail
    # returning a non-None value.  Running both concurrently on the heavy
    # pool halves user latency (~500ms -> ~250ms) at the cost of one extra
    # thread slot, which the heavy semaphore absorbs.
    detail = None
    history = []
    if cache_data:
        detail, history = await asyncio.gather(
            _to_heavy(
                client.build_stock_detail,
                cusip, cache_data, SUPERINVESTORS_BY_CIK, by_cusip=True,
            ),
            _to_heavy(
                client.build_stock_history,
                cusip, cache_data, SUPERINVESTORS_BY_CIK, by_cusip=True,
            ),
        )
        if not detail:
            history = []  # discard if stock isn't held by any superinvestor

    # Build stock identity from detail or minimal CUSIP info
    if detail:
        stock_info = StockInfo(
            ticker=detail.ticker or "",
            issuer_name=detail.issuer_name,
            cusip=detail.cusip or cusip,
        )
    else:
        stock_info = StockInfo(ticker="", issuer_name=None, cusip=cusip)

    return templates.TemplateResponse(
        "stock.html",
        {
            "request": request,
            "stock_info": stock_info,
            "stock": detail,
            "history": history,
        },
    )


@app.get("/stock/{ticker}", response_class=HTMLResponse)
async def stock_detail(request: Request, ticker: str):
    if not _valid_ticker(ticker):
        return PlainTextResponse("Invalid ticker", status_code=400)
    cache_data = _fund_cache()

    # Try to build superinvestor ownership data (may be None).
    # Parallel for the same reason as stock_detail_by_cusip — see comment there.
    detail = None
    history = []
    if cache_data:
        detail, history = await asyncio.gather(
            _to_heavy(
                client.build_stock_detail, ticker, cache_data, SUPERINVESTORS_BY_CIK
            ),
            _to_heavy(
                client.build_stock_history, ticker, cache_data, SUPERINVESTORS_BY_CIK
            ),
        )
        if not detail:
            history = []

    # Resolve basic stock identity (works for any ticker)
    # Always call resolve_stock_info to get logo_domain; uses cache first (instant)
    stock_info = await asyncio.to_thread(
        client.resolve_stock_info, ticker, cache_data or {}
    )
    # Overlay with detail data if available (more accurate name/cusip)
    if detail:
        stock_info.issuer_name = detail.issuer_name or stock_info.issuer_name
        stock_info.cusip = detail.cusip or stock_info.cusip
        stock_info.ticker = detail.ticker or stock_info.ticker

    # Check if user is watching this ticker (point query — no full set fetch)
    watching = False
    if request.state.user:
        uid = request.state.user.get("sub", "")
        if uid:
            watching = await asyncio.to_thread(
                supabase_cache.is_ticker_watched, uid, ticker
            )

    # Build related stocks for internal linking (top holdings of this stock's holders)
    related_stocks: list[dict] = []
    if detail and cache_data:
        _seen = {ticker.upper()}
        for holder in detail.holders[:3]:
            fd = cache_data.get(holder.fund_cik, {})
            for h in fd.get("all_holdings", [])[:10]:
                t = h.get("ticker")
                if t and t.upper() not in _seen:
                    _seen.add(t.upper())
                    related_stocks.append(
                        {"ticker": t, "issuer": h.get("issuer", "")}
                    )
                if len(related_stocks) >= 8:
                    break
            if len(related_stocks) >= 8:
                break

    return templates.TemplateResponse(
        "stock.html",
        {
            "request": request,
            "stock_info": stock_info,
            "stock": detail,
            "history": history,
            "related_stocks": related_stocks,
            "watching": watching,
        },
    )


# --- Analyst Ratings API (lazy-loaded via HTMX) ---


@app.get("/api/analysts/{ticker}", response_class=HTMLResponse)
@_with_l2_fallback("stock:analysts:{ticker}", category="stock", l1_ttl=300, l2_ttl=7200)
async def analyst_ratings(request: Request, ticker: str):
    ticker = ticker.upper().strip()
    if not _valid_ticker(ticker):
        return PlainTextResponse("Invalid ticker", status_code=400)
    # Run ratings + price fetch concurrently (independent)
    from filings import market_data
    from filings import analyst_scraper as _as

    async def _fetch_price():
        try:
            prices = await asyncio.to_thread(
                market_data.get_current_prices_batch, [ticker]
            )
            return prices.get(ticker)
        except Exception:
            return None

    (ratings, profiles, data_view), current_price = await asyncio.gather(
        _to_heavy(analysts.get_analyst_ratings_with_profiles, ticker),
        _fetch_price(),
    )
    consensus = analysts.get_consensus_summary_from_raw(ratings, data_view)

    # Pre-group ratings by firm (ordered by most recent rating date per firm)
    # Each group: {firm, logo_url, latest: <most recent rating dict>, ratings: [...all desc]}
    firm_groups: dict[str, dict] = {}
    firm_order: list[str] = []
    for r in ratings[:200]:
        firm_key = (r.get("firm") or "").strip().lower() or "unknown"
        if firm_key not in firm_groups:
            firm_groups[firm_key] = {
                "firm": r.get("firm") or "Unknown",
                "logo_url": _as.get_firm_logo_url(r.get("firm") or ""),
                "latest": r,  # first encountered = most recent (ratings are date-desc sorted)
                "ratings": [],
            }
            firm_order.append(firm_key)
        firm_groups[firm_key]["ratings"].append(r)
    grouped_ratings = [firm_groups[k] for k in firm_order]

    return templates.TemplateResponse(
        "partials/analyst_ratings.html",
        {
            "request": request,
            "ratings": ratings[:100],      # kept for consensus (not rendered directly)
            "grouped_ratings": grouped_ratings,
            "profiles": profiles,
            "consensus": consensus,
            "ticker": ticker,
            "data_view": data_view,
            "analyst_photo_set": _analyst_photo_set,
            "get_firm_logo_url": _as.get_firm_logo_url,
            "current_price": current_price,
        },
    )


@app.get("/api/sentiment/{ticker}", response_class=HTMLResponse)
@_with_l2_fallback("stock:sentiment:{ticker}", category="stock", l1_ttl=300, l2_ttl=7200)
async def sentiment_data(request: Request, ticker: str):
    if not _valid_ticker(ticker):
        return PlainTextResponse("Invalid ticker", status_code=400)
    data = await _to_heavy(sentiment.get_sentiment_data, ticker)
    return templates.TemplateResponse(
        "partials/sentiment.html",
        {
            "request": request,
            "ticker": ticker.upper(),
            "cnn": data.get("cnn_fear_greed"),
            "finnhub": data.get("finnhub"),
            "apewisdom": data.get("apewisdom"),
            "alphavantage": data.get("alphavantage"),
            "google_trends": data.get("google_trends"),
            "short_interest": data.get("short_interest"),
            "short_interest_history": data.get("short_interest_history"),
            "has_finnhub_key": sentiment.has_finnhub_key(),
            "has_alphavantage_key": sentiment.has_alphavantage_key(),
        },
    )


@app.get("/api/vitals/{ticker}", response_class=HTMLResponse)
@_with_l2_fallback("stock:vitals:{ticker}", category="stock", l1_ttl=300, l2_ttl=7200)
async def vitals_data(request: Request, ticker: str):
    if not _valid_ticker(ticker):
        return PlainTextResponse("Invalid ticker", status_code=400)

    data = await _to_heavy(vitals.get_vitals_data, ticker)
    return templates.TemplateResponse(
        "partials/vitals.html",
        {
            "request": request,
            "ticker": ticker.upper(),
            "glassdoor": data.get("glassdoor"),
            "pdl": data.get("pdl"),
            "appstore": data.get("appstore"),
            "has_glassdoor_key": vitals.has_glassdoor_key(),
            "has_pdl_key": vitals.has_pdl_key(),
            "glassdoor_age": vitals.get_glassdoor_age_str(ticker),
            "glassdoor_quota_exhausted": vitals.get_glassdoor_quota_info()["exhausted"],
            "pdl_quota_exhausted": vitals.get_pdl_quota_info()["exhausted"],
        },
    )


def _extract_chart_data(data: dict) -> dict:
    """Pull specific rows into flat arrays suitable for ECharts."""

    def _row_values(stmt: dict | None, label: str, periods: list[str]) -> list:
        """Get values for *label* across *periods* (reversed for chart L→R)."""
        if not stmt:
            return []
        for row in stmt.get("rows", []):
            if row["label"] == label:
                return [row["values"].get(p) for p in reversed(periods)]
        return [None] * len(periods)

    chart: dict = {}
    for freq in ("annual", "quarterly"):
        fd = data.get(freq)
        if not fd:
            continue
        periods = fd["income"]["periods"] if fd.get("income") else []
        labels = [p[:4] if freq == "annual" else f"{p[5:7]}/{p[:4]}" for p in reversed(periods)]

        chart[freq] = {
            "labels": labels,
            "income": {
                "revenue": _row_values(fd.get("income"), "Revenue", periods),
                "net_income": _row_values(fd.get("income"), "Net Income", periods),
                "gross_profit": _row_values(fd.get("income"), "Gross Profit", periods),
                "operating_income": _row_values(fd.get("income"), "Operating Income", periods),
                "operating_margin": _row_values(fd.get("ratios"), "Operating Margin", periods),
            },
            "balance": {
                "total_assets": _row_values(fd.get("balance"), "Total Assets", periods),
                "current_assets": _row_values(fd.get("balance"), "Total Current Assets", periods),
                "total_liabilities": _row_values(fd.get("balance"), "Total Liabilities", periods),
                "current_liabilities": _row_values(fd.get("balance"), "Total Current Liabilities", periods),
                "total_equity": _row_values(fd.get("balance"), "Total Equity", periods),
            },
            "cashflow": {
                "operating_cf": _row_values(fd.get("cashflow"), "Operating Cash Flow", periods),
                "investing_cf": _row_values(fd.get("cashflow"), "Investing Cash Flow", periods),
                "financing_cf": _row_values(fd.get("cashflow"), "Financing Cash Flow", periods),
                "capex": _row_values(fd.get("cashflow"), "Capital Expenditures", periods),
                "free_cf": _row_values(fd.get("ratios"), "Free Cash Flow", periods),
            },
            "ratios": {
                "gross_margin": _row_values(fd.get("ratios"), "Gross Margin", periods),
                "operating_margin": _row_values(fd.get("ratios"), "Operating Margin", periods),
                "net_margin": _row_values(fd.get("ratios"), "Net Margin", periods),
                "roe": _row_values(fd.get("ratios"), "ROE", periods),
                "fcf_margin": _row_values(fd.get("ratios"), "FCF Margin", periods),
            },
        }
    return chart


@app.get("/api/financials/{ticker}", response_class=HTMLResponse)
@_with_l2_fallback("stock:financials:{ticker}", category="stock", l1_ttl=300, l2_ttl=7200)
async def api_financials(request: Request, ticker: str):
    """Financial statements (Income, Balance Sheet, Cash Flow, Ratios) from SEC XBRL."""
    if not _valid_ticker(ticker):
        return PlainTextResponse("Invalid ticker", status_code=400)

    from filings import fundamentals

    data = await _to_heavy(fundamentals.get_fundamentals, ticker)
    if not data:
        return HTMLResponse(
            '<p class="text-muted" style="text-align:center;padding:2em 0;">'
            "No financial data available for this company.</p>"
        )
    chart_data = _extract_chart_data(data)
    return templates.TemplateResponse(
        "partials/financials.html",
        {"request": request, "data": data, "chart_data": chart_data},
    )


@app.get("/api/financials/{ticker}/history", response_class=HTMLResponse)
@_with_l2_fallback("stock:financials_history:{ticker}", category="stock", l1_ttl=600, l2_ttl=14400)
async def api_financials_history(request: Request, ticker: str):
    """Full historical financial statements from cold storage."""
    if not _valid_ticker(ticker):
        return PlainTextResponse("Invalid ticker", status_code=400)

    from filings import fundamentals

    data = await _to_heavy(fundamentals.get_full_history, ticker)
    if not data:
        return HTMLResponse(
            '<p class="text-muted" style="text-align:center;padding:2em 0;">'
            "No historical data available. This ticker may not have been backfilled yet.</p>"
        )
    chart_data = _extract_chart_data(data)
    return templates.TemplateResponse(
        "partials/financials.html",
        {"request": request, "data": data, "chart_data": chart_data},
    )


@app.get("/api/earnings/{ticker}", response_class=HTMLResponse)
@_with_l2_fallback("stock:earnings:{ticker}", category="stock", l1_ttl=300, l2_ttl=7200)
async def earnings_data(request: Request, ticker: str):
    """Earnings history tab: quarterly EPS results + forward estimates."""
    ticker = ticker.upper().strip()
    if not _valid_ticker(ticker):
        return PlainTextResponse("Invalid ticker", status_code=400)

    from filings import earnings

    data = await _to_heavy(earnings.get_earnings_data, ticker)

    return templates.TemplateResponse(
        "partials/earnings.html",
        {
            "request": request,
            "ticker": ticker,
            "history": data.get("history", []),
            "forward_estimates": data.get("forward_estimates"),
            "streak": data.get("streak", {}),
            "source": data.get("source", ""),
        },
    )


@app.get("/api/estimates/{ticker}", response_class=HTMLResponse)
@_with_l2_fallback("stock:estimates:{ticker}", category="stock", l1_ttl=300, l2_ttl=7200)
async def forward_estimates(request: Request, ticker: str):
    """Forward analyst estimates (EPS + Revenue) for the Estimates pill."""
    ticker = ticker.upper().strip()
    if not _valid_ticker(ticker):
        return PlainTextResponse("Invalid ticker", status_code=400)

    from filings import earnings

    data = await _to_heavy(earnings.get_forward_estimates, ticker)

    return templates.TemplateResponse(
        "partials/estimates.html",
        {
            "request": request,
            "ticker": ticker,
            "eps": data.get("eps", []) if data else [],
            "revenue": data.get("revenue", []) if data else [],
        },
    )


@app.get("/api/web-traffic/{ticker}", response_class=HTMLResponse)
@_with_l2_fallback("stock:webtraffic:{ticker}", category="stock", l1_ttl=300, l2_ttl=7200)
async def web_traffic_data(request: Request, ticker: str):
    if not _valid_ticker(ticker):
        return PlainTextResponse("Invalid ticker", status_code=400)

    data = await _to_heavy(web_traffic.get_web_traffic_data, ticker)
    return templates.TemplateResponse(
        "partials/web_traffic.html",
        {
            "request": request,
            "ticker": ticker.upper(),
            "relevance": data.get("relevance"),
            "cloudflare": data.get("cloudflare"),
            "tranco": data.get("tranco"),
            "wikipedia": data.get("wikipedia"),
            "has_cf_token": bool(web_traffic._get_cf_token()),
        },
    )


@app.get("/api/signals/{ticker}", response_class=HTMLResponse)
@_with_l2_fallback("stock:signals:{ticker}", category="stock", l1_ttl=300, l2_ttl=7200)
async def signals_data(request: Request, ticker: str):
    """Combined signals tab: sentiment + web traffic + Google Trends."""
    if not _valid_ticker(ticker):
        return PlainTextResponse("Invalid ticker", status_code=400)

    # Fetch all three data sources in parallel with per-source timeouts
    # to prevent one hung source (e.g. Google Trends) from blocking the tab
    sent_data, wt_data, gt_data = await asyncio.gather(
        _safe_fetch(_to_heavy(sentiment.get_sentiment_data, ticker), f"signals/{ticker}/sentiment", timeout=_SIGNALS_TIMEOUT, default={}),
        _safe_fetch(_to_heavy(web_traffic.get_web_traffic_data, ticker), f"signals/{ticker}/webtraffic", timeout=_SIGNALS_TIMEOUT),
        _safe_fetch(_to_heavy(google_trends.get_trends_summary, ticker), f"signals/{ticker}/gtrends", timeout=_SIGNALS_TIMEOUT),
    )

    return templates.TemplateResponse(
        "partials/signals.html",
        {
            "request": request,
            "ticker": ticker.upper(),
            # Sentiment
            "cnn": sent_data.get("cnn_fear_greed"),
            "finnhub": sent_data.get("finnhub"),
            "apewisdom": sent_data.get("apewisdom"),
            "alphavantage": sent_data.get("alphavantage"),
            "has_finnhub_key": sentiment.has_finnhub_key(),
            "has_alphavantage_key": sentiment.has_alphavantage_key(),
            # Google Trends
            "gt_keywords": gt_data.get("keywords") if gt_data else None,
            "gt_trend": gt_data.get("trend") if gt_data else None,
            # Web Traffic
            "wt_relevance": wt_data.get("relevance"),
            "wt_cloudflare": wt_data.get("cloudflare"),
            "wt_tranco": wt_data.get("tranco"),
            "wt_wikipedia": wt_data.get("wikipedia"),
            "has_cf_token": bool(web_traffic._get_cf_token()),
        },
    )


@app.get("/api/signals/{ticker}/short-interest", response_class=HTMLResponse)
@_with_l2_fallback("stock:short_interest:{ticker}", category="stock", l1_ttl=300, l2_ttl=7200)
async def signals_short_interest(request: Request, ticker: str):
    """Per-ticker short interest history for the Signals tab pill."""
    ticker = ticker.upper().strip()
    if not _valid_ticker(ticker):
        return PlainTextResponse("Invalid ticker", status_code=400)
    rows = await _to_heavy(supabase_cache.get_short_interest_history, ticker, 24)
    return templates.TemplateResponse(
        "partials/short_interest_ticker.html",
        {"request": request, "ticker": ticker, "history": rows},
    )


@app.get("/api/company-filings/{ticker}", response_class=HTMLResponse)
@_with_l2_fallback("stock:filings:{ticker}", category="stock", l1_ttl=600, l2_ttl=14400)
async def company_filings_tab(request: Request, ticker: str):
    if not _valid_ticker(ticker):
        return PlainTextResponse("Invalid ticker", status_code=400)
    filings = await asyncio.to_thread(company_filings.get_company_filings, ticker)
    return templates.TemplateResponse(
        "partials/company_filings.html",
        {
            "request": request,
            "filings": filings,
            "ticker": ticker.upper(),
        },
    )


# --- Unusual Options Activity ---


@app.get("/options", response_class=HTMLResponse)
async def unusual_options_page(request: Request):
    """Full page with SSR data for Googlebot."""
    from filings import unusual_options

    feed: list[dict] = []
    heatmap_json: str = "[]"
    try:
        _feed, _heatmap = await asyncio.gather(
            asyncio.to_thread(unusual_options.get_unusual_options_feed),
            asyncio.to_thread(unusual_options.get_options_heatmap_data),
        )
        feed = _feed or []
        heatmap_json = json_module.dumps(_heatmap or [])
    except Exception:
        pass  # Graceful fallback — JS will retry via HTMX

    return templates.TemplateResponse(
        "unusual_options.html",
        {
            "request": request,
            "feed": feed,
            "heatmap_json": heatmap_json,
        },
    )


@app.get("/api/options/feed", response_class=HTMLResponse)
async def options_feed_api(
    request: Request,
    sentiment: str = "",
    sort: str = "premium",
    ticker: str = "",
):
    """HTMX partial: sortable/filterable unusual options feed."""
    from filings import unusual_options

    feed = await asyncio.to_thread(
        unusual_options.get_unusual_options_feed,
        sentiment=sentiment,
        sort_by=sort,
        ticker=ticker,
    )
    return templates.TemplateResponse(
        "partials/options_feed.html",
        {"request": request, "feed": feed},
    )


@app.get("/api/options/heatmap", response_class=HTMLResponse)
async def options_heatmap_api(request: Request):
    """HTMX partial: sector heatmap for unusual options premium."""
    from filings import unusual_options

    heatmap = await asyncio.to_thread(unusual_options.get_options_heatmap_data)
    return templates.TemplateResponse(
        "partials/options_heatmap.html",
        {"request": request, "heatmap_json": json_module.dumps(heatmap or [])},
    )


@app.get("/api/options/convergence", response_class=HTMLResponse)
async def options_convergence_api(
    request: Request,
    signals: str = "",
    min_signals: int = 2,
):
    """HTMX partial: cross-reference convergence engine results."""
    from filings import convergence

    # Parse signal filter
    signals_filter: set[str] | None = None
    valid_types = {"options", "insider", "congress", "short", "superinvestor"}
    if signals:
        requested = {s.strip().lower() for s in signals.split(",")}
        signals_filter = requested & valid_types
        if not signals_filter:
            signals_filter = None

    min_signals = max(2, min(min_signals, 5))

    cache_data = _fund_cache()
    results = await asyncio.to_thread(
        convergence.build_convergence,
        cache_data=cache_data,
        superinvestors_by_cik=SUPERINVESTORS_BY_CIK,
        signals_filter=signals_filter,
        min_signals=min_signals,
    )

    return templates.TemplateResponse(
        "partials/options_convergence.html",
        {
            "request": request,
            "results": results,
            "signal_count": len(results),
            "min_signals": min_signals,
        },
    )


@app.get("/api/options/clusters", response_class=HTMLResponse)
async def options_clusters_api(request: Request, limit: int = 25):
    """HTMX partial: clustered unusual options activity."""
    from filings import unusual_options

    clusters = await asyncio.to_thread(unusual_options.get_options_clusters, limit)
    return templates.TemplateResponse(
        "partials/options_clusters.html",
        {"request": request, "clusters": clusters},
    )


@app.get("/api/options/ivrank", response_class=HTMLResponse)
@_with_l2_fallback("options:ivrank", category="options", l1_ttl=300, l2_ttl=3600)
async def options_ivrank_api(request: Request):
    """HTMX partial: IV Rank table for most-active options tickers + market P/C ratios."""
    from filings import cboe_data

    ivrank_data, putcall_equity, putcall_index = await asyncio.gather(
        _to_heavy(cboe_data.get_iv_rank_batch),
        _to_heavy(cboe_data.get_put_call_ratio, "equity"),
        _to_heavy(cboe_data.get_put_call_ratio, "index"),
    )

    return templates.TemplateResponse(
        "partials/options_ivrank.html",
        {
            "request": request,
            "ivrank_data": ivrank_data or [],
            "putcall_equity": (putcall_equity or [])[-20:],
            "putcall_index": (putcall_index or [])[-20:],
            "putcall_equity_json": json_module.dumps((putcall_equity or [])[-60:]),
            "putcall_index_json": json_module.dumps((putcall_index or [])[-60:]),
        },
    )


@app.get("/api/stock/{ticker}/options", response_class=HTMLResponse)
async def stock_options_api(request: Request, ticker: str):
    """HTMX partial: unusual options activity for a specific stock."""
    from filings import unusual_options

    if not _valid_ticker(ticker):
        return PlainTextResponse("", status_code=400)

    feed = await asyncio.to_thread(
        unusual_options.get_ticker_options_activity, ticker.upper()
    )
    return templates.TemplateResponse(
        "partials/stock_options.html",
        {"request": request, "feed": feed, "ticker": ticker.upper()},
    )


# --- Insider Trading ---


@app.get("/insider-trading", response_class=HTMLResponse)
async def insider_trading_redirect(request: Request):
    """Backward-compat redirect: /insider-trading → /insiders."""
    return RedirectResponse(url="/insiders", status_code=301)


# Legacy v1 insider-trading handler — UNREACHABLE after the redirect above.
async def _legacy_v1_insider_trading_unused(request: Request):
    # Pre-fetch initial trade data so Googlebot sees real content (not just a loading spinner).
    # The JS client will still fetch via /api/insider-trades for tab switching & caching.
    trades: list = []
    chart_data_json: str = "[]"
    try:
        _trades, _chart = await asyncio.gather(
            asyncio.to_thread(insider_trading.get_latest_insider_trades, ""),
            asyncio.to_thread(insider_trading.get_insider_chart_data, 10, ""),
        )
        trades = _trades or []
        chart_data_json = json_module.dumps(_chart or [])
    except Exception:
        pass  # Graceful fallback — JS will retry via fetch()

    grouped = _group_insider_trades(trades)

    return templates.TemplateResponse(
        "insider_trading.html",
        {
            "request": request,
            "trades": trades,
            "grouped": grouped,
            "chart_data_json": chart_data_json,
        },
    )


# --- Support / Panda Fund ---

# Stripe Embedded Checkout (backend SDK creates sessions, frontend mounts overlay)
_STRIPE_PUBLISHABLE_KEY = os.environ.get("STRIPE_PUBLISHABLE_KEY", "")
_STRIPE_SECRET_KEY = os.environ.get("STRIPE_SECRET_KEY", "")
_STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "")
# Price IDs for each tier (created in Stripe Dashboard)
_STRIPE_PRICE_ONETIME = os.environ.get("STRIPE_PRICE_ONETIME", "")
_STRIPE_PRICE_BAMBOO = os.environ.get("STRIPE_PRICE_BAMBOO", "")  # $5/mo
_STRIPE_PRICE_PANDA = os.environ.get("STRIPE_PRICE_PANDA", "")  # $15/mo
_STRIPE_PRICE_GIANT_PANDA = os.environ.get("STRIPE_PRICE_GIANT_PANDA", "")  # $30/mo

# Feedback form link (Tally embed, or any iframe-friendly form URL)
_FEEDBACK_LINK = os.environ.get(
    "FEEDBACK_LINK",
    "https://tally.so/r/5BzA1d",
)

_PANDA_FUND_MONTHLY_GOAL = 200  # Cap displayed on frontend

# What the fund covers (labels only, no dollar amounts exposed)
_PANDA_FUND_LINE_ITEMS = [
    "Data APIs (SEC EDGAR, Glassdoor, People Data Labs)",
    "Cloud hosting (Railway)",
    "Database (Supabase Postgres)",
    "Domain & DNS",
    "AI coding assistants",
]

async def _support_page_context(request: Request, extra: dict | None = None) -> dict:
    """Build the shared template context for /support and /support/thank-you."""
    from calendar import month_name as _month_names

    pf = await _get_panda_fund_stats()
    current_month_name = _month_names[datetime.now().month]

    # Funding history from Supabase (Feb 2025 launch → current month)
    history = await asyncio.to_thread(supabase_cache.get_funding_history)

    ctx: dict = {
        "request": request,
        "stripe_publishable_key": _STRIPE_PUBLISHABLE_KEY,
        "stripe_configured": bool(_STRIPE_SECRET_KEY and _STRIPE_PUBLISHABLE_KEY),
        "price_onetime": _STRIPE_PRICE_ONETIME,
        "price_bamboo": _STRIPE_PRICE_BAMBOO,
        "price_panda": _STRIPE_PRICE_PANDA,
        "price_giant_panda": _STRIPE_PRICE_GIANT_PANDA,
        "feedback_link": _FEEDBACK_LINK,
        "monthly_goal": pf["goal"],
        "raised_this_month": pf["raised"],
        "progress_pct": pf["pct"],
        "goal_reached": pf["raised"] >= pf["goal"],
        "current_month_name": current_month_name,
        "line_items": _PANDA_FUND_LINE_ITEMS,
        "funding_history_months": [h["month"] for h in history],
        "funding_history_raised": [min(h["raised"], pf["goal"]) for h in history],
    }
    if extra:
        ctx.update(extra)
    return ctx


@app.get("/support", response_class=HTMLResponse)
async def support_page(request: Request):
    ctx = await _support_page_context(request)
    return templates.TemplateResponse("support.html", ctx)


# /privacy, /faq moved to filings.routers.static_pages (audit-sprint-4).


@app.post("/api/create-checkout-session")
async def create_checkout_session(request: Request):
    """Create a Stripe Embedded Checkout session."""
    import stripe

    if not _STRIPE_SECRET_KEY:
        return JSONResponse({"error": "Stripe not configured"}, status_code=503)

    stripe.api_key = _STRIPE_SECRET_KEY

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid request"}, status_code=400)

    price_id = body.get("price_id", "")
    mode = body.get("mode", "payment")

    # Validate price_id format
    if not price_id or not _re.match(r"^price_[A-Za-z0-9]+$", price_id):
        return JSONResponse({"error": "Invalid price"}, status_code=400)

    if mode not in ("payment", "subscription"):
        return JSONResponse({"error": "Invalid mode"}, status_code=400)

    # Build return URL from request base
    base = str(request.base_url).rstrip("/")
    return_url = f"{base}/support/thank-you?session_id={{CHECKOUT_SESSION_ID}}"

    try:
        session = stripe.checkout.Session.create(
            ui_mode="embedded",
            line_items=[{"price": price_id, "quantity": 1}],
            mode=mode,
            return_url=return_url,
        )
    except stripe.StripeError as e:
        return JSONResponse({"error": str(e)}, status_code=400)

    return JSONResponse({"clientSecret": session.client_secret})


@app.get("/api/session-status")
async def session_status(session_id: str = ""):
    """Retrieve Stripe Checkout session status for the return page."""
    import stripe

    if not _STRIPE_SECRET_KEY or not session_id:
        return JSONResponse({"error": "Invalid request"}, status_code=400)

    stripe.api_key = _STRIPE_SECRET_KEY

    if not _re.match(r"^cs_(test|live)_[A-Za-z0-9]+$", session_id):
        return JSONResponse({"error": "Invalid session"}, status_code=400)

    try:
        session = stripe.checkout.Session.retrieve(session_id)
    except stripe.StripeError:
        return JSONResponse({"error": "Session not found"}, status_code=404)

    return JSONResponse(
        {
            "status": session.status,
            "customer_email": (
                session.customer_details.email if session.customer_details else None
            ),
        }
    )


@app.get("/support/thank-you", response_class=HTMLResponse)
async def checkout_return(request: Request):
    """Post-checkout return page — reuses support.html with thank-you flag."""
    ctx = await _support_page_context(request, extra={"show_thank_you": True})
    return templates.TemplateResponse("support.html", ctx)


@app.post("/api/stripe-webhook")
async def stripe_webhook(request: Request):
    """Receive and verify Stripe webhook events.

    Listens for:
    - ``checkout.session.completed``  — one-time payments
    - ``invoice.payment_succeeded``   — recurring subscription payments

    Both events trigger a row insert into the ``supporters`` Supabase table
    so the /support page shows a live, accurate raised total.

    Idempotent: the Stripe event ID is used as a unique key, so retried
    deliveries from Stripe are safely ignored.
    """
    import stripe

    if not _STRIPE_SECRET_KEY:
        return JSONResponse({"error": "Stripe not configured"}, status_code=503)

    stripe.api_key = _STRIPE_SECRET_KEY

    payload = await request.body()
    sig_header = request.headers.get("stripe-signature", "")

    # ── Signature verification (mandatory) ────────────────────────────
    if not _STRIPE_WEBHOOK_SECRET:
        logger.error(
            "stripe_webhook: STRIPE_WEBHOOK_SECRET not set — rejecting request. "
            "Set this env var in Railway to enable webhook processing."
        )
        return JSONResponse(
            {"error": "Webhook secret not configured"}, status_code=503
        )

    try:
        event = stripe.Webhook.construct_event(
            payload, sig_header, _STRIPE_WEBHOOK_SECRET
        )
    except stripe.SignatureVerificationError:
        logger.warning("stripe_webhook: invalid signature — request rejected")
        return JSONResponse({"error": "Invalid signature"}, status_code=400)
    except Exception as exc:
        logger.warning("stripe_webhook: payload parse error: %s", exc)
        return JSONResponse({"error": "Bad payload"}, status_code=400)

    event_type = event.get("type", "")
    event_id = event.get("id", "")
    month = datetime.now().strftime("%Y-%m")

    logger.info("stripe_webhook: received event type=%s id=%s", event_type, event_id)

    try:
        # ── checkout.session.completed — one-time payments ────────────────
        if event_type == "checkout.session.completed":
            session = event["data"]["object"]
            if session.get("payment_status") != "paid":
                # Async payment (e.g. bank transfer) — wait for payment_intent.succeeded
                return JSONResponse({"status": "ok"})

            amount_cents = session.get("amount_total") or 0
            currency = session.get("currency", "usd")
            mode = session.get("mode", "payment")
            session_id = session.get("id", "")
            customer_details = session.get("customer_details") or {}
            customer_email = customer_details.get("email", "")

            created = session.get("created")
            if created:
                month = datetime.fromtimestamp(created, tz=timezone.utc).strftime("%Y-%m")

            await asyncio.to_thread(
                supabase_cache.record_supporter,
                event_id,
                session_id,
                customer_email,
                amount_cents,
                currency,
                mode,
                month,
            )
            logger.info(
                "stripe_webhook: recorded checkout.session.completed — %d %s (%s)",
                amount_cents,
                currency,
                mode,
            )

        # ── invoice.payment_succeeded / invoice.paid — subscription renewals ─
        # Stripe uses both names depending on API version; handle both.
        elif event_type in ("invoice.payment_succeeded", "invoice.paid"):
            invoice = event["data"]["object"]
            # Skip $0 invoices (e.g. trial start)
            amount_cents = invoice.get("amount_paid") or 0
            if amount_cents == 0:
                return JSONResponse({"status": "ok"})

            currency = invoice.get("currency", "usd")
            session_id = invoice.get("subscription", "") or invoice.get("id", "")
            customer_email = invoice.get("customer_email", "")

            created = invoice.get("created")
            if created:
                month = datetime.fromtimestamp(created, tz=timezone.utc).strftime("%Y-%m")

            await asyncio.to_thread(
                supabase_cache.record_supporter,
                event_id,
                session_id,
                customer_email,
                amount_cents,
                currency,
                "subscription",
                month,
            )
            logger.info(
                "stripe_webhook: recorded invoice payment — %d %s",
                amount_cents,
                currency,
            )

        else:
            # Unhandled event type — acknowledge receipt so Stripe doesn't retry
            logger.info("stripe_webhook: unhandled event type %s (ignored)", event_type)

    except Exception:
        logger.exception(
            "stripe_webhook: unhandled error processing event type=%s id=%s",
            event_type,
            event_id,
        )
        # Still return 200 so Stripe doesn't keep retrying — the error is logged
        # and can be replayed manually from the Stripe dashboard.

    return JSONResponse({"status": "ok"})


# --- Deployment / AUM Tracking ---


def _deployment_cache() -> dict:
    """Return the current deployment cache dict from app.state."""
    return getattr(app.state, "deployment_cache", {})


@app.get("/deployment", response_class=HTMLResponse)
async def deployment_redirect(request: Request):
    """Backward-compat redirect: /deployment → /funds?view=Capital%20Deployed."""
    return RedirectResponse(url="/funds?view=Capital%20Deployed", status_code=301)


# Legacy v1 deployment handler — UNREACHABLE after the redirect above.
async def _legacy_v1_deployment_unused(request: Request):
    """Deployment leaderboard — ranks funds by equity deployment ratio."""
    deploy_data = _deployment_cache()
    leaderboard = aum_data.build_deployment_leaderboard(deploy_data)

    return templates.TemplateResponse(
        "deployment.html",
        {
            "request": request,
            "leaderboard": leaderboard,
            "total_funds": len(SUPERINVESTORS),
            "funds_with_data": len(leaderboard),
            "cache_age": cache.get_cache_age_str(_fund_cache()),
        },
    )


@app.get("/api/deployment-leaderboard", response_class=HTMLResponse)
async def deployment_leaderboard_partial(request: Request):
    """HTMX partial for deployment leaderboard (lazy-loaded on /funds tab)."""
    deploy_data = _deployment_cache()
    leaderboard = aum_data.build_deployment_leaderboard(deploy_data)

    return templates.TemplateResponse(
        "partials/deployment_leaderboard.html",
        {"request": request, "leaderboard": leaderboard},
    )


@app.get("/api/deployment/{cik}", response_class=HTMLResponse)
async def deployment_card_partial(request: Request, cik: str):
    """HTMX partial: deployment card for an individual investor page."""
    if not _valid_cik(cik):
        return PlainTextResponse("", status_code=204)

    deploy_data = _deployment_cache()
    metrics = deploy_data.get(cik)

    if not metrics or not metrics.get("data_source"):
        return PlainTextResponse("", status_code=204)

    si = SUPERINVESTORS_BY_CIK.get(cik)
    return templates.TemplateResponse(
        "partials/deployment_card.html",
        {"request": request, "metrics": metrics, "si": si},
    )


_deployment_sync_lock = asyncio.Lock()


@app.post("/api/deployment/sync")
async def trigger_deployment_sync(request: Request):
    """Trigger a deployment data sync (admin-only, for testing).

    Protected: requires CSRF origin check + SYNC_SECRET header.
    Concurrency-guarded: only one sync can run at a time.
    Always forces a fresh sync (bypasses the cache freshness gate).
    """
    # ── Auth: CSRF origin + shared secret ──
    csrf_err = _check_csrf_origin(request)
    if csrf_err:
        return csrf_err
    expected = os.environ.get("SYNC_SECRET", "")
    provided = request.headers.get("X-Sync-Secret", "")
    if not expected or provided != expected:
        return JSONResponse({"error": "Unauthorized"}, status_code=403)

    # ── Concurrency guard ──
    if _deployment_sync_lock.locked():
        return JSONResponse(
            {"error": "Sync already in progress"}, status_code=429
        )

    async with _deployment_sync_lock:
        fund_cache_data = _fund_cache()
        if not fund_cache_data:
            return JSONResponse({"error": "No fund cache available"}, status_code=503)

        result = await asyncio.to_thread(
            aum_data.sync_all_deployment_data, SUPERINVESTORS, fund_cache_data, force=True
        )

        # Reload deployment cache
        app.state.deployment_cache = await asyncio.to_thread(
            aum_data.load_all_deployment_data
        )

    return JSONResponse(result)


# --- Notifications ---

_SAFE_SINCE_DEFAULT = "2000-01-01T00:00:00Z"


def _validated_since(since: str) -> str:
    """Return *since* if it's a valid ISO 8601 timestamp, else the safe default."""
    try:
        if len(since) > 40:  # Prevent oversized params
            return _SAFE_SINCE_DEFAULT
        datetime.fromisoformat(since.replace("Z", "+00:00"))
        return since
    except Exception:
        return _SAFE_SINCE_DEFAULT


def _time_ago(iso_str: str) -> str:
    """Convert an ISO 8601 timestamp to a human-readable 'time ago' string."""
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        diff = datetime.now(timezone.utc) - dt
        seconds = int(diff.total_seconds())
        if seconds < 60:
            return "just now"
        minutes = seconds // 60
        if minutes < 60:
            return f"{minutes}m ago"
        hours = minutes // 60
        if hours < 24:
            return f"{hours}h ago"
        days = hours // 24
        if days < 7:
            return f"{days}d ago"
        return f"{days // 7}w ago"
    except Exception:
        return ""


# ── In-memory cache for bell state (global notifications, short TTL) ──
from filings.caching import TTLCache as _TTLCache
_BELL_CACHE_TTL = 15  # seconds — collapses identical polls across users
_bell_cache = _TTLCache(ttl=_BELL_CACHE_TTL, max_size=200)


def _get_cached_bell_state(since: str) -> tuple[int, dict | None]:
    """Return (count, latest) from cache or DB.  15-second TTL."""
    cached = _bell_cache.get(since)
    if cached is not None:
        return cached
    count, latest = supabase_cache.get_bell_state(since)
    result = (count, latest)
    _bell_cache.set(since, result)
    return result


@app.get("/api/notifications/bell", response_class=HTMLResponse)
async def notification_bell(
    request: Request,
    since: str = "2000-01-01T00:00:00Z",
    first_visit: int = 0,
):
    """Return bell icon + dot indicator as an HTML partial.

    ``since`` is the client's last-seen timestamp from localStorage.
    ``first_visit`` — if 1, always show the dot so new users discover the bell.
    """
    since = _validated_since(since)
    count, latest = await asyncio.to_thread(_get_cached_bell_state, since)

    # First-time visitors always see the red dot as a discovery prompt
    if first_visit in (1,) and count == 0:
        count = 1

    # If new notifications appeared, send toast data via HX-Trigger
    headers = {}
    if count > 0 and latest:
        trigger_data = {
            "ppNewNotification": {
                "id": latest.get("id", ""),
                "type": latest.get("toast_type", "alert"),
                "title": latest.get("title", ""),
                "message": latest.get("message", ""),
                "icon": latest.get("icon", "🔔"),
                "link": latest.get("link", ""),
            }
        }
        headers["HX-Trigger"] = json_module.dumps(trigger_data)

    return templates.TemplateResponse(
        "partials/notification_bell.html",
        {"request": request, "count": count},
        headers=headers,
    )


@app.get("/api/notifications/recent", response_class=HTMLResponse)
async def notification_recent(request: Request, since: str = "2000-01-01T00:00:00Z"):
    """Return dropdown content with the 8 most recent notifications."""
    since = _validated_since(since)
    # Fetch 9 to detect if more exist — avoids a separate count query
    all_notifs = await asyncio.to_thread(
        supabase_cache.get_recent_notifications, 9
    )

    has_more = len(all_notifs) > 8
    display_notifs = all_notifs[:8]

    for n in display_notifs:
        n["time_ago"] = _time_ago(n.get("created_at", ""))
        n["is_new"] = n.get("created_at", "") > since

    # Append a single "Support the Panda" CTA at the end of the dropdown
    if display_notifs:
        display_notifs = _inject_support_ctas(display_notifs, interval=len(display_notifs))

    return templates.TemplateResponse(
        "partials/notification_dropdown.html",
        {
            "request": request,
            "notifications": display_notifs,
            "has_more": has_more,
        },
    )


@app.get("/api/notifications/count")
async def notification_count(request: Request, since: str = "2000-01-01T00:00:00Z"):
    """Return notification count as JSON."""
    since = _validated_since(since)
    count, _ = await asyncio.to_thread(_get_cached_bell_state, since)
    return JSONResponse({"count": count})


_NOTIF_TYPES = ["13f_change", "youtube", "reddit_velocity", "congress_trade", "insider_trade", "feature_release"]


def _inject_support_ctas(notifs: list[dict], interval: int = 10) -> list[dict]:
    """Inject synthetic 'Support the Panda' CTAs into a notification list.

    Inserts a CTA after every *interval* real notifications.
    The CTA is NOT stored in the database — it is purely a render-time
    injection.
    """
    if not notifs or interval < 1:
        return notifs

    _cta = {
        "id": "__support_cta__",
        "type": "support_cta",
        "title": "Support the Panda 🐼",
        "message": "Paper Panda is free and ad-free. Help keep it running!",
        "icon": "🐼",
        "toast_type": "",
        "link": "/support",
        "metadata": {},
        "created_at": "",
        "time_ago": "",
        "is_new": False,
        "is_cta": True,
    }

    result: list[dict] = []
    for i, n in enumerate(notifs):
        result.append(n)
        if (i + 1) % interval == 0:
            result.append(_cta)
    return result


@app.get("/notifications", response_class=HTMLResponse)
async def notifications_page(
    request: Request,
    page: int = Query(1, ge=1, le=100),
    types: str = Query("", description="Comma-separated notification types to show"),
):
    """Full notification history page (last 48 hours), with optional type filter."""
    # Parse type filter from query string
    active_types: list[str] = []
    if types.strip():
        active_types = [t.strip() for t in types.split(",") if t.strip() in _NOTIF_TYPES]
    filter_types = active_types or None  # None = show all

    per_page = 30
    offset = (page - 1) * per_page
    # Fetch 1 extra row to detect next page — pagination is pushed to SQL
    page_notifs = await asyncio.to_thread(
        supabase_cache.get_recent_notifications,
        per_page + 1, filter_types, offset,
    )
    has_next = len(page_notifs) > per_page
    page_notifs = page_notifs[:per_page]

    for n in page_notifs:
        n["time_ago"] = _time_ago(n.get("created_at", ""))

    # Inject "Support the Panda" CTAs every 10th notification
    page_notifs = _inject_support_ctas(page_notifs, interval=10)

    # Get user's watchlist tickers for heart badges
    wl_tickers: set[str] = set()
    is_watchlist_filter = types.strip() == "watchlist"
    if request.state.user:
        uid = request.state.user.get("sub", "")
        if uid:
            wl_tickers = await asyncio.to_thread(
                supabase_cache.get_user_watchlist_tickers, uid
            )
    # If watchlist filter active, post-filter by ticker
    if is_watchlist_filter and wl_tickers:
        page_notifs = [
            n for n in page_notifs
            if not n.get("is_cta") and (n.get("metadata") or {}).get("ticker", "").upper() in wl_tickers
        ]

    return templates.TemplateResponse(
        "notifications.html",
        {
            "request": request,
            "notifications": page_notifs,
            "page": page,
            "has_next": has_next and not is_watchlist_filter,
            "all_types": _NOTIF_TYPES,
            "active_types": active_types,
            "watchlist_tickers": wl_tickers,
            "is_watchlist_filter": is_watchlist_filter,
        },
    )


# --- Retail Traders (hidden — no nav link) ---

_FINANCE_YOUTUBERS = [
    {
        "name": "Financial Education",
        "channel": "https://youtube.com/@FinancialEducation",
        "schedule": "Daily",
        "topics": "Stock Picks, Market Analysis",
    },
    {
        "name": "Joseph Carlson",
        "channel": "https://youtube.com/@JosephCarlsonShow",
        "schedule": "2x/week",
        "topics": "Dividend Investing, Portfolio Updates",
    },
    {
        "name": "Tevis (FunOfInvesting)",
        "channel": "https://youtube.com/@FunofInvesting",
        "schedule": "Daily",
        "topics": "Stock Picks, Market Analysis",
    },
    {
        "name": "MattMoney",
        "channel": "https://youtube.com/@RealMattMoney",
        "schedule": "Daily",
        "topics": "Investing, Market News",
    },
    {
        "name": "Kross Roads",
        "channel": "https://youtube.com/@Kross_Roads",
        "schedule": "Daily",
        "topics": "Stock Analysis, Growth Investing",
    },
    {
        "name": "Dividend Streams",
        "channel": "https://youtube.com/@DividendStreams",
        "schedule": "Daily",
        "topics": "Dividends, Income Investing",
    },
    {
        "name": "Futurenvesting",
        "channel": "https://youtube.com/@Futurenvesting",
        "schedule": "Daily",
        "topics": "Investing, Future Trends",
    },
    {
        "name": "Amit Investing",
        "channel": "https://youtube.com/@amitinvesting",
        "schedule": "Daily",
        "topics": "Stock Picks, Market Analysis",
    },
    {
        "name": "Couch Investor",
        "channel": "https://youtube.com/@couch_Investor",
        "schedule": "Daily",
        "topics": "Investing, Portfolio Strategy",
    },
    {
        "name": "Endicott Invests",
        "channel": "https://youtube.com/@EndicottInvests",
        "schedule": "Daily",
        "topics": "Stock Analysis, Investing",
    },
    {
        "name": "Kris Patel",
        "channel": "https://youtube.com/@KrisPatel99",
        "schedule": "Daily",
        "topics": "Stock Picks, Market News",
    },
]


_ALT_SIGNALS_KEY = os.environ.get("MACRO_PAGE_KEY", "panda2026")


@app.get("/alternative-signals", response_class=HTMLResponse)
async def alternative_signals_page(request: Request):
    """Alternative signals page — Google Trends + short interest dashboard (key-protected)."""
    if request.query_params.get("key") != _ALT_SIGNALS_KEY:
        return templates.TemplateResponse(
            "under_construction.html",
            {"request": request},
            status_code=200,
        )
    quick_tickers = [
        "HOOD", "NKE", "AAPL", "TSLA", "COIN", "AMZN", "NFLX", "NVDA",
        "META", "LULU", "PLTR", "SOFI",
    ]
    return templates.TemplateResponse(
        "alternative_signals.html",
        {
            "request": request,
            "macro_categories": google_trends.MACRO_CATEGORIES,
            "quick_tickers": quick_tickers,
        },
    )


# ── Google Trends API endpoints ─────────────────────────────────────


@app.get("/api/google-trends/trending", response_class=HTMLResponse)
async def gt_trending_api(request: Request):
    """Fetch trending Google searches with ticker matching."""
    trending = await _to_heavy(google_trends.fetch_trending_searches)
    # Pass None (API failure) vs [] (worked, nothing market-relevant) as distinct states
    return templates.TemplateResponse(
        "partials/google_trends_trending.html",
        {"request": request, "trending": trending},
    )


@app.get("/api/macro/indicators", response_class=HTMLResponse)
@_with_l2_fallback("macro:indicators", category="macro", l1_ttl=600, l2_ttl=14400)
async def macro_indicators_api(request: Request):
    """FRED macro indicators snapshot — sparkline cards + historical charts."""
    if not _screener_authed(request):
        return PlainTextResponse("Unauthorized", status_code=401)
    from filings import fred_indicators, fred_data

    data, chart_data = await asyncio.gather(
        _to_heavy(fred_indicators.fetch_indicators),
        _to_heavy(fred_data.get_dashboard_data),
    )
    # Map chart series → indicator groups
    _chart_group = {
        "FEDFUNDS": "rates", "GS10": "rates", "T10Y2Y": "rates",
        "CPIAUCSL": "inflation", "UNRATE": "employment", "GDP": "consumer",
    }
    grouped_charts: dict[str, list] = {}
    for sid, cdata in (chart_data or {}).items():
        grp = _chart_group.get(sid)
        if grp:
            grouped_charts.setdefault(grp, []).append(cdata)

    return templates.TemplateResponse(
        "partials/macro_indicators.html",
        {
            "request": request,
            "data": data,
            "charts": grouped_charts,
            "charts_json": json_module.dumps(grouped_charts, default=str),
        },
    )


@app.get("/api/google-trends/macro", response_class=HTMLResponse)
async def gt_macro_api(request: Request, category: str = ""):
    """Fetch macro trend chart for a category."""
    if not _screener_authed(request):
        return PlainTextResponse("Unauthorized", status_code=401)
    cat = category if category in google_trends.MACRO_CATEGORIES else None
    trend_data = await _to_heavy(google_trends.fetch_macro_trends, cat)
    # Use validated cat (not raw category) for chart ID to prevent XSS
    chart_id = (cat or "overview").lower().replace(" ", "-").replace("/", "-")
    return templates.TemplateResponse(
        "partials/google_trends_macro.html",
        {"request": request, "trend_data": trend_data, "chart_id": chart_id},
    )


@app.get("/api/google-trends/ticker/{ticker}", response_class=HTMLResponse)
async def gt_ticker_api(request: Request, ticker: str):
    """Fetch Google Trends keywords and interest data for a ticker."""
    ticker = ticker.upper().strip()
    if not _valid_ticker(ticker):
        return HTMLResponse("<div>Invalid ticker.</div>", status_code=400)

    summary = await _to_heavy(google_trends.get_trends_summary, ticker)
    return templates.TemplateResponse(
        "partials/google_trends_ticker.html",
        {
            "request": request,
            "keywords": summary.get("keywords"),
            "trend": summary.get("trend"),
        },
    )


@app.get("/api/alt-signals/short-interest", response_class=HTMLResponse)
async def alt_signals_short_interest(request: Request):
    """HTMX partial: short interest leaderboard tables."""
    data = await asyncio.to_thread(supabase_cache.get_cached, "short_interest_leaderboard")
    if not data:
        # Cron hasn't run yet — build leaderboard on-the-fly from the history table.
        # Also attach guru overlap so the table is fully populated.
        try:
            fund_cache = await asyncio.to_thread(cache.load_cache_from_supabase)
            ticker_map = client.build_ticker_ownership_map(
                fund_cache or {}, SUPERINVESTORS_BY_CIK
            )
        except Exception:
            ticker_map = {}
        data = await asyncio.to_thread(
            supabase_cache.build_leaderboard_from_db, ticker_map
        )
        # Store in cache so subsequent requests are instant (12h TTL matches cron cadence)
        if data:
            await asyncio.to_thread(
                supabase_cache.set_cached,
                "short_interest_leaderboard",
                "alt_signals",
                data,
                43200,  # 12 hours
            )
    if not data:
        return HTMLResponse(
            '<div style="text-align: center; padding: 2em 0;">'
            '<p style="color: var(--pp-text-muted); font-size: 0.95em;">'
            "Short interest data is not yet available. "
            "Please check back shortly."
            "</p></div>"
        )
    return templates.TemplateResponse(
        "partials/short_interest_leaderboard.html",
        {
            "request": request,
            "highest_short": data.get("highest_short", []),
            "trending_short": data.get("trending_short", []),
            "metadata": data.get("metadata", {}),
        },
    )


# --- Macro Earnings Scorecard ---


@app.get("/macro", response_class=HTMLResponse)
async def macro_page(
    request: Request,
    index: str = "all",
    quarter: str = "",
    sector: str = "",
):
    """Macro page — aggregated earnings-season dashboard + market breadth."""
    if not _screener_authed(request):
        return templates.TemplateResponse(
            "screener_gate.html",
            {
                "request": request,
                "error": None,
                "gate_title": "Macro Dashboard",
                "gate_action": "/macro/auth",
                "gate_note": "The macro dashboard includes earnings scorecards, market breadth, economic calendars, and trend analysis. Contact us for access.",
            },
        )

    from filings import earnings_scorecard
    from filings import market_breadth
    from filings import fred_calendar

    if index not in earnings_scorecard.INDEX_CHOICES:
        index = "all"
    quarters = earnings_scorecard.get_available_quarters()
    if not quarter or quarter not in quarters:
        quarter = quarters[0]
    if sector and sector not in earnings_scorecard.SECTORS:
        sector = ""

    return templates.TemplateResponse(
        "macro.html",
        {
            "request": request,
            "indices": earnings_scorecard.INDEX_CHOICES,
            "current_index": index,
            "quarters": quarters,
            "current_quarter": quarter,
            "sectors": earnings_scorecard.SECTORS,
            "current_sector": sector,
            "breadth_indices": market_breadth.INDEX_CHOICES,
            "breadth_periods": market_breadth.PERIOD_CHOICES,
            "calendar_periods": earnings_scorecard.CALENDAR_PERIODS,
            "economic_periods": fred_calendar.PERIOD_CHOICES,
            "economic_impacts": fred_calendar.IMPACT_CHOICES,
            "economic_countries": fred_calendar.COUNTRY_CHOICES,
            "macro_categories": google_trends.MACRO_CATEGORIES,
        },
    )


@app.post("/macro/auth")
async def macro_auth(request: Request):
    """Validate password and set auth cookie for macro page."""
    form = await request.form()
    password = form.get("password", "")
    if password == _SCREENER_PASSWORD:
        resp = RedirectResponse("/macro", status_code=303)
        resp.set_cookie(
            "scr_auth",
            _SCREENER_AUTH_TOKEN,
            max_age=60 * 60 * 24 * 30,
            httponly=True,
            samesite="lax",
        )
        return resp
    return templates.TemplateResponse(
        "screener_gate.html",
        {
            "request": request,
            "error": "Incorrect password. Please try again.",
            "gate_title": "Macro Dashboard",
            "gate_action": "/macro/auth",
            "gate_note": "The macro dashboard includes earnings scorecards, market breadth, economic calendars, and trend analysis. Contact us for access.",
        },
    )


@app.get("/api/macro/scorecard", response_class=HTMLResponse)
@_with_l2_fallback("macro:scorecard:{index}:{quarter}:{sector}", category="macro", l1_ttl=600, l2_ttl=14400)
async def macro_scorecard_api(
    request: Request,
    index: str = "all",
    quarter: str = "",
    sector: str = "",
):
    """HTMX endpoint — returns the earnings scorecard partial."""
    if not _screener_authed(request):
        raise HTTPException(status_code=401, detail="Unauthorized")

    from filings import earnings_scorecard

    if index not in earnings_scorecard.INDEX_CHOICES:
        index = "all"

    data, trend = await asyncio.gather(
        _to_heavy(
            earnings_scorecard.fetch_earnings_data,
            index, quarter or None, sector or None,
        ),
        _to_heavy(earnings_scorecard.fetch_historical_beat_rates, index),
    )

    quarters = earnings_scorecard.get_available_quarters()
    current_quarter = data.get("quarter", quarter or quarters[0])
    current_sector = sector if sector in earnings_scorecard.SECTORS else ""

    return templates.TemplateResponse(
        "partials/earnings_scorecard.html",
        {
            "request": request,
            "data": data,
            "trend_data": trend,
            "quarters": quarters,
            "current_quarter": current_quarter,
            "sectors": earnings_scorecard.SECTORS,
            "current_sector": current_sector,
        },
    )


@app.get("/api/macro/breadth", response_class=HTMLResponse)
@_with_l2_fallback("macro:breadth:{index}:{period}", category="macro", l1_ttl=600, l2_ttl=14400)
async def macro_breadth_api(
    request: Request,
    index: str = "sp500",
    period: str = "1d",
):
    """HTMX endpoint — returns the market breadth partial."""
    if not _screener_authed(request):
        raise HTTPException(status_code=401, detail="Unauthorized")

    from filings import market_breadth

    if index not in market_breadth.INDEX_CHOICES:
        index = "sp500"
    if period not in market_breadth.PERIOD_CHOICES:
        period = "1d"

    data, ad_line = await asyncio.gather(
        _to_heavy(market_breadth.fetch_breadth_data, index, period),
        _to_heavy(market_breadth.fetch_ad_line_history, index),
    )

    # Detect divergence between A/D momentum and index price
    divergence = market_breadth.detect_divergence(ad_line)

    return templates.TemplateResponse(
        "partials/market_breadth.html",
        {
            "request": request,
            "data": data,
            "ad_line": ad_line,
            "divergence": divergence,
        },
    )


@app.get("/api/macro/calendar", response_class=HTMLResponse)
@_with_l2_fallback("macro:calendar:{index}:{period}", category="macro", l1_ttl=600, l2_ttl=14400)
async def macro_calendar_api(
    request: Request,
    index: str = "all",
    period: str = "this_week",
):
    """HTMX endpoint — returns the earnings calendar partial."""
    if not _screener_authed(request):
        raise HTTPException(status_code=401, detail="Unauthorized")

    from filings import earnings_scorecard

    if index not in earnings_scorecard.INDEX_CHOICES:
        index = "all"
    if period not in earnings_scorecard.CALENDAR_PERIODS:
        period = "this_week"

    # Build superinvestor ticker set from fund cache
    ownership_map = _get_ownership_map()
    si_tickers = set(ownership_map.keys()) if ownership_map else set()

    data = await _to_heavy(
        earnings_scorecard.fetch_earnings_calendar,
        index, period, si_tickers,
    )

    # Attach ownership details (which SI names own each ticker)
    if ownership_map:
        for date_group in data.get("upcoming", []):
            for entry in date_group.get("entries", []):
                sym = entry.get("symbol", "")
                if sym in ownership_map:
                    entry["si_names"] = ownership_map[sym]
        for entry in data.get("just_reported", []):
            sym = entry.get("symbol", "")
            if sym in ownership_map:
                entry["si_names"] = ownership_map[sym]

    return templates.TemplateResponse(
        "partials/earnings_calendar_macro.html",
        {
            "request": request,
            "data": data,
            "periods": earnings_scorecard.CALENDAR_PERIODS,
            "current_period": period,
        },
    )


@app.get("/api/macro/economic", response_class=HTMLResponse)
@_with_l2_fallback("macro:economic:{period}:{country}:{impact}", category="macro", l1_ttl=600, l2_ttl=14400)
async def macro_economic_api(
    request: Request,
    period: str = "this_week",
    country: str = "us",
    impact: str = "all",
):
    """HTMX endpoint — returns the economic dashboard partial."""
    if not _screener_authed(request):
        raise HTTPException(status_code=401, detail="Unauthorized")

    from filings import fred_calendar

    if period not in fred_calendar.PERIOD_CHOICES:
        period = "this_week"
    if country not in fred_calendar.COUNTRY_CHOICES:
        country = "us"
    if impact not in fred_calendar.IMPACT_CHOICES:
        impact = "all"

    data = await _to_heavy(
        fred_calendar.fetch_economic_events,
        period, country, impact,
    )

    return templates.TemplateResponse(
        "partials/economic_dashboard.html",
        {"request": request, "data": data},
    )


@app.get("/api/macro/volatility", response_class=HTMLResponse)
@_with_l2_fallback("macro:volatility:{type}", category="macro", l1_ttl=600, l2_ttl=14400)
async def macro_volatility_api(
    request: Request,
    type: str = "total",
):
    """HTMX endpoint — returns volatility dashboard partial (P/C ratio, VIX term structure, SKEW)."""
    if not _screener_authed(request):
        raise HTTPException(status_code=401, detail="Unauthorized")

    from filings import cboe_data

    if type not in ("total", "index", "equity"):
        type = "total"

    putcall, vix_term, skew = await asyncio.gather(
        _to_heavy(cboe_data.get_put_call_ratio, type),
        _to_heavy(cboe_data.get_vix_term_structure),
        _to_heavy(cboe_data.get_skew_index),
    )

    return templates.TemplateResponse(
        "partials/macro_volatility.html",
        {
            "request": request,
            "putcall": putcall or [],
            "putcall_json": json_module.dumps(putcall or []),
            "vix_term": vix_term or {},
            "vix_term_json": json_module.dumps(vix_term or {}),
            "skew": skew or [],
            "skew_json": json_module.dumps(skew or []),
            "ratio_type": type,
        },
    )


# ─── FRED Economic Indicators ───────────────────────────────────────
@app.get("/api/macro/fred", response_class=HTMLResponse)
@_with_l2_fallback("macro:fred", category="macro", l1_ttl=600, l2_ttl=14400)
async def macro_fred_api(request: Request):
    """HTMX endpoint — FRED economic indicators (GDP, CPI, unemployment, rates)."""
    if not _screener_authed(request):
        raise HTTPException(status_code=401, detail="Unauthorized")

    from filings import fred_data

    data = await _to_heavy(fred_data.get_dashboard_data)
    return templates.TemplateResponse(
        "partials/macro_economic.html",
        {
            "request": request,
            "indicators": data or {},
            "indicators_json": json_module.dumps(data or {}, default=str),
        },
    )


# ─── Treasury Yield Curve ───────────────────────────────────────────
@app.get("/api/macro/treasury", response_class=HTMLResponse)
@_with_l2_fallback("macro:treasury", category="macro", l1_ttl=600, l2_ttl=14400)
async def macro_treasury_api(request: Request):
    """HTMX endpoint — Treasury yield curve + national debt."""
    if not _screener_authed(request):
        raise HTTPException(status_code=401, detail="Unauthorized")

    from filings import treasury_data

    data = await _to_heavy(treasury_data.get_treasury_dashboard)
    return templates.TemplateResponse(
        "partials/macro_treasury.html",
        {
            "request": request,
            "yield_curve": (data or {}).get("yield_curve") or {},
            "yield_curve_json": json_module.dumps((data or {}).get("yield_curve") or {}, default=str),
            "debt": (data or {}).get("debt") or {},
            "debt_json": json_module.dumps((data or {}).get("debt") or {}, default=str),
        },
    )


# ─── FX Rates ───────────────────────────────────────────────────────
@app.get("/api/macro/fx", response_class=HTMLResponse)
@_with_l2_fallback("macro:fx", category="macro", l1_ttl=600, l2_ttl=14400)
async def macro_fx_api(request: Request):
    """HTMX endpoint — FX rates dashboard."""
    if not _screener_authed(request):
        raise HTTPException(status_code=401, detail="Unauthorized")

    from filings import frankfurter

    data = await _to_heavy(frankfurter.get_fx_dashboard)
    return templates.TemplateResponse(
        "partials/macro_fx.html",
        {
            "request": request,
            "fx_data": data or {},
            "fx_json": json_module.dumps(data or {}, default=str),
        },
    )


# ─── WSB Sentiment (per-ticker) ─────────────────────────────────────
@app.get("/api/stock/{ticker}/wsb", response_class=HTMLResponse)
@_with_l2_fallback("stock:wsb:{ticker}", category="stock", l1_ttl=300, l2_ttl=3600)
async def stock_wsb_api(request: Request, ticker: str):
    """HTMX endpoint — WSB sentiment badge for a stock."""
    if not _valid_ticker(ticker):
        return HTMLResponse("")
    from filings import wsb_sentiment

    data = await _to_heavy(wsb_sentiment.get_ticker_sentiment, ticker.upper())
    return templates.TemplateResponse(
        "partials/stock_wsb_sentiment.html",
        {
            "request": request,
            "ticker": ticker.upper(),
            "wsb": data,  # None if ticker not in WSB top 50
        },
    )


# ─── Earnings Calendar (standalone page) ──────────────────────────────
@app.get("/earnings-calendar", response_class=HTMLResponse)
async def earnings_calendar_redirect(request: Request):
    """Backward-compat redirect: /earnings-calendar → /macro?tab=earnings."""
    return RedirectResponse(url="/macro?tab=earnings", status_code=301)


# Legacy v1 earnings-calendar handler — UNREACHABLE after the redirect above.
async def _legacy_v1_earnings_calendar_unused(request: Request):
    """Earnings Calendar page — interactive visual calendar of upcoming earnings."""
    from filings import earnings_calendar

    if not earnings_calendar.FEATURE_ENABLED:
        return templates.TemplateResponse(
            "under_construction.html",
            {"request": request},
            status_code=200,
        )

    return templates.TemplateResponse(
        "earnings_calendar.html",
        {"request": request},
    )


@app.get("/api/earnings-calendar/grid", response_class=HTMLResponse)
async def earnings_calendar_grid_api(
    request: Request,
    view: str = "weekly",
    offset: int = 0,
):
    """HTMX endpoint — returns the earnings calendar grid partial."""
    from filings import earnings_calendar
    from datetime import datetime, timedelta

    if view not in ("weekly", "monthly"):
        view = "weekly"
    offset = max(-24, min(24, offset))

    now = datetime.now()

    if view == "monthly":
        target_month = now.month + offset
        target_year = now.year
        while target_month > 12:
            target_month -= 12
            target_year += 1
        while target_month < 1:
            target_month += 12
            target_year -= 1

        data = await _to_heavy(
            earnings_calendar.get_month_view, target_year, target_month,
        )

        # Build month cells for template
        import calendar as cal_mod
        first_day = datetime(target_year, target_month, 1)
        # weekday(): Monday=0 ... Sunday=6
        start_weekday = first_day.weekday()
        days_in_month = cal_mod.monthrange(target_year, target_month)[1]

        by_date = data.get("by_date", {})
        max_count = max((len(v) for v in by_date.values()), default=0) or 1

        month_cells = []
        # Pad leading empty cells
        for _ in range(start_weekday):
            month_cells.append({"empty": True})

        for day_num in range(1, days_in_month + 1):
            d = datetime(target_year, target_month, day_num)
            date_str = d.strftime("%Y-%m-%d")
            day_entries = by_date.get(date_str, [])
            count = len(day_entries)
            top_tickers = [e["ticker"] for e in day_entries[:3]]

            # Heat level 0-4
            if count == 0:
                heat = 0
            elif count <= max_count * 0.25:
                heat = 1
            elif count <= max_count * 0.5:
                heat = 2
            elif count <= max_count * 0.75:
                heat = 3
            else:
                heat = 4

            month_cells.append({
                "empty": False,
                "day": day_num,
                "date": date_str,
                "count": count,
                "heat": heat,
                "top_tickers": top_tickers,
                "is_today": date_str == now.strftime("%Y-%m-%d"),
                "is_weekend": d.weekday() >= 5,
            })

        # Pad trailing empty cells
        trailing = (7 - len(month_cells) % 7) % 7
        for _ in range(trailing):
            month_cells.append({"empty": True})

        return templates.TemplateResponse(
            "partials/earnings_calendar_grid.html",
            {
                "request": request,
                "view": "monthly",
                "stats": data.get("stats", {}),
                "weeks": [],
                "month_cells": month_cells,
                "logo_tickers": _logo_set,
            },
        )
    else:
        # Weekly view
        days_since_monday = now.weekday()
        target_monday = now - timedelta(days=days_since_monday) + timedelta(weeks=offset)
        target_friday = target_monday + timedelta(days=4)

        data = await _to_heavy(
            earnings_calendar.get_earnings_calendar,
            target_monday.strftime("%Y-%m-%d"),
            target_friday.strftime("%Y-%m-%d"),
            1,
        )

        return templates.TemplateResponse(
            "partials/earnings_calendar_grid.html",
            {
                "request": request,
                "view": "weekly",
                "stats": data.get("stats", {}),
                "weeks": data.get("weeks", []),
                "month_cells": [],
                "logo_tickers": _logo_set,
            },
        )


@app.get("/api/earnings-calendar/day", response_class=HTMLResponse)
async def earnings_calendar_day_api(
    request: Request,
    date: str = "",
):
    """HTMX endpoint — returns the day detail table partial."""
    from filings import earnings_calendar
    from datetime import datetime

    if not date:
        date = datetime.now().strftime("%Y-%m-%d")
    elif not re.match(r"^\d{4}-\d{2}-\d{2}$", date):
        return PlainTextResponse("Invalid date format", status_code=400)

    data = await _to_heavy(
        earnings_calendar.get_earnings_calendar, date, date, 1,
    )
    entries = data.get("by_date", {}).get(date, [])
    has_actuals = any(e.get("eps_actual") is not None for e in entries)

    try:
        dt = datetime.strptime(date, "%Y-%m-%d")
        date_label = dt.strftime("%b %d %Y")    # canonical MMM DD YYYY
    except ValueError:
        date_label = date

    return templates.TemplateResponse(
        "partials/earnings_calendar_day.html",
        {
            "request": request,
            "entries": entries,
            "date": date,
            "date_label": date_label,
            "has_actuals": has_actuals,
            "logo_tickers": _logo_set,
        },
    )


_retail_cache = _HtmlCache(90)  # 90s — shorter than data cache TTL to pick up refreshes


@app.get("/retail", response_class=HTMLResponse)
async def retail_page(request: Request, view: str = "sentiment"):
    if view not in ("sentiment", "leaderboard", "calendar"):
        view = "sentiment"

    # Return cached HTML if fresh (avoids blocking on external APIs)
    if (cached := _retail_cache.get(view)) is not None:
        return cached

    # Fetch all three data sources independently — any failure returns None,
    # never blocks the page.  10-second overall timeout prevents Railway kill.
    fear_greed, apewisdom, high_impact_events = await asyncio.gather(
        _safe_fetch(_to_heavy(sentiment._get_cnn_fear_greed), "cnn_fear_greed"),
        _safe_fetch(_to_heavy(sentiment._get_apewisdom_all), "apewisdom"),
        _safe_fetch(asyncio.to_thread(youtube.get_high_impact_events, 9), "youtube"),
    )

    # Normalise: apewisdom returns list or None
    if not apewisdom:
        apewisdom = []

    # Compute summary stats from ApeWisdom data
    top_stocks = apewisdom[:5] if apewisdom else []
    biggest_mover = None
    if apewisdom:
        best = max(
            apewisdom,
            key=lambda s: (
                (s.get("rank_24h_ago") or s.get("rank", 0)) - s.get("rank", 0)
            ),
            default=None,
        )
        if best and (best.get("rank_24h_ago") or 0) > best.get("rank", 0):
            biggest_mover = best

    has_guru_data = bool(_fund_cache())

    resp = templates.TemplateResponse(
        "retail.html",
        {
            "request": request,
            "view": view,
            "fear_greed": fear_greed,
            "top_stocks": top_stocks,
            "biggest_mover": biggest_mover,
            "youtubers": _FINANCE_YOUTUBERS,
            "has_guru_data": has_guru_data,
            "high_impact_events": high_impact_events or [],
        },
    )

    _retail_cache.set(resp, view)
    return resp


@app.get("/api/retail/leaderboard", response_class=HTMLResponse)
async def retail_leaderboard_api(request: Request):
    all_data, fear_greed = await _fetch_retail_data()
    ownership_map = _get_ownership_map()
    enriched = sentiment.build_retail_leaderboard_data(
        all_data, ownership_map, fear_greed
    )
    return templates.TemplateResponse(
        "partials/retail_leaderboard_v2.html",
        {
            "request": request,
            "rows": enriched["leaderboard_rows"],
            "fear_greed": fear_greed,
            "metadata": enriched["metadata"],
        },
    )


@app.get("/api/retail/leaderboard-data")
async def retail_leaderboard_data(request: Request):
    """Enriched leaderboard JSON for treemap, bubble chart, and guru toggle."""
    all_data, fear_greed = await _fetch_retail_data()
    ownership_map = _get_ownership_map()
    result = sentiment.build_retail_leaderboard_data(
        all_data, ownership_map, fear_greed
    )
    return JSONResponse(content=result)


@app.get("/api/retail/calendar", response_class=HTMLResponse)
async def retail_calendar_api(request: Request):
    """HTML partial for the Calendar tab (lazy-loaded).

    Uses tiered fallback -- channels always available via L3 static list.
    Never returns an error: worst case is empty events + static channels.
    """
    try:
        events, channels, recent = await asyncio.gather(
            asyncio.to_thread(youtube.get_upcoming_events, 50),
            asyncio.to_thread(youtube.get_channels),
            asyncio.to_thread(youtube.get_recent_uploads, 50),
        )
    except Exception:
        logger.exception("Calendar API: unexpected error in data fetch")
        events, channels, recent = [], youtube._STATIC_CHANNELS, []

    # Inject channel thumbnail into each event + recent upload for avatar display
    ch_thumbs = {ch["channel_id"]: ch.get("thumbnail_url", "") for ch in channels}
    for ev in events:
        ev["channel_thumbnail"] = ch_thumbs.get(ev.get("channel_id", ""), "")
    for upl in recent:
        upl["channel_thumbnail"] = ch_thumbs.get(upl.get("channel_id", ""), "")

    calendar_data = youtube.build_calendar_data(events, channels, recent)
    return templates.TemplateResponse(
        "partials/retail_calendar.html",
        {
            "request": request,
            "events": calendar_data["events"],
            "channels": calendar_data["channels"],
            "recent_uploads": calendar_data["recent_uploads"],
            "stats": calendar_data["stats"],
            "high_impact": calendar_data["high_impact"],
        },
    )


@app.get("/api/retail/calendar-data")
async def retail_calendar_data(request: Request):
    """JSON data for calendar charts/interactivity."""
    try:
        events, channels, recent = await asyncio.gather(
            asyncio.to_thread(youtube.get_upcoming_events, 50),
            asyncio.to_thread(youtube.get_channels),
            asyncio.to_thread(youtube.get_recent_uploads, 50),
        )
    except Exception:
        logger.exception("Calendar data API: unexpected error in data fetch")
        events, channels, recent = [], youtube._STATIC_CHANNELS, []

    # Inject channel thumbnail into each event + recent upload for avatar display
    ch_thumbs = {ch["channel_id"]: ch.get("thumbnail_url", "") for ch in channels}
    for ev in events:
        ev["channel_thumbnail"] = ch_thumbs.get(ev.get("channel_id", ""), "")
    for upl in recent:
        upl["channel_thumbnail"] = ch_thumbs.get(upl.get("channel_id", ""), "")

    calendar_data = youtube.build_calendar_data(events, channels, recent)
    return JSONResponse(content=calendar_data)


def _group_insider_trades(trades: list) -> list[dict]:
    """Group insider trades by ticker for the accordion view."""
    ticker_groups: dict[str, dict] = {}
    for t in trades:
        key = (t.ticker or "").upper()
        if not key:
            continue
        if key not in ticker_groups:
            ticker_groups[key] = {
                "ticker": t.ticker,
                "company_name": t.company_name,
                "trades": [],
                "buy_count": 0,
                "sell_count": 0,
                "buy_value": 0.0,
                "sell_value": 0.0,
            }
        g = ticker_groups[key]
        g["trades"].append(t)
        val = insider_trading.parse_dollar_value(t.value)
        if "Purchase" in t.trade_type:
            g["buy_count"] += 1
            g["buy_value"] += val
        else:
            g["sell_count"] += 1
            g["sell_value"] += val
    return sorted(
        ticker_groups.values(),
        key=lambda g: g["buy_value"] + g["sell_value"],
        reverse=True,
    )


@app.get("/api/insider-trades", response_class=HTMLResponse)
async def insider_trades_api(
    request: Request, filter: str = "all", period: str = "",
):
    # Convert period to a trade_date cutoff.
    # SEC trade dates are US Eastern — use ET so "today" matches user intent.
    now_et = datetime.now(ZoneInfo("America/New_York"))
    if period == "today":
        since_date = now_et.strftime("%Y-%m-%d")
    elif period == "week":
        since_date = (now_et - timedelta(days=7)).strftime("%Y-%m-%d")
    elif period == "month":
        since_date = (now_et - timedelta(days=30)).strftime("%Y-%m-%d")
    else:
        # Default: current quarter (Q1=Jan, Q2=Apr, Q3=Jul, Q4=Oct)
        q_month = ((now_et.month - 1) // 3) * 3 + 1
        since_date = f"{now_et.year}-{q_month:02d}-01"

    trade_type = {"buys": "p", "sells": "s", "all": ""}.get(filter, "")
    trades, chart_data = await asyncio.gather(
        asyncio.to_thread(
            insider_trading.get_latest_insider_trades, trade_type, 100, since_date,
        ),
        asyncio.to_thread(
            insider_trading.get_insider_chart_data, 10, trade_type, since_date,
        ),
    )

    grouped = _group_insider_trades(trades)

    return templates.TemplateResponse(
        "partials/insider_trades.html",
        {
            "request": request,
            "trades": trades,
            "grouped": grouped,
            "chart_data_json": json_module.dumps(chart_data),
        },
    )


@app.get("/api/insider-trades/{ticker}", response_class=HTMLResponse)
@_with_l2_fallback("stock:insider_trades:{ticker}", category="stock", l1_ttl=300, l2_ttl=7200)
async def stock_insider_trades_api(request: Request, ticker: str):
    if not _valid_ticker(ticker):
        return PlainTextResponse("Invalid ticker", status_code=400)
    trades = await asyncio.to_thread(insider_trading.get_ticker_insider_trades, ticker)
    display = await asyncio.to_thread(insider_trading.prepare_ticker_display, trades)
    return templates.TemplateResponse(
        "partials/stock_insider_trades.html",
        {
            "request": request,
            "trades": trades,
            "ticker": ticker.upper(),
            "insiders": display["insiders"],
            "quarters": display["quarters"],
            "chart_json": json_module.dumps(display["chart"]),
            "per_insider_chart_json": json_module.dumps(display["per_insider_chart"]),
        },
    )


# ── Congress Trading endpoints ────────────────────────────────────


@app.get("/politician/{member_id}", response_class=HTMLResponse)
async def politician_page(request: Request, member_id: str):
    """Politician profile page — congressional trading activity."""
    if not _valid_member_id(member_id):
        raise HTTPException(status_code=400, detail="Invalid member ID")

    # Check per-politician cache (OrderedDict — O(1) LRU eviction)
    now = time_module.time()
    cached = _politician_cache.get(member_id)
    if cached and (now - cached[0]) < _POLITICIAN_CACHE_TTL:
        _politician_cache.move_to_end(member_id)  # mark as recently used
        display = cached[1]
    else:
        # Run member + trades fetch concurrently (independent queries)
        member, trades = await asyncio.gather(
            asyncio.to_thread(supabase_cache.get_congress_member, member_id),
            asyncio.to_thread(
                supabase_cache.get_congress_trades_by_member, member_id
            ),
        )
        if not member:
            raise HTTPException(status_code=404, detail="Politician not found")
        display = congress_trading.prepare_politician_display(trades or [], member)
        # LRU eviction: remove oldest (first item) if at capacity
        if len(_politician_cache) >= _POLITICIAN_CACHE_MAX:
            _politician_cache.popitem(last=False)  # O(1) eviction
        _politician_cache[member_id] = (now, display)

    return templates.TemplateResponse(
        "politician.html",
        {"request": request, **display},
    )


@app.get("/api/stock/{ticker}/ohlcv")
async def stock_ohlcv_api(
    ticker: str,
    period: str = Query("1Y", pattern="^(1M|3M|6M|1Y|5Y)$"),
):
    """Return OHLCV candlestick data for a stock ticker."""
    if not _valid_ticker(ticker):
        return JSONResponse({"error": "Invalid ticker"}, status_code=400)
    data = await _to_heavy(market_data.get_stock_ohlcv, ticker.upper(), period)
    if data is None:
        return JSONResponse({"error": "Data unavailable"}, status_code=404)
    return JSONResponse(data)


@app.get("/api/stock/{ticker}/congress", response_class=HTMLResponse)
@_with_l2_fallback("stock:congress:{ticker}", category="stock", l1_ttl=300, l2_ttl=7200)
async def stock_congress_api(request: Request, ticker: str):
    """Stock page Congress subtab — congressional trading for a ticker."""
    if not _valid_ticker(ticker):
        return PlainTextResponse("Invalid ticker", status_code=400)
    trades = await asyncio.to_thread(
        supabase_cache.get_congress_trades_by_ticker, ticker
    )
    display = congress_trading.prepare_stock_congress_display(
        ticker.upper(), trades or []
    )
    return templates.TemplateResponse(
        "partials/stock_congress.html",
        {"request": request, **display},
    )


# ── Congress page: /congress with in-memory cache ─────────────────

_congress_page_cache: dict = {"data": None, "ts": 0.0}
_CONGRESS_PAGE_TTL = 900  # 15 minutes

# Congress notification tracking — persisted to api_cache so deploys
# don't reset the watermark and miss filings.
_congress_last_notified_filing: str = ""
_CONGRESS_NOTIF_MAX = 10  # cap per refresh cycle
_CONGRESS_WATERMARK_KEY = "congress_notif_watermark"


async def _emit_congress_notifications(recent_trades: list[dict]) -> None:
    """Check for new congress filings and create notifications.

    Compares ``filing_date`` of recent trades against the last filing
    date we already notified about.  Watermark persisted to Supabase
    api_cache so it survives deploys.
    """
    global _congress_last_notified_filing

    if not recent_trades:
        return

    # Load watermark from Supabase on first call (survives deploys)
    if not _congress_last_notified_filing:
        try:
            cached = await asyncio.to_thread(
                supabase_cache.get_cached, _CONGRESS_WATERMARK_KEY
            )
            if cached and cached.get("watermark"):
                _congress_last_notified_filing = cached["watermark"]
                logger.info(
                    "Congress notifications: loaded watermark from cache: %s",
                    _congress_last_notified_filing,
                )
            else:
                # True first boot — seed to 7 days ago to catch recent filings
                _congress_last_notified_filing = (
                    datetime.now(timezone.utc) - timedelta(days=7)
                ).strftime("%Y-%m-%d")
                logger.info(
                    "Congress notifications: no saved watermark, seeding to %s",
                    _congress_last_notified_filing,
                )
        except Exception:
            _congress_last_notified_filing = (
                datetime.now(timezone.utc) - timedelta(days=7)
            ).strftime("%Y-%m-%d")
            logger.warning("Congress notifications: failed to load watermark, seeding to %s", _congress_last_notified_filing)

    # Filter trades filed after our watermark, with a real ticker
    new_trades = [
        t for t in recent_trades
        if (t.get("filing_date") or "") > _congress_last_notified_filing
        and t.get("ticker")
    ]
    if not new_trades:
        return

    # Sort by estimated amount descending — largest trades are most noteworthy
    def _mid(t: dict) -> float:
        low = t.get("amount_low") or 0
        high = t.get("amount_high") or 0
        return (low + high) / 2

    new_trades.sort(key=_mid, reverse=True)
    capped = new_trades[:_CONGRESS_NOTIF_MAX]

    notif_rows = []
    for t in capped:
        notif = notifications.create_congress_trade_notification(t)
        if notif:
            notif_rows.append(notif)

    if notif_rows:
        n = await asyncio.to_thread(supabase_cache.upsert_notifications, notif_rows)
        logger.info("Created %d congress trade notifications", n)

    # Advance watermark to the newest filing_date we just processed
    max_filing = max(t.get("filing_date", "") for t in new_trades)
    if max_filing > _congress_last_notified_filing:
        _congress_last_notified_filing = max_filing
        # Persist to Supabase so it survives deploys
        try:
            await asyncio.to_thread(
                supabase_cache.set_cached,
                _CONGRESS_WATERMARK_KEY,
                {"watermark": max_filing},
                ttl_hours=24 * 365,  # effectively permanent
            )
        except Exception:
            logger.warning("Failed to persist congress watermark")

# Per-politician profile cache (LRU-bounded, 15-min TTL)
# OrderedDict gives O(1) eviction via move_to_end + popitem
from collections import OrderedDict as _OrderedDict
_politician_cache: _OrderedDict[str, tuple[float, dict]] = _OrderedDict()
_POLITICIAN_CACHE_TTL = 900  # 15 minutes
_POLITICIAN_CACHE_MAX = 50   # max entries (LRU eviction)


_congress_page_lock = asyncio.Lock()


async def _get_congress_page_data() -> dict:
    """Fetch and cache all display data for the /congress page.

    Uses asyncio.gather for concurrent DB fetches and a lock to
    prevent thundering-herd cache stampede on TTL expiry.
    """
    now = time_module.time()
    if (
        _congress_page_cache["data"] is not None
        and (now - _congress_page_cache["ts"]) < _CONGRESS_PAGE_TTL
    ):
        return _congress_page_cache["data"]

    async with _congress_page_lock:
        # Double-check after acquiring lock (another request may have filled it)
        now = time_module.time()
        if (
            _congress_page_cache["data"] is not None
            and (now - _congress_page_cache["ts"]) < _CONGRESS_PAGE_TTL
        ):
            return _congress_page_cache["data"]

        # Concurrent DB fetches — these are independent queries
        members, all_trades, recent_trades = await asyncio.gather(
            asyncio.to_thread(supabase_cache.get_all_congress_members),
            asyncio.to_thread(supabase_cache.get_congress_all_ticker_trades, 50000),
            asyncio.to_thread(supabase_cache.get_congress_trades_recent_months, 6, 5000),
        )

        data = congress_trading.prepare_congress_page_data(
            members or [], all_trades or [], recent_trades or []
        )
        # Store raw recent trades so activity endpoint can re-filter
        data["_recent_trades"] = recent_trades or []

        _congress_page_cache["data"] = data
        _congress_page_cache["ts"] = time_module.time()

        # Emit notifications for any new filings since last refresh
        try:
            await _emit_congress_notifications(recent_trades or [])
        except Exception:
            logger.warning("Congress notification emission failed", exc_info=True)

        return data


@app.get("/congress", response_class=HTMLResponse)
async def congress_page(request: Request, view: str = "congress"):
    """Congress trading page — chamber visualization + holdings analytics."""
    if view not in ("congress", "holdings", "activity"):
        view = "congress"

    data = await _get_congress_page_data()

    return templates.TemplateResponse(
        "congress.html",
        {
            "request": request,
            "view": view,
            **data,
        },
    )


@app.get("/api/congress-activity", response_class=HTMLResponse)
async def congress_activity_api(
    request: Request,
    timeframe: str = "ALL",
    chamber: str = "all",
    party: str = "all",
):
    """Lazy-loaded activity dashboard for the Congress page.

    Accepts optional filter query params: timeframe (1W/1M/3M/ALL),
    chamber (all/house/senate), party (all/democrat/republican).
    """
    # Validate filter values
    if timeframe not in ("1W", "1M", "3M", "ALL"):
        timeframe = "ALL"
    if chamber.lower() not in ("all", "house", "senate"):
        chamber = "all"
    if party.lower() not in ("all", "democrat", "republican"):
        party = "all"

    # Fetch recent trades (6-month window covers all timeframes)
    page_data = await _get_congress_page_data()
    recent_trades = page_data.get("_recent_trades", [])

    # Build filtered activity dashboard (CPU-bound — run in thread)
    activity_data = await asyncio.to_thread(
        congress_trading.prepare_congress_activity,
        recent_trades,
        timeframe=timeframe,
        chamber=chamber,
        party=party,
        limit=200,
    )

    return templates.TemplateResponse(
        "partials/congress_activity.html",
        {"request": request, **activity_data},
    )


@app.get("/api/congress-trending", response_class=HTMLResponse)
async def congress_trending_api(request: Request):
    """Lazy-loaded trending chart partial for Congress — used on homepage."""
    data = await _get_congress_page_data()
    return templates.TemplateResponse(
        "partials/congress_trending.html",
        {"request": request, "trending": data.get("trending", [])},
    )


@app.get("/api/trending-combined", response_class=HTMLResponse)
async def trending_combined_api(request: Request):
    """Lazy-loaded combined trending chart — superinvestors + congress."""
    cache_data = _fund_cache()
    if not cache_data:
        return HTMLResponse(
            '<article>'
            '<p class="text-muted" aria-busy="true">Loading fund data...</p>'
            '</article>'
            '<div hx-get="/api/trending-combined" hx-trigger="load delay:5s" hx-swap="outerHTML"></div>'
        )

    # Fetch both datasets concurrently
    si_task = asyncio.to_thread(
        market_data.build_most_added_table, cache_data, SUPERINVESTORS_BY_CIK
    )
    cg_task = _get_congress_page_data()
    si_entries, cg_page_data = await asyncio.gather(si_task, cg_task)

    # Use the full 6-month congress data (same window as the congress page)
    # so the combined chart matches what users see on /congress.
    cg_trending = cg_page_data.get("trending", [])

    # Normalize known ticker renames so both datasets merge correctly.
    # 13F filings resolve tickers from CUSIP which can lag behind renames.
    _TICKER_ALIASES: dict[str, str] = {
        "FB": "META",
        "TWTR": "X",
    }

    # Merge datasets — union of tickers (canonical form)
    combined: dict[str, dict] = {}

    for e in si_entries:
        t = e.get("ticker")
        if not t:
            continue
        t = _TICKER_ALIASES.get(t, t)
        if t in combined:
            # Same ticker from a previous entry (shouldn't happen, but be safe)
            combined[t]["si_count"] += e["add_count"]
            combined[t]["si_adders"].extend(e.get("adders", []))
        else:
            combined[t] = {
                "ticker": t,
                "name": e.get("issuer_name", t),
                "si_count": e["add_count"],
                "si_adders": e.get("adders", []),
                "cg_count": 0,
                "cg_democrat": 0,
                "cg_republican": 0,
                "cg_traders": [],
            }

    for c in cg_trending:
        t = _TICKER_ALIASES.get(c["ticker"], c["ticker"])
        if t in combined:
            combined[t]["cg_count"] = c["add_count"]
            combined[t]["cg_democrat"] = c.get("democrat", 0)
            combined[t]["cg_republican"] = c.get("republican", 0)
            combined[t]["cg_traders"] = c.get("top_traders", [])
        else:
            combined[t] = {
                "ticker": t,
                "name": c.get("name", t),
                "si_count": 0,
                "si_adders": [],
                "cg_count": c["add_count"],
                "cg_democrat": c.get("democrat", 0),
                "cg_republican": c.get("republican", 0),
                "cg_traders": c.get("top_traders", []),
            }

    # Sort by total buyers desc, take top 30
    merged = sorted(
        combined.values(),
        key=lambda x: (x["si_count"] + x["cg_count"], x["si_count"]),
        reverse=True,
    )[:30]

    return templates.TemplateResponse(
        "partials/trending_combined.html",
        {"request": request, "merged": merged},
    )


# ── Insider insights: response cache + yfinance helper ────────────
_insights_cache: "OrderedDict[str, tuple[float, str]]"
try:
    _insights_cache = _OrderedDict()
except Exception:
    _insights_cache = {}  # type: ignore[assignment]
_INSIGHTS_TTL = 900     # 15 min — insights change at most daily
_INSIGHTS_MAX = 50      # cap entries to bound memory (~0.2 MB at 4 KB/entry)


def _insights_cache_set(key: str, html: str) -> None:
    """LRU-capped cache setter for rendered insights HTML."""
    _insights_cache[key] = (time_module.time(), html)
    if hasattr(_insights_cache, "move_to_end"):
        _insights_cache.move_to_end(key)
    while len(_insights_cache) > _INSIGHTS_MAX:
        if hasattr(_insights_cache, "popitem"):
            _insights_cache.popitem(last=False)  # evict oldest
        else:
            oldest = next(iter(_insights_cache))
            _insights_cache.pop(oldest, None)


async def _get_current_price_yf(ticker: str) -> float | None:
    """Fetch current stock price via yfinance (single Ticker object).

    Hard 8-second timeout prevents thread pool starvation when Yahoo
    Finance is slow or unresponsive.  Uses the app's shared thread pool
    instead of spawning a new ThreadPoolExecutor per call.
    """

    def _fetch():
        from filings.client import get_yfinance_info

        info = get_yfinance_info(ticker)
        p = info.get("currentPrice") or info.get("regularMarketPrice")
        if p and isinstance(p, (int, float)) and p > 0:
            return float(p)
        return None

    try:
        return await asyncio.wait_for(asyncio.to_thread(_fetch), timeout=8)
    except (asyncio.TimeoutError, Exception):
        return None


@app.get("/api/insider-insights/{ticker}", response_class=HTMLResponse)
async def insider_insights_api(request: Request, ticker: str):
    """Return insider purchase insights HTML partial for a stock."""
    if not _valid_ticker(ticker):
        return HTMLResponse("")

    key = ticker.upper()

    # ── L1: in-memory HTML cache (15 min TTL, LRU-capped) ──
    cached = _insights_cache.get(key)
    if cached and (time_module.time() - cached[0]) < _INSIGHTS_TTL:
        return HTMLResponse(cached[1])

    purchases = await asyncio.to_thread(
        supabase_cache.get_insider_purchases, key
    )
    if not purchases or len(purchases) < 2:
        return HTMLResponse("")  # Not enough data for insights

    # Single yfinance call (best-effort, non-blocking, 8s hard timeout)
    current_price = await _get_current_price_yf(key)

    insights_data = insider_insights.compute_insider_insights(
        ticker, purchases, current_price
    )
    if not insights_data:
        return HTMLResponse("")

    response = templates.TemplateResponse(
        "partials/insider_insights.html",
        {"request": request, "insights": insights_data},
    )

    # Cache the rendered HTML (LRU-capped)
    html_body = response.body.decode("utf-8")
    _insights_cache_set(key, html_body)

    return response


# --- Market Data API (heatmap, most-added, ticker search) ---


@app.get("/api/ticker-search-index")
async def ticker_search_index(request: Request):
    cache_data = _fund_cache()
    data = await _to_heavy(market_data.get_ticker_search_list, cache_data)
    # Strip fields the client doesn't need to reduce payload (~8000 items)
    slim = []
    for item in data:
        entry: dict = {
            "ticker": item["ticker"],
            "name": item["name"],
            "type": item["type"],
        }
        if item.get("held_by_super"):
            entry["held_by_super"] = True
        if item.get("in_sp500"):
            entry["in_sp500"] = True
        if item.get("exchange"):
            entry["exchange"] = item["exchange"]
        if item.get("sector"):
            entry["sector"] = item["sector"]
        if item.get("cik"):
            entry["cik"] = item["cik"]
        # Politician-specific fields needed by client search
        if item.get("member_id"):
            entry["member_id"] = item["member_id"]
        if item.get("party"):
            entry["party"] = item["party"]
        if item.get("chamber"):
            entry["chamber"] = item["chamber"]
        slim.append(entry)
    return JSONResponse(content=slim)


# --- Activity Feed Intelligence Dashboard (HTMX partial) ---


@app.get("/api/activity-feed", response_class=HTMLResponse)
async def api_activity_feed(
    request: Request,
    timeframe: str = "ALL",
    ptype: str = "guru",
    page: int = 1,
):
    """Enriched activity feed with conviction scores, clusters, and stats.

    Lazy-loaded via HTMX from the Activity tab on /grand-portfolio.
    Returns a partial HTML template (no <html>/<body> wrapper).
    Cached in Supabase for 30 minutes per timeframe+ptype.
    """
    if timeframe not in ("1W", "1M", "ALL"):
        timeframe = "ALL"
    if ptype not in ("guru", "institutional"):
        ptype = "guru"
    if page < 1:
        page = 1

    PER_PAGE = 50

    cache_data = _fund_cache()
    if not cache_data:
        return HTMLResponse(
            '<article><p class="text-muted">No activity data available yet. '
            "Data will load as superinvestor portfolios are cached.</p></article>"
        )

    clusters = []
    solo_items = []
    stats = {}
    has_prices = False

    # ── Check Supabase cache ──
    sb_cache_key = f"activity_feed:{timeframe}:{ptype}"
    try:
        cached = await asyncio.to_thread(supabase_cache.get_cached, sb_cache_key)
        if cached and isinstance(cached, dict):
            clusters_raw = cached.get("clusters", [])
            solo_raw = cached.get("solo_items", [])
            stats = cached.get("stats", {})
            stats.setdefault("total_buy_value", 0)
            stats.setdefault("total_sell_value", 0)
            stats.setdefault("net_dollar_flow", 0)
            stats.setdefault("value_sentiment", "NEUTRAL")
            has_prices = cached.get("has_prices", False)

            # Reconstruct dataclass instances from dicts
            from filings.models import EnrichedActivityItem, ActivityCluster

            solo_items = []
            for s in solo_raw:
                s.setdefault("fund_total_holdings", 0)
                s.setdefault("portfolio_impact", 0.0)
                s.setdefault("pct_share_change", None)
                s.setdefault("trade_value", 0.0)
                solo_items.append(EnrichedActivityItem(**s))
            clusters = []
            for c in clusters_raw:
                raw_items = c.pop("items", [])
                c.setdefault("buy_value", 0.0)
                c.setdefault("sell_value", 0.0)
                c.setdefault("net_flow", 0.0)
                items = []
                for i in raw_items:
                    i.setdefault("fund_total_holdings", 0)
                    i.setdefault("portfolio_impact", 0.0)
                    i.setdefault("pct_share_change", None)
                    i.setdefault("trade_value", 0.0)
                    items.append(EnrichedActivityItem(**i))
                clusters.append(ActivityCluster(**c, items=items))
    except Exception as e:
        logger.debug("Activity feed cache miss: %s", e)

    # ── Build fresh data if not from cache ──
    if not stats:
        all_tickers = set()
        for fund_data in cache_data.values():
            for h in fund_data.get("all_holdings", []):
                t = h.get("ticker")
                if t:
                    all_tickers.add(t)

        price_data = await asyncio.to_thread(
            market_data.get_current_prices_batch, list(all_tickers)
        )

        clusters, solo_items, stats = await asyncio.to_thread(
            client.build_enriched_activity_feed,
            cache_data,
            SUPERINVESTORS_BY_CIK,
            price_data,
            timeframe,
            ptype,
        )

        has_prices = bool(price_data)

        # ── Cache to Supabase ──
        try:
            from dataclasses import asdict

            serialized = {
                "clusters": [
                    {
                        **{k: v for k, v in asdict(c).items() if k != "items"},
                        "items": [asdict(i) for i in c.items],
                    }
                    for c in clusters
                ],
                "solo_items": [asdict(i) for i in solo_items],
                "stats": stats,
                "has_prices": has_prices,
            }
            await asyncio.to_thread(
                supabase_cache.set_cached,
                cache_key=sb_cache_key,
                category="activity_feed",
                data=serialized,
                ttl_seconds=3600,
            )
        except Exception as e:
            logger.debug("Activity feed cache write failed: %s", e)

    # ── Paginate solo_items ──
    total_solo = len(solo_items)
    start = (page - 1) * PER_PAGE
    paginated_solo = solo_items[start : start + PER_PAGE]
    has_more = (start + PER_PAGE) < total_solo
    next_page = page + 1 if has_more else None

    ctx = {
        "request": request,
        "solo_items": paginated_solo,
        "stats": stats,
        "timeframe": timeframe,
        "ptype": ptype,
        "has_prices": has_prices,
        "has_more": has_more,
        "next_page": next_page,
        "total_solo": total_solo,
        "page": page,
    }

    # Page 2+: return rows-only partial (appends to existing table)
    if page > 1:
        return templates.TemplateResponse("partials/activity_feed_rows.html", ctx)

    # Page 1: full dashboard with stats, controls, clusters, table
    ctx["clusters"] = clusters
    return templates.TemplateResponse("partials/activity_feed_content.html", ctx)


# ── Ticker Tape (curated stock scroll) ────────────────────────────────

TICKER_TAPE_SYMBOLS = [
    # Mega-cap tech (matches original TradingView tape)
    "AAPL", "MSFT", "AMZN", "GOOGL", "TSLA", "NVDA", "META",
    # Financials / Healthcare / Consumer
    "JPM", "V", "BRK-B", "UNH", "LLY", "JNJ", "HD", "WMT", "COST",
    # Other popular large-caps
    "NFLX", "AMD", "CRM", "AVGO", "XOM", "PG", "ADBE", "ORCL",
]


_ticker_tape_cache = _HtmlCache(120)  # 2-min L1 — matches browser SWR
_L2_TICKER_TAPE_KEY = "homepage:ticker_tape"
_L2_TICKER_TAPE_TTL = 3600  # 1h L2 fallback


@app.get("/api/ticker-tape", response_class=HTMLResponse)
async def ticker_tape_api(request: Request):
    """Lazy-loaded scrolling ticker tape for the homepage."""
    cached = _ticker_tape_cache.get()
    if cached:
        return cached
    if not getattr(app.state, "market_data_ready", False):
        # Cold start — try L2 before showing spinner
        stale = await asyncio.to_thread(_l2_get_html, _L2_TICKER_TAPE_KEY)
        if stale:
            return HTMLResponse(stale)
        return HTMLResponse(
            '<div hx-get="/api/ticker-tape" hx-trigger="load delay:5s" '
            'hx-swap="outerHTML">'
            '<p class="text-muted" style="text-align:center;font-size:0.85em;" '
            'aria-busy="true">Loading market data...</p></div>'
        )

    mkt, sparklines = await asyncio.gather(
        _to_heavy(market_data.get_sp500_market_data, "1D"),
        _to_heavy(market_data.get_sparkline_points, TICKER_TAPE_SYMBOLS, 20),
    )
    if not mkt or "_metadata" not in mkt:
        # Fetch failed — try L2 before spinner
        stale = await asyncio.to_thread(_l2_get_html, _L2_TICKER_TAPE_KEY)
        if stale:
            return HTMLResponse(stale)
        return HTMLResponse(
            '<div hx-get="/api/ticker-tape" hx-trigger="load delay:5s" '
            'hx-swap="outerHTML">'
            '<p class="text-muted" style="text-align:center;font-size:0.85em;" '
            'aria-busy="true">Market data loading...</p></div>'
        )

    tickers: list[dict] = []
    for sym in TICKER_TAPE_SYMBOLS:
        entry = mkt.get(sym)
        if entry and isinstance(entry, dict):
            tickers.append({
                "ticker": sym,
                "price": entry["price"],
                "pct_change": entry["pct_change"],
                "spark": sparklines.get(sym, []),
            })

    resp = templates.TemplateResponse(
        "partials/ticker_tape.html",
        {"request": request, "tickers": tickers},
    )
    _ticker_tape_cache.set(resp)
    asyncio.create_task(asyncio.to_thread(_l2_set_html, _L2_TICKER_TAPE_KEY, "homepage", resp.body, _L2_TICKER_TAPE_TTL))
    return resp


# ── Market Overview (custom replacement for TradingView) ──────────────

_MEGA_CAP_SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA"]

_MEGA_CAP_NAMES = {
    "AAPL": "Apple", "MSFT": "Microsoft", "GOOGL": "Alphabet",
    "AMZN": "Amazon", "NVDA": "Nvidia", "META": "Meta", "TSLA": "Tesla",
}


_overview_cache = _HtmlCache(300)  # 5-min L1 TTL
_L2_OVERVIEW_KEY = "homepage:market_overview"
_L2_OVERVIEW_TTL = 3600  # 1h L2 fallback


@app.get("/api/market-overview", response_class=HTMLResponse)
async def market_overview_api(request: Request):
    """Lazy-loaded custom Market Overview widget."""
    if not getattr(app.state, "market_data_ready", False):
        return HTMLResponse(
            '<div hx-get="/api/market-overview" hx-trigger="load delay:5s" '
            'hx-swap="outerHTML">'
            '<article><p aria-busy="true"><span class="spinner"></span> '
            "Loading market data...</p></article></div>"
        )

    # Return cached HTML if fresh (avoids re-fetching + re-rendering)
    if (cached := _overview_cache.get()) is not None:
        return cached

    try:
        # Fetch all data concurrently
        idx_task = _to_heavy(market_data.get_index_market_data)
        sp_task = _to_heavy(market_data.get_sp500_market_data, "1D")
        spark_task = asyncio.to_thread(
            market_data.get_sparkline_points, _MEGA_CAP_SYMBOLS, 20
        )
        idx_data, sp_data, spark_data = await asyncio.gather(
            idx_task, sp_task, spark_task
        )

        # Build tabs data
        indices = []
        commodities = []
        for sym, entry in (idx_data or {}).items():
            item = {
                "symbol": sym,
                "name": entry["name"],
                "price": entry["price"],
                "pct_change": entry["pct_change"],
                "point_change": entry.get("point_change", 0),
                "spark": entry.get("spark", []),
                "history": entry.get("history", []),
            }
            if entry["tab"] == "indices":
                indices.append(item)
            else:
                commodities.append(item)

        # Fetch all mega-cap chart data concurrently (was sequential)
        _valid_mega = [
            (sym, (sp_data or {}).get(sym))
            for sym in _MEGA_CAP_SYMBOLS
            if isinstance((sp_data or {}).get(sym), dict)
        ]
        chart_tasks = [
            _to_heavy(market_data.get_overview_chart_data, sym)
            for sym, _ in _valid_mega
        ]
        chart_results = await asyncio.gather(*chart_tasks) if chart_tasks else []

        mega_caps = []
        for (sym, entry), chart_data in zip(_valid_mega, chart_results):
            mega_caps.append({
                "symbol": sym,
                "name": _MEGA_CAP_NAMES.get(sym, sym),
                "price": entry["price"],
                "pct_change": entry["pct_change"],
                "point_change": round(
                    entry["price"] * entry["pct_change"] / 100, 2
                ),
                "spark": spark_data.get(sym, []),
                "history": chart_data["history"] if chart_data else [],
                "link": f"/stock/{sym}",
            })

        # Preserve order: indices follow _INDEX_SYMBOLS key order
        idx_order = [s for s in market_data._INDEX_SYMBOLS if market_data._INDEX_SYMBOLS[s]["tab"] == "indices"]
        indices.sort(key=lambda x: idx_order.index(x["symbol"]) if x["symbol"] in idx_order else 99)
        cmd_order = [s for s in market_data._INDEX_SYMBOLS if market_data._INDEX_SYMBOLS[s]["tab"] == "commodities"]
        commodities.sort(key=lambda x: cmd_order.index(x["symbol"]) if x["symbol"] in cmd_order else 99)

        if not indices and not mega_caps and not commodities:
            raise ValueError("All market data sources returned empty")

    except Exception:
        logger.warning("market_overview_api: fetch failed, trying L2", exc_info=True)
        stale = await asyncio.to_thread(_l2_get_html, _L2_OVERVIEW_KEY)
        if stale:
            return HTMLResponse(stale)
        return HTMLResponse(
            '<div hx-get="/api/market-overview" hx-trigger="load delay:5s" '
            'hx-swap="outerHTML">'
            '<article><p aria-busy="true"><span class="spinner"></span> '
            "Loading market data...</p></article></div>"
        )

    tabs = {
        "indices": indices,
        "mega_caps": mega_caps,
        "commodities": commodities,
    }

    resp = templates.TemplateResponse(
        "partials/market_overview.html",
        {
            "request": request,
            "tabs_json": json_module.dumps(tabs),
            "has_data": bool(indices or mega_caps or commodities),
        },
    )

    _overview_cache.set(resp)
    asyncio.create_task(asyncio.to_thread(_l2_set_html, _L2_OVERVIEW_KEY, "homepage", resp.body, _L2_OVERVIEW_TTL))
    return resp


@app.get("/api/market-overview-chart/{symbol:path}", response_class=JSONResponse)
async def market_overview_chart_api(
    request: Request,
    symbol: str,
    period: str = Query("1M", regex="^(1D|1W|1M|3M|1Y)$"),
):
    """Return chart data for a specific symbol (stock, index, or commodity)."""
    chart = await _to_heavy(market_data.get_overview_chart_data, symbol, period)
    if chart is None:
        return JSONResponse({"error": "Symbol not found"}, status_code=404)
    return JSONResponse(chart)


_live_activity_cache = _HtmlCache(300)  # 5-min TTL
_L2_LIVE_ACTIVITY_KEY = "homepage:live_activity"
_L2_LIVE_ACTIVITY_TTL = 3600  # 1h L2 fallback


@app.get("/api/live-activity", response_class=HTMLResponse)
async def live_activity_api(request: Request):
    """Return live activity feed HTML partial for homepage bento card."""
    cached = _live_activity_cache.get()
    if cached:
        return cached
    try:
        notifs = await asyncio.to_thread(
            supabase_cache.get_recent_notifications, 7
        )
        if not notifs:
            raise ValueError("empty notifications")
        for n in notifs:
            n["time_ago"] = _time_ago(n.get("created_at", ""))
        resp = templates.TemplateResponse(
            "partials/live_activity.html",
            {"request": request, "notifications": notifs},
        )
        _live_activity_cache.set(resp)
        asyncio.create_task(asyncio.to_thread(_l2_set_html, _L2_LIVE_ACTIVITY_KEY, "homepage", resp.body, _L2_LIVE_ACTIVITY_TTL))
        return resp
    except Exception:
        logger.warning("live_activity_api: fetch failed, trying L2", exc_info=True)
        stale = await asyncio.to_thread(_l2_get_html, _L2_LIVE_ACTIVITY_KEY)
        if stale:
            return HTMLResponse(stale)
        return HTMLResponse(
            '<p class="text-muted" style="text-align:center;padding:1em;">Activity feed temporarily unavailable.</p>'
            '<div hx-get="/api/live-activity" hx-trigger="load delay:5s" hx-swap="outerHTML"></div>'
        )


_market_news_cache = _HtmlCache(300)  # 5-min L1 TTL
_L2_NEWS_KEY = "homepage:market_news"
_L2_NEWS_TTL = 7200  # 2h L2 fallback


@app.get("/api/market-news", response_class=HTMLResponse)
async def market_news_api(request: Request):
    """Return market news HTML partial (lazy-loaded via HTMX)."""
    cached = _market_news_cache.get()
    if cached:
        return cached
    articles = await _to_heavy(market_data.get_market_news)
    if not articles:
        # Try L2 stale fallback before showing error
        stale = await asyncio.to_thread(_l2_get_html, _L2_NEWS_KEY)
        if stale:
            return HTMLResponse(stale)
        return HTMLResponse(
            "<article>"
            '<p class="text-muted" style="text-align:center;">Market news temporarily unavailable.</p>'
            "</article>"
            '<div hx-get="/api/market-news" hx-trigger="load delay:5s" hx-swap="outerHTML"></div>'
        )
    resp = templates.TemplateResponse(
        "partials/market_news.html",
        {"request": request, "articles": articles},
    )
    _market_news_cache.set(resp)
    asyncio.create_task(asyncio.to_thread(_l2_set_html, _L2_NEWS_KEY, "homepage", resp.body, _L2_NEWS_TTL))
    return resp


_retail_sentiment_cache = _HtmlCache(1800)  # 30-min HTML TTL — data changes ~once/day
_L2_RETAIL_SENTIMENT_KEY = "homepage:retail_sentiment"
_L2_RETAIL_SENTIMENT_TTL = 7200  # 2h L2 fallback


@app.get("/api/retail-sentiment", response_class=HTMLResponse)
async def retail_sentiment_api(request: Request):
    """Return retail sentiment HTML partial (lazy-loaded via HTMX)."""
    cached = _retail_sentiment_cache.get()
    if cached:
        return cached
    data = await _to_heavy(sentiment.get_retail_sentiment_overview)
    if not data or (data.get("fear_greed") is None and not data.get("top_movers")):
        # Try L2 stale fallback before showing error
        stale = await asyncio.to_thread(_l2_get_html, _L2_RETAIL_SENTIMENT_KEY)
        if stale:
            return HTMLResponse(stale)
        return HTMLResponse(
            '<p class="text-muted" style="text-align:center;padding:2em 0.5em;">'
            "Sentiment data temporarily unavailable.</p>"
            '<div hx-get="/api/retail-sentiment" hx-trigger="load delay:5s" hx-swap="outerHTML"></div>'
        )
    resp = templates.TemplateResponse(
        "partials/retail_sentiment.html",
        {"request": request, **data},
    )
    _retail_sentiment_cache.set(resp)
    asyncio.create_task(asyncio.to_thread(_l2_set_html, _L2_RETAIL_SENTIMENT_KEY, "homepage", resp.body, _L2_RETAIL_SENTIMENT_TTL))
    return resp


_L2_HEATMAP_KEY = "homepage:heatmap"
_L2_HEATMAP_TTL = 3600  # 1h L2 fallback
_heatmap_cache = _HtmlCache(120)  # 2-min L1 — keyed by period


@app.get("/api/heatmap", response_class=HTMLResponse)
async def heatmap(request: Request, period: str = "1D"):
    # Validate period
    if period not in ("1D", "1W", "1M"):
        period = "1D"

    cached = _heatmap_cache.get(period)
    if cached:
        return cached

    if not getattr(app.state, "market_data_ready", False):
        # Cold start — try L2 before showing spinner
        stale = await asyncio.to_thread(_l2_get_html, f"{_L2_HEATMAP_KEY}:{period}")
        if stale:
            return HTMLResponse(stale)
        return HTMLResponse(
            "<article>"
            '<p aria-busy="true">Loading S&P 500 market data (first load takes ~30s)...</p>'
            "</article>"
            '<div hx-get="/api/heatmap" hx-trigger="load delay:5s" hx-swap="outerHTML"></div>'
        )

    mkt, constituents = await asyncio.gather(
        _to_heavy(market_data.get_sp500_market_data, period),
        _to_heavy(market_data.get_sp500_constituents),
    )
    if not mkt or "_metadata" not in mkt:
        stale = await asyncio.to_thread(_l2_get_html, f"{_L2_HEATMAP_KEY}:{period}")
        if stale:
            return HTMLResponse(stale)
        return HTMLResponse(
            '<article>'
            '<p class="text-muted" aria-busy="true">Market data loading...</p>'
            '</article>'
            f'<div hx-get="/api/heatmap?period={period}" hx-trigger="load delay:5s" hx-swap="outerHTML"></div>'
        )

    ownership_map = _get_ownership_map()
    super_ticker_counts = {t: len(holders) for t, holders in ownership_map.items()}

    heatmap_data = market_data.build_heatmap_data(
        mkt, constituents, super_ticker_counts, period=period
    )
    metadata = mkt.get("_metadata", {})

    resp = templates.TemplateResponse(
        "partials/heatmap.html",
        {
            "request": request,
            "heatmap_json": json_module.dumps(heatmap_data),
            "metadata": metadata,
            "period": period,
        },
    )
    _heatmap_cache.set(resp, period)
    asyncio.create_task(asyncio.to_thread(_l2_set_html, f"{_L2_HEATMAP_KEY}:{period}", "homepage", resp.body, _L2_HEATMAP_TTL))
    return resp


@app.get("/api/heatmap-data")
async def heatmap_data_api(request: Request, period: str = "1D"):
    """JSON-only heatmap data for client-side period switching (no full re-render)."""
    if period not in ("1D", "1W", "1M"):
        period = "1D"

    if not getattr(app.state, "market_data_ready", False):
        return JSONResponse({"error": "loading"}, status_code=503)

    mkt, constituents = await asyncio.gather(
        _to_heavy(market_data.get_sp500_market_data, period),
        _to_heavy(market_data.get_sp500_constituents),
    )
    if not mkt or "_metadata" not in mkt:
        return JSONResponse({"error": "loading"}, status_code=503)

    ownership_map = _get_ownership_map()
    super_ticker_counts = {t: len(holders) for t, holders in ownership_map.items()}

    heatmap_data = market_data.build_heatmap_data(
        mkt, constituents, super_ticker_counts, period=period
    )
    metadata = mkt.get("_metadata", {})

    period_labels = {"1D": "today", "1W": "this week", "1M": "this month"}

    return JSONResponse({
        "data": heatmap_data,
        "period_label": period_labels.get(period, period),
        "metadata": {
            "count": metadata.get("count", 0),
            "fetched_at": metadata.get("fetched_at", ""),
            "period_label": period_labels.get(period, period),
        },
    })


_most_added_cache = _HtmlCache(900)  # 15 minutes — data changes infrequently


@app.get("/api/most-added", response_class=HTMLResponse)
async def most_added(request: Request):
    cache_data = _fund_cache()
    if not cache_data:
        return HTMLResponse(
            '<article>'
            '<p class="text-muted" aria-busy="true">Loading fund data...</p>'
            '</article>'
            '<div hx-get="/api/most-added" hx-trigger="load delay:5s" hx-swap="outerHTML"></div>'
        )

    # Return cached HTML if fresh
    if (cached := _most_added_cache.get()) is not None:
        return cached

    entries = await asyncio.to_thread(
        market_data.build_most_added_table, cache_data, SUPERINVESTORS_BY_CIK
    )

    tickers_to_lookup = [e["ticker"] for e in entries if e.get("ticker")]

    range_data = {}
    if tickers_to_lookup:
        range_data = await asyncio.to_thread(
            market_data.get_52_week_range_bulk, tickers_to_lookup
        )

    # Parallelize analyst lookups with web-layer cache (30 min TTL)
    now = time_module.time()
    consensus_map: dict[str, dict | None] = {}
    stale_tickers: list[str] = []

    for e in entries:
        t = e.get("ticker")
        if not t:
            continue
        cached = _consensus_cache.get(t)
        if cached and (now - cached[0]) < _CONSENSUS_TTL:
            consensus_map[t] = cached[1]
            try:
                _consensus_cache.move_to_end(t)  # mark as recently used for LRU
            except (AttributeError, KeyError):
                pass
        else:
            stale_tickers.append(t)

    if stale_tickers:

        async def _lookup_consensus(t: str) -> tuple[str, dict | None]:
            try:
                ratings = await _to_heavy(analysts.get_analyst_ratings, t)
                result = analysts.get_consensus_summary(ratings)
            except Exception:
                result = None
            _consensus_cache_set(t, (time_module.time(), result))
            return t, result

        tasks = [_lookup_consensus(t) for t in stale_tickers]
        results = await asyncio.gather(*tasks)
        consensus_map.update(dict(results))

    for entry in entries:
        ticker = entry.get("ticker")
        if ticker:
            entry["consensus"] = consensus_map.get(ticker)
            r = range_data.get(ticker)
            entry["range_pct"] = r["pct_of_range"] if r else None
            entry["current_price"] = r["current"] if r else None
            entry["range_low"] = r["low"] if r else None
            entry["range_high"] = r["high"] if r else None
        else:
            entry["consensus"] = None
            entry["range_pct"] = None
            entry["current_price"] = None
            entry["range_low"] = None
            entry["range_high"] = None

    resp = templates.TemplateResponse(
        "partials/most_added.html",
        {
            "request": request,
            "entries": entries,
        },
    )

    _most_added_cache.set(resp)
    return resp


# ═══════════════════════════════════════════════════════════════════════
# Authentication pages
# ═══════════════════════════════════════════════════════════════════════


# /profile, /login, /signup, /logout, /api/webhooks/clerk moved to
# filings.routers.auth_admin (audit-sprint-6).


# ═══════════════════════════════════════════════════════════════════════
# Watchlist API moved to filings.routers.watchlist (audit-sprint-4).
# ═══════════════════════════════════════════════════════════════════════


# Admin panel (/admin, /admin/user/{user_id}) moved to
# filings.routers.auth_admin (audit-sprint-6).


# ═══════════════════════════════════════════════════════════════════════
# Infrastructure endpoints
# ═══════════════════════════════════════════════════════════════════════


async def _get_congress_sync_summary() -> dict:
    """Build a summary dict of congress sync status for /health/detail."""
    try:
        logs = await asyncio.to_thread(supabase_cache.get_latest_congress_sync, 5)
        if not logs:
            return {"status": "no_data", "last_sync": None, "recent_runs": []}
        latest = logs[0]
        return {
            "status": latest.get("status", "unknown"),
            "last_sync": latest.get("started_at"),
            "last_trades_found": latest.get("new_trades", 0),
            "last_duration_secs": latest.get("duration_secs"),
            "recent_runs": [
                {
                    "started_at": r.get("started_at"),
                    "status": r.get("status"),
                    "new_trades": r.get("new_trades", 0),
                    "pages_scraped": r.get("pages_scraped", 0),
                }
                for r in logs
            ],
        }
    except Exception:
        return {"status": "check_failed"}


_HEALTH_SECRET: str = os.environ.get("HEALTH_SECRET", "")


@app.get("/health/detail")
async def health_detail(request: Request):
    """Detailed health info gated behind a secret header.

    Set ``HEALTH_SECRET`` env var and pass ``X-Health-Secret`` header
    to access.  Returns 404 when secret is missing or wrong (hides
    endpoint existence from attackers).
    """
    provided = request.headers.get("x-health-secret", "")
    if not _HEALTH_SECRET or provided != _HEALTH_SECRET:
        raise StarletteHTTPException(status_code=404)

    uptime = round(time_module.time() - _app_start_time)
    cache_data = _fund_cache()

    # Count stale funds for observability
    all_ciks = [si.cik for si in SUPERINVESTORS]
    stale_count = sum(
        1
        for cik in all_ciks
        if cik not in cache_data or cache.is_fund_stale(cache_data.get(cik, {}))
    )

    import resource

    rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1024 * 1024)

    return JSONResponse(
        {
            "status": "ok",
            "uptime_seconds": uptime,
            "memory_mb": round(rss_mb, 1),
            "cache_entries": len(cache_data),
            "cache_age": cache.get_cache_age_str(cache_data),
            "total_funds": len(SUPERINVESTORS),
            "stale_funds": stale_count,
            "insider_title_cache_size": len(insider_trading._title_cache),
            "insider_trade_cache_size": len(insider_trading._cache),
            "market_data_ready": getattr(app.state, "market_data_ready", False),
            "supabase_connected": supabase_cache.is_available(),
            "background_refresh": {
                "enabled": _ENABLE_BACKGROUND_REFRESH,
                "status": getattr(app.state, "refresh_status", "unknown"),
                "progress": getattr(app.state, "refresh_progress", {}),
                "in_progress_ciks": len(_refresh_in_progress),
            },
            "vitals_cache": vitals.get_vitals_cache_info(),
            "congress_sync": await _get_congress_sync_summary(),
        }
    )


# /robots.txt and /llms.txt moved to filings.routers.static_pages.


# ── Sitemap cache (regenerated at most once per hour) ─────────────────
_sitemap_cache: dict[str, object] = {"xml": None, "ts": 0.0}
_SITEMAP_TTL = 3600  # 1 hour


@app.get("/sitemap.xml")
async def sitemap_xml():
    """Dynamic sitemap: static pages + all superinvestor holdings + stock pages
    + congress pages + politician pages.
    Cached for 1 hour to avoid O(n*m) iteration on every crawler request.
    """
    now = time_module.monotonic()
    if _sitemap_cache["xml"] and (now - _sitemap_cache["ts"]) < _SITEMAP_TTL:
        return PlainTextResponse(
            _sitemap_cache["xml"], media_type="application/xml"
        )

    base_url = "https://paperpanda.io"
    today = date.today().isoformat()

    # ── Static pages (no redirects — only real destination URLs) ──
    urls = [
        f"  <url><loc>{base_url}/</loc><lastmod>{today}</lastmod><changefreq>daily</changefreq><priority>1.0</priority></url>",
        f"  <url><loc>{base_url}/funds</loc><lastmod>{today}</lastmod><changefreq>daily</changefreq><priority>0.9</priority></url>",
        f"  <url><loc>{base_url}/insider-trading</loc><lastmod>{today}</lastmod><changefreq>daily</changefreq><priority>0.8</priority></url>",
        f"  <url><loc>{base_url}/congress</loc><lastmod>{today}</lastmod><changefreq>daily</changefreq><priority>0.8</priority></url>",
        f"  <url><loc>{base_url}/retail</loc><lastmod>{today}</lastmod><changefreq>daily</changefreq><priority>0.8</priority></url>",
        f"  <url><loc>{base_url}/alternative-signals</loc><lastmod>{today}</lastmod><changefreq>weekly</changefreq><priority>0.7</priority></url>",
        f"  <url><loc>{base_url}/macro</loc><lastmod>{today}</lastmod><changefreq>daily</changefreq><priority>0.7</priority></url>",
        f"  <url><loc>{base_url}/deployment</loc><lastmod>{today}</lastmod><changefreq>weekly</changefreq><priority>0.7</priority></url>",
        f"  <url><loc>{base_url}/support</loc><lastmod>{today}</lastmod><changefreq>monthly</changefreq><priority>0.5</priority></url>",
        f"  <url><loc>{base_url}/notifications</loc><lastmod>{today}</lastmod><changefreq>daily</changefreq><priority>0.4</priority></url>",
    ]

    # ── Superinvestor fund pages ──
    cache_data = _fund_cache()
    for si in SUPERINVESTORS:
        # Use filing_date as lastmod if available
        fd = cache_data.get(si.cik, {}).get("filing_date", "") if cache_data else ""
        lastmod = f"<lastmod>{fd}</lastmod>" if fd else ""
        urls.append(
            f"  <url><loc>{base_url}/holdings/{si.cik}</loc>"
            f"{lastmod}<changefreq>weekly</changefreq><priority>0.7</priority></url>"
        )

    # ── Stock pages (all unique tickers held by superinvestors) ──
    seen_tickers: set[str] = set()
    if cache_data:
        for cik, fund_data in cache_data.items():
            for h in fund_data.get("all_holdings", []):
                ticker = h.get("ticker")
                if ticker and ticker not in seen_tickers:
                    seen_tickers.add(ticker)
                    urls.append(
                        f"  <url><loc>{base_url}/stock/{ticker}</loc>"
                        f"<changefreq>weekly</changefreq><priority>0.6</priority></url>"
                    )

    # ── Politician pages ──
    try:
        members = await asyncio.to_thread(supabase_cache.get_all_congress_members)
        if members:
            for m in members:
                mid = m.get("member_id", "")
                if mid:
                    urls.append(
                        f"  <url><loc>{base_url}/politician/{mid}</loc>"
                        f"<changefreq>weekly</changefreq><priority>0.5</priority></url>"
                    )
    except Exception:
        pass  # Politician pages are best-effort

    xml = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        + "\n".join(urls)
        + "\n"
        "</urlset>\n"
    )

    _sitemap_cache["xml"] = xml
    _sitemap_cache["ts"] = now

    return PlainTextResponse(xml, media_type="application/xml")


# ═══════════════════════════════════════════════════════════════════════
# Stock Screener — DCF / Monte Carlo / Comps valuation tool
# ═══════════════════════════════════════════════════════════════════════

_SCREENER_PASSWORD = os.environ.get("SCREENER_PASSWORD", "paperpanda2026")
import hashlib as _hashlib
_SCREENER_AUTH_TOKEN = _hashlib.sha256(
    f"scr:{_SCREENER_PASSWORD}".encode()
).hexdigest()[:32]


def _screener_authed(request: Request) -> bool:
    """Check if the screener auth cookie is valid."""
    return request.cookies.get("scr_auth", "") == _SCREENER_AUTH_TOKEN


@app.get("/screener", response_class=HTMLResponse)
async def screener_page(request: Request):
    """Interactive stock valuation screener (DCF, Monte Carlo, Comps)."""
    return templates.TemplateResponse("screener.html", {"request": request})


@app.get("/api/screener/peers", response_class=JSONResponse)
async def api_screener_peers(request: Request, tickers: str = ""):
    """Batch-fetch valuation data for selected peers (parallel)."""
    if not request.state.user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    import asyncio
    from filings import screener

    raw = [t.strip().upper() for t in tickers.split(",") if t.strip()]
    valid = [t for t in raw if _valid_ticker(t)][:10]
    if not valid:
        return JSONResponse([])

    tasks = [_to_heavy(screener.get_peer_valuation, t) for t in valid]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    peers = []
    for i, r in enumerate(results):
        if isinstance(r, Exception):
            logger.warning("Peer valuation failed for %s: %s", valid[i], r)
        elif r is None:
            logger.warning("Peer valuation returned None for %s", valid[i])
        else:
            peers.append(r)
    logger.info("Peer valuation: requested=%d, returned=%d", len(valid), len(peers))
    return JSONResponse(peers)


@app.get("/api/screener/{ticker}", response_class=JSONResponse)
async def api_screener_data(request: Request, ticker: str):
    """Return all data needed for client-side valuation calculations.

    Fetches yfinance info, SEC fundamentals, and forward estimates in
    parallel via asyncio.gather, then assembles the result on the heavy pool.
    """
    if not request.state.user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    if not _valid_ticker(ticker):
        return PlainTextResponse("Invalid ticker", status_code=400)
    from filings import screener

    ticker = ticker.upper()

    # Run all 3 independent data fetches in parallel
    yf_info, fund_data, fwd_estimates = await asyncio.gather(
        _to_heavy(screener.fetch_yf_info, ticker),
        _to_heavy(screener.fetch_fundamentals, ticker),
        _to_heavy(screener.fetch_forward_estimates, ticker),
    )

    if not yf_info.get("_resolved_price"):
        return JSONResponse({"error": "No data available"}, status_code=404)

    # Assembly + 52w/beta fallbacks — run on heavy pool to avoid
    # blocking the event loop
    data = await _to_heavy(
        screener.assemble_screener_data,
        ticker, yf_info, fund_data, fwd_estimates,
    )
    if not data:
        return JSONResponse({"error": "No data available"}, status_code=404)
    return JSONResponse(data)


# ═══════════════════════════════════════════════════════════════════════
# Rate limiting decorators (applied only if slowapi installed)
# ═══════════════════════════════════════════════════════════════════════

if _has_limiter:
    # Page routes — generous limits; these are full-page renders cached at L1/L2.
    # NOTE: v1 page handlers below (homepage, funds_page, stock_detail, etc.)
    # are unreachable in production — the redesign router is registered
    # before them and wins matching paths.  Decorating them is harmless;
    # these lines can be removed alongside the v1 handlers in a future cleanup.
    homepage = limiter.limit("60/minute")(homepage)
    funds_page = limiter.limit("30/minute")(funds_page)
    stock_detail = limiter.limit("60/minute")(stock_detail)
    stock_detail_by_cusip = limiter.limit("60/minute")(stock_detail_by_cusip)
    # Per-ticker API endpoints (stock page tabs — L2 cached, serve fast)
    fund_row = limiter.limit("30/minute")(fund_row)
    analyst_ratings = limiter.limit("60/minute")(analyst_ratings)
    sentiment_data = limiter.limit("60/minute")(sentiment_data)
    vitals_data = limiter.limit("60/minute")(vitals_data)
    company_filings_tab = limiter.limit("60/minute")(company_filings_tab)
    insider_trades_api = limiter.limit("30/minute")(insider_trades_api)
    stock_insider_trades_api = limiter.limit("60/minute")(stock_insider_trades_api)
    stock_congress_api = limiter.limit("60/minute")(stock_congress_api)
    politician_page = limiter.limit("60/minute")(politician_page)
    congress_page = limiter.limit("60/minute")(congress_page)
    congress_activity_api = limiter.limit("60/minute")(congress_activity_api)
    congress_trending_api = limiter.limit("60/minute")(congress_trending_api)
    trending_combined_api = limiter.limit("30/minute")(trending_combined_api)
    insider_insights_api = limiter.limit("60/minute")(insider_insights_api)
    # Per-ticker endpoints not previously covered (audit epic A)
    api_financials = limiter.limit("60/minute")(api_financials)
    api_financials_history = limiter.limit("30/minute")(api_financials_history)
    earnings_data = limiter.limit("60/minute")(earnings_data)
    forward_estimates = limiter.limit("60/minute")(forward_estimates)
    web_traffic_data = limiter.limit("60/minute")(web_traffic_data)
    signals_data = limiter.limit("60/minute")(signals_data)
    heatmap_data_api = limiter.limit("60/minute")(heatmap_data_api)
    # Homepage widget endpoints — L2 cached, cheap to serve, loaded together
    ticker_search_index = limiter.limit("30/minute")(ticker_search_index)
    alt_signals_short_interest = limiter.limit("30/minute")(alt_signals_short_interest)
    retail_leaderboard_api = limiter.limit("60/minute")(retail_leaderboard_api)
    retail_leaderboard_data = limiter.limit("60/minute")(retail_leaderboard_data)
    retail_calendar_api = limiter.limit("60/minute")(retail_calendar_api)
    retail_calendar_data = limiter.limit("60/minute")(retail_calendar_data)
    ticker_tape_api = limiter.limit("60/minute")(ticker_tape_api)
    market_overview_api = limiter.limit("60/minute")(market_overview_api)
    market_overview_chart_api = limiter.limit("60/minute")(market_overview_chart_api)
    market_news_api = limiter.limit("60/minute")(market_news_api)
    retail_sentiment_api = limiter.limit("60/minute")(retail_sentiment_api)
    heatmap = limiter.limit("60/minute")(heatmap)
    most_added = limiter.limit("60/minute")(most_added)
    api_activity_feed = limiter.limit("60/minute")(api_activity_feed)
    portfolio_chart_data = limiter.limit("60/minute")(portfolio_chart_data)
    compare_api = limiter.limit("30/minute")(compare_api)
    # Google Trends endpoints (GT rate-limits are strict, be conservative)
    gt_trending_api = limiter.limit("15/minute")(gt_trending_api)
    gt_macro_api = limiter.limit("15/minute")(gt_macro_api)
    gt_ticker_api = limiter.limit("15/minute")(gt_ticker_api)
    # Convergence engine (aggregates multiple signal sources)
    options_convergence_api = limiter.limit("20/minute")(options_convergence_api)
    # Auth pages (bot/scraper prevention) — login_page and signup_page moved
    # to filings.routers.auth_admin; default 60/min limit still applies.
    # TODO: re-apply strict 10/minute limit inside the auth_admin router.
    # Infrastructure monitoring
    health_detail = limiter.limit("5/minute")(health_detail)
    # Notification endpoints (polled frequently — generous limits)
    notification_bell = limiter.limit("120/minute")(notification_bell)
    notification_recent = limiter.limit("60/minute")(notification_recent)
    notification_count = limiter.limit("120/minute")(notification_count)
    notifications_page = limiter.limit("30/minute")(notifications_page)


# ═══════════════════════════════════════════════════════════════════════
# Entry point
# ═══════════════════════════════════════════════════════════════════════


def main():
    """Entry point for `uv run filings-web`."""
    # Auto-load .env for local development (skipped on Railway)
    if not os.environ.get("RAILWAY_ENVIRONMENT"):
        from pathlib import Path

        env_file = Path(__file__).resolve().parents[2] / ".env"
        if env_file.exists():
            from dotenv import load_dotenv

            load_dotenv(env_file)

    port = int(os.environ.get("PORT", 8000))
    host = os.environ.get("HOST", "127.0.0.1")
    reload = os.environ.get("RAILWAY_ENVIRONMENT") is None
    # Production: use multiple workers for concurrency; dev: single with reload
    workers = int(os.environ.get("WEB_CONCURRENCY", 1)) if not reload else 1
    uvicorn.run(
        "filings.web:app",
        host=host,
        port=port,
        reload=reload,
        workers=workers,
    )
