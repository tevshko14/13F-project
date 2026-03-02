"""13F Filing Viewer — FastAPI web application.

Production-ready with: security headers, request logging, exception
handlers, rate limiting, health check, structured logging, Sentry,
and Supabase-backed persistent cache.
"""

import os as _os

# ── EDGAR rate limit (must be set before edgartools is imported) ──────
# Default is 9 req/sec; 5 is conservative and avoids 429 errors.
_os.environ.setdefault("EDGAR_RATE_LIMIT_PER_SEC", "5")

import asyncio
from concurrent.futures import ThreadPoolExecutor
import json as json_module
import logging
import os
import re as _re
import time as time_module
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
from pathlib import Path

import uvicorn
from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.responses import (
    HTMLResponse,
    JSONResponse,
    PlainTextResponse,
    RedirectResponse,
    StreamingResponse,
)
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.gzip import GZipMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware

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
)
from filings.models import SuperinvestorSummary, StockInfo
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


# ═══════════════════════════════════════════════════════════════════════
# Rate limiting (optional — graceful fallback if slowapi not installed)
# ═══════════════════════════════════════════════════════════════════════

try:
    from slowapi import Limiter, _rate_limit_exceeded_handler
    from slowapi.errors import RateLimitExceeded

    def _real_ip(request: Request) -> str:
        """Return the real client IP, trusting Railway's X-Forwarded-For header.

        Railway (and most reverse proxies) append the true client IP as the
        first value in X-Forwarded-For.  Falling back to request.client.host
        would give the proxy's internal IP, causing all users to share one
        rate-limit bucket.
        """
        forwarded_for = request.headers.get("x-forwarded-for")
        if forwarded_for:
            # Header may be a comma-separated list; first entry is the origin
            return forwarded_for.split(",")[0].strip()
        if request.client:
            return request.client.host
        return "unknown"

    limiter = Limiter(key_func=_real_ip, default_limits=["60/minute"])
    _has_limiter = True
except ImportError:
    limiter = None
    _has_limiter = False
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
# Per-CIK locks so the background sweep and request-triggered refreshes
# can run concurrently for *different* funds without blocking each other.
_refresh_locks: dict[str, asyncio.Lock] = {}
_refresh_locks_mu = asyncio.Lock()          # guards the dict itself
_refresh_in_progress: set[str] = set()


async def _get_refresh_lock(cik: str) -> asyncio.Lock:
    """Return (creating if needed) the per-CIK refresh lock."""
    async with _refresh_locks_mu:
        if cik not in _refresh_locks:
            _refresh_locks[cik] = asyncio.Lock()
        return _refresh_locks[cik]


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
    # ── Expand the default thread pool ──────────────────────────────────
    # Python defaults to min(32, cpu+4) ≈ 8 threads.  This app has 50+
    # asyncio.to_thread call sites plus background tasks that make slow
    # HTTP calls (SEC EDGAR, yfinance, ApeWisdom).  A larger pool prevents
    # user requests from queuing behind background work.
    _pool_size = int(os.environ.get("WORKER_THREADS", "32"))
    loop = asyncio.get_running_loop()
    loop.set_default_executor(ThreadPoolExecutor(max_workers=_pool_size))

    # ── Try Supabase first (persists across Railway deploys) ──
    # Timeout after 30s so workers don't get killed by gunicorn if Supabase is slow
    try:
        app.state.fund_cache = await asyncio.wait_for(
            asyncio.to_thread(cache.load_cache_from_supabase),
            timeout=30,
        )
    except (asyncio.TimeoutError, Exception) as exc:
        logger.warning(
            "Supabase startup cache load failed (%s), falling back to disk", exc
        )
        app.state.fund_cache = {}

    if not app.state.fund_cache:
        # Fallback: load from disk (local dev, or Supabase unavailable)
        app.state.fund_cache = cache.load_cache()

    # Initialize background refresh state
    app.state.refresh_status = "disabled"
    app.state.refresh_progress = {"total": 0, "done": 0, "failed": 0}

    # Load deployment/AUM data from Supabase (non-blocking)
    try:
        app.state.deployment_cache = await asyncio.to_thread(
            aum_data.load_all_deployment_data
        )
    except Exception:
        app.state.deployment_cache = {}

    # Track background tasks for clean shutdown
    _bg_tasks: set[asyncio.Task] = set()

    def _track(coro, name: str):
        task = asyncio.create_task(coro, name=name)
        _bg_tasks.add(task)
        task.add_done_callback(_bg_tasks.discard)
        return task

    # Prefetch S&P 500 market data in background (~30-60s on cold start)
    _track(_prefetch_market_data(app), "prefetch_market")

    # Run retention cleanup in background (keep DB small)
    _track(asyncio.to_thread(supabase_cache.run_retention_cleanup), "retention_cleanup")

    # Self-heal: refresh any stale funds in background
    if _ENABLE_BACKGROUND_REFRESH:
        app.state.refresh_status = "pending"
        _track(_delayed_refresh_sweep(app), "refresh_sweep")

    # Reddit velocity notification scanner (runs every 30 min)
    _track(_reddit_velocity_scanner(), "reddit_scanner")

    yield

    # ── Shutdown cleanup ──────────────────────────────────────────────
    # Cancel all background tasks so they don't leak on worker recycle
    for task in _bg_tasks:
        task.cancel()
    if _bg_tasks:
        await asyncio.gather(*_bg_tasks, return_exceptions=True)

    global _posthog_http
    if _posthog_http and not _posthog_http.is_closed:
        await _posthog_http.aclose()
        _posthog_http = None


# ═══════════════════════════════════════════════════════════════════════
# App creation
# ═══════════════════════════════════════════════════════════════════════

app = FastAPI(title="PaperPanda", lifespan=lifespan)

# ── GZip compression — ~20-35% reduction on HTML/JSON/CSS responses ──
app.add_middleware(GZipMiddleware, minimum_size=1000)

templates = Jinja2Templates(directory=Path(__file__).parent / "templates")

# Static files (logo, favicon, etc.) — immutable cache for hashed assets
_static_dir = Path(__file__).parent / "static"
app.mount(
    "/static",
    StaticFiles(directory=_static_dir),
    name="static",
)

# Template globals
templates.env.globals["current_year"] = datetime.now().year
templates.env.globals["supabase_url"] = auth.SUPABASE_URL
templates.env.globals["supabase_anon_key"] = auth.SUPABASE_ANON_KEY
templates.env.globals["auth_enabled"] = bool(auth.SUPABASE_ANON_KEY)
templates.env.globals["posthog_key"] = _POSTHOG_KEY

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
_CONSENSUS_MAX = 2000   # ~2 MB worst-case; evict oldest when exceeded


def _consensus_cache_set(key: str, value: tuple[float, dict | None]) -> None:
    """Insert/update *key* in the consensus LRU cache, evicting if needed."""
    _consensus_cache[key] = value
    _consensus_cache.move_to_end(key)      # mark as most-recently used
    while len(_consensus_cache) > _CONSENSUS_MAX:
        _consensus_cache.popitem(last=False)  # evict the oldest (LRU) entry


# ═══════════════════════════════════════════════════════════════════════
# Security helpers
# ═══════════════════════════════════════════════════════════════════════

_TICKER_RE = _re.compile(r"^[A-Za-z][A-Za-z0-9.]{0,11}$")
_CIK_RE = _re.compile(r"^[0-9]{1,10}$")
_CUSIP_RE = _re.compile(r"^[A-Za-z0-9]{6,9}$")
_MEMBER_ID_RE = _re.compile(r"^[A-Za-z0-9_-]{1,40}$")
_ALLOWED_HOST: str = os.environ.get("ALLOWED_HOST", "")  # e.g. "paperpanda.io"


def _valid_ticker(ticker: str) -> bool:
    """Return True if *ticker* looks like a valid stock symbol."""
    return bool(_TICKER_RE.match(ticker))


def _valid_cik(cik: str) -> bool:
    """Return True if *cik* looks like a valid CIK number."""
    return bool(_CIK_RE.match(cik))


def _valid_member_id(member_id: str) -> bool:
    """Return True if *member_id* looks like a valid congress member ID."""
    return bool(_MEMBER_ID_RE.match(member_id))


def _valid_cusip(cusip: str) -> bool:
    """Return True if *cusip* looks like a valid CUSIP identifier."""
    return bool(_CUSIP_RE.match(cusip))


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
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
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
            "script-src 'self' 'unsafe-inline' "
            "https://cdn.jsdelivr.net https://unpkg.com "
            "https://us.i.posthog.com https://us-assets.i.posthog.com "
            "https://js.stripe.com https://s3.tradingview.com; "
            "style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
            "img-src 'self' data: https: blob:; "
            "font-src 'self' data:; "
            "connect-src 'self' "
            "https://us.i.posthog.com https://us-assets.i.posthog.com "
            "https://*.supabase.co "
            "https://js.stripe.com https://*.tradingview.com; "
            "frame-src https://js.stripe.com https://*.tradingview.com https://tally.so; "
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


# Trust Railway's reverse-proxy headers so X-Forwarded-For reaches our rate
# limiter and request.client reflects the real client IP.
app.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=["*"],  # Railway terminates TLS; actual host validation not needed
)
app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(RequestLoggingMiddleware)

# Auth middleware (only when JWT secret is configured)
if auth.JWT_SECRET:
    AuthMiddleware = auth._build_auth_middleware()
    app.add_middleware(AuthMiddleware)
    logger.info("Auth middleware enabled (Supabase JWT validation)")


# ═══════════════════════════════════════════════════════════════════════
# Exception handlers
# ═══════════════════════════════════════════════════════════════════════


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    # HTMX partial requests (api/ endpoints) get inline error fragments
    is_htmx = request.headers.get("HX-Request") == "true"

    if exc.status_code == 429:
        if is_htmx or request.url.path.startswith("/api/"):
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
        "error.html",
        {
            "request": request,
            "message": message,
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
        "error.html",
        {
            "request": request,
            "message": "Something went wrong on our end. Please try again later.",
        },
        status_code=500,
    )


# ═══════════════════════════════════════════════════════════════════════
# Background tasks
# ═══════════════════════════════════════════════════════════════════════


async def _prefetch_market_data(app: FastAPI):
    """Prefetch S&P 500 + index/commodity market data on startup."""
    try:
        app.state.market_data_ready = False
        await asyncio.gather(
            asyncio.to_thread(market_data.get_sp500_market_data),
            asyncio.to_thread(market_data.get_index_market_data),
        )
        app.state.market_data_ready = True
    except Exception:
        app.state.market_data_ready = False


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
        data = await asyncio.to_thread(cache.refresh_single_fund, cik)
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

_REDDIT_SCAN_INTERVAL = 30 * 60  # 30 minutes


async def _reddit_velocity_scanner() -> None:
    """Periodically check ApeWisdom for velocity spikes and create notifications."""
    await asyncio.sleep(60)  # let startup settle
    while True:
        try:
            apewisdom = await asyncio.to_thread(sentiment._get_apewisdom_all)
            if apewisdom:
                notif_rows: list[dict] = []
                for item in apewisdom:
                    ticker = (item.get("ticker") or "").upper()
                    if not ticker:
                        continue
                    mentions = int(item.get("mentions") or 0)
                    mentions_24h = int(item.get("mentions_24h_ago") or 0)
                    if mentions_24h > 0:
                        velocity_pct = ((mentions - mentions_24h) / mentions_24h) * 100
                    else:
                        velocity_pct = 0.0
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
                    n = await asyncio.to_thread(
                        supabase_cache.upsert_notifications, notif_rows
                    )
                    if n:
                        logger.info("Created %d Reddit velocity notifications", n)
        except Exception as exc:
            logger.error("Reddit velocity scan failed: %s", exc)

        await asyncio.sleep(_REDDIT_SCAN_INTERVAL)


# ═══════════════════════════════════════════════════════════════════════
# Pages
# ═══════════════════════════════════════════════════════════════════════

# --- Health check (for UptimeRobot / load balancers) ---


@app.api_route("/health", methods=["GET", "HEAD"])
async def health_check():
    """Health check that also detects thread pool starvation.

    If we can't get a response from the thread pool within 3 seconds,
    the pool is likely exhausted by hung yfinance/Finnhub calls and
    the app is effectively dead — return 503 so Railway restarts us.
    """
    try:
        await asyncio.wait_for(
            asyncio.to_thread(lambda: True), timeout=3
        )
        return JSONResponse({"status": "ok"})
    except (asyncio.TimeoutError, Exception):
        logger.error("health_check: thread pool appears starved — returning 503")
        return JSONResponse(
            {"status": "unhealthy", "reason": "thread pool exhausted"},
            status_code=503,
        )


# --- Homepage: dashboard with market data & widgets ---


@app.api_route("/", methods=["GET", "HEAD"], response_class=HTMLResponse)
async def homepage(request: Request):
    monthly_goal = _PANDA_FUND_MONTHLY_GOAL
    current_month = datetime.now().strftime("%Y-%m")

    # Primary: live total from Supabase supporters table (same as /support)
    raised_cents = await asyncio.to_thread(
        supabase_cache.get_monthly_raised_cents, current_month
    )
    if raised_cents > 0:
        raw_raised = raised_cents // 100
    else:
        raw_raised = int(os.environ.get("PANDA_FUND_RAISED", "0"))

    raised_this_month = min(raw_raised, monthly_goal)
    progress_pct = (
        min(100, round(raised_this_month / monthly_goal * 100)) if monthly_goal else 0
    )
    return templates.TemplateResponse(
        "home.html",
        {
            "request": request,
            "panda_raised": raised_this_month,
            "panda_goal": monthly_goal,
            "panda_pct": progress_pct,
            "stripe_publishable_key": _STRIPE_PUBLISHABLE_KEY,
            "stripe_configured": bool(_STRIPE_SECRET_KEY and _STRIPE_PUBLISHABLE_KEY),
            "price_onetime": _STRIPE_PRICE_ONETIME,
            "price_bamboo": _STRIPE_PRICE_BAMBOO,
            "price_panda": _STRIPE_PRICE_PANDA,
            "price_giant_panda": _STRIPE_PRICE_GIANT_PANDA,
        },
    )


# --- Superinvestors: portfolio list ---


@app.get("/superinvestors", response_class=HTMLResponse)
async def superinvestors_page(request: Request):
    """Backward-compat redirect to /funds."""
    return RedirectResponse(url="/funds?view=funds", status_code=301)


@app.get("/grand-portfolio", response_class=HTMLResponse)
async def grand_portfolio_redirect(request: Request):
    """Backward-compat redirect: /grand-portfolio → /funds."""
    view = request.query_params.get("view", "funds")
    if view not in ("funds", "holdings", "activity", "deployment"):
        view = "funds"
    return RedirectResponse(url=f"/funds?view={view}", status_code=301)


# --- Lazy-load a single fund row (HTMX) ---


@app.get("/api/fund-row/{cik}", response_class=HTMLResponse)
async def fund_row(request: Request, cik: str):
    if not _valid_cik(cik):
        return PlainTextResponse("Invalid CIK", status_code=400)
    cik_normalized = cik.lstrip("0") or cik
    si = SUPERINVESTORS_BY_CIK.get(cik_normalized) or SUPERINVESTORS_BY_CIK.get(cik)

    # ── Serve from L1 in-memory cache (fast path) ──
    cached = app.state.fund_cache.get(cik_normalized) or app.state.fund_cache.get(cik)

    # ── L1 miss → try Supabase L2 (stale OK) as fallback ──
    if not cached:
        data, _is_fresh = await asyncio.to_thread(
            supabase_cache.get_cached_with_stale, f"13f:{cik_normalized}"
        )
        if isinstance(data, dict):
            cached = data
            # Promote to L1 so subsequent requests are fast
            app.state.fund_cache[cik_normalized] = cached

    if cached:
        # Trigger background refresh if this fund is stale
        if (
            _ENABLE_BACKGROUND_REFRESH
            and cache.is_fund_stale(cached)
            and cik_normalized not in _refresh_in_progress
        ):
            asyncio.create_task(_trigger_single_refresh(app, cik_normalized))

        top_tickers = [
            h.get("ticker") or h.get("issuer", "?")[:8]
            for h in cached.get("top_holdings", [])[:5]
        ]
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
async def holdings(request: Request, cik: str, top_n: int = Query(25, ge=1, le=200)):
    if not _valid_cik(cik):
        return PlainTextResponse("Invalid CIK", status_code=400)
    si = SUPERINVESTORS_BY_CIK.get(cik)
    cache_data = _fund_cache()
    cached = cache_data.get(cik)

    # ── L1 miss → try Supabase L2 (stale OK) as fallback ──
    if not cached:
        data, _is_fresh = await asyncio.to_thread(
            supabase_cache.get_cached_with_stale, f"13f:{cik}"
        )
        if isinstance(data, dict):
            cached = data
            # Promote to L1 so subsequent requests are fast
            cache_data[cik] = cached

    if cached:
        # Trigger background refresh if this fund is stale
        if (
            _ENABLE_BACKGROUND_REFRESH
            and cache.is_fund_stale(cached)
            and cik not in _refresh_in_progress
        ):
            asyncio.create_task(_trigger_single_refresh(app, cik))

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

    return templates.TemplateResponse(
        "investor.html",
        {
            "request": request,
            "fund": fund,
            "holdings": holdings_list,
            "top_n": top_n,
            "investor_name": si.display_name if si else None,
            "quarterly_changes": quarterly_changes,
        },
    )


# --- Compare page (redirects to investor page) ---


@app.get("/compare/{cik}")
async def compare(request: Request, cik: str):
    if not _valid_cik(cik):
        return PlainTextResponse("Invalid CIK", status_code=400)
    return RedirectResponse(url=f"/holdings/{cik}", status_code=302)


# --- Compare API (lazy-loaded into investor page Compare tab) ---


@app.get("/api/compare/{cik}", response_class=HTMLResponse)
async def compare_api(request: Request, cik: str, top_n: int = Query(25, ge=1, le=200)):
    if not _valid_cik(cik):
        return PlainTextResponse("Invalid CIK", status_code=400)
    cache_data = _fund_cache()
    cached = cache_data.get(cik)

    # ── L1 miss → try Supabase L2 (stale OK) as fallback ──
    if not cached:
        data, _is_fresh = await asyncio.to_thread(
            supabase_cache.get_cached_with_stale, f"13f:{cik}"
        )
        if isinstance(data, dict):
            cached = data
            cache_data[cik] = cached

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
    cache_data = _fund_cache()
    cached = cache_data.get(cik)

    # ── L1 miss → try Supabase L2 (stale OK) as fallback ──
    if not cached:
        data, _is_fresh = await asyncio.to_thread(
            supabase_cache.get_cached_with_stale, f"13f:{cik}"
        )
        if isinstance(data, dict):
            cached = data
            cache_data[cik] = cached

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
    """Redirect to grand portfolio with activity tab active."""
    return RedirectResponse(url="/funds?view=activity", status_code=302)


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
            top_tickers = [
                h.get("ticker") or h.get("issuer", "?")[:8]
                for h in cached.get("top_holdings", [])[:5]
            ]
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

    entries = client.build_grand_portfolio(cache_data, SUPERINVESTORS_BY_CIK)

    # ── Build Consensus Leaders data (top 10 by holder count) ──
    # Compute avg portfolio weight per ticker across holders
    ticker_weights: dict[str, list[float]] = {}
    for cik, fund_data in cache_data.items():
        if cik not in SUPERINVESTORS_BY_CIK:
            continue
        for h in fund_data.get("all_holdings", []):
            t = h.get("ticker")
            if t:
                t_upper = t.upper()
                ticker_weights.setdefault(t_upper, []).append(h.get("pct", 0))

    consensus_data = []
    for e in entries[:10]:
        ticker_key = e.ticker.upper() if e.ticker else None
        weights = ticker_weights.get(ticker_key, []) if ticker_key else []
        avg_weight = round(sum(weights) / len(weights), 2) if weights else 0
        top_holders = e.holders[:3]
        consensus_data.append(
            {
                "ticker": e.ticker or e.cusip[:6],
                "issuer": e.issuer_name,
                "holders": e.num_holders,
                "avg_weight": avg_weight,
                "top_holders": top_holders,
                "combined_value": e.combined_value,
                "link": f"/stock/{e.ticker}" if e.ticker else None,
            }
        )

    # ── Build Recent Momentum data (most added this quarter) ──
    most_added = await asyncio.to_thread(
        market_data.build_most_added_table, cache_data, SUPERINVESTORS_BY_CIK
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

    # Try to build superinvestor ownership data (may be None)
    detail = None
    history = []
    if cache_data:
        detail = client.build_stock_detail(
            cusip, cache_data, SUPERINVESTORS_BY_CIK, by_cusip=True
        )
        if detail:
            history = client.build_stock_history(
                cusip, cache_data, SUPERINVESTORS_BY_CIK, by_cusip=True
            )

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

    # Try to build superinvestor ownership data (may be None)
    detail = None
    history = []
    if cache_data:
        detail = client.build_stock_detail(ticker, cache_data, SUPERINVESTORS_BY_CIK)
        if detail:
            history = client.build_stock_history(
                ticker, cache_data, SUPERINVESTORS_BY_CIK
            )

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

    return templates.TemplateResponse(
        "stock.html",
        {
            "request": request,
            "stock_info": stock_info,
            "stock": detail,
            "history": history,
        },
    )


# --- Analyst Ratings API (lazy-loaded via HTMX) ---


@app.get("/api/analysts/{ticker}", response_class=HTMLResponse)
async def analyst_ratings(request: Request, ticker: str):
    if not _valid_ticker(ticker):
        return PlainTextResponse("Invalid ticker", status_code=400)
    ratings = await asyncio.to_thread(analysts.get_analyst_ratings, ticker)
    consensus = analysts.get_consensus_summary(ratings)
    return templates.TemplateResponse(
        "partials/analyst_ratings.html",
        {
            "request": request,
            "ratings": ratings[:50],
            "consensus": consensus,
            "ticker": ticker.upper(),
        },
    )


@app.get("/api/sentiment/{ticker}", response_class=HTMLResponse)
async def sentiment_data(request: Request, ticker: str):
    if not _valid_ticker(ticker):
        return PlainTextResponse("Invalid ticker", status_code=400)
    data = await asyncio.to_thread(sentiment.get_sentiment_data, ticker)
    return templates.TemplateResponse(
        "partials/sentiment.html",
        {
            "request": request,
            "ticker": ticker.upper(),
            "cnn": data.get("cnn_fear_greed"),
            "finnhub": data.get("finnhub"),
            "apewisdom": data.get("apewisdom"),
            "alphavantage": data.get("alphavantage"),
            "has_finnhub_key": sentiment.has_finnhub_key(),
            "has_alphavantage_key": sentiment.has_alphavantage_key(),
        },
    )


@app.get("/api/vitals/{ticker}", response_class=HTMLResponse)
async def vitals_data(request: Request, ticker: str):
    if not _valid_ticker(ticker):
        return PlainTextResponse("Invalid ticker", status_code=400)
    # ── Paywall: disabled while auth is not active ──
    # if auth.JWT_SECRET:
    #     user = getattr(request.state, "user", None)
    #     profile = getattr(request.state, "profile", None)
    #     is_premium = bool(profile and profile.get("tier") == "premium")
    #     if not user or not is_premium:
    #         return templates.TemplateResponse("partials/vitals_paywall.html", {
    #             "request": request,
    #             "ticker": ticker.upper(),
    #             "user": user,
    #         })

    data = await asyncio.to_thread(vitals.get_vitals_data, ticker)
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


@app.get("/api/company-filings/{ticker}", response_class=HTMLResponse)
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


# --- Insider Trading ---


@app.get("/insider-trading", response_class=HTMLResponse)
async def insider_trading_page(request: Request):
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
    return templates.TemplateResponse(
        "insider_trading.html",
        {
            "request": request,
            "trades": trades,
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
    """Build the shared template context for /support and /support/thank-you.

    Reads the current month's raised total from Supabase (via supporters table)
    and falls back to the PANDA_FUND_RAISED env var when Supabase is unavailable.
    """
    from calendar import month_name as _month_names

    monthly_goal = _PANDA_FUND_MONTHLY_GOAL
    current_month = datetime.now().strftime("%Y-%m")
    current_month_name = _month_names[datetime.now().month]

    # Primary: live total from Supabase supporters table
    raised_cents = await asyncio.to_thread(
        supabase_cache.get_monthly_raised_cents, current_month
    )
    if raised_cents > 0:
        raw_raised = raised_cents // 100
    else:
        # Fallback: manually maintained env var (used before webhook was built,
        # or when Supabase is unavailable)
        raw_raised = int(os.environ.get("PANDA_FUND_RAISED", "0"))

    raised_this_month = min(raw_raised, monthly_goal)
    progress_pct = (
        min(100, round(raised_this_month / monthly_goal * 100)) if monthly_goal else 0
    )

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
        "monthly_goal": monthly_goal,
        "raised_this_month": raised_this_month,
        "progress_pct": progress_pct,
        "goal_reached": raw_raised >= monthly_goal,
        "current_month_name": current_month_name,
        "line_items": _PANDA_FUND_LINE_ITEMS,
        "funding_history_months": [h["month"] for h in history],
        "funding_history_raised": [min(h["raised"], monthly_goal) for h in history],
    }
    if extra:
        ctx.update(extra)
    return ctx


@app.get("/support", response_class=HTMLResponse)
async def support_page(request: Request):
    ctx = await _support_page_context(request)
    return templates.TemplateResponse("support.html", ctx)


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


# ── PostHog reverse proxy ─────────────────────────────────────────────────────
# Proxying PostHog through our own domain bypasses ad blockers that block
# requests to us.i.posthog.com / us-assets.i.posthog.com directly.
# The frontend snippet points api_host at /ingest so all PostHog traffic
# routes through paperpanda.io and is indistinguishable from first-party calls.

_PH_ASSET_HOST = "https://us-assets.i.posthog.com"
_PH_API_HOST = "https://us.i.posthog.com"

# Shared httpx client for PostHog proxy — enables connection pooling & reuse.
# Lazy-initialized on first request to avoid import-time side effects.
import httpx as _httpx

_posthog_http: _httpx.AsyncClient | None = None


def _get_posthog_client() -> _httpx.AsyncClient:
    global _posthog_http
    if _posthog_http is None or _posthog_http.is_closed:
        _posthog_http = _httpx.AsyncClient(timeout=10)
    return _posthog_http


@app.api_route("/ingest/static/{path:path}", methods=["GET", "HEAD"])
async def posthog_asset_proxy(path: str, request: Request):
    """Proxy PostHog's JS bundle through our domain to defeat ad blockers."""
    url = f"{_PH_ASSET_HOST}/static/{path}"
    params = dict(request.query_params)
    try:
        hc = _get_posthog_client()
        resp = await hc.get(url, params=params)
        headers = {
            k: v for k, v in resp.headers.items()
            if k.lower() not in ("content-encoding", "transfer-encoding", "connection")
        }
        return StreamingResponse(
            iter([resp.content]),
            status_code=resp.status_code,
            headers=headers,
        )
    except Exception as exc:
        logger.warning("posthog_asset_proxy error: %s", exc)
        return JSONResponse({"error": "proxy error"}, status_code=502)


@app.api_route("/ingest/{path:path}", methods=["GET", "POST", "OPTIONS"])
async def posthog_ingest_proxy(path: str, request: Request):
    """Proxy PostHog event ingestion through our domain to defeat ad blockers."""
    url = f"{_PH_API_HOST}/{path}"
    params = dict(request.query_params)
    body = await request.body()
    headers = {
        k: v for k, v in request.headers.items()
        if k.lower() not in ("host", "content-length", "transfer-encoding", "connection")
    }
    try:
        hc = _get_posthog_client()
        resp = await hc.request(
            method=request.method,
            url=url,
            params=params,
            content=body,
            headers=headers,
        )
        resp_headers = {
            k: v for k, v in resp.headers.items()
            if k.lower() not in ("content-encoding", "transfer-encoding", "connection")
        }
        return StreamingResponse(
            iter([resp.content]),
            status_code=resp.status_code,
            headers=resp_headers,
        )
    except Exception as exc:
        logger.warning("posthog_ingest_proxy error: %s", exc)
        return JSONResponse({"error": "proxy error"}, status_code=502)


# --- Deployment / AUM Tracking ---


def _deployment_cache() -> dict:
    """Return the current deployment cache dict from app.state."""
    return getattr(app.state, "deployment_cache", {})


@app.get("/deployment", response_class=HTMLResponse)
async def deployment_page(request: Request):
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
_bell_cache: dict[str, tuple[float, int, dict | None]] = {}
_BELL_CACHE_TTL = 15  # seconds — collapses identical polls across users


def _get_cached_bell_state(since: str) -> tuple[int, dict | None]:
    """Return (count, latest) from cache or DB.  15-second TTL."""
    now = time_module.monotonic()
    cached = _bell_cache.get(since)
    if cached and (now - cached[0]) < _BELL_CACHE_TTL:
        return cached[1], cached[2]
    count, latest = supabase_cache.get_bell_state(since)
    _bell_cache[since] = (now, count, latest)
    # Evict stale keys (keep cache dict small)
    if len(_bell_cache) > 50:
        stale = [k for k, v in _bell_cache.items() if (now - v[0]) > _BELL_CACHE_TTL]
        for k in stale:
            _bell_cache.pop(k, None)
    return count, latest


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


_NOTIF_TYPES = ["13f_change", "youtube", "reddit_velocity", "congress_trade"]


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

    return templates.TemplateResponse(
        "notifications.html",
        {
            "request": request,
            "notifications": page_notifs,
            "page": page,
            "has_next": has_next,
            "all_types": _NOTIF_TYPES,
            "active_types": active_types,
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


@app.get("/retail", response_class=HTMLResponse)
async def retail_page(request: Request, view: str = "sentiment"):
    if view not in ("sentiment", "leaderboard", "calendar"):
        view = "sentiment"

    fear_greed, apewisdom, high_impact_events = await asyncio.gather(
        asyncio.to_thread(sentiment._get_cnn_fear_greed),
        asyncio.to_thread(sentiment._get_apewisdom_all),
        asyncio.to_thread(youtube.get_high_impact_events, 9),
    )

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

    return templates.TemplateResponse(
        "retail.html",
        {
            "request": request,
            "view": view,
            "fear_greed": fear_greed,
            "top_stocks": top_stocks,
            "biggest_mover": biggest_mover,
            "youtubers": _FINANCE_YOUTUBERS,
            "has_guru_data": has_guru_data,
            "high_impact_events": high_impact_events,
        },
    )


@app.get("/api/retail/leaderboard", response_class=HTMLResponse)
async def retail_leaderboard_api(request: Request):
    all_data, fear_greed = await asyncio.gather(
        asyncio.to_thread(sentiment._get_apewisdom_all),
        asyncio.to_thread(sentiment._get_cnn_fear_greed),
    )
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
    all_data, fear_greed = await asyncio.gather(
        asyncio.to_thread(sentiment._get_apewisdom_all),
        asyncio.to_thread(sentiment._get_cnn_fear_greed),
    )
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


@app.get("/api/insider-trades", response_class=HTMLResponse)
async def insider_trades_api(request: Request, filter: str = "all"):
    trade_type = {"buys": "p", "sells": "s", "all": ""}.get(filter, "")
    trades, chart_data = await asyncio.gather(
        asyncio.to_thread(insider_trading.get_latest_insider_trades, trade_type),
        asyncio.to_thread(insider_trading.get_insider_chart_data, 10, trade_type),
    )
    return templates.TemplateResponse(
        "partials/insider_trades.html",
        {
            "request": request,
            "trades": trades,
            "chart_data_json": json_module.dumps(chart_data),
        },
    )


@app.get("/api/insider-trades/{ticker}", response_class=HTMLResponse)
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
        member = await asyncio.to_thread(
            supabase_cache.get_congress_member, member_id
        )
        if not member:
            raise HTTPException(status_code=404, detail="Politician not found")
        trades = await asyncio.to_thread(
            supabase_cache.get_congress_trades_by_member, member_id
        )
        display = congress_trading.prepare_politician_display(trades or [], member)
        # LRU eviction: remove oldest (first item) if at capacity
        if len(_politician_cache) >= _POLITICIAN_CACHE_MAX:
            _politician_cache.popitem(last=False)  # O(1) eviction
        _politician_cache[member_id] = (now, display)

    return templates.TemplateResponse(
        "politician.html",
        {"request": request, **display},
    )


@app.get("/api/stock/{ticker}/congress", response_class=HTMLResponse)
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

# Congress notification tracking — set to today on first boot to avoid
# retroactively flooding notifications from the entire cold archive.
_congress_last_notified_filing: str = ""
_CONGRESS_NOTIF_MAX = 10  # cap per refresh cycle


async def _emit_congress_notifications(recent_trades: list[dict]) -> None:
    """Check for new congress filings and create notifications.

    Compares ``filing_date`` of recent trades against the last filing
    date we already notified about.  On first call, initialises the
    watermark to today so old archive data doesn't trigger alerts.
    """
    global _congress_last_notified_filing

    if not recent_trades:
        return

    # First boot: seed watermark to today (no retroactive flood)
    if not _congress_last_notified_filing:
        _congress_last_notified_filing = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        logger.info(
            "Congress notifications: initialised watermark to %s",
            _congress_last_notified_filing,
        )
        return

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

# Per-politician profile cache (LRU-bounded, 15-min TTL)
# OrderedDict gives O(1) eviction via move_to_end + popitem
from collections import OrderedDict as _OrderedDict
_politician_cache: _OrderedDict[str, tuple[float, dict]] = _OrderedDict()
_POLITICIAN_CACHE_TTL = 900  # 15 minutes
_POLITICIAN_CACHE_MAX = 100  # max entries (LRU eviction)


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

    # Build filtered activity dashboard
    activity_data = congress_trading.prepare_congress_activity(
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
_INSIGHTS_MAX = 100     # cap entries to bound memory (~0.4 MB at 4 KB/entry)


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
        import yfinance as yf
        from filings.market_data import _yf_session
        info = yf.Ticker(ticker, session=_yf_session).info
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
    data = await asyncio.to_thread(market_data.get_ticker_search_list, cache_data)
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
                ttl_seconds=1800,
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


@app.get("/api/ticker-tape", response_class=HTMLResponse)
async def ticker_tape_api(request: Request):
    """Lazy-loaded scrolling ticker tape for the homepage."""
    if not getattr(app.state, "market_data_ready", False):
        return HTMLResponse(
            '<div hx-get="/api/ticker-tape" hx-trigger="load delay:5s" '
            'hx-swap="outerHTML">'
            '<p class="text-muted" style="text-align:center;font-size:0.85em;" '
            'aria-busy="true">Loading market data...</p></div>'
        )

    mkt, sparklines = await asyncio.gather(
        asyncio.to_thread(market_data.get_sp500_market_data, "1D"),
        asyncio.to_thread(market_data.get_sparkline_points, TICKER_TAPE_SYMBOLS, 20),
    )
    if not mkt or "_metadata" not in mkt:
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

    return templates.TemplateResponse(
        "partials/ticker_tape.html",
        {"request": request, "tickers": tickers},
    )


# ── Market Overview (custom replacement for TradingView) ──────────────

_MEGA_CAP_SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA"]

_MEGA_CAP_NAMES = {
    "AAPL": "Apple", "MSFT": "Microsoft", "GOOGL": "Alphabet",
    "AMZN": "Amazon", "NVDA": "Nvidia", "META": "Meta", "TSLA": "Tesla",
}


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

    # Fetch all data concurrently
    idx_task = asyncio.to_thread(market_data.get_index_market_data)
    sp_task = asyncio.to_thread(market_data.get_sp500_market_data, "1D")
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
        asyncio.to_thread(market_data.get_overview_chart_data, sym)
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

    tabs = {
        "indices": indices,
        "mega_caps": mega_caps,
        "commodities": commodities,
    }

    return templates.TemplateResponse(
        "partials/market_overview.html",
        {
            "request": request,
            "tabs_json": json_module.dumps(tabs),
            "has_data": bool(indices or mega_caps or commodities),
        },
    )


@app.get("/api/market-overview-chart/{symbol:path}", response_class=JSONResponse)
async def market_overview_chart_api(
    request: Request,
    symbol: str,
    period: str = Query("1M", regex="^(1D|1W|1M|3M|1Y)$"),
):
    """Return chart data for a specific symbol (stock, index, or commodity)."""
    chart = await asyncio.to_thread(market_data.get_overview_chart_data, symbol, period)
    if chart is None:
        return JSONResponse({"error": "Symbol not found"}, status_code=404)
    return JSONResponse(chart)


@app.get("/api/market-news", response_class=HTMLResponse)
async def market_news_api(request: Request):
    """Return market news HTML partial (lazy-loaded via HTMX)."""
    articles = await asyncio.to_thread(market_data.get_market_news)
    if not articles:
        return HTMLResponse(
            "<article>"
            '<p class="text-muted" style="text-align:center;">Market news temporarily unavailable.</p>'
            "</article>"
            '<div hx-get="/api/market-news" hx-trigger="load delay:5s" hx-swap="outerHTML"></div>'
        )
    return templates.TemplateResponse(
        "partials/market_news.html",
        {"request": request, "articles": articles},
    )


@app.get("/api/retail-sentiment", response_class=HTMLResponse)
async def retail_sentiment_api(request: Request):
    """Return retail sentiment HTML partial (lazy-loaded via HTMX)."""
    data = await asyncio.to_thread(sentiment.get_retail_sentiment_overview)
    if not data or (data.get("fear_greed") is None and not data.get("top_movers")):
        return HTMLResponse(
            '<p class="text-muted" style="text-align:center;padding:2em 0.5em;">'
            "Sentiment data temporarily unavailable.</p>"
            '<div hx-get="/api/retail-sentiment" hx-trigger="load delay:5s" hx-swap="outerHTML"></div>'
        )
    return templates.TemplateResponse(
        "partials/retail_sentiment.html",
        {"request": request, **data},
    )


@app.get("/api/heatmap", response_class=HTMLResponse)
async def heatmap(request: Request, period: str = "1D"):
    # Validate period
    if period not in ("1D", "1W", "1M"):
        period = "1D"

    if not getattr(app.state, "market_data_ready", False):
        return HTMLResponse(
            "<article>"
            '<p aria-busy="true">Loading S&P 500 market data (first load takes ~30s)...</p>'
            "</article>"
            '<div hx-get="/api/heatmap" hx-trigger="load delay:5s" hx-swap="outerHTML"></div>'
        )

    mkt, constituents = await asyncio.gather(
        asyncio.to_thread(market_data.get_sp500_market_data, period),
        asyncio.to_thread(market_data.get_sp500_constituents),
    )
    if not mkt or "_metadata" not in mkt:
        return HTMLResponse(
            '<article>'
            '<p class="text-muted" aria-busy="true">Market data loading...</p>'
            '</article>'
            f'<div hx-get="/api/heatmap?period={period}" hx-trigger="load delay:5s" hx-swap="outerHTML"></div>'
        )

    ownership_map = _get_ownership_map()
    super_ticker_counts = {t: len(holders) for t, holders in ownership_map.items()}

    heatmap_data = market_data.build_heatmap_data(
        mkt, constituents, super_ticker_counts
    )
    metadata = mkt.get("_metadata", {})

    return templates.TemplateResponse(
        "partials/heatmap.html",
        {
            "request": request,
            "heatmap_json": json_module.dumps(heatmap_data),
            "metadata": metadata,
            "period": period,
        },
    )


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
                ratings = await asyncio.to_thread(analysts.get_analyst_ratings, t)
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

    return templates.TemplateResponse(
        "partials/most_added.html",
        {
            "request": request,
            "entries": entries,
        },
    )


# ═══════════════════════════════════════════════════════════════════════
# Authentication pages
# ═══════════════════════════════════════════════════════════════════════


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    if not auth.SUPABASE_ANON_KEY:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse("login.html", {"request": request})


@app.get("/signup", response_class=HTMLResponse)
async def signup_page(request: Request):
    if not auth.SUPABASE_ANON_KEY:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse("signup.html", {"request": request})


@app.get("/reset-password", response_class=HTMLResponse)
async def reset_password_page(request: Request):
    if not auth.SUPABASE_ANON_KEY:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse("reset_password.html", {"request": request})


@app.get("/logout")
async def logout(request: Request):
    response = RedirectResponse(url="/", status_code=302)
    response.delete_cookie("sb-access-token", path="/")
    response.delete_cookie("sb-refresh-token", path="/")
    return response


@app.post("/api/auth/set-session")
async def set_session(request: Request):
    """Set auth cookies server-side with HttpOnly + Secure flags."""
    csrf_err = _check_csrf_origin(request)
    if csrf_err:
        return csrf_err
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid json"}, status_code=400)

    access_token = body.get("access_token", "")
    refresh_token = body.get("refresh_token", "")
    expires_in = body.get("expires_in", 3600)

    if not access_token or not isinstance(access_token, str):
        return JSONResponse({"error": "missing access_token"}, status_code=400)
    if not refresh_token or not isinstance(refresh_token, str):
        return JSONResponse({"error": "missing refresh_token"}, status_code=400)

    response = JSONResponse({"ok": True})
    response.set_cookie(
        "sb-access-token",
        access_token,
        max_age=int(expires_in),
        path="/",
        httponly=True,
        secure=True,
        samesite="lax",
    )
    response.set_cookie(
        "sb-refresh-token",
        refresh_token,
        max_age=604800,
        path="/",
        httponly=True,
        secure=True,
        samesite="lax",
    )
    return response


@app.post("/api/auth/clear-session")
async def clear_session(request: Request):
    """Clear auth cookies server-side."""
    csrf_err = _check_csrf_origin(request)
    if csrf_err:
        return csrf_err
    response = JSONResponse({"ok": True})
    response.delete_cookie("sb-access-token", path="/")
    response.delete_cookie("sb-refresh-token", path="/")
    return response


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


@app.get("/robots.txt")
async def robots_txt():
    content = (
        "User-agent: *\n"
        "Allow: /\n"
        "Disallow: /api/\n"
        "\n"
        "Sitemap: https://paperpanda.io/sitemap.xml\n"
    )
    return PlainTextResponse(content, media_type="text/plain")


# ── Sitemap cache (regenerated at most once per hour) ─────────────────
_sitemap_cache: dict[str, object] = {"xml": None, "ts": 0.0}
_SITEMAP_TTL = 3600  # 1 hour


@app.get("/sitemap.xml")
async def sitemap_xml():
    """Dynamic sitemap: static pages + all superinvestor holdings + stock pages.
    Cached for 1 hour to avoid O(n*m) iteration on every crawler request.
    """
    now = time_module.monotonic()
    if _sitemap_cache["xml"] and (now - _sitemap_cache["ts"]) < _SITEMAP_TTL:
        return PlainTextResponse(
            _sitemap_cache["xml"], media_type="application/xml"
        )

    base_url = "https://paperpanda.io"

    # ── Static pages (no redirects — only real destination URLs) ──
    urls = [
        f"  <url><loc>{base_url}/</loc><changefreq>daily</changefreq><priority>1.0</priority></url>",
        f"  <url><loc>{base_url}/funds</loc><changefreq>daily</changefreq><priority>0.9</priority></url>",
        f"  <url><loc>{base_url}/insider-trading</loc><changefreq>daily</changefreq><priority>0.8</priority></url>",
        f"  <url><loc>{base_url}/retail</loc><changefreq>daily</changefreq><priority>0.8</priority></url>",
        f"  <url><loc>{base_url}/deployment</loc><changefreq>weekly</changefreq><priority>0.7</priority></url>",
        f"  <url><loc>{base_url}/support</loc><changefreq>monthly</changefreq><priority>0.5</priority></url>",
        f"  <url><loc>{base_url}/notifications</loc><changefreq>daily</changefreq><priority>0.4</priority></url>",
    ]

    # ── Superinvestor fund pages ──
    for si in SUPERINVESTORS:
        urls.append(
            f"  <url><loc>{base_url}/holdings/{si.cik}</loc>"
            f"<changefreq>weekly</changefreq><priority>0.7</priority></url>"
        )

    # ── Stock pages (all unique tickers held by superinvestors) ──
    cache_data = _fund_cache()
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
# Rate limiting decorators (applied only if slowapi installed)
# ═══════════════════════════════════════════════════════════════════════

if _has_limiter:
    # Page routes (prevent scraping / DoS on expensive full-page renders)
    homepage = limiter.limit("30/minute")(homepage)
    funds_page = limiter.limit("20/minute")(funds_page)
    holdings = limiter.limit("20/minute")(holdings)
    stock_detail = limiter.limit("30/minute")(stock_detail)
    stock_detail_by_cusip = limiter.limit("30/minute")(stock_detail_by_cusip)
    insider_trading_page = limiter.limit("20/minute")(insider_trading_page)
    # Per-ticker API endpoints
    fund_row = limiter.limit("10/minute")(fund_row)
    analyst_ratings = limiter.limit("30/minute")(analyst_ratings)
    sentiment_data = limiter.limit("30/minute")(sentiment_data)
    vitals_data = limiter.limit("30/minute")(vitals_data)
    company_filings_tab = limiter.limit("30/minute")(company_filings_tab)
    insider_trades_api = limiter.limit("20/minute")(insider_trades_api)
    stock_insider_trades_api = limiter.limit("30/minute")(stock_insider_trades_api)
    stock_congress_api = limiter.limit("30/minute")(stock_congress_api)
    politician_page = limiter.limit("30/minute")(politician_page)
    congress_page = limiter.limit("30/minute")(congress_page)
    congress_activity_api = limiter.limit("30/minute")(congress_activity_api)
    congress_trending_api = limiter.limit("30/minute")(congress_trending_api)
    trending_combined_api = limiter.limit("15/minute")(trending_combined_api)
    insider_insights_api = limiter.limit("30/minute")(insider_insights_api)
    # Expensive aggregate / external-API endpoints
    ticker_search_index = limiter.limit("20/minute")(ticker_search_index)
    retail_leaderboard_api = limiter.limit("15/minute")(retail_leaderboard_api)
    retail_leaderboard_data = limiter.limit("15/minute")(retail_leaderboard_data)
    retail_calendar_api = limiter.limit("15/minute")(retail_calendar_api)
    retail_calendar_data = limiter.limit("15/minute")(retail_calendar_data)
    ticker_tape_api = limiter.limit("15/minute")(ticker_tape_api)
    market_overview_api = limiter.limit("15/minute")(market_overview_api)
    market_overview_chart_api = limiter.limit("30/minute")(market_overview_chart_api)
    market_news_api = limiter.limit("15/minute")(market_news_api)
    retail_sentiment_api = limiter.limit("15/minute")(retail_sentiment_api)
    heatmap = limiter.limit("10/minute")(heatmap)
    most_added = limiter.limit("10/minute")(most_added)
    api_activity_feed = limiter.limit("15/minute")(api_activity_feed)
    portfolio_chart_data = limiter.limit("15/minute")(portfolio_chart_data)
    compare_api = limiter.limit("15/minute")(compare_api)
    # Auth pages (bot/scraper prevention)
    login_page = limiter.limit("10/minute")(login_page)
    signup_page = limiter.limit("10/minute")(signup_page)
    reset_password_page = limiter.limit("5/minute")(reset_password_page)
    # Auth session cookie endpoints
    set_session = limiter.limit("10/minute")(set_session)
    clear_session = limiter.limit("10/minute")(clear_session)
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
