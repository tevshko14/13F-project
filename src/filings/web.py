"""13F Filing Viewer — FastAPI web application.

Production-ready with: security headers, request logging, exception
handlers, rate limiting, health check, structured logging, and Sentry.
"""

import asyncio
import json as json_module
import logging
import os
import time as time_module
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

import uvicorn
from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.base import BaseHTTPMiddleware

from filings import client, cache, analysts, market_data, sentiment, vitals, company_filings, insider_trading, supabase_cache, auth
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


# ═══════════════════════════════════════════════════════════════════════
# Rate limiting (optional — graceful fallback if slowapi not installed)
# ═══════════════════════════════════════════════════════════════════════

try:
    from slowapi import Limiter, _rate_limit_exceeded_handler
    from slowapi.util import get_remote_address
    from slowapi.errors import RateLimitExceeded

    limiter = Limiter(key_func=get_remote_address, default_limits=["60/minute"])
    _has_limiter = True
except ImportError:
    limiter = None
    _has_limiter = False
    logger.info("slowapi not installed — rate limiting disabled")


# ═══════════════════════════════════════════════════════════════════════
# App startup time (for /health uptime)
# ═══════════════════════════════════════════════════════════════════════

_app_start_time = time_module.time()


# ═══════════════════════════════════════════════════════════════════════
# Lifespan
# ═══════════════════════════════════════════════════════════════════════

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Load cache on startup, trigger background refresh if stale.

    Hydration priority:
      1. Supabase L2 (survives Railway deploys)
      2. Disk cache (local dev / Supabase down)
    """
    # ── Try Supabase first (persists across Railway deploys) ──
    app.state.fund_cache = await asyncio.to_thread(cache.load_cache_from_supabase)

    if not app.state.fund_cache:
        # Fallback: load from disk (local dev, or Supabase unavailable)
        app.state.fund_cache = cache.load_cache()

    app.state.refreshing = False

    if cache.is_cache_stale() and app.state.fund_cache:
        asyncio.create_task(_background_refresh(app))

    # Start periodic polling for new filings
    poll_task = asyncio.create_task(_poll_loop(app))

    # Prefetch S&P 500 market data in background (~30-60s on cold start)
    asyncio.create_task(_prefetch_market_data(app))

    yield

    poll_task.cancel()


# ═══════════════════════════════════════════════════════════════════════
# App creation
# ═══════════════════════════════════════════════════════════════════════

app = FastAPI(title="PaperPanda", lifespan=lifespan)

templates = Jinja2Templates(
    directory=Path(__file__).parent / "templates"
)

# Static files (logo, favicon, etc.)
app.mount("/static", StaticFiles(directory=Path(__file__).parent / "static"), name="static")

# Template globals
templates.env.globals["current_year"] = datetime.now().year
templates.env.globals["supabase_url"] = auth.SUPABASE_URL
templates.env.globals["supabase_anon_key"] = auth.SUPABASE_ANON_KEY
templates.env.globals["auth_enabled"] = bool(auth.SUPABASE_ANON_KEY)

# Attach rate limiter
if _has_limiter:
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


# ═══════════════════════════════════════════════════════════════════════
# Middleware
# ═══════════════════════════════════════════════════════════════════════

class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
        # HSTS only on HTTPS (Railway terminates TLS)
        if request.url.scheme == "https" or request.headers.get("x-forwarded-proto") == "https":
            response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
        return response


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start = time_module.time()
        response = await call_next(request)
        duration_ms = round((time_module.time() - start) * 1000)
        logger.info("%s %s %s %dms", request.method, request.url.path, response.status_code, duration_ms)
        return response


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
    if exc.status_code == 404:
        message = "The page you're looking for doesn't exist."
    elif exc.status_code == 429:
        message = "Too many requests. Please slow down and try again in a minute."
    else:
        message = exc.detail or "An unexpected error occurred."
    return templates.TemplateResponse("error.html", {
        "request": request,
        "message": message,
    }, status_code=exc.status_code)


@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled exception on %s %s", request.method, request.url.path)
    return templates.TemplateResponse("error.html", {
        "request": request,
        "message": "Something went wrong on our end. Please try again later.",
    }, status_code=500)


# ═══════════════════════════════════════════════════════════════════════
# Background tasks
# ═══════════════════════════════════════════════════════════════════════

async def _background_refresh(app: FastAPI):
    """Refresh stale superinvestor funds in background.

    Uses per-fund TTL: only re-fetches funds whose `_last_refreshed`
    timestamp is older than the effective TTL. Skips recently-refreshed
    funds to avoid unnecessary SEC EDGAR API calls.

    On failure, keeps existing stale data (stale-while-revalidate).
    """
    app.state.refreshing = True
    all_ciks = [si.cik for si in SUPERINVESTORS]
    stale_ciks = cache.get_stale_ciks(app.state.fund_cache, all_ciks)

    if not stale_ciks:
        logger.info("No stale funds to refresh (%d funds all fresh)", len(all_ciks))
        app.state.refreshing = False
        return

    logger.info("Refreshing %d/%d stale funds", len(stale_ciks), len(all_ciks))
    dirty_count = 0
    for cik in stale_ciks:
        try:
            data = await asyncio.to_thread(cache.refresh_single_fund, cik)
            if data:
                app.state.fund_cache[cik] = data
                dirty_count += 1
                if dirty_count % 10 == 0:
                    await asyncio.to_thread(cache.save_cache, app.state.fund_cache)
        except Exception:
            pass
        await asyncio.sleep(1)

    if dirty_count > 0 and dirty_count % 10 != 0:
        await asyncio.to_thread(cache.save_cache, app.state.fund_cache)

    logger.info("Background refresh complete: %d funds updated", dirty_count)
    app.state.refreshing = False


async def _poll_loop(app: FastAPI):
    """Periodically trigger background refresh to detect new filings.

    Uses smart polling intervals from notifications module:
      - Filing season (±15 days of deadline): every 2 hours
      - Off-season: every 12 hours

    The background refresh itself only re-fetches stale funds (per-fund TTL),
    so even frequent polling doesn't cause excessive API calls when data is fresh.
    """
    while True:
        try:
            from filings.notifications import get_poll_interval_seconds
            interval = get_poll_interval_seconds()
            await asyncio.sleep(interval)
            if not getattr(app.state, "refreshing", False):
                asyncio.create_task(_background_refresh(app))
        except asyncio.CancelledError:
            break
        except Exception:
            await asyncio.sleep(3600)  # fallback: retry in 1h


async def _prefetch_market_data(app: FastAPI):
    """Prefetch S&P 500 market data on startup (runs in background thread)."""
    try:
        app.state.market_data_ready = False
        await asyncio.to_thread(market_data.get_sp500_market_data)
        app.state.market_data_ready = True
    except Exception:
        app.state.market_data_ready = False


# ═══════════════════════════════════════════════════════════════════════
# Pages
# ═══════════════════════════════════════════════════════════════════════

# --- Homepage: dashboard with market data & widgets ---

@app.get("/", response_class=HTMLResponse)
async def homepage(request: Request):
    return templates.TemplateResponse("home.html", {
        "request": request,
    })


# --- Superinvestors: portfolio list ---

@app.get("/superinvestors", response_class=HTMLResponse)
async def superinvestors_page(request: Request):
    """Redirect to grand portfolio with funds tab active."""
    return RedirectResponse(url="/grand-portfolio?view=funds", status_code=302)


# --- Lazy-load a single fund row (HTMX) ---

@app.get("/api/fund-row/{cik}", response_class=HTMLResponse)
async def fund_row(request: Request, cik: str):
    cik_normalized = cik.lstrip("0") or cik
    si = SUPERINVESTORS_BY_CIK.get(cik_normalized) or SUPERINVESTORS_BY_CIK.get(cik)

    # ── Cache hit: serve immediately (zero SEC calls) ──
    cached = app.state.fund_cache.get(cik_normalized) or app.state.fund_cache.get(cik)
    if cached:
        top_tickers = [
            h.get("ticker") or h.get("issuer", "?")[:8]
            for h in cached.get("top_holdings", [])[:5]
        ]
        return templates.TemplateResponse("partials/fund_row.html", {
            "request": request,
            "si": si,
            "data": cached,
            "top_tickers": top_tickers,
        })

    # ── Cache miss: fetch from SEC, persist to all tiers ──
    try:
        data = await asyncio.to_thread(client.get_fund_summary, cik)
        stamped = cache.stamp_fund_data(data)
        app.state.fund_cache[cik_normalized] = stamped

        # Persist to Supabase L2 (non-fatal)
        supabase_cache.set_cached(
            cache_key=f"13f:{cik_normalized}",
            category="13f",
            data=stamped,
            ttl_seconds=cache._get_effective_ttl_seconds(),
        )

        asyncio.create_task(asyncio.to_thread(cache.save_cache, app.state.fund_cache))
        top_tickers = [
            h.get("ticker") or h.get("issuer", "?")[:8]
            for h in stamped.get("top_holdings", [])[:5]
        ]
        return templates.TemplateResponse("partials/fund_row.html", {
            "request": request,
            "si": si,
            "data": stamped,
            "top_tickers": top_tickers,
        })
    except Exception as e:
        return templates.TemplateResponse("partials/fund_row_error.html", {
            "request": request,
            "si": si,
            "error": str(e),
        })


# --- Search page ---

@app.get("/search", response_class=HTMLResponse)
async def search_page(request: Request, q: str = Query("")):
    query = q.strip()
    if not query:
        return templates.TemplateResponse("search.html", {"request": request})

    results = await asyncio.to_thread(client.search_managers, query)

    return templates.TemplateResponse("search.html", {
        "request": request,
        "query": query,
        "results": results,
    })


# --- Enhanced Holdings page ---

@app.get("/holdings/{cik}", response_class=HTMLResponse)
async def holdings(request: Request, cik: str, top_n: int = Query(25)):
    si = SUPERINVESTORS_BY_CIK.get(cik)
    cache_data = getattr(app.state, "fund_cache", {})
    cached = cache_data.get(cik)

    if cached:
        # ── Cache hit: build from stored data (zero SEC calls) ──
        fund, holdings_list = client.get_enriched_holdings_from_cache(cached, cik, top_n)
    else:
        # ── Cache miss: fall back to live SEC API ──
        try:
            fund, holdings_list = await asyncio.to_thread(
                client.get_enriched_holdings, cik, top_n
            )
        except (ValueError, Exception) as e:
            return templates.TemplateResponse("error.html", {
                "request": request,
                "message": str(e),
            }, status_code=404)

    # Build quarterly changes with ticker enrichment
    quarterly_changes = []
    if cached:
        raw_quarters = cached.get("quarterly_changes", [])
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
            quarterly_changes.append({
                "period": q.get("period", ""),
                "report_period": q.get("report_period", ""),
                "filing_date": q.get("filing_date", ""),
                "changes": enriched_changes,
            })

    return templates.TemplateResponse("investor.html", {
        "request": request,
        "fund": fund,
        "holdings": holdings_list,
        "top_n": top_n,
        "investor_name": si.display_name if si else None,
        "quarterly_changes": quarterly_changes,
    })


# --- Compare page (redirects to investor page) ---

@app.get("/compare/{cik}")
async def compare(request: Request, cik: str):
    return RedirectResponse(url=f"/holdings/{cik}", status_code=302)


# --- Compare API (lazy-loaded into investor page Compare tab) ---

@app.get("/api/compare/{cik}", response_class=HTMLResponse)
async def compare_api(request: Request, cik: str, top_n: int = Query(25)):
    cache_data = getattr(app.state, "fund_cache", {})
    cached = cache_data.get(cik)

    if cached:
        # ── Cache hit: reconstruct comparison from stored data ──
        current, previous, changes = client.get_compare_from_cache(cached, cik, top_n)
        if previous is None:
            return templates.TemplateResponse("partials/compare_content.html", {
                "request": request,
                "error": "Only one quarter available — nothing to compare yet.",
            })
    else:
        # ── Cache miss: fall back to live SEC API ──
        try:
            current, previous, changes = await asyncio.to_thread(
                client.compare_quarters, cik, top_n
            )
        except (ValueError, Exception) as e:
            return templates.TemplateResponse("partials/compare_content.html", {
                "request": request,
                "error": str(e),
            })

    return templates.TemplateResponse("partials/compare_content.html", {
        "request": request,
        "current": current,
        "previous": previous,
        "changes": changes,
    })


# --- Portfolio Pie Chart Data (lazy-loaded into investor page) ---

@app.get("/api/portfolio-chart/{cik}")
async def portfolio_chart_data(request: Request, cik: str):
    """Return top-10 holdings with cross-investor ownership counts.

    Used by the ECharts donut on the investor profile page.
    """
    cache_data = getattr(app.state, "fund_cache", {})
    cached = cache_data.get(cik)
    if not cached:
        return JSONResponse(content=[])

    # Build ownership map once (single pass through all investors)
    ownership_map = client.build_ticker_ownership_map(
        cache_data, SUPERINVESTORS_BY_CIK
    )

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

        result.append({
            "ticker": ticker or cusip[:6],
            "issuer": h.get("issuer", ""),
            "pct": round(pct, 2),
            "value": h.get("value", 0),
            "activity": activity,
            "quarter": period,
            "also_held_by": len(other_owners),
            "owner_names": other_owners[:5],
        })

    return JSONResponse(content=result)


# --- Activity Feed ---

@app.get("/activity", response_class=HTMLResponse)
async def activity_feed(request: Request):
    """Redirect to grand portfolio with activity tab active."""
    return RedirectResponse(url="/grand-portfolio?view=activity", status_code=302)


# --- Grand Portfolio ---

@app.get("/grand-portfolio", response_class=HTMLResponse)
async def grand_portfolio(request: Request, view: str = "funds"):
    if view not in ("funds", "holdings", "activity"):
        view = "funds"

    cache_data = getattr(app.state, "fund_cache", {})

    # ── Build superinvestor summaries (for the Superinvestors tab) ──
    si_summaries = []
    for si in SUPERINVESTORS:
        cached = cache_data.get(si.cik)
        if cached:
            top_tickers = [
                h.get("ticker") or h.get("issuer", "?")[:8]
                for h in cached.get("top_holdings", [])[:5]
            ]
            si_summaries.append(SuperinvestorSummary(
                cik=si.cik,
                display_name=si.display_name,
                fund_name=cached.get("name", si.fund_name),
                portfolio_value=cached.get("total_value", 0),
                num_holdings=cached.get("total_holdings", 0),
                top_holdings=top_tickers,
                report_period=cached.get("report_period", ""),
                filing_date=cached.get("filing_date", ""),
            ))
        else:
            si_summaries.append(None)

    if not cache_data:
        return templates.TemplateResponse("grand_portfolio.html", {
            "request": request,
            "entries": [],
            "empty": True,
            "consensus_json": "[]",
            "momentum_json": "[]",
            "activities": [],
            "view": view,
            "superinvestors": SUPERINVESTORS,
            "summaries": si_summaries,
            "cache_age": cache.get_cache_age_str(),
            "refreshing": getattr(app.state, "refreshing", False),
        })

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
        consensus_data.append({
            "ticker": e.ticker or e.cusip[:6],
            "issuer": e.issuer_name,
            "holders": e.num_holders,
            "avg_weight": avg_weight,
            "top_holders": top_holders,
            "combined_value": e.combined_value,
            "link": f"/stock/{e.ticker}" if e.ticker else None,
        })

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
        momentum_data.append({
            "ticker": ticker,
            "issuer": ma.get("issuer_name", ""),
            "add_count": ma.get("add_count", 0),
            "adders": ma.get("adders", []),
            "total_value": ma.get("total_value", 0),
            "is_trending": is_trending,
            "link": f"/stock/{ma['ticker']}" if ma.get("ticker") else None,
        })

    # ── Build Activity Feed data (for the Activity tab) ──
    activities = client.build_activity_feed(cache_data, SUPERINVESTORS_BY_CIK)

    return templates.TemplateResponse("grand_portfolio.html", {
        "request": request,
        "entries": entries[:100],
        "consensus_json": json_module.dumps(consensus_data),
        "momentum_json": json_module.dumps(momentum_data),
        "activities": activities[:100],
        "view": view,
        "superinvestors": SUPERINVESTORS,
        "summaries": si_summaries,
        "cache_age": cache.get_cache_age_str(),
        "refreshing": getattr(app.state, "refreshing", False),
    })


# --- Stock Detail ---

@app.get("/stock/cusip/{cusip}", response_class=HTMLResponse)
async def stock_detail_by_cusip(request: Request, cusip: str):
    cache_data = getattr(app.state, "fund_cache", {})

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

    return templates.TemplateResponse("stock.html", {
        "request": request,
        "stock_info": stock_info,
        "stock": detail,
        "history": history,
    })


@app.get("/stock/{ticker}", response_class=HTMLResponse)
async def stock_detail(request: Request, ticker: str):
    cache_data = getattr(app.state, "fund_cache", {})

    # Try to build superinvestor ownership data (may be None)
    detail = None
    history = []
    if cache_data:
        detail = client.build_stock_detail(
            ticker, cache_data, SUPERINVESTORS_BY_CIK
        )
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

    return templates.TemplateResponse("stock.html", {
        "request": request,
        "stock_info": stock_info,
        "stock": detail,
        "history": history,
    })


# --- Analyst Ratings API (lazy-loaded via HTMX) ---

@app.get("/api/analysts/{ticker}", response_class=HTMLResponse)
async def analyst_ratings(request: Request, ticker: str):
    ratings = await asyncio.to_thread(analysts.get_analyst_ratings, ticker)
    consensus = analysts.get_consensus_summary(ratings)
    return templates.TemplateResponse("partials/analyst_ratings.html", {
        "request": request,
        "ratings": ratings[:50],
        "consensus": consensus,
        "ticker": ticker.upper(),
    })


@app.get("/api/sentiment/{ticker}", response_class=HTMLResponse)
async def sentiment_data(request: Request, ticker: str):
    data = await asyncio.to_thread(sentiment.get_sentiment_data, ticker)
    return templates.TemplateResponse("partials/sentiment.html", {
        "request": request,
        "ticker": ticker.upper(),
        "cnn": data.get("cnn_fear_greed"),
        "finnhub": data.get("finnhub"),
        "apewisdom": data.get("apewisdom"),
        "alphavantage": data.get("alphavantage"),
        "has_finnhub_key": sentiment.has_finnhub_key(),
        "has_alphavantage_key": sentiment.has_alphavantage_key(),
    })


@app.get("/api/vitals/{ticker}", response_class=HTMLResponse)
async def vitals_data(request: Request, ticker: str):
    # ── Paywall: Vitals is premium-only when auth is enabled ──
    if auth.JWT_SECRET:
        user = getattr(request.state, "user", None)
        profile = getattr(request.state, "profile", None)
        is_premium = bool(profile and profile.get("tier") == "premium")
        if not user or not is_premium:
            return templates.TemplateResponse("partials/vitals_paywall.html", {
                "request": request,
                "ticker": ticker.upper(),
                "user": user,
            })

    data = await asyncio.to_thread(vitals.get_vitals_data, ticker)
    return templates.TemplateResponse("partials/vitals.html", {
        "request": request,
        "ticker": ticker.upper(),
        "glassdoor": data.get("glassdoor"),
        "pdl": data.get("pdl"),
        "appstore": data.get("appstore"),
        "has_glassdoor_key": vitals.has_glassdoor_key(),
        "has_pdl_key": vitals.has_pdl_key(),
        "glassdoor_age": vitals.get_glassdoor_age_str(ticker),
        "glassdoor_quota_exhausted": vitals.get_glassdoor_quota_info()["exhausted"],
    })


@app.get("/api/company-filings/{ticker}", response_class=HTMLResponse)
async def company_filings_tab(request: Request, ticker: str):
    filings = await asyncio.to_thread(company_filings.get_company_filings, ticker)
    return templates.TemplateResponse("partials/company_filings.html", {
        "request": request,
        "filings": filings,
        "ticker": ticker.upper(),
    })


# --- Insider Trading ---

@app.get("/insider-trading", response_class=HTMLResponse)
async def insider_trading_page(request: Request):
    return templates.TemplateResponse("insider_trading.html", {
        "request": request,
    })


@app.get("/api/insider-trades", response_class=HTMLResponse)
async def insider_trades_api(request: Request, filter: str = "all"):
    trade_type = {"buys": "p", "sells": "s", "all": ""}.get(filter, "")
    trades = await asyncio.to_thread(
        insider_trading.get_latest_insider_trades, trade_type
    )
    return templates.TemplateResponse("partials/insider_trades.html", {
        "request": request,
        "trades": trades,
    })


@app.get("/api/insider-trades/{ticker}", response_class=HTMLResponse)
async def stock_insider_trades_api(request: Request, ticker: str):
    trades = await asyncio.to_thread(
        insider_trading.get_ticker_insider_trades, ticker
    )
    return templates.TemplateResponse("partials/stock_insider_trades.html", {
        "request": request,
        "trades": trades,
        "ticker": ticker.upper(),
    })


# --- Market Data API (heatmap, most-added, ticker search) ---

@app.get("/api/ticker-search-index")
async def ticker_search_index(request: Request):
    cache_data = getattr(app.state, "fund_cache", {})
    data = await asyncio.to_thread(market_data.get_ticker_search_list, cache_data)
    # Strip fields the client doesn't need to reduce payload (~8000 items)
    slim = []
    for item in data:
        entry: dict = {"ticker": item["ticker"], "name": item["name"], "type": item["type"]}
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
        slim.append(entry)
    return JSONResponse(content=slim)


@app.get("/api/heatmap", response_class=HTMLResponse)
async def heatmap(request: Request, period: str = "1D"):
    # Validate period
    if period not in ("1D", "1W", "1M"):
        period = "1D"

    if not getattr(app.state, "market_data_ready", False):
        return HTMLResponse(
            '<article>'
            '<p aria-busy="true">Loading S&P 500 market data (first load takes ~30s)...</p>'
            '</article>'
            '<div hx-get="/api/heatmap" hx-trigger="load delay:5s" hx-swap="outerHTML"></div>'
        )

    mkt = await asyncio.to_thread(market_data.get_sp500_market_data, period)
    if not mkt or "_metadata" not in mkt:
        return HTMLResponse(
            '<article><p class="text-muted">Market data unavailable.</p></article>'
        )

    constituents = await asyncio.to_thread(market_data.get_sp500_constituents)

    cache_data = getattr(app.state, "fund_cache", {})
    super_ticker_counts: dict[str, int] = {}
    for cik, fund_data in cache_data.items():
        if cik in SUPERINVESTORS_BY_CIK:
            seen_tickers: set[str] = set()
            for h in fund_data.get("all_holdings", []):
                t = h.get("ticker")
                if t:
                    t_upper = t.upper()
                    if t_upper not in seen_tickers:
                        seen_tickers.add(t_upper)
                        super_ticker_counts[t_upper] = super_ticker_counts.get(t_upper, 0) + 1

    heatmap_data = market_data.build_heatmap_data(mkt, constituents, super_ticker_counts)
    metadata = mkt.get("_metadata", {})

    return templates.TemplateResponse("partials/heatmap.html", {
        "request": request,
        "heatmap_json": json_module.dumps(heatmap_data),
        "metadata": metadata,
        "period": period,
    })


@app.get("/api/most-added", response_class=HTMLResponse)
async def most_added(request: Request):
    cache_data = getattr(app.state, "fund_cache", {})
    if not cache_data:
        return HTMLResponse(
            '<article><p class="text-muted">No data available yet.</p></article>'
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

    for entry in entries:
        ticker = entry.get("ticker")
        if ticker:
            try:
                ratings = await asyncio.to_thread(analysts.get_analyst_ratings, ticker)
                consensus = analysts.get_consensus_summary(ratings)
                entry["consensus"] = consensus
            except Exception:
                entry["consensus"] = None

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

    return templates.TemplateResponse("partials/most_added.html", {
        "request": request,
        "entries": entries,
    })


# ═══════════════════════════════════════════════════════════════════════
# Authentication pages
# ═══════════════════════════════════════════════════════════════════════

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})


@app.get("/signup", response_class=HTMLResponse)
async def signup_page(request: Request):
    return templates.TemplateResponse("signup.html", {"request": request})


@app.get("/reset-password", response_class=HTMLResponse)
async def reset_password_page(request: Request):
    return templates.TemplateResponse("reset_password.html", {"request": request})


@app.get("/logout")
async def logout(request: Request):
    response = RedirectResponse(url="/", status_code=302)
    response.delete_cookie("sb-access-token", path="/")
    response.delete_cookie("sb-refresh-token", path="/")
    return response


# --- Manual Refresh ---

@app.post("/refresh", response_class=HTMLResponse)
async def trigger_refresh(request: Request):
    if not getattr(app.state, "refreshing", False):
        asyncio.create_task(_background_refresh(app))
    return HTMLResponse(
        '<p aria-busy="true">Refreshing data in the background... '
        "This page will update automatically.</p>"
    )


# ═══════════════════════════════════════════════════════════════════════
# Infrastructure endpoints
# ═══════════════════════════════════════════════════════════════════════

@app.get("/health")
async def health_check(request: Request):
    uptime = round(time_module.time() - _app_start_time)
    cache_data = getattr(app.state, "fund_cache", {})
    all_ciks = [si.cik for si in SUPERINVESTORS]
    stale_ciks = cache.get_stale_ciks(cache_data, all_ciks)
    return JSONResponse({
        "status": "ok",
        "uptime_seconds": uptime,
        "cache_entries": len(cache_data),
        "cache_age": cache.get_cache_age_str(),
        "cache_ttl": str(cache._get_effective_ttl()),
        "stale_funds": len(stale_ciks),
        "total_funds": len(all_ciks),
        "refreshing": getattr(app.state, "refreshing", False),
        "market_data_ready": getattr(app.state, "market_data_ready", False),
        "supabase_connected": supabase_cache.is_available(),
    })


@app.get("/robots.txt")
async def robots_txt():
    content = (
        "User-agent: *\n"
        "Allow: /\n"
        "Disallow: /api/\n"
        "Disallow: /refresh\n"
        "\n"
        "Sitemap: https://paperpanda.io/sitemap.xml\n"
    )
    return PlainTextResponse(content, media_type="text/plain")


@app.get("/sitemap.xml")
async def sitemap_xml():
    base_url = "https://paperpanda.io"
    urls = [
        f"  <url><loc>{base_url}/</loc><changefreq>daily</changefreq><priority>1.0</priority></url>",
        f"  <url><loc>{base_url}/superinvestors</loc><changefreq>daily</changefreq><priority>0.9</priority></url>",
        f"  <url><loc>{base_url}/activity</loc><changefreq>daily</changefreq><priority>0.8</priority></url>",
        f"  <url><loc>{base_url}/grand-portfolio</loc><changefreq>daily</changefreq><priority>0.8</priority></url>",
        f"  <url><loc>{base_url}/insider-trading</loc><changefreq>daily</changefreq><priority>0.8</priority></url>",
    ]
    for si in SUPERINVESTORS:
        urls.append(
            f"  <url><loc>{base_url}/holdings/{si.cik}</loc>"
            f"<changefreq>weekly</changefreq><priority>0.7</priority></url>"
        )

    xml = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        + "\n".join(urls) + "\n"
        "</urlset>\n"
    )
    return PlainTextResponse(xml, media_type="application/xml")


# ═══════════════════════════════════════════════════════════════════════
# Rate limiting decorators (applied only if slowapi installed)
# ═══════════════════════════════════════════════════════════════════════

if _has_limiter:
    fund_row = limiter.limit("10/minute")(fund_row)
    analyst_ratings = limiter.limit("30/minute")(analyst_ratings)
    sentiment_data = limiter.limit("30/minute")(sentiment_data)
    vitals_data = limiter.limit("30/minute")(vitals_data)
    company_filings_tab = limiter.limit("30/minute")(company_filings_tab)
    insider_trades_api = limiter.limit("20/minute")(insider_trades_api)
    stock_insider_trades_api = limiter.limit("30/minute")(stock_insider_trades_api)
    trigger_refresh = limiter.limit("5/minute")(trigger_refresh)


# ═══════════════════════════════════════════════════════════════════════
# Entry point
# ═══════════════════════════════════════════════════════════════════════

def main():
    """Entry point for `uv run filings-web`."""
    port = int(os.environ.get("PORT", 8000))
    host = os.environ.get("HOST", "127.0.0.1")
    reload = os.environ.get("RAILWAY_ENVIRONMENT") is None
    uvicorn.run(
        "filings.web:app",
        host=host,
        port=port,
        reload=reload,
    )
