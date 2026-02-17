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
from fastapi.templating import Jinja2Templates
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.base import BaseHTTPMiddleware

from filings import client, cache, analysts, market_data, sentiment
from filings.models import SuperinvestorSummary
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
    """Load cache on startup, trigger background refresh if stale."""
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

app = FastAPI(title="13F Filing Viewer", lifespan=lifespan)

templates = Jinja2Templates(
    directory=Path(__file__).parent / "templates"
)

# Template globals
templates.env.globals["current_year"] = datetime.now().year

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
    """Refresh cache for all superinvestors in background."""
    app.state.refreshing = True
    dirty_count = 0
    for si in SUPERINVESTORS:
        try:
            data = await asyncio.to_thread(cache.refresh_single_fund, si.cik)
            if data:
                app.state.fund_cache[si.cik] = data
                dirty_count += 1
                if dirty_count % 10 == 0:
                    await asyncio.to_thread(cache.save_cache, app.state.fund_cache)
        except Exception:
            pass
        await asyncio.sleep(1)

    if dirty_count % 10 != 0:
        await asyncio.to_thread(cache.save_cache, app.state.fund_cache)
    app.state.refreshing = False


async def _poll_loop(app: FastAPI):
    """Periodically trigger background refresh to detect new filings."""
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

# --- Homepage: Superinvestor list ---

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    cache_data = getattr(app.state, "fund_cache", {})

    summaries = []
    for si in SUPERINVESTORS:
        cached = cache_data.get(si.cik)
        if cached:
            top_tickers = [
                h.get("ticker") or h.get("issuer", "?")[:8]
                for h in cached.get("top_holdings", [])[:5]
            ]
            summaries.append(SuperinvestorSummary(
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
            summaries.append(None)

    return templates.TemplateResponse("index.html", {
        "request": request,
        "superinvestors": SUPERINVESTORS,
        "summaries": summaries,
        "cache_age": cache.get_cache_age_str(),
        "refreshing": getattr(app.state, "refreshing", False),
    })


# --- Lazy-load a single fund row (HTMX) ---

@app.get("/api/fund-row/{cik}", response_class=HTMLResponse)
async def fund_row(request: Request, cik: str):
    cik_normalized = cik.lstrip("0") or cik
    si = SUPERINVESTORS_BY_CIK.get(cik_normalized) or SUPERINVESTORS_BY_CIK.get(cik)
    try:
        data = await asyncio.to_thread(client.get_fund_summary, cik)
        app.state.fund_cache[cik_normalized] = data
        asyncio.create_task(asyncio.to_thread(cache.save_cache, app.state.fund_cache))
        top_tickers = [
            h.get("ticker") or h.get("issuer", "?")[:8]
            for h in data.get("top_holdings", [])[:5]
        ]
        return templates.TemplateResponse("partials/fund_row.html", {
            "request": request,
            "si": si,
            "data": data,
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
    try:
        fund, holdings_list = await asyncio.to_thread(
            client.get_enriched_holdings, cik, top_n
        )
    except (ValueError, Exception) as e:
        return templates.TemplateResponse("error.html", {
            "request": request,
            "message": str(e),
        }, status_code=404)

    return templates.TemplateResponse("investor.html", {
        "request": request,
        "fund": fund,
        "holdings": holdings_list,
        "top_n": top_n,
        "investor_name": si.display_name if si else None,
    })


# --- Compare page (redirects to investor page) ---

@app.get("/compare/{cik}")
async def compare(request: Request, cik: str):
    return RedirectResponse(url=f"/holdings/{cik}", status_code=302)


# --- Compare API (lazy-loaded into investor page Compare tab) ---

@app.get("/api/compare/{cik}", response_class=HTMLResponse)
async def compare_api(request: Request, cik: str, top_n: int = Query(25)):
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


# --- Activity Feed ---

@app.get("/activity", response_class=HTMLResponse)
async def activity_feed(request: Request):
    cache_data = getattr(app.state, "fund_cache", {})
    if not cache_data:
        return templates.TemplateResponse("activity.html", {
            "request": request,
            "activities": [],
            "empty": True,
        })

    activities = client.build_activity_feed(cache_data, SUPERINVESTORS_BY_CIK)
    return templates.TemplateResponse("activity.html", {
        "request": request,
        "activities": activities[:100],
    })


# --- Grand Portfolio ---

@app.get("/grand-portfolio", response_class=HTMLResponse)
async def grand_portfolio(request: Request):
    cache_data = getattr(app.state, "fund_cache", {})
    if not cache_data:
        return templates.TemplateResponse("grand_portfolio.html", {
            "request": request,
            "entries": [],
            "empty": True,
        })

    entries = client.build_grand_portfolio(cache_data, SUPERINVESTORS_BY_CIK)
    return templates.TemplateResponse("grand_portfolio.html", {
        "request": request,
        "entries": entries[:100],
    })


# --- Stock Detail ---

@app.get("/stock/cusip/{cusip}", response_class=HTMLResponse)
async def stock_detail_by_cusip(request: Request, cusip: str):
    cache_data = getattr(app.state, "fund_cache", {})
    if not cache_data:
        return templates.TemplateResponse("error.html", {
            "request": request,
            "message": "No cached data available. Visit the homepage to load data.",
        }, status_code=404)

    detail = client.build_stock_detail(
        cusip, cache_data, SUPERINVESTORS_BY_CIK, by_cusip=True
    )
    if not detail:
        return templates.TemplateResponse("error.html", {
            "request": request,
            "message": f'No superinvestor holds a stock with CUSIP "{cusip}".',
        }, status_code=404)

    history = client.build_stock_history(
        cusip, cache_data, SUPERINVESTORS_BY_CIK, by_cusip=True
    )

    return templates.TemplateResponse("stock.html", {
        "request": request,
        "stock": detail,
        "history": history,
    })


@app.get("/stock/{ticker}", response_class=HTMLResponse)
async def stock_detail(request: Request, ticker: str):
    cache_data = getattr(app.state, "fund_cache", {})
    if not cache_data:
        return templates.TemplateResponse("error.html", {
            "request": request,
            "message": "No cached data available. Visit the homepage to load data.",
        }, status_code=404)

    detail = client.build_stock_detail(
        ticker, cache_data, SUPERINVESTORS_BY_CIK
    )
    if not detail:
        return templates.TemplateResponse("error.html", {
            "request": request,
            "message": f'No superinvestor holds a stock with ticker "{ticker.upper()}".',
        }, status_code=404)

    history = client.build_stock_history(
        ticker, cache_data, SUPERINVESTORS_BY_CIK
    )

    return templates.TemplateResponse("stock.html", {
        "request": request,
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


# --- Market Data API (heatmap, most-added, ticker search) ---

@app.get("/api/ticker-search-index")
async def ticker_search_index(request: Request):
    cache_data = getattr(app.state, "fund_cache", {})
    data = await asyncio.to_thread(market_data.get_ticker_search_list, cache_data)
    return JSONResponse(content=data)


@app.get("/api/heatmap", response_class=HTMLResponse)
async def heatmap(request: Request):
    if not getattr(app.state, "market_data_ready", False):
        return HTMLResponse(
            '<article>'
            '<p aria-busy="true">Loading S&P 500 market data (first load takes ~30s)...</p>'
            '</article>'
            '<div hx-get="/api/heatmap" hx-trigger="load delay:5s" hx-swap="outerHTML"></div>'
        )

    mkt = await asyncio.to_thread(market_data.get_sp500_market_data)
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
    return JSONResponse({
        "status": "ok",
        "uptime_seconds": uptime,
        "cache_entries": len(cache_data),
        "cache_age": cache.get_cache_age_str(),
        "refreshing": getattr(app.state, "refreshing", False),
        "market_data_ready": getattr(app.state, "market_data_ready", False),
    })


@app.get("/robots.txt")
async def robots_txt():
    content = (
        "User-agent: *\n"
        "Allow: /\n"
        "Disallow: /api/\n"
        "Disallow: /refresh\n"
        "\n"
        "Sitemap: https://13f-viewer.up.railway.app/sitemap.xml\n"
    )
    return PlainTextResponse(content, media_type="text/plain")


@app.get("/sitemap.xml")
async def sitemap_xml():
    base_url = "https://13f-viewer.up.railway.app"
    urls = [
        f"  <url><loc>{base_url}/</loc><changefreq>daily</changefreq><priority>1.0</priority></url>",
        f"  <url><loc>{base_url}/activity</loc><changefreq>daily</changefreq><priority>0.8</priority></url>",
        f"  <url><loc>{base_url}/grand-portfolio</loc><changefreq>daily</changefreq><priority>0.8</priority></url>",
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
