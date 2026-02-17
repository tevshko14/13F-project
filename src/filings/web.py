import asyncio
import json as json_module
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from filings import client, cache, watchlist, notifications, analysts, market_data
from filings.models import SuperinvestorSummary
from filings.superinvestors import SUPERINVESTORS, SUPERINVESTORS_BY_CIK


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Load cache on startup, trigger background refresh if stale."""
    app.state.fund_cache = cache.load_cache()
    app.state.refreshing = False
    app.state.sse_clients: list[asyncio.Queue] = []

    # Initialize notification system (first-run: marks existing filings as "seen")
    notifications.initialize_if_needed(app.state.fund_cache)

    if cache.is_cache_stale() and app.state.fund_cache:
        asyncio.create_task(_background_refresh(app))

    # Start periodic polling for new filings
    poll_task = asyncio.create_task(_notification_poll_loop(app))

    # Prefetch S&P 500 market data in background (~30-60s on cold start)
    asyncio.create_task(_prefetch_market_data(app))

    yield

    poll_task.cancel()


app = FastAPI(title="13F Filing Viewer", lifespan=lifespan)

templates = Jinja2Templates(
    directory=Path(__file__).parent / "templates"
)

# Expose watchlist + notification helpers to every template
templates.env.globals["get_watchlist"] = watchlist.load_watchlist
templates.env.globals["is_in_watchlist"] = watchlist.is_in_watchlist
templates.env.globals["get_unread_count"] = notifications.get_unread_count


async def _background_refresh(app: FastAPI):
    """Refresh cache for all superinvestors in background.

    Also detects new filings and checks for watchlist matches.
    Saves cache to disk in batches (every 10 funds) to avoid blocking I/O.
    """
    app.state.refreshing = True
    dirty_count = 0
    for si in SUPERINVESTORS:
        try:
            data = await asyncio.to_thread(cache.refresh_single_fund, si.cik)
            if data:
                # Check for new filing BEFORE updating in-memory cache
                new_filing_date = data.get("filing_date", "")
                is_new = notifications.is_new_filing(si.cik, new_filing_date)

                # Update in-memory cache immediately
                app.state.fund_cache[si.cik] = data
                dirty_count += 1

                # Batch disk writes: save every 10 funds (not every single one)
                if dirty_count % 10 == 0:
                    await asyncio.to_thread(cache.save_cache, app.state.fund_cache)

                # If new filing detected, check watchlist matches
                if is_new and new_filing_date:
                    wl = watchlist.load_watchlist()
                    if wl:
                        matches = notifications.check_watchlist_matches(
                            si.cik, si.display_name, data, wl
                        )
                        for notif in matches:
                            notifications.add_notification(notif)
                            await _broadcast_sse(app, notif)
                    notifications.mark_filing_seen(si.cik, new_filing_date)
        except Exception:
            pass
        await asyncio.sleep(1)  # Rate limit: don't overwhelm SEC

    # Final save to catch any remaining dirty data
    if dirty_count % 10 != 0:
        await asyncio.to_thread(cache.save_cache, app.state.fund_cache)
    app.state.refreshing = False


async def _notification_poll_loop(app: FastAPI):
    """Periodically trigger background refresh to detect new filings.

    Polling interval adapts to filing season (2h during, 12h outside).
    """
    while True:
        try:
            interval = notifications.get_poll_interval_seconds()
            await asyncio.sleep(interval)
            if not getattr(app.state, "refreshing", False):
                asyncio.create_task(_background_refresh(app))
        except asyncio.CancelledError:
            break
        except Exception:
            pass


async def _broadcast_sse(app: FastAPI, notif: dict):
    """Push a notification to all connected SSE clients."""
    for queue in getattr(app.state, "sse_clients", []):
        await queue.put(notif)


async def _prefetch_market_data(app: FastAPI):
    """Prefetch S&P 500 market data on startup (runs in background thread)."""
    try:
        app.state.market_data_ready = False
        await asyncio.to_thread(market_data.get_sp500_market_data)
        app.state.market_data_ready = True
    except Exception:
        app.state.market_data_ready = False


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
            summaries.append(None)  # Will trigger lazy loading

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
    cik_normalized = cik.lstrip("0") or cik       # "0001067983" -> "1067983"
    si = SUPERINVESTORS_BY_CIK.get(cik_normalized) or SUPERINVESTORS_BY_CIK.get(cik)
    try:
        data = await asyncio.to_thread(client.get_fund_summary, cik)
        app.state.fund_cache[cik_normalized] = data
        # Save to disk in a background thread (non-blocking)
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
    """Redirect old /compare URLs to the consolidated investor page."""
    return RedirectResponse(url=f"/holdings/{cik}", status_code=302)


# --- Compare API (lazy-loaded into investor page Compare tab) ---

@app.get("/api/compare/{cik}", response_class=HTMLResponse)
async def compare_api(request: Request, cik: str, top_n: int = Query(25)):
    """Return compare quarter partial for an investor (loaded on tab click)."""
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
        "activities": activities[:100],  # Limit to top 100
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
        "entries": entries[:100],  # Top 100 stocks
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
    """Return analyst ratings partial for a ticker (loaded on tab click)."""
    ratings = await asyncio.to_thread(analysts.get_analyst_ratings, ticker)
    consensus = analysts.get_consensus_summary(ratings)
    return templates.TemplateResponse("partials/analyst_ratings.html", {
        "request": request,
        "ratings": ratings[:50],  # Limit to 50 most recent
        "consensus": consensus,
        "ticker": ticker.upper(),
    })


# --- Market Data API (heatmap, most-added, ticker search) ---

@app.get("/api/ticker-search-index")
async def ticker_search_index(request: Request):
    """JSON autocomplete index for the nav ticker search bar."""
    cache_data = getattr(app.state, "fund_cache", {})
    data = await asyncio.to_thread(market_data.get_ticker_search_list, cache_data)
    from fastapi.responses import JSONResponse
    return JSONResponse(content=data)


@app.get("/api/heatmap", response_class=HTMLResponse)
async def heatmap(request: Request):
    """S&P 500 heatmap partial (ECharts treemap). Auto-retries if data not ready."""
    if not getattr(app.state, "market_data_ready", False):
        # Data still loading — return a polling stub
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

    # Build set of tickers held by superinvestors
    cache_data = getattr(app.state, "fund_cache", {})
    super_tickers: set[str] = set()
    for cik, fund_data in cache_data.items():
        if cik in SUPERINVESTORS_BY_CIK:
            for h in fund_data.get("all_holdings", []):
                t = h.get("ticker")
                if t:
                    super_tickers.add(t.upper())

    heatmap_data = market_data.build_heatmap_data(mkt, constituents, super_tickers)
    metadata = mkt.get("_metadata", {})

    return templates.TemplateResponse("partials/heatmap.html", {
        "request": request,
        "heatmap_json": json_module.dumps(heatmap_data),
        "metadata": metadata,
    })


@app.get("/api/most-added", response_class=HTMLResponse)
async def most_added(request: Request):
    """Most-added-by-superinvestors table partial, enriched with analyst consensus + 52W range."""
    cache_data = getattr(app.state, "fund_cache", {})
    if not cache_data:
        return HTMLResponse(
            '<article><p class="text-muted">No data available yet.</p></article>'
        )

    entries = await asyncio.to_thread(
        market_data.build_most_added_table, cache_data, SUPERINVESTORS_BY_CIK
    )

    # Enrich each entry with analyst consensus and 52-week range
    tickers_to_lookup = [e["ticker"] for e in entries if e.get("ticker")]

    # Get 52-week ranges in bulk (cached)
    range_data = {}
    if tickers_to_lookup:
        range_data = await asyncio.to_thread(
            market_data.get_52_week_range_bulk, tickers_to_lookup
        )

    # Get analyst consensus per ticker (uses existing 5-min cache)
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
        else:
            entry["consensus"] = None
            entry["range_pct"] = None

    return templates.TemplateResponse("partials/most_added.html", {
        "request": request,
        "entries": entries,
    })


# --- Watchlist API ---

@app.post("/api/watchlist/{ticker}", response_class=HTMLResponse)
async def watchlist_add(request: Request, ticker: str):
    """Add a ticker to the watchlist. Returns star button + OOB sidebar."""
    form = await request.form()
    cusip = form.get("cusip", "")
    issuer_name = form.get("issuer_name", "")
    watchlist.add_to_watchlist(ticker, cusip=cusip, issuer_name=issuer_name)

    # Build a minimal stock-like object for the star template
    stock_obj = type("S", (), {
        "ticker": ticker.upper(),
        "cusip": cusip,
        "issuer_name": issuer_name,
    })()
    return templates.TemplateResponse("partials/watchlist_response.html", {
        "request": request,
        "stock": stock_obj,
    })


@app.delete("/api/watchlist/{ticker}", response_class=HTMLResponse)
async def watchlist_remove(request: Request, ticker: str):
    """Remove a ticker from the watchlist."""
    watchlist.remove_from_watchlist(ticker)

    hx_target = request.headers.get("hx-target", "")

    # If triggered from the stock page star button → return star + OOB sidebar
    if hx_target == "watchlist-star":
        # Look up stock info from cache to rebuild star button context
        cache_data = getattr(app.state, "fund_cache", {})
        detail = client.build_stock_detail(
            ticker, cache_data, SUPERINVESTORS_BY_CIK
        )
        stock_obj = type("S", (), {
            "ticker": ticker.upper(),
            "cusip": getattr(detail, "cusip", "") if detail else "",
            "issuer_name": getattr(detail, "issuer_name", "") if detail else "",
        })()
        return templates.TemplateResponse("partials/watchlist_response.html", {
            "request": request,
            "stock": stock_obj,
        })

    # If triggered from sidebar × button → return just the sidebar content
    return templates.TemplateResponse("partials/watchlist_sidebar.html", {
        "request": request,
    })


@app.get("/api/watchlist-sidebar", response_class=HTMLResponse)
async def watchlist_sidebar_refresh(request: Request):
    """Full sidebar content refresh (fallback)."""
    return templates.TemplateResponse("partials/watchlist_sidebar.html", {
        "request": request,
    })


# --- Notification API ---

@app.get("/api/notifications/stream")
async def notification_stream(request: Request):
    """SSE endpoint for real-time in-app notifications."""
    queue: asyncio.Queue = asyncio.Queue()
    app.state.sse_clients.append(queue)

    async def event_generator():
        try:
            while True:
                notif = await queue.get()
                data = json_module.dumps(notif)
                yield f"event: notification\ndata: {data}\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            if queue in app.state.sse_clients:
                app.state.sse_clients.remove(queue)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    )


@app.get("/api/notifications/bell", response_class=HTMLResponse)
async def notification_bell(request: Request):
    """Return the bell icon partial (for HTMX polling)."""
    return templates.TemplateResponse("partials/notification_bell.html", {
        "request": request,
    })


@app.get("/notifications", response_class=HTMLResponse)
async def notifications_page(request: Request):
    """Full notification history page."""
    all_notifs = notifications.get_all_notifications(limit=50)
    return templates.TemplateResponse("notifications.html", {
        "request": request,
        "notifications": all_notifs,
        "unread_count": notifications.get_unread_count(),
    })


@app.post("/api/notifications/read/{notification_id}", response_class=HTMLResponse)
async def mark_read(request: Request, notification_id: str):
    """Mark a single notification as read."""
    notifications.mark_notification_read(notification_id)
    return HTMLResponse("")


@app.post("/api/notifications/read-all", response_class=HTMLResponse)
async def mark_all_read(request: Request):
    """Mark all notifications as read. Returns updated bell."""
    notifications.mark_all_read()
    return templates.TemplateResponse("partials/notification_bell.html", {
        "request": request,
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


def main():
    """Entry point for `uv run filings-web`."""
    import os
    port = int(os.environ.get("PORT", 8000))
    host = os.environ.get("HOST", "127.0.0.1")
    reload = os.environ.get("RAILWAY_ENVIRONMENT") is None  # Only reload locally
    uvicorn.run(
        "filings.web:app",
        host=host,
        port=port,
        reload=reload,
    )
