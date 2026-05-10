"""Watchlist API router — authenticated per-user watchlist + notification prefs.

Moved out of web.py during the audit-sprint-4 refactor.  All endpoints
require a Clerk-authenticated session (set on ``request.state.user`` by
the auth middleware registered in web.py).

Routes:
  GET    /watchlist                       — HTML dashboard page
  POST   /api/watchlist                   — add a ticker
  DELETE /api/watchlist/{ticker}          — remove a ticker
  GET    /api/watchlist                   — list tickers + recent signals
  GET    /api/watchlist/check/{ticker}    — is this ticker watched?
  GET    /api/watchlist/preferences       — notification prefs
  PUT    /api/watchlist/preferences       — update notification prefs
"""

from __future__ import annotations

import asyncio

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

from filings import supabase_cache
from filings.app_state import templates, valid_ticker

router = APIRouter()


def _require_user(request: Request) -> str | None:
    """Return user_id from Clerk auth, or None if not authenticated."""
    if not request.state.user:
        return None
    return request.state.user.get("sub", "") or None


# ── JSON API ─────────────────────────────────────────────────────────


@router.post("/api/watchlist", response_class=JSONResponse)
async def api_watchlist_add(request: Request):
    """Add a ticker to the user's watchlist."""
    user_id = _require_user(request)
    if not user_id:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    try:
        body = await request.json()
        ticker = body.get("ticker", "").strip().upper()
    except Exception:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)
    if not ticker or not valid_ticker(ticker):
        return JSONResponse({"error": "Invalid ticker"}, status_code=400)
    ok = await asyncio.to_thread(supabase_cache.add_to_watchlist, user_id, ticker)
    if not ok:
        return JSONResponse({"error": "Failed to add"}, status_code=500)
    return JSONResponse({"ok": True, "ticker": ticker})


@router.delete("/api/watchlist/{ticker}", response_class=JSONResponse)
async def api_watchlist_remove(request: Request, ticker: str):
    """Remove a ticker from the user's watchlist."""
    user_id = _require_user(request)
    if not user_id:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    ticker = ticker.strip().upper()
    if not ticker or not valid_ticker(ticker):
        return JSONResponse({"error": "Invalid ticker"}, status_code=400)
    ok = await asyncio.to_thread(
        supabase_cache.remove_from_watchlist, user_id, ticker
    )
    if not ok:
        return JSONResponse({"error": "Failed to remove"}, status_code=500)
    return JSONResponse({"ok": True, "ticker": ticker})


@router.get("/api/watchlist", response_class=JSONResponse)
async def api_watchlist_list(request: Request):
    """Return the user's full watchlist with enrichment data."""
    user_id = _require_user(request)
    if not user_id:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)

    watchlist = await asyncio.to_thread(supabase_cache.get_user_watchlist, user_id)
    if not watchlist:
        return JSONResponse({"tickers": [], "signals": {}})

    tickers = [w["ticker"] for w in watchlist]

    # Enrich with recent notifications (last 7 days) for watched tickers
    signals: dict[str, list[dict]] = {t: [] for t in tickers}
    try:
        all_notifs = await asyncio.to_thread(
            supabase_cache.get_recent_notifications, 200
        )
        for n in all_notifs:
            meta = n.get("metadata") or {}
            nticker = (meta.get("ticker") or "").upper()
            if nticker in signals:
                signals[nticker].append({
                    "type": n.get("type"),
                    "title": n.get("title"),
                    "message": n.get("message"),
                    "link": n.get("link"),
                    "created_at": n.get("created_at"),
                })
        for t in signals:
            signals[t] = signals[t][:3]
    except Exception:
        pass  # Enrichment is best-effort

    return JSONResponse({
        "tickers": [
            {"ticker": w["ticker"], "added_at": w["added_at"]}
            for w in watchlist
        ],
        "signals": signals,
    })


@router.get("/api/watchlist/check/{ticker}", response_class=JSONResponse)
async def api_watchlist_check(request: Request, ticker: str):
    """Check if a ticker is on the user's watchlist."""
    user_id = _require_user(request)
    if not user_id:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    ticker = ticker.strip().upper()
    watched = await asyncio.to_thread(
        supabase_cache.is_ticker_watched, user_id, ticker
    )
    return JSONResponse({"watched": watched})


@router.get("/api/watchlist/preferences", response_class=JSONResponse)
async def api_watchlist_prefs_get(request: Request):
    """Get the user's notification preferences."""
    user_id = _require_user(request)
    if not user_id:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    prefs = await asyncio.to_thread(
        supabase_cache.get_notification_preferences, user_id
    )
    return JSONResponse({"preferences": prefs})


@router.put("/api/watchlist/preferences", response_class=JSONResponse)
async def api_watchlist_prefs_update(request: Request):
    """Update the user's notification preferences (partial update)."""
    user_id = _require_user(request)
    if not user_id:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)

    # Allowlist of updatable fields (prevents privilege-escalation via
    # arbitrary column writes)
    allowed = {
        "notify_superinvestor_activity", "notify_insider_trading",
        "notify_congress_trading", "notify_options_activity",
        "notify_convergence_signals", "insider_min_value",
        "insider_title_filter", "digest_enabled", "digest_time",
        "digest_timezone", "realtime_email_enabled",
        "telegram_enabled", "telegram_chat_id",
    }
    prefs = {k: v for k, v in body.items() if k in allowed}
    if not prefs:
        return JSONResponse({"error": "No valid fields"}, status_code=400)

    ok = await asyncio.to_thread(
        supabase_cache.upsert_notification_preferences, user_id, prefs
    )
    if not ok:
        return JSONResponse({"error": "Failed to save"}, status_code=500)
    return JSONResponse({"ok": True})


# ── HTML page ────────────────────────────────────────────────────────
# `/watchlist` is owned by the v2 redesign router
# (`filings.routers.redesign_preview.preview_watchlist`).  The v1
# `watchlist.html` template is unreached in production; the API
# endpoints above (POST/GET /api/watchlist) are still the live
# backend that both v1 + v2 read from.
