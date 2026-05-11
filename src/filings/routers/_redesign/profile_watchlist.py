"""Profile + Watchlist pages (v2 redesign).

These two routes lived together before Watchlist was broken out into
its own sidenav entry, and they still share most of their helpers:

  * ``_resolve_profile_user`` — page-hero user dict from session state
  * ``_fetch_profile_watchlist`` — Supabase-backed list + market-data
    enrichment used to populate the watchlist table
  * ``_PROFILE_*`` constants — illustrative mock data for the Profile
    page tabs that still need a real prefs/billing schema before
    they're meaningful

Live data: watchlist table (rows + KPI strip).  Profile tab bodies are
illustrative mock until the user-prefs / billing schema lands.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from filings.app_state import templates
from filings.concurrency import to_heavy, to_supabase
from filings.routers._redesign.helpers import (
    _initials_from_name,
    _shell_context,
    is_profile_preview_enabled,
)

logger = logging.getLogger(__name__)

router = APIRouter()


# ── Helpers shared by /profile + /watchlist ──────────────────────────


def _resolve_profile_user(request: Request) -> dict | None:
    """Build the page-hero user dict from real session state.

    Reads ``request.state.profile`` (the Supabase ``profiles`` row,
    loaded by the auth middleware) and ``request.state.user`` (the
    Clerk JWT claims).  Returns ``None`` when neither is available --
    the caller should redirect to /login.

    Plan + member_since use sensible fallbacks today because the
    underlying columns (``plan_tier``, reliable ``created_at``) don't
    exist on the ``profiles`` table yet.  Once Stripe-backed
    subscriptions land + the schema migration runs, this helper picks
    them up automatically.
    """
    profile = getattr(request.state, "profile", None) if hasattr(request, "state") else None
    user = getattr(request.state, "user", None) if hasattr(request, "state") else None
    if not isinstance(profile, dict) and not isinstance(user, dict):
        return None

    profile = profile if isinstance(profile, dict) else {}
    user = user if isinstance(user, dict) else {}

    # Name: prefer Supabase display_name (set by the Clerk webhook
    # from first+last name), fall back to JWT `name`, finally to the
    # email local-part so we never show a literal blank.
    name = (
        (profile.get("display_name") or "").strip()
        or (user.get("name") or "").strip()
    )
    email = (profile.get("email") or user.get("email") or "").strip()
    if not name and email:
        name = email.split("@", 1)[0]

    initials = _initials_from_name(name)
    if not initials and email:
        initials = (email[:1] or "").upper()

    # Plan: free-tier default until Stripe-subscription rows land.
    # The profiles row may already carry a `plan_tier` in some envs --
    # honour it when present so this helper survives the migration.
    plan_raw = (profile.get("plan_tier") or "free").strip().lower()
    plan = {"free": "Free plan", "pro": "Pro plan", "team": "Team plan"}.get(
        plan_raw, plan_raw.title() + " plan",
    )

    # Member since: prefer `created_at` from the profiles row when the
    # column exists (Supabase typically auto-adds it).  Fall back to
    # a generic label so the page never shows "since None".
    member_since = "—"
    created_at = profile.get("created_at") or user.get("iat")
    if created_at:
        try:
            # `created_at` is ISO 8601 string (Supabase) or epoch int (JWT iat)
            if isinstance(created_at, (int, float)):
                dt = datetime.fromtimestamp(created_at, tz=timezone.utc)
            else:
                dt = datetime.fromisoformat(str(created_at).replace("Z", "+00:00"))
            member_since = dt.strftime("%b %Y")
        except (ValueError, TypeError):
            pass

    return {
        "initials":     initials or "PP",
        "name":         name or "Account",
        "email":        email or "",
        "plan":         plan,
        "member_since": member_since,
    }


def _profile_format_added(iso_str: str) -> str:
    """Convert ISO timestamp → "MMM DD, YYYY" for the table column."""
    if not iso_str:
        return "—"
    try:
        d = datetime.fromisoformat(iso_str)
        return d.strftime("%b %d %Y")
    except Exception:
        return iso_str[:10]


def _name_map_from_fund_cache(fund_cache: dict | None) -> dict[str, str]:
    """Build a ticker -> issuer name lookup from in-memory 13F holdings.

    Walks every fund's ``all_holdings`` once and keeps the first
    non-empty name seen per ticker.  The slim-holding shape stores the
    issuer name under ``issuer`` (not ``issuer_name``) -- see
    ``cache._slim_holding``.  Cheap because fund_cache lives in process;
    covers the universe of tickers any superinvestor has held
    (effectively the S&P + popular small/mid caps).  Tickers outside
    this universe fall back to displaying the ticker itself.
    """
    out: dict[str, str] = {}
    if not fund_cache:
        return out
    for fund in fund_cache.values():
        for h in fund.get("all_holdings") or []:
            t = (h.get("ticker") or "").upper()
            if t and t not in out:
                name = (h.get("issuer") or h.get("issuer_name") or "").strip()
                if name:
                    out[t] = name
    return out


def _format_price(p) -> str:
    if p is None:
        return "—"
    try:
        return f"${float(p):,.2f}"
    except (TypeError, ValueError):
        return "—"


def _format_chg_pct(p) -> tuple[str, str]:
    """Return (display_str, tone) where tone is 'up'/'down'/'flat'."""
    if p is None:
        return "—", "flat"
    try:
        val = float(p)
    except (TypeError, ValueError):
        return "—", "flat"
    tone = "up" if val > 0 else "down" if val < 0 else "flat"
    return f"{val:+.2f}%", tone


async def _fetch_profile_watchlist(
    user_id: str, fund_cache: dict | None = None,
) -> tuple[list[dict], dict]:
    """Read the user's Supabase-backed watchlist + enrich for the page.

    Returns ``(rows, kpis)`` where:
      * ``rows`` -- per-ticker dicts ready for the table.  Each row gets
        a ``spark_series`` (1M, normalised 0-1, from the cached S&P
        close DataFrame), a current ``price``, day ``chg_pct``, an
        issuer ``name`` (resolved from fund_cache when possible), and
        a ``signals`` count (recent notifications matching the ticker).
        Tickers outside the S&P-covered universe show em-dashes for
        spark/price/chg instead of broken/empty values.
      * ``kpis`` -- aggregate counts for the KPI strip at the top
        (total tickers, total signals (7d), insider notif tickers,
        13F/super notif tickers).

    Same Supabase source as the `/api/watchlist` JSON endpoint, so
    the page and API stay consistent.  Notification + market-data
    enrichment is best-effort -- failures degrade to em-dashes but
    the table still renders.
    """
    empty_kpis = {"total": 0, "signals": 0, "with_insider": 0, "with_super": 0}
    if not user_id:
        return [], empty_kpis

    try:
        from filings import supabase_cache
        entries = await to_supabase(supabase_cache.get_user_watchlist, user_id) or []
    except Exception as exc:
        logger.warning("Watchlist load failed for %s: %s", user_id, exc)
        entries = []

    if not entries:
        return [], empty_kpis

    tickers = [(e.get("ticker") or "").upper() for e in entries]
    tickers = [t for t in tickers if t]

    # Parallel enrichment: sparklines + day-period market data +
    # recent notifications.  `get_sp500_market_data("1D")` yields
    # {ticker: {"price": ..., "pct_change": ...}} for every S&P
    # ticker in one cached call (30-min TTL), so we avoid per-ticker
    # network calls in the request path.
    from filings import market_data, supabase_cache
    spark_task = to_heavy(market_data.get_sparkline_points, tickers, 20)
    md_task    = to_heavy(market_data.get_sp500_market_data, "1D")
    notif_task = to_supabase(supabase_cache.get_recent_notifications, 200)
    try:
        spark_map, md_map, notifs = await asyncio.gather(
            spark_task, md_task, notif_task, return_exceptions=True,
        )
        if isinstance(spark_map, Exception): spark_map = {}
        if isinstance(md_map, Exception):    md_map = {}
        if isinstance(notifs, Exception):    notifs = []
    except Exception as exc:
        logger.debug("Watchlist enrichment failed for %s: %s", user_id, exc)
        spark_map, md_map, notifs = {}, {}, []

    # Bucket notifications by ticker (matches the /api/watchlist API
    # enrichment so both surfaces see the same counts).
    signals_by_ticker: dict[str, list[dict]] = {t: [] for t in tickers}
    for n in notifs or []:
        meta = n.get("metadata") or {}
        nt = (meta.get("ticker") or "").upper()
        if nt in signals_by_ticker:
            signals_by_ticker[nt].append(n)

    # Ticker -> issuer name lookup, built once from in-memory fund_cache.
    name_map = _name_map_from_fund_cache(fund_cache)

    kpis = {
        "total":        len(tickers),
        "signals":      sum(len(v) for v in signals_by_ticker.values()),
        "with_insider": sum(
            1 for sigs in signals_by_ticker.values()
            if any((s.get("type") or "").lower().startswith("insider") for s in sigs)
        ),
        "with_super": sum(
            1 for sigs in signals_by_ticker.values()
            if any(((s.get("type") or "").lower().startswith(("13f", "fund", "super")))
                   for s in sigs)
        ),
    }

    rows = []
    for e in entries:
        ticker = (e.get("ticker") or "").upper()
        sigs = signals_by_ticker.get(ticker, [])
        md = md_map.get(ticker) if isinstance(md_map, dict) else None
        price_raw = (md or {}).get("price")
        chg_raw = (md or {}).get("pct_change")
        chg_str, chg_tone = _format_chg_pct(chg_raw)
        rows.append({
            "ticker":       ticker,
            "name":         (
                e.get("issuer_name") or name_map.get(ticker) or ticker
            ),
            "price":        _format_price(price_raw),
            "price_raw":    price_raw,           # for data-sort-value
            "chg":          chg_str,
            "chg_raw":      chg_raw,             # for data-sort-value
            "chg_tone":     chg_tone,            # 'up' / 'down' / 'flat'
            "alerts":       str(len(sigs)) if sigs else "none",
            "alerts_n":     len(sigs),
            "earnings":     "—",
            "added":        _profile_format_added(e.get("added_at", "")),
            "spark_series": spark_map.get(ticker, []) if isinstance(spark_map, dict) else [],
        })
    return rows, kpis


# ── Mock content for not-yet-wired tabs ──────────────────────────────
# These tab bodies need user-prefs / billing schema we don't have yet.
# Design-faithful rows render real-shape content so the page feels
# complete to a viewer without lying about what's wired.


_PROFILE_LISTS_MOCK = [
    "Core",
    "AI infra",
    "Smart-money buys",
    "Earnings this week",
    "Energy",
]

_PROFILE_ALERTS_MOCK: list[dict] = [
    {"ticker": "NVDA",  "type": "Insider buy",      "rule": "Open-market BUY ≥ $1M",    "channel": "Email + Push",  "status": "active", "triggered": "3 days ago"},
    {"ticker": "PLTR",  "type": "Congress trade",   "rule": "Any new BUY",              "channel": "Email",         "status": "active", "triggered": "yesterday"},
    {"ticker": "GME",   "type": "Sentiment spike",  "rule": "WSB mentions ≥ 5,000 / 4h","channel": "Push",          "status": "active", "triggered": "now"},
    {"ticker": "BRK.B", "type": "13F filing",       "rule": "New filing posted",        "channel": "Email",         "status": "active", "triggered": "22 days ago"},
    {"ticker": "COIN",  "type": "Price",            "rule": "Cross above $250",         "channel": "Email + Push",  "status": "active", "triggered": "never"},
    {"ticker": "AAPL",  "type": "Earnings",         "rule": "24h before earnings",      "channel": "Email",         "status": "active", "triggered": "in 8 days"},
    {"ticker": "OXY",   "type": "Insider cluster",  "rule": "≥ 3 insiders BUY in 30d",  "channel": "Email",         "status": "paused", "triggered": "42 days ago"},
]

_PROFILE_ACCOUNT_PROFILE = [
    ("Display name",  "Tev McNeill"),
    ("Email",         "tev@paperpanda.io"),
    ("Username",      "@tev"),
    ("Time zone",     "America / New York"),
    ("Default range", "6 months"),
]
_PROFILE_ACCOUNT_PREFS = [
    ("Theme",                "System (auto)"),
    ("Number format",        "Compact ($1.2B)"),
    ("Default home view",    "Markets"),
    ("Email digest",         "Weekly · Mondays"),
    ("Notification sounds",  "On"),
]
_PROFILE_ACCOUNT_SECURITY = [
    ("Password",          "Last changed 4 months ago"),
    ("Two-factor",        "Enabled · Authenticator"),
    ("Active sessions",   "2 devices"),
    ("API keys",          "1 key · last used yesterday"),
]
_PROFILE_ACCOUNT_CONNECTED = [
    {"name": "Google",      "value": "tev@gmail.com",  "status": "connected"},
    {"name": "X / Twitter", "value": "@tev",           "status": "connected"},
    {"name": "Discord",     "value": "—",              "status": "not connected"},
    {"name": "Slack",       "value": "—",              "status": "not connected"},
]

_PROFILE_PLANS = [
    {"name": "Free",     "price": "$0",   "period": "/mo",
     "features": ["3 watchlists", "5 alerts", "15-min delayed quotes", "Basic 13F access"],
     "cta":      "Downgrade", "current": False},
    {"name": "Pro",      "price": "$24",  "period": "/mo",
     "features": ["25 watchlists", "Unlimited alerts", "Real-time quotes",
                  "Full 13F + insider + Congress", "API access", "Custom screeners"],
     "cta":      "Current plan", "current": True,  "accent": True},
    {"name": "Premium",  "price": "$72",  "period": "/mo",
     "features": ["Everything in Pro", "Options flow + dark pools",
                  "AI-generated reports", "Priority support",
                  "Slack / Discord webhooks", "Multi-user (3 seats)"],
     "cta":      "Upgrade", "current": False},
]

_PROFILE_BILLING = [
    ("Plan",          "Pro · $24 / month"),
    ("Next charge",   "May 14, 2026 · $24.00"),
    ("Payment",       "Visa •••• 4280"),
    ("Billing email", "tev@paperpanda.io"),
]

_PROFILE_INVOICES = [
    {"date": "Apr 14, 2026", "label": "Pro · monthly", "amount": "$24.00", "status": "Paid"},
    {"date": "Mar 14, 2026", "label": "Pro · monthly", "amount": "$24.00", "status": "Paid"},
    {"date": "Feb 14, 2026", "label": "Pro · monthly", "amount": "$24.00", "status": "Paid"},
    {"date": "Jan 14, 2026", "label": "Pro · monthly", "amount": "$24.00", "status": "Paid"},
    {"date": "Dec 14, 2025", "label": "Pro · monthly", "amount": "$24.00", "status": "Paid"},
]


_PROFILE_TABS = ("Account", "Alerts", "Subscription")

# Quick-start picks shown on the empty Watchlist page -- mega-caps the
# average user recognises, mixing tech / finance / consumer so the row
# doesn't feel like a single sector.  Order roughly by household-name
# recognition.
_WATCHLIST_POPULAR_TICKERS = (
    "NVDA", "AAPL", "MSFT", "GOOGL", "AMZN",
    "TSLA", "META", "AMD",
)


# ── Routes ────────────────────────────────────────────────────────────


@router.get("/watchlist", response_class=HTMLResponse)
async def preview_watchlist(request: Request):
    """Standalone watchlist page -- own URL + own sidenav entry.

    Previously rendered as a tab inside /profile; broken out so it
    gets first-class navigation alongside the section groups
    (Markets / Signals / Watchlist / Profile).

    Requires auth -- unauthenticated visitors bounce to /login.
    """
    user = _resolve_profile_user(request)
    if user is None:
        return RedirectResponse(url="/login", status_code=302)

    user_id = (
        (getattr(request.state, "user", None) or {}).get("sub")
        if hasattr(request, "state") else None
    )
    fund_cache = getattr(request.app.state, "fund_cache", None)
    rows, kpis = await _fetch_profile_watchlist(user_id or "", fund_cache=fund_cache)
    held = {r["ticker"] for r in rows}
    # Filter popular tickers to those the user doesn't already track,
    # so they can't double-add and the row stays useful on a non-empty
    # watchlist.
    popular = [t for t in _WATCHLIST_POPULAR_TICKERS if t not in held]

    ctx = {
        "request":         request,
        **(await _shell_context(request, "Watchlist")),
        "user":            user,
        "watch_rows":      rows,
        "watch_empty":     len(rows) == 0,
        "watch_kpis":      kpis,
        "popular_tickers": popular,
    }
    return templates.TemplateResponse("_redesign/watchlist.html", ctx)


@router.get("/profile", response_class=HTMLResponse)
async def preview_profile(request: Request, tab: str = "Account"):
    """Profile page — Account / Alerts / Subscription tabs.

    Watchlist used to be a tab here but is now its own page (/watchlist)
    with a dedicated sidenav group.  ``?tab=`` deep-links one of the
    three remaining tabs so the sidenav's Profile → Account|Alerts|
    Subscription items land on the right pane.

    Gated behind ``PP_PROFILE_PREVIEW=1`` -- the tab bodies still
    render mock data (no prefs/billing schema), so prod 302s home
    until the wiring lands.  Local dev sets the env var to iterate.
    Requires auth in addition to the gate -- unauthenticated visitors
    bounce to /login.
    """
    if not is_profile_preview_enabled():
        return RedirectResponse(url="/", status_code=302)
    user = _resolve_profile_user(request)
    if user is None:
        return RedirectResponse(url="/login", status_code=302)

    if tab not in _PROFILE_TABS:
        tab = "Account"

    ctx = {
        "request":    request,
        **(await _shell_context(request, "Profile")),
        "user":       user,
        "profile_tab": tab,
        # New tab payloads — illustrative content for now.
        "profile_alerts":           _PROFILE_ALERTS_MOCK,
        "profile_alerts_active":    sum(1 for a in _PROFILE_ALERTS_MOCK if a["status"] == "active"),
        "profile_alerts_quota":     25,
        "profile_account_profile":  _PROFILE_ACCOUNT_PROFILE,
        "profile_account_prefs":    _PROFILE_ACCOUNT_PREFS,
        "profile_account_security": _PROFILE_ACCOUNT_SECURITY,
        "profile_account_connected":_PROFILE_ACCOUNT_CONNECTED,
        "profile_plans":            _PROFILE_PLANS,
        "profile_billing":          _PROFILE_BILLING,
        "profile_invoices":         _PROFILE_INVOICES,
    }
    return templates.TemplateResponse("_redesign/profile.html", ctx)
