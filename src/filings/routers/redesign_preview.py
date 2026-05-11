"""Redesign preview router — env-gated dev mount for the in-progress visual
redesign of PaperPanda.

Mounted only when ``PP_REDESIGN_PREVIEW=1`` is set in the environment.
Without that flag the router is *never registered* on the FastAPI app, so
even if this branch accidentally shipped to Railway the preview routes
would not exist (no env var on Railway = inert).

All routes live under ``/_v2/...`` and render templates from
``filings/templates/_redesign/`` with mock data baked in (mirroring the
shape of ``design_handoff_paperpanda/data.js``).  Real-data wiring
happens at the final "flip" step where production route handlers are
pointed at these templates and mock data is replaced with live feeds.
"""

from __future__ import annotations

import asyncio
import functools
import logging
import time
from typing import Callable

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from filings.app_state import templates
from filings.concurrency import (
    to_light,
    to_supabase,
)

# Shared helpers extracted from this file -- see _redesign/helpers.py.
# We re-import the names locally so the routes still defined in this
# file (home/stock/funds/etc., before they're broken out into sub-
# modules of their own) keep working unchanged.  ``is_enabled`` is
# imported for re-export to ``web.py`` (which does
# ``redesign_preview.is_enabled()`` to gate router mounting).
from filings.routers._redesign.helpers import (
    _bounded,
    _build_cusip_ticker_map,
    _shell_context,
    is_enabled,  # noqa: F401 — re-export for web.py
    is_placeholders_enabled,
)

logger = logging.getLogger(__name__)


router = APIRouter(prefix="", tags=["redesign"])


def _placeholder_route(path: str, **kwargs):
    """Decorator that registers a route only when placeholders are enabled.

    Used for screener / options pages which still ship visual-only / mock
    content; they should be browsable locally but absent in production.
    """
    def wrap(fn):
        if is_placeholders_enabled():
            return router.get(path, **kwargs)(fn)
        return fn
    return wrap


# SWR background-refresh helpers moved to _redesign.stock.


# ─────────────────────────────────────────────────────────────────────────────
# HOME + /_pages + L2 warmer -- moved to filings.routers._redesign.home.
# warm_homepage_caches / warm_l2_caches re-exported below for web.py compat.
# ─────────────────────────────────────────────────────────────────────────────



# ─────────────────────────────────────────────────────────────────────────────
# MACRO — Indicators tab is the live (default).  Other 4 tabs (Yields,
# FX & Commodities, Calendar, Heatmap) render placeholder panels until
# their real-data wiring lands in a follow-up.
# ─────────────────────────────────────────────────────────────────────────────
# MACRO -- moved to filings.routers._redesign.macro (audit-sprint-7).
# ─────────────────────────────────────────────────────────────────────────────



# ─────────────────────────────────────────────────────────────────────────────
# FUNDS -- moved to filings.routers._redesign.funds (audit-sprint-7).
# ─────────────────────────────────────────────────────────────────────────────



# ─────────────────────────────────────────────────────────────────────────────
# OPTIONS — Unusual flow (default tab) is mock-only.
# Per-print sweep classification needs an OPRA-licensed vendor (Cheddar,
# Unusual Whales, Polygon options trades) which we don't have.  Existing
# unusual_options.py + cboe_data.py give IV rank + aggregate, not prints.
# Page renders the design's chrome with mock data; flagged in the audit.
# ─────────────────────────────────────────────────────────────────────────────


_OPTIONS_FLOW_MOCK = [
    {"tk": "NVDA", "type": "CALL", "strike": 160, "exp": "May 16", "premium": "$8.4M", "vol":  "84,210", "oi": "12,420", "iv": "58%",  "spot": "142.18", "upside":  "+12.5%", "side": "BUY",  "notable": False},
    {"tk": "GME",  "type": "CALL", "strike":  35, "exp": "May 23", "premium": "$4.2M", "vol":  "42,840", "oi":  "1,824", "iv": "142%", "spot":  "28.42", "upside":  "+23.1%", "side": "BUY",  "notable": True},
    {"tk": "TSLA", "type": "PUT",  "strike": 160, "exp": "May 16", "premium": "$3.1M", "vol":  "12,840", "oi":  "8,420", "iv": "68%",  "spot": "184.20", "upside":  "-13.1%", "side": "BUY",  "notable": False},
    {"tk": "COIN", "type": "CALL", "strike": 240, "exp": "Jun 20", "premium": "$2.8M", "vol":   "8,420", "oi":  "4,210", "iv": "82%",  "spot": "218.42", "upside":  "+10.0%", "side": "BUY",  "notable": False},
    {"tk": "PLTR", "type": "CALL", "strike":  40, "exp": "May 16", "premium": "$2.4M", "vol":  "24,820", "oi": "18,420", "iv": "94%",  "spot":  "34.21", "upside":  "+16.9%", "side": "BUY",  "notable": False},
    {"tk": "AAPL", "type": "PUT",  "strike": 175, "exp": "May 30", "premium": "$1.8M", "vol":  "18,420", "oi": "24,140", "iv": "32%",  "spot": "184.20", "upside":   "-5.0%", "side": "SELL", "notable": False},
    {"tk": "AMD",  "type": "CALL", "strike": 185, "exp": "Jun 20", "premium": "$1.4M", "vol":   "6,820", "oi":  "3,810", "iv": "64%",  "spot": "172.41", "upside":   "+7.3%", "side": "BUY",  "notable": False},
    {"tk": "SMCI", "type": "PUT",  "strike":  80, "exp": "May 16", "premium": "$1.2M", "vol":  "12,420", "oi":  "4,820", "iv": "118%", "spot":  "88.42", "upside":   "-9.5%", "side": "BUY",  "notable": False},
]


@_placeholder_route("/options", response_class=HTMLResponse)
async def preview_options(request: Request):
    """Options page — Unusual flow (default).  Mock data; needs OPRA vendor."""
    kpi = [
        {"label": "Premium · today",   "value": "$1.84B",        "delta": None,        "up": None},
        {"label": "Unusual prints",    "value": "3,420",         "delta": "+18%",      "up": True},
        {"label": "Call / Put ratio",  "value": "1.82",          "delta": "bullish",   "up": True},
        {"label": "VIX",               "value": "14.2",          "delta": "-1.4",      "up": True},
        {"label": "Top sweep",         "value": "$NVDA $8.4M",   "delta": None,        "up": None},
        {"label": "Top dark pool",     "value": "$AAPL $42M",    "delta": None,        "up": None},
    ]
    ctx = {
        "request":      request,
        **(await _shell_context(request, "Options")),
        "options_kpi":  kpi,
        "options_flow": _OPTIONS_FLOW_MOCK,
        "options_tab":  "Unusual flow",
    }
    return templates.TemplateResponse("_redesign/options.html", ctx)


# ─────────────────────────────────────────────────────────────────────────────
# STOCK -- moved to filings.routers._redesign.stock (audit-sprint-7).
# build_stock_data_bundle re-exported below for web.py compat.
# ─────────────────────────────────────────────────────────────────────────────



# ─────────────────────────────────────────────────────────────────────────────
# SCREENER — single-page filter rail + result table.  Uses mock results
# because we don't yet have a multi-criteria screener engine.  The filter
# rail is visual-only — flagged in the gap audit as a `filter_rail`
# primitive needing a server-side /api/screener/run endpoint to power it.
# ─────────────────────────────────────────────────────────────────────────────


_SCREENER_PRESETS = [
    "All",                      # No filter — full universe
    "Smart-money buys",         # ≥ 5 superinvestors holding
    "13F new positions",        # any fund newly opened a position last quarter
    "Insider clusters",         # ≥ 3 insiders BUYing in 30d
    "Congress momentum",        # ≥ 1 Congress BUY in 90d
    "Magnificent 7",            # specific 7 mega-caps
    "Dividend aristocrats",     # placeholder — needs dividend metadata
    "Quality compounders",      # placeholder — needs fundamentals
    "Cheap & growing",          # placeholder — needs P/E + growth
]

# Magnificent 7 universe — used by the preset of the same name.
_MAG7 = {"AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA"}

_SCREENER_SORTS = ["Smart money", "Day %", "Insider buys", "Congress buys", "Ticker"]


def _format_compact_count(n: int, singular: str, plural: str | None = None) -> str:
    """e.g. _format_compact_count(5, "fund") → '5 funds'."""
    plural = plural or (singular + "s")
    return f"{n} {singular if n == 1 else plural}"


async def _fetch_screener_signals(months: int = 3) -> dict:
    """Fetch the three signal feeds the screener layers on top of the
    S&P 500 universe — 13F holdings (per-ticker fund counts), insider
    trades (last ~30d, by ticker), and Congress trades (last 3 months,
    by ticker)."""
    try:
        from filings import insider_trading, supabase_cache
    except Exception:
        return {"insiders": [], "congress": []}

    insiders, congress = await asyncio.gather(
        to_supabase(insider_trading.get_latest_insider_trades, "", 200, ""),
        to_supabase(supabase_cache.get_congress_trades_recent_months, months, 5000),
        return_exceptions=True,
    )
    if isinstance(insiders, Exception):
        logger.warning("Screener insiders fetch failed: %s", insiders)
        insiders = []
    if isinstance(congress, Exception):
        logger.warning("Screener congress fetch failed: %s", congress)
        congress = []
    return {"insiders": insiders or [], "congress": congress or []}


def _screener_count_funds_holding(request: Request) -> dict[str, int]:
    """Walk app.state.fund_cache once and count distinct funds holding each
    ticker.  Cached on app.state so repeated screener loads are free."""
    fund_cache = getattr(request.app.state, "fund_cache", {}) or {}
    if not fund_cache:
        return {}
    cached = getattr(request.app.state, "_pp_screener_funds_by_ticker", None)
    if isinstance(cached, dict):
        return cached

    by_ticker: dict[str, int] = {}
    for fund_data in fund_cache.values():
        seen: set[str] = set()
        for h in fund_data.get("all_holdings") or []:
            t = (h.get("ticker") or "").upper()
            if t and t not in seen:
                seen.add(t)
        for t in seen:
            by_ticker[t] = by_ticker.get(t, 0) + 1
    try:
        request.app.state._pp_screener_funds_by_ticker = by_ticker
    except Exception:
        pass
    return by_ticker


def _screener_count_funds_new_positions(request: Request) -> dict[str, int]:
    """Count funds that opened a brand-new position in each ticker last
    quarter.  Reads `changes` per fund, rolls up by ticker."""
    fund_cache = getattr(request.app.state, "fund_cache", {}) or {}
    if not fund_cache:
        return {}
    cached = getattr(request.app.state, "_pp_screener_new_by_ticker", None)
    if isinstance(cached, dict):
        return cached

    cusip_to_ticker = _build_cusip_ticker_map(fund_cache)
    by_ticker: dict[str, int] = {}
    for fund_data in fund_cache.values():
        for c in fund_data.get("changes") or []:
            status = (c.get("status") or "").upper()
            if status not in ("NEW", "NEWLY ADDED"):
                continue
            cusip = c.get("cusip", "")
            ticker = (cusip_to_ticker.get(cusip) or "").upper()
            if not ticker:
                continue
            by_ticker[ticker] = by_ticker.get(ticker, 0) + 1
    try:
        request.app.state._pp_screener_new_by_ticker = by_ticker
    except Exception:
        pass
    return by_ticker


def _screener_count_insiders(insider_trades: list) -> dict[str, dict[str, int]]:
    """Roll insider trades into {ticker: {buys, sells}}."""
    out: dict[str, dict[str, int]] = {}
    for tr in insider_trades:
        ticker = (getattr(tr, "ticker", None) or "").upper()
        if not ticker:
            continue
        bucket = out.setdefault(ticker, {"buys": 0, "sells": 0})
        if "Purchase" in (getattr(tr, "trade_type", "") or ""):
            bucket["buys"] += 1
        else:
            bucket["sells"] += 1
    return out


def _screener_count_congress(congress_rows: list) -> dict[str, dict[str, int]]:
    """Roll Congress trades into {ticker: {buys, sells}}."""
    out: dict[str, dict[str, int]] = {}
    for r in congress_rows:
        ticker = (r.get("ticker") or "").upper()
        if not ticker:
            continue
        bucket = out.setdefault(ticker, {"buys": 0, "sells": 0})
        ttype = (r.get("trade_type") or "").lower()
        if ttype in ("buy", "purchase"):
            bucket["buys"] += 1
        elif ttype in ("sell", "sale"):
            bucket["sells"] += 1
    return out


# Shared GICS-sector → short label map.  Used by the screener results table,
# the macro heatmap, and the insiders-companies sector chip.
_SECTOR_SHORT_LABELS: dict[str, str] = {
    "Information Technology": "Tech",
    "Communication Services": "Comm.",
    "Consumer Discretionary": "Cons Disc",
    "Consumer Staples":       "Cons Staples",
    "Financials":             "Financial",
    "Health Care":            "Health",
    "Industrials":            "Industrial",
    "Energy":                 "Energy",
    "Materials":              "Materials",
    "Utilities":              "Utilities",
    "Real Estate":            "Real Estate",
}


def _screener_build_dataset(
    request: Request,
    *,
    insiders: list,
    congress: list,
) -> list[dict]:
    """Join the S&P 500 universe with smart-money / insider / Congress
    aggregates.  One row per ticker — already sorted by smart-money flow."""
    try:
        from filings import market_data
    except Exception:
        return []

    constituents = market_data.get_sp500_constituents() or []
    market_1d   = market_data.get_sp500_market_data("1D") or {}
    funds_by_t  = _screener_count_funds_holding(request)
    new_by_t    = _screener_count_funds_new_positions(request)
    ins_by_t    = _screener_count_insiders(insiders)
    cong_by_t   = _screener_count_congress(congress)

    rows: list[dict] = []
    for c in constituents:
        ticker = (c.get("ticker") or "").upper()
        if not ticker:
            continue
        market_rec = market_1d.get(ticker)
        price = (market_rec or {}).get("price") if isinstance(market_rec, dict) else None
        pct   = (market_rec or {}).get("pct_change") if isinstance(market_rec, dict) else None

        funds_held = funds_by_t.get(ticker, 0)
        funds_new  = new_by_t.get(ticker, 0)
        ins        = ins_by_t.get(ticker, {"buys": 0, "sells": 0})
        cong       = cong_by_t.get(ticker, {"buys": 0, "sells": 0})

        # Smart-money column: prefer "+N new" when there were new positions
        # this quarter, otherwise show the held-by count.
        smart_str = "—"
        smart_score = 0
        if funds_new > 0:
            smart_str = f"+{funds_new} new"
            smart_score = funds_new * 2 + funds_held
        elif funds_held > 0:
            smart_str = _format_compact_count(funds_held, "fund")
            smart_score = funds_held

        # Insiders column: prefer the dominant direction.
        ins_buys, ins_sells = ins["buys"], ins["sells"]
        if ins_buys > ins_sells:
            ins_str = _format_compact_count(ins_buys, "BUY", "BUYs")
        elif ins_sells > 0:
            ins_str = _format_compact_count(ins_sells, "SELL", "SELLs")
        else:
            ins_str = "—"

        # Congress column: same dominant-direction treatment.
        cong_buys, cong_sells = cong["buys"], cong["sells"]
        if cong_buys > cong_sells:
            cong_str = _format_compact_count(cong_buys, "BUY", "BUYs")
        elif cong_sells > 0:
            cong_str = _format_compact_count(cong_sells, "SELL", "SELLs")
        else:
            cong_str = "—"

        rows.append({
            "tk":           ticker,
            "name":         c.get("name") or "",
            "sec":          _SECTOR_SHORT_LABELS.get(c.get("sector") or "", c.get("sector") or "—"),
            "mc":           "—",
            "pe":           "—",
            "price":        f"{price:,.2f}" if isinstance(price, (int, float)) else "—",
            "chg":          (pct / 100.0) if isinstance(pct, (int, float)) else 0.0,
            "smart":        smart_str,
            "smart_score":  smart_score,
            "funds_held":   funds_held,
            "funds_new":    funds_new,
            "ins":          ins_str,
            "ins_buys":     ins_buys,
            "ins_sells":    ins_sells,
            "cong":         cong_str,
            "cong_buys":    cong_buys,
            "cong_sells":   cong_sells,
            "is_mag7":      ticker in _MAG7,
        })

    rows.sort(key=lambda r: r["smart_score"], reverse=True)
    return rows


# Each preset maps to a (row → bool) predicate.  "All", "My filters", and
# the not-yet-derivable presets are absent → fall through to the no-op.
_SCREENER_PRESET_PREDICATES: dict[str, Callable[[dict], bool]] = {
    "Smart-money buys":   lambda r: r["funds_held"] >= 5,
    "13F new positions":  lambda r: r["funds_new"]  >= 1,
    "Insider clusters":   lambda r: r["ins_buys"]   >= 3,
    "Congress momentum":  lambda r: r["cong_buys"]  >= 1,
    "Magnificent 7":      lambda r: r["is_mag7"],
}


def _screener_apply_preset(rows: list[dict], preset: str) -> list[dict]:
    """Filter dataset based on the active preset."""
    pred = _SCREENER_PRESET_PREDICATES.get(preset)
    return [r for r in rows if pred(r)] if pred else rows


def _screener_apply_sort(rows: list[dict], sort_key: str) -> list[dict]:
    """Sort dataset by the active column."""
    sort_map = {
        "Smart money":   ("smart_score",  True),
        "Day %":         ("chg",          True),
        "Insider buys":  ("ins_buys",     True),
        "Congress buys": ("cong_buys",    True),
        "Ticker":        ("tk",           False),
    }
    field, desc = sort_map.get(sort_key, ("smart_score", True))
    if field == "tk":
        return sorted(rows, key=lambda r: r[field])
    return sorted(rows, key=lambda r: r.get(field) or 0, reverse=desc)


def _screener_filter_groups(dataset: list[dict]) -> list[tuple[str, list[tuple[str, str]]]]:
    """Filter rail values — derived from the live dataset where possible
    so the chips reflect the actual signal landscape, not hardcoded labels."""
    if not dataset:
        return _SCREENER_FILTER_GROUPS_FALLBACK

    # Single pass over the universe instead of four scans, one per chip.
    holding_at_least_5 = new_positions = insider_clusters = cong_momentum = 0
    for r in dataset:
        if r["funds_held"] >= 5: holding_at_least_5 += 1
        if r["funds_new"]  >= 1: new_positions      += 1
        if r["ins_buys"]   >= 3: insider_clusters   += 1
        if r["cong_buys"]  >= 1: cong_momentum      += 1

    return [
        ("Smart money", [
            ("13F adds (Q1)",   f"≥ 5 funds · {holding_at_least_5} match"),
            ("13F new",         f"Yes · {new_positions} match"),
            ("13F position %",  "≥ 1% of fund"),
            ("Top investor",    "Buffett, Ackman, +3"),
        ]),
        ("Insiders", [
            ("Open-market BUY", "Last 30d"),
            ("Cluster",         f"≥ 3 insiders · {insider_clusters} match"),
            ("10b5-1 only",     "Off"),
        ]),
        ("Congress", [
            ("Trades · 90d",    f"≥ 1 BUY · {cong_momentum} match"),
            ("Disclosure lag",  "Any"),
        ]),
        ("Fundamentals", [
            ("Mkt cap",         "$10B – $5T"),
            ("P/E",             "5 – 80"),
            ("Revenue growth",  "≥ 10% YoY"),
            ("FCF margin",      "≥ 15%"),
        ]),
        ("Price", [
            ("Day change",      "Any"),
            ("52w from high",   "≤ 25%"),
            ("RSI",             "30 – 80"),
        ]),
    ]


_SCREENER_FILTER_GROUPS_FALLBACK = [
    ("Smart money", [
        ("13F adds (Q1)",  "≥ 5 funds"),
        ("13F new",        "Yes"),
        ("13F position %", "≥ 1% of fund"),
        ("Top investor",   "Buffett, Ackman, +3"),
    ]),
    ("Insiders", [
        ("Open-market BUY", "Last 30d"),
        ("Cluster",         "≥ 3 insiders"),
        ("10b5-1 only",     "Off"),
    ]),
    ("Congress", [
        ("Trades · 90d",   "≥ 1 BUY"),
        ("Disclosure lag", "Any"),
    ]),
    ("Fundamentals", [
        ("Mkt cap",        "$10B – $5T"),
        ("P/E",            "5 – 80"),
        ("Revenue growth", "≥ 10% YoY"),
        ("FCF margin",     "≥ 15%"),
    ]),
    ("Price", [
        ("Day change",      "Any"),
        ("52w from high",   "≤ 25%"),
        ("RSI",             "30 – 80"),
    ]),
]


@_placeholder_route("/screener", response_class=HTMLResponse)
async def preview_screener(
    request: Request,
    preset: str = "All",
    sort:   str = "Smart money",
    limit:  int = 30,
):
    """Screener — joins the S&P 500 universe with smart-money / insider /
    Congress aggregates, applies the requested preset filter and sort, and
    paginates the top *limit* rows.  No real engine yet; filter rail values
    are read-only chips reflecting the live dataset."""
    if preset not in _SCREENER_PRESETS:
        preset = "All"
    if sort not in _SCREENER_SORTS:
        sort = "Smart money"
    limit = max(min(int(limit or 30), 200), 5)

    bounded = functools.partial(_bounded, page="Screener page")

    # The dataset is preset/sort-independent and the join across 500 SP500
    # tickers + signal feeds is hot-path bloat when re-run every request.
    # Memoize on app-state with a 5-minute TTL — short enough that fresh
    # insider / congress trades surface without lingering staleness.
    cache_slot = getattr(request.app.state, "_pp_screener_dataset", None)
    now = time.time()
    if cache_slot and (now - cache_slot[0]) < 300:
        dataset = cache_slot[1]
    else:
        signals = await bounded(
            _fetch_screener_signals(months=3),
            timeout=8.0, fallback={"insiders": [], "congress": []}, name="signals",
        )
        def _build():
            return _screener_build_dataset(
                request,
                insiders=signals.get("insiders") or [],
                congress=signals.get("congress") or [],
            )
        dataset = await to_light(_build)
        try:
            request.app.state._pp_screener_dataset = (now, dataset)
        except Exception:
            pass

    filtered = _screener_apply_preset(dataset, preset)
    sorted_rows = _screener_apply_sort(filtered, sort)
    visible = sorted_rows[:limit]

    ctx = {
        "request":         request,
        **(await _shell_context(request, "Screener")),
        "screener_presets":       _SCREENER_PRESETS,
        "screener_active_preset": preset,
        "screener_sorts":         _SCREENER_SORTS,
        "screener_active_sort":   sort,
        "screener_filters":       _screener_filter_groups(dataset),
        "screener_results":       visible,
        "screener_universe":      f"{len(dataset):,}" if dataset else "—",
        "screener_match_count":   len(filtered),
        "screener_visible_count": len(visible),
    }
    return templates.TemplateResponse("_redesign/screener.html", ctx)


# ─────────────────────────────────────────────────────────────────────────────
# CONGRESS — moved to filings.routers._redesign.congress (audit-sprint-7).
# Dropped ~400 LOC of dead helpers from the prior architecture iteration.
# ─────────────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────────────────────
# INSIDERS — moved to filings.routers._redesign.insiders (audit-sprint-7).
# `_insiders_action` + `_insiders_format_title` stay importable from
# helpers.py (home's fetcher still uses them).
# ─────────────────────────────────────────────────────────────────────────────




# ─────────────────────────────────────────────────────────────────────────────
# PROFILE + WATCHLIST — moved to filings.routers._redesign.profile_watchlist.
# ─────────────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────────────────────
# NOTIFICATIONS — moved to filings.routers._redesign.notifications.
# ─────────────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────────────────────
# RETAIL -- moved to filings.routers._redesign.retail (audit-sprint-7).
# ─────────────────────────────────────────────────────────────────────────────



# ─────────────────────────────────────────────────────────────────────────────
# MACRO (continued) -- moved to filings.routers._redesign.macro.
# ─────────────────────────────────────────────────────────────────────────────



# ─────────────────────────────────────────────────────────────────────────────
# SUPPORT — moved to filings.routers._redesign.support (audit-sprint-7).
# ─────────────────────────────────────────────────────────────────────────────


# Sub-router includes -- each feature module owns its own routes + helpers
# and registers them on its own APIRouter; we compose them into this
# file's `router` so web.py's single `include_router(redesign_preview.router)`
# picks them up transparently.
from filings.routers._redesign import support as _support_routes  # noqa: E402
from filings.routers._redesign import profile_watchlist as _profile_watchlist_routes  # noqa: E402
from filings.routers._redesign import insiders as _insiders_routes  # noqa: E402
from filings.routers._redesign import notifications as _notifications_routes  # noqa: E402
from filings.routers._redesign import congress as _congress_routes  # noqa: E402
from filings.routers._redesign import macro as _macro_routes  # noqa: E402
from filings.routers._redesign import retail as _retail_routes  # noqa: E402
from filings.routers._redesign import funds as _funds_routes  # noqa: E402
from filings.routers._redesign import stock as _stock_routes  # noqa: E402
from filings.routers._redesign import home as _home_routes  # noqa: E402

router.include_router(_support_routes.router)
router.include_router(_profile_watchlist_routes.router)
router.include_router(_insiders_routes.router)
router.include_router(_notifications_routes.router)
router.include_router(_congress_routes.router)
router.include_router(_macro_routes.router)
router.include_router(_retail_routes.router)
router.include_router(_funds_routes.router)
router.include_router(_stock_routes.router)
router.include_router(_home_routes.router)

# Re-exports for ``web.py`` -- preserves the public surface so
# external imports of the form
#   from filings.routers.redesign_preview import X
# keep working unchanged after the per-feature split.
from filings.routers._redesign.stock import build_stock_data_bundle  # noqa: E402,F401
from filings.routers._redesign.home import (  # noqa: E402,F401
    warm_homepage_caches,
    warm_l2_caches,
)
