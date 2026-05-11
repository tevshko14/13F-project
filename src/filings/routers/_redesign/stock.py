"""Stock page (v2 redesign).

Two routes:

  * ``GET /stock/{ticker}``             -- full page (Overview / Financials /
                                            Ownership / Forecasts / Signals /
                                            Sentiment / News tabs)
  * ``GET /stock/{ticker}/chart/{period}`` -- partial: candlestick + volume
                                            SVG geometry for the range-chip
                                            click handler

Both share the heavy ``build_stock_data_bundle`` orchestrator (also
exported to ``web.py`` -- which imports it from this module via the
re-export in ``redesign_preview``).  Bundle data flows through the
stale-while-revalidate cache layer in ``stock_bundle``, so warm hits
are sub-200ms while cold bundles fan out 6 yfinance + Supabase calls
in parallel under a 30s SWR timeout.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse

from filings import stock_bundle
from filings.app_state import templates
from filings.cache_l2 import l2_cached as _l2_cached
from filings.concurrency import (
    fire_and_forget,
    is_heavy_saturated,
    to_heavy,
    to_light,
    to_supabase,
    to_upstream,
)
from filings.routers._redesign.helpers import (
    _compact_range_str,
    _maybe_rate_limit,
    _request_fund_cache,
    _shell_context,
)

logger = logging.getLogger(__name__)

router = APIRouter()


# ── SWR background refresh state (stock bundle write-through) ──

# Tickers currently being refreshed by an SWR background task.
# Used to debounce stampedes -- when 100 concurrent requests hit a
# stale ticker, we serve the stale bundle to all of them but only
# fire ONE bg refresh.  Mutated only on the asyncio loop.
_swr_refreshing: set[str] = set()

# Global cap on concurrent SWR background refreshes.  Each refresh
# fans out ~6 to_heavy calls inside `build_stock_data_bundle`; without
# a cap, a sustained crawler hitting many distinct stale tickers could
# pin the entire heavy-pool semaphore on bg work and starve organic
# request fanout.  Sized below the heavy-pool semaphore (8) so the
# warmer + organic to_heavy traffic always have headroom.
_SWR_MAX_CONCURRENT = 6

# Per-refresh timeout: caps how long any single SWR bg refresh can
# hold the per-ticker debounce flag.  Without this a hung upstream
# would leave the flag set indefinitely and the ticker would silently
# stop refreshing until MAX_STALE_AGE_S forced a sync rebuild.
_SWR_REFRESH_TIMEOUT_S = 30.0


def _track_bg(coro, *, name: str):
    """Spawn *coro* as a fire-and-forget task owned by this router.

    Thin wrapper around :func:`concurrency.fire_and_forget` with
    ``swallow=False`` -- bundle writebacks log their own errors
    internally, so we want exceptions to surface normally rather
    than be re-logged.
    """
    return fire_and_forget(coro, name=name, swallow=False)


async def _swr_refresh_bundle(
    ticker: str, fund_cache: dict, write_tier: str,
) -> None:
    """Background SWR refresh: rebuild the bundle and write it back.

    Errors are logged + swallowed; the user already got their stale
    response.  Bounded by `_SWR_REFRESH_TIMEOUT_S` so a hung upstream
    can't pin the per-ticker debounce flag forever.  ``finally``
    clears the flag so the next stale request can re-arm.
    """
    t_up = ticker.upper()
    try:
        async with asyncio.timeout(_SWR_REFRESH_TIMEOUT_S):
            bundle, source_status = await build_stock_data_bundle(fund_cache, ticker)
            await asyncio.to_thread(
                stock_bundle.set_bundle, ticker, bundle,
                tier=write_tier, source_status=source_status,
            )
    except (asyncio.TimeoutError, TimeoutError) as exc:
        logger.debug("SWR bg refresh timed out for %s: %s", ticker, exc)
    except Exception as exc:
        logger.debug("SWR bg refresh failed for %s: %s", ticker, exc)
    finally:
        _swr_refreshing.discard(t_up)



# ─────────────────────────────────────────────────────────────────────────────
# STOCK — Overview (default tab) is wired to real OHLCV + quote data.
# Financials / Ownership / Forecasts / Signals are wired alongside.
# (The Vitals tab and its `_stock_build_vitals` helper are kept in the file
# but unreferenced from the route — restore by re-adding the segment entry
# and the gather-call when ready.)
# ─────────────────────────────────────────────────────────────────────────────


def _stock_format_price(v: float | None) -> str:
    if v is None:
        return "—"
    return f"{v:,.2f}"


def _stock_format_volume(v: float | None) -> str:
    if v is None:
        return "—"
    v = float(v)
    if v >= 1e9:
        return f"{v / 1e9:.2f}B"
    if v >= 1e6:
        return f"{v / 1e6:.1f}M"
    if v >= 1e3:
        return f"{v / 1e3:.0f}K"
    return f"{int(v):,}"


def _stock_format_mcap(v: int | float | None) -> str:
    if not v:
        return "—"
    a = abs(float(v))
    if a >= 1e12: return f"${a / 1e12:.2f}T"
    if a >= 1e9:  return f"${a / 1e9:.1f}B"
    if a >= 1e6:  return f"${a / 1e6:.0f}M"
    return f"${a:,.0f}"


def _stock_format_pe(v: float | None) -> str:
    if v is None or v <= 0:
        return "—"
    return f"{v:.1f}"


def _stock_format_employees(n: int | float | None) -> str:
    if not n:
        return "—"
    try:
        return f"{int(n):,}"
    except (TypeError, ValueError):
        return "—"


def _stock_format_hq(info: dict, finnhub_profile: dict) -> str:
    parts = [info.get("city"), info.get("state"), info.get("country")]
    parts = [p for p in parts if p]
    if parts:
        return ", ".join(parts)
    fh_country = (finnhub_profile or {}).get("country")
    return fh_country or "—"


def _stock_format_ipo(info: dict, finnhub_profile: dict) -> str:
    """Format IPO date — yfinance gives an epoch, Finnhub gives "YYYY-MM-DD"."""
    from datetime import datetime, timezone

    epoch = info.get("firstTradeDateEpochUtc") or info.get("firstTradeDateMilliseconds")
    if epoch:
        try:
            ts = float(epoch)
            if ts > 1e11:  # millis
                ts /= 1000
            return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%b %d %Y")
        except (TypeError, ValueError, OSError):
            pass
    fh_ipo = (finnhub_profile or {}).get("ipo")
    if fh_ipo:
        try:
            return datetime.strptime(fh_ipo, "%Y-%m-%d").strftime("%b %d %Y")
        except (TypeError, ValueError):
            return str(fh_ipo)
    return "—"


def _stock_resolve_logo(info: dict, finnhub_profile: dict) -> str:
    """Pick a company-logo URL.

    Order of preference:
    1. yfinance ``website`` → ``https://logo.clearbit.com/<domain>`` (sharp,
       transparent PNG).
    2. Finnhub ``logo`` URL (always available when profile2 returned).
    3. ``""`` — caller should fall back to the ticker badge.
    """
    from urllib.parse import urlparse
    website = (info.get("website") or "").strip()
    if website:
        parsed = urlparse(website if "://" in website else f"https://{website}")
        domain = parsed.netloc or parsed.path
        if domain.startswith("www."):
            domain = domain[4:]
        if domain:
            return f"https://logo.clearbit.com/{domain}"
    return (finnhub_profile or {}).get("logo") or ""


def _stock_format_website(info: dict, finnhub_profile: dict) -> str:
    """Strip protocol/www so the panel shows just the bare domain."""
    from urllib.parse import urlparse
    url = (info.get("website") or (finnhub_profile or {}).get("weburl") or "").strip()
    if not url:
        return "—"
    parsed = urlparse(url if "://" in url else f"https://{url}")
    domain = parsed.netloc or parsed.path
    return domain[4:] if domain.startswith("www.") else domain or "—"


def _stock_extract_ceo(info: dict) -> str:
    """Find the CEO entry in yfinance ``companyOfficers``.  Falls back to the
    first listed officer if no title contains "CEO" (common for Berkshire-style
    holdings that list executives without a CEO line)."""
    officers = info.get("companyOfficers") or []
    for o in officers:
        title = (o.get("title") or "").lower()
        if "chief executive" in title or "ceo" in title:
            return o.get("name") or "—"
    if officers and officers[0].get("name"):
        return officers[0]["name"]
    return "—"


def _stock_truncate_blurb(text: str | None, max_len: int = 280) -> str:
    if not text:
        return ""
    text = text.strip()
    if len(text) <= max_len:
        return text
    cut = text[:max_len].rsplit(" ", 1)[0]
    return cut + "…"


# yfinance returns short codes (NMS, NYQ, …); map to display labels.
_EXCHANGE_LABELS = {
    "NMS": "NASDAQ", "NGM": "NASDAQ", "NCM": "NASDAQ",
    "NYQ": "NYSE",   "NYE": "NYSE",
    "ASE": "AMEX",   "PCX": "NYSE Arca",
    "BATS": "BATS",  "CXI": "CBOE",
}


# ─────────────────────────────────────────────────────────────────────────────
# STOCK PAGE — secondary-tab mock data.  Mirrors design_handoff stock-tab-*.jsx
# fixtures.  Used as visual scaffolding while the real wiring lands tab by tab.
# ─────────────────────────────────────────────────────────────────────────────

_STOCK_MOCK_FIN_KPIS = [
    {"label": "Revenue (TTM)",    "value": "$158.2B", "delta": "+109% YoY", "up": True},
    {"label": "Net Income (TTM)", "value": "$88.4B",  "delta": "+121% YoY", "up": True},
    {"label": "FCF (TTM)",        "value": "$76.0B",  "delta": "+24% QoQ",  "up": True},
    {"label": "Gross Margin",     "value": "75.7%",   "delta": "+0.7pp",    "up": True},
]

_STOCK_OWN_FUNDS = [
    {"rank": "01", "fund": "Vanguard Group",      "manager": "—",                  "shares": "212.4M", "value": "$30.2B",  "port": "4.1%",  "chg":  0.012},
    {"rank": "02", "fund": "BlackRock Inc.",      "manager": "—",                  "shares": "189.7M", "value": "$26.9B",  "port": "3.8%",  "chg":  0.024},
    {"rank": "03", "fund": "Berkshire Hathaway",  "manager": "Warren Buffett",     "shares":  "32.1M", "value":  "$4.6B",  "port": "0.9%",  "chg":  0.084},
    {"rank": "04", "fund": "Pershing Square",     "manager": "Bill Ackman",        "shares":  "18.4M", "value":  "$2.6B",  "port": "22.4%", "chg":  0.214},
    {"rank": "05", "fund": "Coatue Management",   "manager": "Philippe Laffont",   "shares":  "12.9M", "value":  "$1.8B",  "port": "14.2%", "chg":  0.0  },
    {"rank": "06", "fund": "Tiger Global",        "manager": "Chase Coleman",      "shares":  "11.2M", "value":  "$1.6B",  "port": "11.4%", "chg": -0.041},
    {"rank": "07", "fund": "Citadel Advisors",    "manager": "Ken Griffin",        "shares":   "9.4M", "value":  "$1.3B",  "port": "1.8%",  "chg":  1.0  },
    {"rank": "08", "fund": "Bridgewater",         "manager": "Ray Dalio",          "shares":   "6.8M", "value":  "$0.97B", "port": "4.4%",  "chg": -0.094},
    {"rank": "09", "fund": "Third Point",         "manager": "Daniel Loeb",        "shares":   "5.2M", "value":  "$0.74B", "port": "12.6%", "chg":  0.318},
    {"rank": "10", "fund": "Appaloosa",           "manager": "David Tepper",       "shares":   "4.1M", "value":  "$0.58B", "port": "9.4%",  "chg":  0.158},
]

_STOCK_OWN_CONGRESS = [
    {"person": "Pelosi, Nancy",    "party": "D", "chamber": "House",  "action": "BUY",  "size": "$1M–5M",    "date": "Apr 28, 2026", "days":  4},
    {"person": "Tuberville, T.",   "party": "R", "chamber": "Senate", "action": "BUY",  "size": "$50K–100K", "date": "Apr 27, 2026", "days":  5},
    {"person": "Crenshaw, Dan",    "party": "R", "chamber": "House",  "action": "BUY",  "size": "$15K–50K",  "date": "Apr 26, 2026", "days":  6},
    {"person": "Khanna, Ro",       "party": "D", "chamber": "House",  "action": "BUY",  "size": "$15K–50K",  "date": "Apr 18, 2026", "days": 14},
    {"person": "Bresnahan, Rob",   "party": "R", "chamber": "House",  "action": "BUY",  "size": "$1K–15K",   "date": "Apr 12, 2026", "days": 20},
    {"person": "Marshall, Roger",  "party": "R", "chamber": "Senate", "action": "SELL", "size": "$15K–50K",  "date": "Mar 28, 2026", "days": 34},
]

_STOCK_OWN_INSIDERS = [
    {"person": "Huang, Jen-Hsun", "role": "CEO",                  "action": "SELL", "shares": "120,000", "value": "$17.0M", "date": "Apr 28, 2026", "remaining": "877.5M"},
    {"person": "Kress, Colette",  "role": "CFO",                  "action": "SELL", "shares":  "25,000", "value":  "$3.6M", "date": "Apr 25, 2026", "remaining":   "4.2M"},
    {"person": "Dally, William",  "role": "Chief Scientist",      "action": "SELL", "shares":  "18,500", "value":  "$2.6M", "date": "Apr 22, 2026", "remaining":   "1.8M"},
    {"person": "Coxe, Tench",     "role": "Director",             "action": "SELL", "shares":  "50,000", "value":  "$7.1M", "date": "Apr 18, 2026", "remaining":   "0.8M"},
    {"person": "Perry, Aarti",    "role": "Chief People Officer", "action": "SELL", "shares":   "4,200", "value":  "$0.6M", "date": "Apr 14, 2026", "remaining":   "0.2M"},
]

_STOCK_OWN_FILINGS = [
    {"type": "10-Q",    "filed": "Feb 26, 2026", "period": "Q4 2025",            "desc": "Quarterly report",                                "size": "4.2 MB"},
    {"type": "8-K",     "filed": "Feb 26, 2026", "period": "Earnings release",   "desc": "Q4 2025 results: $39.3B revenue, +78% YoY",        "size": "612 KB"},
    {"type": "DEF 14A", "filed": "Feb 03, 2026", "period": "Proxy 2026",         "desc": "Annual meeting · executive comp",                  "size": "2.1 MB"},
    {"type": "10-K",    "filed": "Feb 21, 2025", "period": "FY 2025",            "desc": "Annual report",                                    "size": "6.8 MB"},
    {"type": "4",       "filed": "Apr 28, 2026", "period": "Insider — Huang J.", "desc": "Sale 120,000 sh",                                  "size": "38 KB"},
    {"type": "4",       "filed": "Apr 25, 2026", "period": "Insider — Kress C.", "desc": "Sale 25,000 sh",                                   "size": "36 KB"},
    {"type": "13F-HR",  "filed": "Apr 15, 2026", "period": "Holders Q1 2026",    "desc": "87 institutional holders",                         "size": "—"},
]

_STOCK_FCT_RATINGS = [
    ("Buy",         24, "var(--pp-up)"),
    ("Overweight",  18, "var(--pp-up)"),
    ("Hold",         7, "var(--pp-accent)"),
    ("Underweight",  2, "var(--pp-down)"),
    ("Sell",         1, "var(--pp-down)"),
]

_STOCK_FCT_ANALYSTS = [
    {"firm": "Morgan Stanley", "analyst": "Joseph Moore",  "rating": "OVERWEIGHT", "target": 200, "prev": 170, "date": "Apr 28"},
    {"firm": "Goldman Sachs",  "analyst": "Toshiya Hari",  "rating": "BUY",        "target": 185, "prev": 185, "date": "Apr 24"},
    {"firm": "BofA",           "analyst": "Vivek Arya",    "rating": "BUY",        "target": 190, "prev": 165, "date": "Apr 22"},
    {"firm": "Wells Fargo",    "analyst": "Aaron Rakers",  "rating": "OVERWEIGHT", "target": 175, "prev": 150, "date": "Apr 18"},
    {"firm": "Barclays",       "analyst": "Tom O'Malley",  "rating": "OVERWEIGHT", "target": 160, "prev": 160, "date": "Apr 15"},
    {"firm": "Citi",           "analyst": "Atif Malik",    "rating": "BUY",        "target": 180, "prev": 170, "date": "Apr 12"},
    {"firm": "Mizuho",         "analyst": "Vijay Rakesh",  "rating": "BUY",        "target": 165, "prev": 140, "date": "Apr 08"},
    {"firm": "HSBC",           "analyst": "Frank Lee",     "rating": "HOLD",       "target": 120, "prev": 120, "date": "Apr 04"},
    {"firm": "DZ Bank",        "analyst": "Ingo Wermann",  "rating": "SELL",       "target": 100, "prev": 110, "date": "Mar 28"},
]

_STOCK_FCT_REV_EST = [
    {"period": "Q1 2026", "est": "$43.5B",  "low": "$41.2B",  "high": "$46.0B",  "yoy": "+78%", "is_total": False},
    {"period": "Q2 2026", "est": "$48.2B",  "low": "$45.8B",  "high": "$51.6B",  "yoy": "+62%", "is_total": False},
    {"period": "Q3 2026", "est": "$52.4B",  "low": "$48.1B",  "high": "$56.2B",  "yoy": "+54%", "is_total": False},
    {"period": "FY2026",  "est": "$182.4B", "low": "$172.5B", "high": "$198.0B", "yoy": "+40%", "is_total": True},
]

_STOCK_FCT_EPS_REVISIONS = [29.4, 30.1, 31.2, 32.4, 33.1, 33.8, 34.5, 35.2, 35.8, 36.4, 36.9, 37.4]

_STOCK_SIGNALS = [
    {"name": "Insider activity",   "score": -0.4, "weight": "high",   "detail": "5 SELL Form 4s · 0 BUYs · 6mo",                            "verdict": "BEARISH"},
    {"name": "Congressional flow", "score": +0.7, "weight": "medium", "detail": "5 BUYs · 1 SELL · 90d (Pelosi $1-5M Apr 28)",              "verdict": "BULLISH"},
    {"name": "13F adds vs. cuts",  "score": +0.5, "weight": "high",   "detail": "42 funds added · 18 cut · Q1 2026",                        "verdict": "BULLISH"},
    {"name": "Analyst revisions",  "score": +0.8, "weight": "high",   "detail": "+27% EPS revision over 90d · 34 upgrades",                 "verdict": "BULLISH"},
    {"name": "Retail sentiment",   "score": +0.6, "weight": "low",    "detail": "WSB +71 · 2,891 mentions · #3 most discussed",             "verdict": "BULLISH"},
    {"name": "Price momentum",     "score": +0.4, "weight": "medium", "detail": "50d > 200d MA · RSI 64 · uptrend intact",                  "verdict": "BULLISH"},
    {"name": "Valuation",          "score": -0.3, "weight": "medium", "detail": "P/E 68.4 vs sector 32.1 · PEG 1.4",                        "verdict": "CAUTION"},
    {"name": "Short interest",     "score": +0.1, "weight": "low",    "detail": "1.2% of float · stable WoW",                               "verdict": "NEUTRAL"},
]

_STOCK_VITALS_DIVS = [
    {"ex": "Mar 12, 2026", "pay": "Apr 02, 2026", "amount": "$0.01", "yield": "0.03%"},
    {"ex": "Dec 11, 2025", "pay": "Jan 02, 2026", "amount": "$0.01", "yield": "0.03%"},
    {"ex": "Sep 12, 2025", "pay": "Oct 02, 2025", "amount": "$0.01", "yield": "0.03%"},
    {"ex": "Jun 12, 2025", "pay": "Jul 03, 2025", "amount": "$0.01", "yield": "0.03%"},
]

_STOCK_VITALS_EVENTS = [
    {"date": "May 21", "event": "Q1 2026 Earnings",  "when": "After close",      "est": "EPS $1.01 · Rev $43.5B"},
    {"date": "Jun 12", "event": "Ex-dividend $0.01", "when": "Pay Jul 03",        "est": "Yield 0.03%"},
    {"date": "Jun 25", "event": "Annual meeting",    "when": "Webcast 11:00 PT", "est": "Proxy items: 5"},
    {"date": "Aug 27", "event": "Q2 2026 Earnings",  "when": "After close",      "est": "EPS $1.18 · Rev $48.2B (consensus)"},
]

_STOCK_VITALS_PEERS = [
    {"ticker": "AMD",  "name": "Advanced Micro Devices", "price": "172.41", "chg":  0.022, "pe": "42.1", "mc": "$278B"},
    {"ticker": "AVGO", "name": "Broadcom Inc.",          "price": "218.92", "chg":  0.018, "pe": "38.4", "mc": "$1.02T"},
    {"ticker": "INTC", "name": "Intel Corp.",            "price":  "21.84", "chg": -0.012, "pe": "—",   "mc": "$94B"},
    {"ticker": "TSM",  "name": "Taiwan Semi",            "price": "218.10", "chg":  0.014, "pe": "32.8", "mc": "$1.13T"},
    {"ticker": "MRVL", "name": "Marvell Technology",     "price": "112.32", "chg":  0.031, "pe": "68.2", "mc": "$98B"},
]

_STOCK_FCT_TARGET_BAND = {"low": 80, "high": 220}
_STOCK_FCT_PRICE_PCT = (142.18 - 80) / (220 - 80) * 100  # current price tick

_STOCK_VITALS_SHARE_STATS = [
    ("Shares outstanding",  "24.66B"),
    ("Float",               "24.06B"),
    ("Insider holdings",    "3.9%"),
    ("Institutional own.",  "65.8%"),
    ("Short interest",      "1.2%"),
    ("Days to cover",       "0.4"),
    ("Beta (5Y)",           "1.74"),
    ("52-week change",      "+58.2%"),
    ("Last split",          "10:1 (Jun 2024)"),
]


def _stock_line_path(values: list[float], y_min: float, y_max: float,
                     vb_w: int = 500, vb_h: int = 180,
                     pad_left: int = 20, pad_right: int = 20,
                     pad_top: int = 10, pad_bottom: int = 10) -> str:
    """Build an SVG path 'M x y L x y …' from a list of values, stretching
    them across the given viewBox.  Used by the margin-trend + EPS-revision
    charts on the Forecasts/Financials tabs."""
    if not values:
        return ""
    n = len(values)
    inner_w = vb_w - pad_left - pad_right
    inner_h = vb_h - pad_top - pad_bottom
    span = max(y_max - y_min, 1e-6)

    def _x(i: int) -> float:
        return pad_left + (i / max(n - 1, 1)) * inner_w

    def _y(v: float) -> float:
        return pad_top + inner_h - ((v - y_min) / span) * inner_h

    parts = [
        f"{'M' if i == 0 else 'L'}{_x(i):.1f} {_y(v):.1f}"
        for i, v in enumerate(values)
    ]
    return " ".join(parts)


def _stock_chart_points(values: list[float], y_min: float, y_max: float,
                        vb_w: int = 500, vb_h: int = 180,
                        pad_left: int = 20, pad_right: int = 20,
                        pad_top: int = 10, pad_bottom: int = 10) -> list[dict]:
    """Companion to _stock_line_path — emits ``[{x, y}, …]`` so the template
    can render dots at each point of the line."""
    if not values:
        return []
    n = len(values)
    inner_w = vb_w - pad_left - pad_right
    inner_h = vb_h - pad_top - pad_bottom
    span = max(y_max - y_min, 1e-6)
    return [
        {
            "x": round(pad_left + (i / max(n - 1, 1)) * inner_w, 1),
            "y": round(pad_top + inner_h - ((v - y_min) / span) * inner_h, 1),
        }
        for i, v in enumerate(values)
    ]


_RATING_BUCKETS = [
    ("Buy",          {"buy", "strong buy", "strongbuy", "outperform", "overweight",
                      "positive", "accumulate", "sector outperform", "market outperform",
                      "top pick", "conviction buy"},                                 "var(--pp-up)"),
    ("Overweight",   {"overweight"},                                                  "var(--pp-up)"),
    ("Hold",         {"hold", "neutral", "equal-weight", "equal weight", "equalweight",
                      "market perform", "sector perform", "in-line", "inline",
                      "peer perform", "sector weight"},                              "var(--pp-accent)"),
    ("Underweight",  {"underweight"},                                                 "var(--pp-down)"),
    ("Sell",         {"sell", "strong sell", "strongsell", "underperform",
                      "negative", "reduce", "sector underperform",
                      "market underperform"},                                        "var(--pp-down)"),
]


def _stock_build_forecasts(ticker: str, current_price: float | None) -> dict:
    """Real analyst consensus + ratings + price targets + earnings + estimates.

    Fans out across:
      - ``analysts.get_analyst_ratings`` — bucketed counts, consensus,
        per-firm full rating history (for the Analysts pane expandables)
      - ``earnings.get_earnings_data`` — quarterly EPS history + streak
        scorecard for the Earnings pane
      - ``earnings.get_forward_estimates`` — EPS + Revenue forward
        analyst estimates for the Estimates pane

    All wrapped in best-effort try/except so a single source failure
    can't blank out the whole tab.  The Analysts pane retains the v2's
    bucket/consensus/target-distribution shape for backwards compat —
    new fields (``grouped``, ``earnings``, ``est``) are additive.
    """
    from datetime import datetime
    from filings import analysts, earnings as earn

    try:
        ratings_objs = analysts.get_analyst_ratings(ticker) or []
    except Exception as exc:
        logger.warning("get_analyst_ratings(%s) failed: %s", ticker, exc)
        ratings_objs = []

    # Earnings + estimates are independent — pull both even if ratings empty.
    try:
        earn_data = earn.get_earnings_data(ticker)
    except Exception as exc:
        logger.warning("get_earnings_data(%s) failed: %s", ticker, exc)
        earn_data = {}
    try:
        est_data = earn.get_forward_estimates(ticker)
    except Exception as exc:
        logger.warning("get_forward_estimates(%s) failed: %s", ticker, exc)
        est_data = {}

    if not ratings_objs:
        return {"has_data": False,
                "ratings":      _STOCK_FCT_RATINGS,
                "ratings_total": sum(r[1] for r in _STOCK_FCT_RATINGS),
                "analysts":     _STOCK_FCT_ANALYSTS,
                "rev_est":      _STOCK_FCT_REV_EST,
                "eps":          _STOCK_FCT_EPS_REVISIONS,
                "target_band":  _STOCK_FCT_TARGET_BAND,
                "now_pct":      _STOCK_FCT_PRICE_PCT,
                "consensus":    "BUY",
                "grouped":      [],
                "current_price": current_price,
                "earnings":     earn_data or {},
                "est_eps":      (est_data or {}).get("eps") or [],
                "est_revenue":  (est_data or {}).get("revenue") or []}

    # ── Firm-grouped FULL rating history (for the Analysts pane expandables).
    # Built from the un-deduped ratings_objs so each firm's group carries
    # its complete rating history.  ratings_objs arrives date-desc, so the
    # first encounter per firm is the most-recent rating.
    firm_groups: dict[str, dict] = {}
    firm_order: list[str] = []
    for r in ratings_objs:
        firm_key = (r.firm or "").strip().lower() or "unknown"
        if firm_key not in firm_groups:
            firm_groups[firm_key] = {
                "firm":    r.firm or "Unknown",
                "latest":  r,
                "ratings": [],
            }
            firm_order.append(firm_key)
        firm_groups[firm_key]["ratings"].append(r)

    grouped: list[dict] = []
    for k in firm_order:
        g = firm_groups[k]
        latest = g["latest"]
        # Action label: latest.action is "upgrade"/"downgrade"/"maintain"/"init".
        action_label = (latest.action or "").lower()
        action_disp = {
            "upgrade":   "Upgrade",
            "downgrade": "Downgrade",
            "maintain":  "Maintain",
            "init":      "Initiated",
            "reiterate": "Reiterate",
        }.get(action_label, latest.action.title() if latest.action else "")

        history: list[dict] = []
        for r in g["ratings"]:
            try:
                d_str = datetime.strptime((r.date or "")[:10], "%Y-%m-%d").strftime("%Y-%m-%d")
            except (TypeError, ValueError):
                d_str = (r.date or "")[:10]
            r_action = (r.action or "").lower()
            history.append({
                "action":  {
                    "upgrade":   "Upgrade",
                    "downgrade": "Downgrade",
                    "maintain":  "Maintain",
                    "init":      "Initiated",
                    "reiterate": "Reiterate",
                }.get(r_action, r.action.title() if r.action else ""),
                "action_kind": r_action,
                "from_grade":  r.from_grade or "",
                "to_grade":    r.to_grade or "",
                "target":      r.current_price_target,
                "prev_target": r.prior_price_target,
                "date":        d_str,
            })

        grouped.append({
            "firm":          g["firm"],
            "latest_action": action_disp,
            "latest_kind":   action_label,
            "latest_grade":  latest.to_grade or "",
            "latest_from":   latest.from_grade or "",
            "latest_target": latest.current_price_target,
            "latest_prev":   latest.prior_price_target,
            "latest_date":   (latest.date or "")[:10],
            "ratings_count": len(g["ratings"]),
            "history":       history,
        })

    # Dedupe by firm — keep the most-recent rating per firm (ratings_objs
    # already arrives sorted date-desc from get_analyst_ratings).
    seen_firms: set[str] = set()
    deduped: list = []
    for r in ratings_objs:
        firm_key = (r.firm or "").strip().lower()
        if not firm_key or firm_key in seen_firms:
            continue
        seen_firms.add(firm_key)
        deduped.append(r)
    ratings_objs = deduped

    # ── Bucket counts ──
    bucket_counts = {b[0]: 0 for b in _RATING_BUCKETS}
    for r in ratings_objs:
        grade = (r.to_grade or "").lower().strip()
        for label, grades, _ in _RATING_BUCKETS:
            # Skip the broad "Buy" bucket if grade matches the narrower
            # "Overweight" bucket so each rating lands in exactly one row.
            if label == "Buy" and grade == "overweight":
                continue
            if grade in grades:
                bucket_counts[label] += 1
                break

    ratings_dist = [
        (label, bucket_counts[label], color)
        for label, _, color in _RATING_BUCKETS
        if bucket_counts[label] > 0
    ]
    ratings_total = sum(c for _, c, _ in ratings_dist) or 1

    # ── Consensus label ──
    rating_dicts = [
        {"to_grade": r.to_grade, "firm": r.firm, "price_target": r.current_price_target}
        for r in ratings_objs
    ]
    consensus = analysts.get_consensus_summary_from_raw(rating_dicts, "firm")
    consensus_label = (consensus.get("consensus_label") or "Hold").upper().replace(" ", " ")

    # ── Price target band + current-price tick ──
    targets = [r.current_price_target for r in ratings_objs if r.current_price_target]
    if targets:
        low, high = min(targets), max(targets)
        if low == high:
            low, high = low * 0.9, high * 1.1
        target_band = {"low": round(low, 2), "high": round(high, 2)}
    else:
        target_band = _STOCK_FCT_TARGET_BAND
    span = target_band["high"] - target_band["low"] or 1
    now_pct = round(((current_price or target_band["low"]) - target_band["low"]) / span * 100, 1)
    now_pct = max(0.0, min(100.0, now_pct))

    # ── Recent analyst actions (top 9 by date desc) ──
    sorted_ratings = sorted(ratings_objs, key=lambda r: r.date or "", reverse=True)
    analyst_rows: list[dict] = []
    for r in sorted_ratings[:9]:
        try:
            d = datetime.strptime((r.date or "")[:10], "%Y-%m-%d").strftime("%b %d")
        except (TypeError, ValueError):
            d = (r.date or "")[:10]
        target = r.current_price_target or 0
        prev = r.prior_price_target if r.prior_price_target is not None else target
        analyst_rows.append({
            "firm":     r.firm or "—",
            "analyst":  r.firm or "—",  # firm-level view doesn't carry analyst name
            "rating":   (r.to_grade or "").upper() or "—",
            "target":   round(target, 2),
            "prev":     round(prev, 2),
            "date":     d,
        })

    return {
        "has_data":       True,
        "ratings":        ratings_dist,
        "ratings_total":  ratings_total,
        "analysts":       analyst_rows,
        # The bottom-of-pane mock panels were removed in favour of the
        # Estimates sub-pane (real EPS + revenue analyst tables).  These
        # fields stay for the Forecasts header / chart math — kept on
        # mock fixtures so consumers (e.g. ``_stock_compute_signals``)
        # don't break, but they're no longer rendered as panels.
        "rev_est":        _STOCK_FCT_REV_EST,
        "eps":            _STOCK_FCT_EPS_REVISIONS,
        "target_band":    target_band,
        "now_pct":        now_pct,
        "consensus":      consensus_label,
        # ── New for the 3-pane redesign ──
        "grouped":        grouped,
        "current_price":  current_price,
        "earnings":       earn_data or {},
        "est_eps":        (est_data or {}).get("eps") or [],
        "est_revenue":    (est_data or {}).get("revenue") or [],
    }


def _stock_compute_signals(
    *,
    sentiment: dict,
    ownership_funds: dict,
    ownership_insiders: dict,
    ownership_congress: dict,
    forecasts: dict,
    payload: dict,
    meta: dict,
    short_interest: dict | None = None,
) -> dict:
    """Synthesize the 8-row Signals table from already-fetched context.

    Each signal returns a normalized score in ``[-1, 1]`` plus a verdict
    string ("BULLISH" / "BEARISH" / "NEUTRAL" / "CAUTION") and a one-line
    detail string for the table cell.  The composite header above the
    table is the weight-averaged signed score across all rows that have
    enough data to score (rows with ``score=None`` are skipped from the
    average and rendered as NEUTRAL).
    """
    def _verdict(score: float, *, neutral_band: float = 0.15) -> str:
        if score is None: return "NEUTRAL"
        if score >  neutral_band: return "BULLISH"
        if score < -neutral_band: return "BEARISH"
        return "NEUTRAL"

    rows: list[dict] = []

    # 1) Insider activity — aggregate Form 4 buys vs sells across insiders.
    ins = ownership_insiders.get("insiders") or []
    buys  = sum(int(p.get("buys")  or 0) for p in ins)
    sells = sum(int(p.get("sells") or 0) for p in ins)
    total = buys + sells
    if total:
        score = (buys - sells) / total
        detail = f"{sells} sells · {buys} buys · {len(ins)} insiders"
    else:
        score, detail = None, "No recent Form 4 activity"
    rows.append({"name": "Insider activity", "score": score, "weight": "high",
                 "detail": detail, "verdict": _verdict(score) if score is not None else "NEUTRAL"})

    # 2) Congressional flow.
    cg = ownership_congress.get("rows") or []
    cg_buys  = sum(1 for r in cg if r.get("action") == "BUY")
    cg_sells = sum(1 for r in cg if r.get("action") == "SELL")
    cg_total = cg_buys + cg_sells
    if cg_total:
        score = (cg_buys - cg_sells) / cg_total
        detail = f"{cg_buys} BUYs · {cg_sells} SELLs"
    else:
        score, detail = None, "No congressional trades"
    rows.append({"name": "Congressional flow", "score": score, "weight": "medium",
                 "detail": detail, "verdict": _verdict(score) if score is not None else "NEUTRAL"})

    # 3) 13F adds vs cuts — counts of NEW/ADD vs REDUCE/SOLD across funds.
    funds = ownership_funds.get("rows") or []
    adds  = sum(1 for r in funds if r.get("chg") and r["chg"] > 0)
    cuts  = sum(1 for r in funds if r.get("chg") and r["chg"] < 0)
    f_total = adds + cuts
    if f_total:
        score = (adds - cuts) / f_total
        detail = f"{adds} funds added · {cuts} cut · top 10"
    else:
        score, detail = None, f"{ownership_funds.get('total_count', 0)} institutional holders"
    rows.append({"name": "13F adds vs. cuts", "score": score, "weight": "high",
                 "detail": detail, "verdict": _verdict(score) if score is not None else "NEUTRAL"})

    # 4) Analyst revisions — ratio of buy-leaning to sell-leaning grades.
    if forecasts.get("has_data"):
        ratings = forecasts.get("ratings") or []
        bull = sum(c for label, c, _ in ratings if label in ("Buy", "Overweight"))
        bear = sum(c for label, c, _ in ratings if label in ("Sell", "Underweight"))
        denom = bull + bear + sum(c for label, c, _ in ratings if label == "Hold")
        if denom:
            score = (bull - bear) / denom
            detail = f"{bull} bullish · {bear} bearish · {forecasts.get('consensus','')}"
        else:
            score, detail = None, "No analyst coverage"
    else:
        score, detail = None, "No analyst coverage"
    rows.append({"name": "Analyst revisions", "score": score, "weight": "high",
                 "detail": detail, "verdict": _verdict(score) if score is not None else "NEUTRAL"})

    # 5) Retail sentiment — already a -100..100 score we normalize.
    if sentiment.get("has_data"):
        s_score = sentiment["score"] / 100
        detail = f"WSB {sentiment['score_str']} · {sentiment['mentions']} mentions"
        rank_str = sentiment.get("rank_str") or ""
        if rank_str:
            detail += f" · {rank_str}"
    else:
        s_score, detail = None, "Not trending on r/WallStreetBets"
    rows.append({"name": "Retail sentiment", "score": s_score, "weight": "low",
                 "detail": detail, "verdict": _verdict(s_score) if s_score is not None else "NEUTRAL"})

    # 6) Price momentum — 50d vs 200d MA on the cached candles.
    candles = payload.get("candles") or []
    p_score, p_detail = None, "Not enough price history"
    if len(candles) >= 200:
        closes = [c[4] for c in candles if len(c) >= 5 and c[4] is not None]
        if len(closes) >= 200:
            ma50  = sum(closes[-50:])  / 50
            ma200 = sum(closes[-200:]) / 200
            spread = (ma50 - ma200) / ma200 if ma200 else 0
            p_score = max(-1.0, min(1.0, spread * 4))  # ±25% spread maps to ±1.0
            arrow = "▲" if ma50 > ma200 else "▼"
            p_detail = f"50d {arrow} 200d MA · spread {spread*100:+.1f}%"
    rows.append({"name": "Price momentum", "score": p_score, "weight": "medium",
                 "detail": p_detail, "verdict": _verdict(p_score) if p_score is not None else "NEUTRAL"})

    # 7) Valuation — P/E inversion (high P/E = bearish signal).
    pe_str = meta.get("pe") or "—"
    v_score, v_detail = None, "P/E unavailable"
    try:
        pe_val = float(pe_str)
        # 0 P/E (loss) = strong bearish; <15 cheap; 15-30 fair; >50 stretched.
        if pe_val <= 0:
            v_score = -0.6; v_detail = f"P/E {pe_val:.1f} (loss-making)"
        elif pe_val < 15:
            v_score = +0.5; v_detail = f"P/E {pe_val:.1f} (cheap)"
        elif pe_val < 30:
            v_score = +0.2; v_detail = f"P/E {pe_val:.1f} (fair)"
        elif pe_val < 50:
            v_score = -0.1; v_detail = f"P/E {pe_val:.1f} (premium)"
        else:
            v_score = -0.3; v_detail = f"P/E {pe_val:.1f} (stretched)"
    except (TypeError, ValueError):
        pass
    v_verdict = "CAUTION" if v_score is not None and -0.3 <= v_score < 0 else _verdict(v_score)
    rows.append({"name": "Valuation", "score": v_score, "weight": "medium",
                 "detail": v_detail,
                 "verdict": v_verdict if v_score is not None else "NEUTRAL"})

    # 8) Short interest — high short = bearish; low = bullish.
    si_str = ""
    si_score = None
    pct_float = (short_interest or {}).get("short_pct_float")
    if isinstance(pct_float, (int, float)) and pct_float >= 0:
        pct = pct_float * 100
        si_str = f"{pct:.1f}%"
        # >5% = bearish, <2% = bullish
        if pct < 2:    si_score = +0.3
        elif pct < 5:  si_score = +0.1
        elif pct < 10: si_score = -0.2
        else:          si_score = -0.5
    si_detail = f"{si_str} of float" if si_str else "Short interest unavailable"
    rows.append({"name": "Short interest", "score": si_score, "weight": "low",
                 "detail": si_detail,
                 "verdict": _verdict(si_score) if si_score is not None else "NEUTRAL"})

    # Replace None scores with 0.0 so the template's |sum / |abs filters
    # don't blow up when a signal had no data to score on.
    for r in rows:
        if r["score"] is None:
            r["score"] = 0.0

    # Composite — weight-average over scored rows.
    weights = {"high": 1.5, "medium": 1.0, "low": 0.5}
    num, den = 0.0, 0.0
    for r in rows:
        if r["score"] is None: continue
        w = weights.get(r["weight"], 1.0)
        num += r["score"] * w
        den += w
    composite = num / den if den else 0.0
    if   composite >=  0.4: composite_label = "STRONG BULLISH"
    elif composite >=  0.15: composite_label = "BULLISH"
    elif composite > -0.15: composite_label = "NEUTRAL"
    elif composite > -0.4:  composite_label = "BEARISH"
    else:                   composite_label = "STRONG BEARISH"

    return {"signals": rows, "composite": composite_label,
            "composite_score": round(composite, 2)}


async def _stock_build_signals_data(ticker: str) -> dict:
    """Fan out the auxiliary Signals-tab data sources in parallel.

    Powers the Sentiment + Short Interest sub-panes (the composite
    8-row table is computed separately in ``_stock_compute_signals``):

      - ``sentiment.get_sentiment_data`` — CNN F&G / Finnhub bullish-pct /
        ApeWisdom mention velocity / AlphaVantage news sentiment / short
        interest (latest snapshot + history)
      - ``google_trends.get_trends_summary`` — categorised keywords +
        12-month trend sparkline
      - ``web_traffic.get_web_traffic_data`` — Tranco rank + Wikipedia
        page-view event detector
    """
    t_up = ticker.upper()

    async def _sentiment() -> dict:
        try:
            from filings import sentiment
            return await to_heavy(sentiment.get_sentiment_data, t_up) or {}
        except Exception as exc:
            logger.debug("sentiment.get_sentiment_data(%s) failed: %s", ticker, exc)
            return {}

    async def _gtrends() -> dict:
        try:
            from filings import google_trends
            return await to_heavy(google_trends.get_trends_summary, t_up) or {}
        except Exception as exc:
            logger.debug("google_trends.get_trends_summary(%s) failed: %s", ticker, exc)
            return {}

    async def _webtraffic() -> dict:
        try:
            from filings import web_traffic
            return await to_heavy(web_traffic.get_web_traffic_data, t_up) or {}
        except Exception as exc:
            logger.debug("web_traffic.get_web_traffic_data(%s) failed: %s", ticker, exc)
            return {}

    sent, gt, wt = await asyncio.gather(_sentiment(), _gtrends(), _webtraffic())

    return {
        # Sentiment overview cards
        "cnn":            sent.get("cnn_fear_greed"),
        "finnhub":        sent.get("finnhub"),
        "apewisdom":      sent.get("apewisdom"),
        "alphavantage":   sent.get("alphavantage"),
        # Search interest
        "gt_keywords":    (gt or {}).get("keywords"),
        "gt_trend":       (gt or {}).get("trend"),
        # Web traffic
        "wt_tranco":      (wt or {}).get("tranco"),
        "wt_wikipedia":   (wt or {}).get("wikipedia"),
        # Short interest (passes through to the Short Interest sub-pane)
        "short_interest":         sent.get("short_interest"),
        "short_interest_history": sent.get("short_interest_history") or [],
    }


async def _stock_build_vitals(request: Request, ticker: str) -> dict:
    """Real share statistics + peers + upcoming events.

    Three independent fetches (yfinance share stats / Finnhub peers + their
    prices / Finnhub earnings calendar) all run concurrently so the panel
    isn't gated on the slowest one.  Dividends still mock — no clean free
    source.
    """
    from filings import client

    t_up = ticker.upper()

    # ── Fan out: yfinance info + peers + events in parallel.  Each is
    #    L2-cached on its own key, so warm hits short-circuit. ──
    async def _yf_info() -> dict:
        try:
            return await to_heavy(client.get_yfinance_info, t_up) or {}
        except Exception:
            return {}

    async def _peer_tickers() -> list[str]:
        async def _compute() -> list[str]:
            key = os.environ.get("FINNHUB_API_KEY", "").strip()
            if not key:
                return []
            from filings.http_client import get_async_client
            try:
                r = await get_async_client().get(
                    "https://finnhub.io/api/v1/stock/peers",
                    params={"symbol": t_up, "token": key},
                )
                r.raise_for_status()
                return [p for p in (r.json() or []) if p and p.upper() != t_up][:6]
            except Exception as exc:
                logger.debug("Finnhub peers fetch failed for %s: %s", ticker, exc)
                return []
        try:
            return await _l2_cached(
                f"redesign:stock:peers:{t_up}", ttl_seconds=86400,
                compute=_compute, category="redesign_stock",
            ) or []
        except Exception:
            return []

    async def _events() -> list[dict]:
        async def _compute() -> list[dict]:
            return await _fetch_stock_events_async(t_up)
        try:
            return await _l2_cached(
                f"redesign:stock:events:{t_up}", ttl_seconds=86400,
                compute=_compute, category="redesign_stock",
            ) or []
        except Exception as exc:
            logger.debug("Stock events L2 cache failed for %s: %s", ticker, exc)
            return []

    info, peer_tickers, events_rows = await asyncio.gather(
        _yf_info(), _peer_tickers(), _events(),
    )

    # ── Institutional ownership: yfinance-reported % held by institutions. ──
    inst_pct = info.get("heldPercentInstitutions")

    def _pct_str(v: float | None) -> str:
        if v is None: return "—"
        # yfinance gives 0.658 for 65.8%; we accept either form.
        if abs(v) <= 1.0: v *= 100
        return f"{v:.1f}%"

    def _shares_str(n: int | float | None) -> str:
        if not n: return "—"
        return _stock_format_volume(n)

    last_split = "—"
    split_factor = info.get("lastSplitFactor")
    split_date_epoch = info.get("lastSplitDate")
    if split_factor and split_date_epoch:
        from datetime import datetime, timezone
        try:
            sd = datetime.fromtimestamp(float(split_date_epoch), tz=timezone.utc)
            last_split = f"{split_factor} ({sd.strftime('%b %Y')})"
        except (TypeError, ValueError):
            last_split = split_factor

    chg_52w = info.get("52WeekChange") or info.get("fiftyTwoWeekChange")

    share_stats = [
        ("Shares outstanding", _shares_str(info.get("sharesOutstanding"))),
        ("Float",              _shares_str(info.get("floatShares"))),
        ("Insider holdings",   _pct_str(info.get("heldPercentInsiders"))),
        ("Institutional own.", _pct_str(inst_pct)),
        ("Short interest",     _pct_str(info.get("shortPercentOfFloat"))),
        ("Days to cover",      f"{info.get('shortRatio'):.1f}" if info.get("shortRatio") else "—"),
        ("Beta (5Y)",          f"{info.get('beta'):.2f}" if info.get("beta") else "—"),
        ("52-week change",     f"{chg_52w*100:+.1f}%" if chg_52w is not None else "—"),
        ("Last split",         last_split),
    ]
    # If yfinance returned nothing, keep the design fixture as scaffold so
    # the panel doesn't show a wall of em-dashes.
    if not info:
        share_stats = list(_STOCK_VITALS_SHARE_STATS)

    # ── Peer prices: enrich the peer ticker list with current prices. ──
    peers_rows: list[dict] = []
    if peer_tickers:
        try:
            from filings import market_data
            prices = await to_heavy(market_data.get_current_prices_batch, peer_tickers) or {}
        except Exception:
            prices = {}
        for pt in peer_tickers[:5]:
            price = prices.get(pt)
            peers_rows.append({
                "ticker": pt,
                "name":   pt,  # market_data batch doesn't include name; ticker as fallback
                "price":  f"{price:.2f}" if price else "—",
                "chg":    0.0,
                "pe":     "—",
                "mc":     "—",
            })
    if not peers_rows:
        peers_rows = list(_STOCK_VITALS_PEERS)

    if not events_rows:
        events_rows = list(_STOCK_VITALS_EVENTS)

    return {
        "share_stats": share_stats,
        "peers":       peers_rows,
        "dividends":   _STOCK_VITALS_DIVS,   # no reliable free source
        "events":      events_rows,
    }


def _stock_build_financials(ticker: str) -> dict:
    """Real financial statements for *ticker* via SEC XBRL.

    Returns the four-tab structure the v2 Financials pane renders
    (Income / Balance / Cash Flow / Key Ratios), at both annual and
    quarterly cadence, plus a flat ``chart_data`` blob the ECharts
    builders consume for the chart-on-top-of-table layout.  Mirrors
    the v1 ``/api/financials/{ticker}`` shape so the existing chart
    builder logic ports cleanly.

    Each statement is ``{periods, labels, rows}`` where rows preserve
    the raw fundamentals shape — ``{label, is_subtotal, is_eps, is_pct,
    is_ratio, is_cash, values: {period: val}}`` — so the template's
    ``fmt_num`` macro can format per-row without a Python pre-pass.

    Returns ``has_data=False`` and the design fixture when XBRL is empty
    so the page never blanks out.
    """
    from datetime import datetime as _dt

    try:
        from filings import fundamentals
        data = fundamentals.get_fundamentals(ticker)
    except Exception as exc:
        logger.warning("get_fundamentals(%s) failed: %s", ticker, exc)
        data = None

    if not data or (not data.get("annual") and not data.get("quarterly")):
        return _stock_financials_fallback()

    def _period_label(p: str, freq: str) -> str:
        # Annual → "FY 2024".  Quarterly → "03/2024" (MM/YYYY).
        try:
            d = _dt.strptime(p, "%Y-%m-%d")
        except (TypeError, ValueError):
            return p
        return f"FY {d.year}" if freq == "annual" else f"{d.month:02d}/{d.year}"

    def _shape_statement(stmt: dict | None, freq: str) -> dict | None:
        # Most-recent first to match v1's L→R period order in the table.
        if not stmt:
            return None
        periods = sorted(stmt.get("periods") or [], reverse=True)
        if not periods:
            return None
        labels = [_period_label(p, freq) for p in periods]
        return {"periods": periods, "labels": labels, "rows": stmt.get("rows") or []}

    def _row_values(stmt: dict | None, label: str) -> list:
        # Chart-friendly oldest-first array (chart x-axis reads L→R old→new).
        if not stmt:
            return []
        periods = sorted(stmt.get("periods") or [])
        for row in stmt.get("rows") or []:
            if row.get("label") == label:
                return [(row.get("values") or {}).get(p) for p in periods]
        return [None] * len(periods)

    out: dict = {"has_data": True}
    chart_data: dict = {}
    for freq in ("annual", "quarterly"):
        fd = data.get(freq)
        if not fd:
            out[freq] = None
            continue
        out[freq] = {
            "income":   _shape_statement(fd.get("income"),   freq),
            "balance":  _shape_statement(fd.get("balance"),  freq),
            "cashflow": _shape_statement(fd.get("cashflow"), freq),
            "ratios":   _shape_statement(fd.get("ratios"),   freq),
        }
        # Chart axis labels are oldest-first.  Use whatever periods the
        # income statement reports — every chart shares this axis.
        periods_asc = sorted(
            (fd.get("income") or {}).get("periods") or [], reverse=False
        )
        chart_data[freq] = {
            "labels": [_period_label(p, freq) for p in periods_asc],
            "income": {
                "revenue":          _row_values(fd.get("income"), "Revenue"),
                "net_income":       _row_values(fd.get("income"), "Net Income"),
                "operating_margin": _row_values(fd.get("ratios"), "Operating Margin"),
            },
            "balance": {
                "current_assets":      _row_values(fd.get("balance"), "Total Current Assets"),
                "total_assets":        _row_values(fd.get("balance"), "Total Assets"),
                "current_liabilities": _row_values(fd.get("balance"), "Total Current Liabilities"),
                "total_liabilities":   _row_values(fd.get("balance"), "Total Liabilities"),
                "total_equity":        _row_values(fd.get("balance"), "Total Equity"),
            },
            "cashflow": {
                "operating_cf": _row_values(fd.get("cashflow"), "Operating Cash Flow"),
                "investing_cf": _row_values(fd.get("cashflow"), "Investing Cash Flow"),
                "financing_cf": _row_values(fd.get("cashflow"), "Financing Cash Flow"),
                "free_cf":      _row_values(fd.get("ratios"),   "Free Cash Flow"),
            },
            "ratios": {
                "gross_margin":     _row_values(fd.get("ratios"), "Gross Margin"),
                "operating_margin": _row_values(fd.get("ratios"), "Operating Margin"),
                "net_margin":       _row_values(fd.get("ratios"), "Net Margin"),
                "roe":              _row_values(fd.get("ratios"), "ROE"),
                "fcf_margin":       _row_values(fd.get("ratios"), "FCF Margin"),
            },
        }
    out["chart_data"] = chart_data

    # KPI strip — annual values, latest period vs prior.
    annual = data.get("annual") or {}
    annual_periods_asc = sorted(
        (annual.get("income") or {}).get("periods") or []
    )[-2:]

    def _annual_two(stmt_key: str, label: str) -> tuple[float | None, float | None]:
        stmt = annual.get(stmt_key) or {}
        for row in stmt.get("rows") or []:
            if row.get("label") == label:
                vals = row.get("values") or {}
                curr = vals.get(annual_periods_asc[-1]) if annual_periods_asc else None
                prev = vals.get(annual_periods_asc[0]) if len(annual_periods_asc) > 1 else None
                return curr, prev
        return None, None

    def _yoy_pct(curr: float | None, prev: float | None) -> str:
        if curr is None or prev is None or prev == 0:
            return ""
        pct = (curr - prev) / abs(prev) * 100
        sign = "+" if pct >= 0 else ""
        return f"{sign}{pct:.0f}% YoY"

    rev_curr, rev_prev = _annual_two("income", "Revenue")
    ni_curr,  ni_prev  = _annual_two("income", "Net Income")
    fcf_curr, fcf_prev = _annual_two("ratios", "Free Cash Flow")
    gm_curr,  gm_prev  = _annual_two("ratios", "Gross Margin")

    latest_label = (
        _period_label(annual_periods_asc[-1], "annual")
        if annual_periods_asc else "FY"
    )
    gm_delta = (
        f"{'+' if gm_curr >= gm_prev else ''}{gm_curr - gm_prev:.1f}pp"
        if gm_curr is not None and gm_prev is not None else ""
    )

    out["kpis"] = [
        {"label": f"Revenue ({latest_label})",
         "value": _stock_format_mcap(rev_curr) if rev_curr else "—",
         "delta": _yoy_pct(rev_curr, rev_prev),
         "up":    (rev_curr or 0) >= (rev_prev or 0)},
        {"label": f"Net Income ({latest_label})",
         "value": _stock_format_mcap(ni_curr) if ni_curr else "—",
         "delta": _yoy_pct(ni_curr, ni_prev),
         "up":    (ni_curr or 0) >= (ni_prev or 0)},
        {"label": f"FCF ({latest_label})",
         "value": _stock_format_mcap(fcf_curr) if fcf_curr else "—",
         "delta": _yoy_pct(fcf_curr, fcf_prev),
         "up":    (fcf_curr or 0) >= (fcf_prev or 0)},
        {"label": "Gross Margin",
         "value": f"{gm_curr:.1f}%" if gm_curr is not None else "—",
         "delta": gm_delta,
         "up":    (gm_curr or 0) >= (gm_prev or 0)},
    ]
    return out


def _stock_financials_fallback() -> dict:
    """Empty-state placeholder used when XBRL has nothing for the ticker."""
    return {
        "has_data":   False,
        "kpis":       _STOCK_MOCK_FIN_KPIS,
        "annual":     None,
        "quarterly":  None,
        "chart_data": {},
    }


def _stock_build_ownership_funds(fund_cache: dict, ticker: str) -> dict:
    """Real 13F ownership data for *ticker* from the in-memory fund cache.

    ``fund_cache`` is the live ``app.state.fund_cache`` dict; pass
    ``{}`` if unavailable.  Plumbed explicitly (rather than via
    ``request.app.state``) so non-request contexts -- the warmer
    cron, admin scripts -- can call this without fabricating a
    Starlette Request.

    Returns:
      - ``rows`` — top-10 holders with fund/manager/shares/value/port%/QoQΔ
      - ``total_count`` / ``total_value`` — all funds holding this ticker
      - ``activity_chart`` — per-quarter aggregate {labels, adds, reduces,
        adds_count, reduces_count} for the Quarterly Activity bar chart
      - ``quarters`` — per-quarter activity history (newest first), each
        with ``label`` / ``key`` / ``entries`` (fund / activity / share
        change / pct change), used by the Quarterly Activity History pills.
    """
    from filings.superinvestors import SUPERINVESTORS_BY_CIK

    if not fund_cache:
        return {"rows": [], "total_count": 0, "total_value": "—",
                "activity_chart": {"labels": [], "adds": [], "reduces": [],
                                   "adds_count": [], "reduces_count": []},
                "quarters": []}

    t_up = ticker.upper()
    rows_raw: list[dict] = []
    combined_value = 0

    for cik, fund_data in fund_cache.items():
        si = SUPERINVESTORS_BY_CIK.get(cik)
        if not si:
            continue

        change_by_cusip: dict[str, dict] = {
            ch["cusip"]: ch for ch in fund_data.get("changes") or []
        }
        total_val = fund_data.get("total_value") or 0

        for h in fund_data.get("all_holdings") or []:
            if (h.get("ticker") or "").upper() != t_up:
                continue
            value  = h.get("value")  or 0
            shares = h.get("shares") or 0
            pct    = h.get("pct")    or 0.0
            if not pct and total_val > 0:
                pct = round(value / total_val * 100, 2)

            change = change_by_cusip.get(h.get("cusip") or "")
            status = (change or {}).get("status")
            share_change = (change or {}).get("share_change") or 0
            if status == "NEW":
                chg = 1.0
            elif status == "CLOSED":
                chg = -1.0
            elif status == "INCREASED" or status == "DECREASED":
                prev = shares - share_change
                chg = (share_change / prev) if prev > 0 else 0
            else:
                chg = 0.0

            combined_value += value
            rows_raw.append({
                "fund":     si.fund_name,
                "manager":  si.display_name,
                "shares":   shares,
                "value":    value,
                "port":     pct,
                "chg":      chg,
            })

    rows_raw.sort(key=lambda r: -r["value"])
    rows = [{
        "rank":    f"{i+1:02d}",
        "fund":    r["fund"],
        "manager": r["manager"],
        "shares":  _stock_format_volume(r["shares"]),
        "value":   _stock_format_mcap(r["value"]),
        "port":    f"{r['port']:.1f}%" if r["port"] else "—",
        "chg":     r["chg"],
    } for i, r in enumerate(rows_raw[:10])]

    # ── Quarterly activity (chart + per-quarter expandable history) ──
    # Use the existing ``client.build_stock_history`` builder which walks
    # every fund's quarterly_changes once.
    try:
        from filings import client
        history = client.build_stock_history(ticker, fund_cache, SUPERINVESTORS_BY_CIK)
    except Exception as exc:
        logger.warning("build_stock_history(%s) failed: %s", ticker, exc)
        history = []

    # Activity chart series — chronological (oldest left to match v1).
    chart_labels: list[str] = []
    chart_adds: list[int] = []
    chart_reduces: list[int] = []
    chart_adds_count: list[int] = []
    chart_reduces_count: list[int] = []

    quarters_history: list[dict] = []
    for q in history:
        adds_shares = 0
        reduces_shares = 0
        adds_n = 0
        reduces_n = 0
        entries: list[dict] = []
        for e in q.entries:
            if e.share_change > 0:
                adds_shares  += e.share_change
                adds_n       += 1
            elif e.share_change < 0:
                reduces_shares += e.share_change  # negative
                reduces_n     += 1
            entries.append({
                "fund":         e.fund_display_name,
                "fund_cik":     e.fund_cik,
                "activity":     e.activity,
                "share_change": e.share_change,
                "pct_change":   e.pct_change,
            })
        quarters_history.append({
            "label":   q.period,
            "key":     q.period.replace(" ", "-").lower(),
            "entries": entries,
        })
        chart_labels.append(q.period)
        chart_adds.append(adds_shares)
        chart_reduces.append(reduces_shares)
        chart_adds_count.append(adds_n)
        chart_reduces_count.append(reduces_n)

    # build_stock_history returns newest-first; flip the chart axis to
    # oldest-left so it reads naturally L→R.  Keep quarters_history
    # newest-first for the pill nav (most recent quarter pre-selected).
    chart_labels.reverse()
    chart_adds.reverse()
    chart_reduces.reverse()
    chart_adds_count.reverse()
    chart_reduces_count.reverse()

    return {
        "rows":        rows,
        "total_count": len(rows_raw),
        "total_value": _stock_format_mcap(combined_value),
        "activity_chart": {
            "labels":        chart_labels,
            "adds":          chart_adds,
            "reduces":       chart_reduces,
            "adds_count":    chart_adds_count,
            "reduces_count": chart_reduces_count,
        },
        "quarters": quarters_history,
    }


def _stock_build_ownership_insiders(ticker: str) -> dict:
    """Real Form 4 insider trades for *ticker* via ``insider_trading``.

    Reuses the v1 ``insider_trading.prepare_ticker_display`` to produce
    the four data slices the redesign panel needs:

      - ``insiders`` — per-insider summary (name, title, buy/sell counts +
        values, last trade date, quarterly_breakdown for the hover tooltip)
      - ``quarters`` — newest-first per-quarter trade groups (each with
        label, key, trades list, buy/sell counts, total value)
      - ``chart`` — Chart.js series data (labels, buy_values, sell_values)
      - ``per_insider_chart`` — same shape keyed by insider name (+__all__)
        for the dropdown filter

    Plus the 4-card insights panel from ``insider_insights``.  Falls back
    to empty structures on error so the template never blows up.
    """
    from filings import insider_trading, supabase_cache, insider_insights as ii

    try:
        trades = insider_trading.get_ticker_insider_trades(ticker) or []
    except Exception as exc:
        logger.warning("get_ticker_insider_trades(%s) failed: %s", ticker, exc)
        trades = []

    if not trades:
        return {"insiders": [], "quarters": [], "chart": None,
                "per_insider_chart": {}, "insights": None,
                "total_count": 0}

    try:
        display = insider_trading.prepare_ticker_display(trades)
    except Exception as exc:
        logger.warning("prepare_ticker_display(%s) failed: %s", ticker, exc)
        display = {"insiders": [], "quarters": [], "chart": None,
                   "per_insider_chart": {}}

    # 4-card insights panel — Buy Activity / Buying Zone / Win Rate /
    # Forward 90d.  Pulls from a separate ``insider_purchases_history``
    # table (cold storage of historical buys), so guard if missing.
    insights = None
    try:
        purchases = supabase_cache.get_insider_purchases(ticker) or []
        if purchases and len(purchases) >= 2:
            insights = ii.compute_insider_insights(ticker, purchases, None)
    except Exception as exc:
        logger.debug("insider_insights for %s failed: %s", ticker, exc)

    return {
        "insiders":          display.get("insiders") or [],
        "quarters":          display.get("quarters") or [],
        "chart":             display.get("chart"),
        "per_insider_chart": display.get("per_insider_chart") or {},
        "insights":          insights,
        "total_count":       len(trades),
    }


def _stock_build_ownership_congress(ticker: str) -> dict:
    """Real STOCK Act congressional trades for *ticker* from Supabase.

    Reuses ``congress_trading.prepare_stock_congress_display`` for the
    party-volume breakdown + per-member aggregation, then formats the top
    10 most-recent trades for the table.  Returns:
      - ``rows`` — top-10 trades with date / party / action / size / delay
      - ``politicians`` — top-12 members trading this ticker for the grid
      - ``party_breakdown`` — Dem vs Rep buy-volume percentages
      - ``total_trades`` / ``total_politicians`` for the panel headers
    """
    from filings import supabase_cache, congress_trading
    from datetime import datetime

    try:
        rows_db = supabase_cache.get_congress_trades_by_ticker(ticker, limit=500) or []
    except Exception as exc:
        logger.warning("get_congress_trades_by_ticker(%s) failed: %s", ticker, exc)
        rows_db = []

    if not rows_db:
        return {"rows": [], "total_count": 0,
                "politicians": [], "party_breakdown": None,
                "total_politicians": 0, "total_trades": 0}

    # Aggregate via the existing v1 helper — gives us politicians +
    # party_breakdown + recent_trades in one pass.
    try:
        agg = congress_trading.prepare_stock_congress_display(ticker, rows_db)
    except Exception as exc:
        logger.warning("prepare_stock_congress_display(%s) failed: %s", ticker, exc)
        agg = {"politicians": [], "party_breakdown": None,
               "recent_trades": [], "total_politicians": 0, "total_trades": 0}

    # Format top-10 trades for the redesign table (same shape as before).
    rows: list[dict] = []
    for r in (agg.get("recent_trades") or rows_db)[:10]:
        party_full = (r.get("party") or "").lower()
        if "democrat" in party_full:   party = "D"
        elif "republican" in party_full: party = "R"
        else:                            party = "I"

        action = "BUY" if (r.get("trade_type") or "").lower() == "buy" else "SELL"

        td = r.get("trade_date") or ""
        fd = r.get("filing_date") or ""
        try:
            t_dt = datetime.strptime(td, "%Y-%m-%d") if td else None
            f_dt = datetime.strptime(fd, "%Y-%m-%d") if fd else None
            days = (f_dt - t_dt).days if t_dt and f_dt else 0
            date_str = (f_dt or t_dt).strftime("%b %d %Y") if (f_dt or t_dt) else ""
        except (TypeError, ValueError):
            days, date_str = 0, fd or td

        size = (r.get("amount_compact")
                or _compact_range_str(r.get("amount_display"))
                or "—")

        rows.append({
            "person":  r.get("politician_name") or "—",
            "party":   party,
            "chamber": (r.get("chamber") or "").title() or "—",
            "action":  action,
            "size":    size,
            "date":    date_str,
            "days":    max(0, days),
        })

    return {
        "rows":              rows,
        "total_count":       len(rows_db),
        "politicians":       (agg.get("politicians") or [])[:12],
        "party_breakdown":   agg.get("party_breakdown"),
        "total_politicians": agg.get("total_politicians", 0),
        "total_trades":      agg.get("total_trades", len(rows_db)),
    }


async def _stock_build_ownership_filings(ticker: str) -> dict:
    """Recent EDGAR filings for *ticker* via SEC submissions.json (24h L2-cached).

    Pulls the company's most recent 12 filings, filters out routine
    ownership reports (Form 3/5), and returns them formatted for the
    Ownership > SEC Filings sub-tab.
    """
    from datetime import datetime

    async def _compute() -> dict:
        try:
            from filings import fundamentals
            cik = fundamentals._resolve_cik(ticker)
        except Exception as exc:
            logger.debug("CIK resolution failed for %s: %s", ticker, exc)
            return {"rows": []}
        if not cik:
            return {"rows": []}

        from filings.http_client import get_async_client
        url = f"https://data.sec.gov/submissions/CIK{int(cik):010d}.json"
        headers = {
            "User-Agent": os.environ.get("SEC_IDENTITY",
                                         "PaperPanda/1.0 (contact@paperpanda.io)"),
            "Accept": "application/json",
        }
        try:
            r = await get_async_client().get(url, headers=headers, timeout=8.0)
            r.raise_for_status()
            payload = r.json() or {}
        except Exception as exc:
            logger.debug("SEC submissions.json failed for %s: %s", ticker, exc)
            return {"rows": []}

        recent = (payload.get("filings") or {}).get("recent") or {}
        forms        = recent.get("form") or []
        dates        = recent.get("filingDate") or []
        descriptions = recent.get("primaryDocDescription") or []
        accessions   = recent.get("accessionNumber") or []
        periods      = recent.get("reportDate") or []
        primary_docs = recent.get("primaryDocument") or []

        rows: list[dict] = []
        # Up the cap to ~50 — the redesign panel scrolls anyway, and
        # exposing more filings makes the form-type filter useful.
        for i in range(min(len(forms), 50)):
            form = forms[i]
            if len(rows) >= 50:
                break
            try:
                d = datetime.strptime(dates[i], "%Y-%m-%d").strftime("%b %d %Y")
            except (TypeError, ValueError, IndexError):
                d = dates[i] if i < len(dates) else ""

            # Build the EDGAR filing-detail URL from accession + primary doc.
            # Accession number is "0001193125-24-001234"; folder format
            # strips the dashes.  Filing index page is at:
            #   /Archives/edgar/data/{cik_no_pad}/{acc_no_clean}/{primary_doc}
            sec_url = ""
            if i < len(accessions) and accessions[i]:
                acc_clean = accessions[i].replace("-", "")
                doc = primary_docs[i] if i < len(primary_docs) else ""
                sec_url = (
                    f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/"
                    f"{acc_clean}/{doc}" if doc
                    else f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany"
                         f"&CIK={int(cik):010d}&type={form}"
                )

            rows.append({
                "type":   form,
                "filed":  d,
                "period": periods[i] if i < len(periods) and periods[i] else "—",
                "desc":   descriptions[i] if i < len(descriptions) else "—",
                "size":   "—",
                "url":    sec_url,
            })
        return {"rows": rows}

    try:
        # ``v2`` cache key suffix: previous shape didn't include ``url``
        # or expand to 50 rows; bump to invalidate stale rows in Supabase.
        return await _l2_cached(
            f"redesign:stock:filings:v2:{ticker.upper()}", ttl_seconds=86400,
            compute=_compute, category="redesign_stock",
        ) or {"rows": []}
    except Exception as exc:
        logger.debug("filings L2 cache failed for %s: %s", ticker, exc)
        return await _compute()


def _stock_build_sentiment(ticker: str) -> dict:
    """Per-ticker retail sentiment derived from ApeWisdom.

    Score is the 24-hour mention velocity (% change vs ``mentions_24h_ago``)
    clamped to ±100 — positive = trending up, negative = falling out of
    favor.  Cohort is the top-3 r/WallStreetBets tickers ranked above
    *ticker* (or top-3 overall when this ticker is the leader).  Returns
    a dict the template can render directly; ``has_data=False`` lets the
    template show a graceful "no data" state.
    """
    from filings import sentiment

    t_up = ticker.upper()
    item = None
    try:
        item = sentiment._get_apewisdom_for_ticker(t_up)
    except Exception as exc:
        logger.debug("ApeWisdom lookup failed for %s: %s", ticker, exc)

    if not item or not item.get("mentions"):
        return {"has_data": False, "score": 0, "score_str": "—", "score_class": "",
                "mentions": "—", "rank_str": "", "cohort": [],
                "note": "", "fill_pct": 0, "fill_left": 50}

    mentions = int(item.get("mentions") or 0)
    mentions_24h = int(item.get("mentions_24h_ago") or 0)
    rank = int(item.get("rank") or 0)

    # Velocity score: 24h mention % change, clamped to ±100.
    if mentions_24h > 0:
        score = round((mentions - mentions_24h) / mentions_24h * 100)
    else:
        score = 100 if mentions > 0 else 0
    score = max(-100, min(100, score))

    # Bar geometry — bar is centered on 50%; positive fills right, negative left.
    fill_w = abs(score) / 2  # half the score % so ±100 fills half the bar each side
    fill_left = 50 if score >= 0 else 50 - fill_w

    score_class = "pp-up" if score > 0 else ("pp-down" if score < 0 else "")
    score_str = f"+{score}" if score > 0 else (str(score) if score < 0 else "0")

    # Cohort: the next-most-discussed tickers above this one (or top 3 if this is #1).
    cohort: list[str] = []
    try:
        all_data = sentiment._get_apewisdom_all() or []
    except Exception:
        all_data = []
    for r in all_data:
        rt = (r.get("ticker") or "").upper()
        if not rt or rt == t_up:
            continue
        if int(r.get("rank") or 9999) < rank or rank == 0:
            cohort.append(rt)
        if len(cohort) >= 3:
            break
    if len(cohort) < 3:
        # Fill from the top of the leaderboard.
        for r in all_data:
            rt = (r.get("ticker") or "").upper()
            if rt and rt != t_up and rt not in cohort:
                cohort.append(rt)
            if len(cohort) >= 3:
                break

    rank_str = f"#{rank} most discussed" if rank else ""
    direction = "up" if score > 0 else ("down" if score < 0 else "flat")
    note = (
        f"Mentions {direction} {abs(score)}% vs. 24 hours ago"
        f" ({mentions_24h:,} → {mentions:,})." if mentions_24h else
        f"{mentions:,} mentions in the last 24 hours."
    )

    return {
        "has_data":    True,
        "score":       score,
        "score_str":   score_str,
        "score_class": score_class,
        "mentions":    f"{mentions:,}",
        "rank_str":    rank_str,
        "cohort":      cohort[:3],
        "note":        note,
        "fill_pct":    round(fill_w, 1),
        "fill_left":   round(fill_left, 1),
    }


_COMPANY_NAME_SUFFIX_RE = re.compile(
    r"\b(?:inc\.?|corp\.?|corporation|ltd\.?|limited|plc|llc|holdings?|"
    r"company|co\.?|n\.v\.|s\.a\.|gmbh|ag|sa)\b\.?",
    re.IGNORECASE,
)


def _company_name_token(name: str | None) -> str:
    """Strip "Inc / Corp / Ltd / …" off a company name so the filter can
    match the *brand* word in headlines (e.g. ``"NVIDIA Corporation"`` →
    ``"NVIDIA"``).  Returns empty string when nothing useful remains."""
    if not name:
        return ""
    cleaned = _COMPANY_NAME_SUFFIX_RE.sub("", name)
    cleaned = re.sub(r"[,.&]+", " ", cleaned)
    return cleaned.strip().split()[0] if cleaned.strip() else ""


async def _fetch_stock_events_async(ticker: str, *, months_ahead: int = 6,
                                    max_items: int = 4) -> list[dict]:
    """Per-ticker forward earnings calendar from Finnhub /calendar/earnings.

    Free-tier endpoint takes ``symbol=`` for filtering; we ask for the next
    *months_ahead* and trim to *max_items*.  Returns rows shaped for the
    Vitals "Upcoming events" panel: ``{date, event, when, est}``.

    L2-cached 24h via the caller — earnings dates are firmed up well in
    advance and rarely change inside that window.
    """
    key = os.environ.get("FINNHUB_API_KEY", "").strip()
    if not key:
        return []

    from datetime import datetime, timedelta, timezone
    from filings.http_client import get_async_client

    today = datetime.now(timezone.utc).date()
    end = today + timedelta(days=months_ahead * 31)
    params = {
        "symbol": ticker.upper(), "token": key,
        "from": today.isoformat(), "to": end.isoformat(),
    }
    try:
        r = await get_async_client().get(
            "https://finnhub.io/api/v1/calendar/earnings", params=params,
        )
        r.raise_for_status()
        raw = (r.json() or {}).get("earningsCalendar") or []
    except Exception as exc:
        logger.debug("Finnhub earnings calendar failed for %s: %s", ticker, exc)
        return []

    raw.sort(key=lambda e: e.get("date") or "")  # ascending by date

    out: list[dict] = []
    for ent in raw[:max_items]:
        try:
            d = datetime.strptime(ent["date"], "%Y-%m-%d")
        except (KeyError, TypeError, ValueError):
            continue

        # "amc" = after market close · "bmo" = before market open · "" = unset
        hour = (ent.get("hour") or "").lower()
        when = "After close" if hour == "amc" else (
            "Before open" if hour == "bmo" else "TBD"
        )

        # Compose "EPS $X.XX · Rev $YY.YB" from the consensus estimates.
        eps_est = ent.get("epsEstimate")
        rev_est = ent.get("revenueEstimate")
        est_parts: list[str] = []
        if eps_est is not None:
            est_parts.append(f"EPS ${float(eps_est):.2f}")
        if rev_est is not None:
            est_parts.append(f"Rev {_stock_format_mcap(float(rev_est))}")
        est = " · ".join(est_parts) or "Consensus pending"

        # Quarter / year from the entry; falls back to month-derived label.
        q_num = ent.get("quarter")
        y_num = ent.get("year")
        if q_num and y_num:
            event_label = f"Q{int(q_num)} {int(y_num)} Earnings"
        else:
            event_label = "Earnings"

        out.append({
            "date":  d.strftime("%b %d"),
            "event": event_label,
            "when":  when,
            "est":   est,
        })
    return out


async def _fetch_company_news_async(ticker: str, *, name: str | None = None,
                                    days_back: int = 21,
                                    max_items: int = 6) -> list[dict]:
    """Per-ticker news from Finnhub /company-news, normalised to the shape
    the stock-page right-rail expects ({src, ago, title, url}).

    Finnhub tags articles where the symbol is one of the *related* tickers
    — that pulls in Broadcom-vs-Nvidia comparisons under both symbols, so
    we also filter the headline for the ticker symbol or the brand word
    (e.g. "NVIDIA").  Uses the shared async HTTP client; L2-cached 30min
    via the caller."""
    key = os.environ.get("FINNHUB_API_KEY", "").strip()
    if not key:
        return []

    from datetime import datetime, timedelta, timezone
    from filings.http_client import get_async_client

    today = datetime.now(timezone.utc).date()
    sym = ticker.upper()
    params = {
        "symbol": sym, "token": key,
        "from": (today - timedelta(days=days_back)).isoformat(),
        "to":   today.isoformat(),
    }

    try:
        r = await get_async_client().get(
            "https://finnhub.io/api/v1/company-news", params=params,
        )
        r.raise_for_status()
        raw = r.json() or []
    except Exception as exc:
        logger.debug("Finnhub company-news failed for %s: %s", ticker, exc)
        return []

    # Filter: keep only articles where the ticker or brand-name word
    # appears in the headline (case-insensitive whole-word match).  This
    # drops articles where NVDA is just a "related" tag on a Magna/Ford
    # piece.
    brand = _company_name_token(name)
    needles = {sym}
    if brand:
        needles.add(brand.upper())

    def _matches(headline: str) -> bool:
        if not headline:
            return False
        upper = headline.upper()
        for n in needles:
            if re.search(rf"\b{re.escape(n)}\b", upper):
                return True
        return False

    raw.sort(key=lambda a: a.get("datetime") or 0, reverse=True)
    items: list[dict] = []
    for art in raw:
        if not _matches(art.get("headline") or ""):
            continue
        ts = art.get("datetime") or 0
        try:
            from filings.market_data import _time_ago
            ago = _time_ago(ts) if ts else ""
        except Exception:
            ago = ""
        items.append({
            "src":   art.get("source") or "—",
            "ago":   ago,
            "title": art.get("headline") or "",
            "url":   art.get("url") or "",
        })
        if len(items) >= max_items:
            break
    return items


async def _fetch_stock_news(ticker: str, name: str | None) -> list[dict]:
    """Wrapper: per-ticker company news with a 30min L2 read-through cache.

    Cache key includes the brand token so a name-resolved fetch doesn't
    overwrite a ticker-only fetch (and vice versa); both surface different
    filter results."""
    brand = _company_name_token(name) or ""
    key_suffix = f"{ticker.upper()}:{brand.upper()}" if brand else ticker.upper()
    async def _compute() -> list[dict]:
        return await _fetch_company_news_async(ticker, name=name, max_items=6)
    try:
        return await _l2_cached(
            f"redesign:stock:news:{key_suffix}", ttl_seconds=1800,
            compute=_compute, category="redesign_stock",
        ) or []
    except Exception as exc:
        logger.debug("Stock news L2 cache failed for %s: %s", ticker, exc)
        return await _fetch_company_news_async(ticker, name=name, max_items=6)


async def _fetch_finnhub_profile(ticker: str) -> dict:
    """Finnhub /stock/profile2 + /stock/metric — yfinance fallback.

    Returns ``{"profile": {...}, "metric": {...}}``.  Empty dicts on miss
    (no API key, network failure, unknown ticker).  Both endpoints are
    fetched concurrently via the shared async HTTP client.
    """
    key = os.environ.get("FINNHUB_API_KEY", "").strip()
    if not key:
        return {"profile": {}, "metric": {}}

    from filings.http_client import get_async_client
    cli = get_async_client()
    sym = ticker.upper()

    async def _profile() -> dict:
        try:
            r = await cli.get(
                "https://finnhub.io/api/v1/stock/profile2",
                params={"symbol": sym, "token": key},
            )
            r.raise_for_status()
            return r.json() or {}
        except Exception as exc:
            logger.debug("Finnhub profile2 fetch failed for %s: %s", ticker, exc)
            return {}

    async def _metric() -> dict:
        try:
            r = await cli.get(
                "https://finnhub.io/api/v1/stock/metric",
                params={"symbol": sym, "metric": "all", "token": key},
            )
            r.raise_for_status()
            return (r.json() or {}).get("metric") or {}
        except Exception as exc:
            logger.debug("Finnhub metric fetch failed for %s: %s", ticker, exc)
            return {}

    profile, metric = await asyncio.gather(_profile(), _metric())
    return {"profile": profile, "metric": metric}


async def _fetch_stock_meta(fund_cache: dict, ticker: str) -> dict:
    """Resolve hero/KPI metadata + About-panel fields for *ticker*.

    ``fund_cache`` is the live ``app.state.fund_cache`` dict (CUSIP
    + holders count are derived from a one-pass walk over it).  Pass
    ``{}`` from non-request contexts.

    Read order:
      1. ``company_about`` Supabase row (filled by ``scripts/backfill_about.py``
         every 6 months; covers CEO / employees / HQ / IPO / website / blurb /
         logo).  When present, we skip yfinance entirely.
      2. Finnhub /stock/profile2 + /stock/metric — always called for
         sector / industry / exchange / mcap / P/E.
      3. yfinance .info — only called when no ``company_about`` row exists.
         On miss we kick off an async writeback so the next visitor reads
         from Supabase instead.

    CUSIP + holders count come from a one-pass walk over the passed
    fund cache (zero network).
    """
    from filings import client, company_about as ca

    if fund_cache is None:
        fund_cache = {}
    t_up = ticker.upper()

    async def _about_row() -> dict | None:
        return await to_supabase(ca.get_row, t_up)

    async def _finnhub() -> dict:
        async def _compute() -> dict:
            return await _fetch_finnhub_profile(t_up)
        try:
            return await _l2_cached(
                f"redesign:stock:finnhub:{t_up}", ttl_seconds=86400,
                compute=_compute, category="redesign_stock",
            ) or {"profile": {}, "metric": {}}
        except Exception as exc:
            logger.debug("Finnhub L2 cache failed for %s: %s", ticker, exc)
            return await _fetch_finnhub_profile(t_up)

    about_row, fh = await asyncio.gather(_about_row(), _finnhub())
    fh_profile = (fh or {}).get("profile") or {}
    fh_metric  = (fh or {}).get("metric")  or {}

    # yfinance is only needed when company_about hasn't been backfilled
    # for this ticker.  Skip the call entirely on hot path — the table
    # is the canonical source.
    info: dict = {}
    if not about_row:
        try:
            info = await to_heavy(client.get_yfinance_info, t_up) or {}
        except Exception as exc:
            logger.warning("get_yfinance_info failed for %s: %s", ticker, exc)
        # Write back to Supabase so the next reader skips yfinance.
        if info or fh_profile:
            async def _writeback() -> None:
                row = await to_heavy(ca.fetch_live, t_up)
                if row.get("source") != "empty":
                    await to_supabase(ca.upsert_row, row)
            asyncio.create_task(_writeback())

    # Walk fund_cache once for holders count + CUSIP/issuer fallback.
    holders = 0
    cusip_from_cache: str | None = None
    for fund_data in fund_cache.values():
        match = False
        for h in fund_data.get("all_holdings") or []:
            if (h.get("ticker") or "").upper() == t_up:
                if not cusip_from_cache and h.get("cusip"):
                    cusip_from_cache = h["cusip"]
                match = True
                break
        if match:
            holders += 1

    industry = info.get("industry") or fh_profile.get("finnhubIndustry") or "—"

    # Sector resolution: prefer yfinance, then GICS lookup from _FALLBACK_SP500
    # for top-200 tickers (Finnhub doesn't provide a GICS-level sector field).
    sector = info.get("sector") or ""
    if not sector:
        try:
            from filings.market_data import _FALLBACK_SP500
            for entry in _FALLBACK_SP500:
                if entry.get("ticker", "").upper() == t_up:
                    sector = entry.get("sector") or ""
                    break
        except Exception:
            pass
    if not sector or sector == industry:
        sector = industry

    exch_code = (info.get("exchange") or "").upper()
    exchange  = _EXCHANGE_LABELS.get(exch_code, exch_code) if exch_code else ""
    if not exchange or exchange == "—":
        # Finnhub returns full names like "NASDAQ NMS - GLOBAL SELECT MARKET".
        fh_exch = (fh_profile.get("exchange") or "").upper()
        if "NASDAQ" in fh_exch: exchange = "NASDAQ"
        elif "NEW YORK" in fh_exch or "NYSE" in fh_exch: exchange = "NYSE"
        elif fh_exch: exchange = fh_exch.split()[0]
        else: exchange = "—"

    cusip = cusip_from_cache or "—"

    mcap_raw = info.get("marketCap")
    if not mcap_raw and fh_profile.get("marketCapitalization"):
        # Finnhub returns mcap in millions of USD.
        mcap_raw = float(fh_profile["marketCapitalization"]) * 1e6

    pe_raw = info.get("trailingPE")
    if (pe_raw is None or pe_raw <= 0) and fh_metric.get("peTTM"):
        try:
            pe_raw = float(fh_metric["peTTM"])
        except (TypeError, ValueError):
            pass

    # About panel: prefer the company_about row when we have one, otherwise
    # fall back to the live yfinance + Finnhub merge (the writeback above
    # populates the row asynchronously so subsequent reads use the table).
    if about_row:
        about = ca.format_for_template(about_row)
    else:
        about = {
            "about_blurb":     _stock_truncate_blurb(info.get("longBusinessSummary")),
            "about_ceo":       _stock_extract_ceo(info),
            "about_employees": _stock_format_employees(
                info.get("fullTimeEmployees") or fh_profile.get("employeeTotal")
            ),
            "about_hq":        _stock_format_hq(info, fh_profile),
            "about_ipo":       _stock_format_ipo(info, fh_profile),
            "about_website":   _stock_format_website(info, fh_profile),
            "logo_url":        _stock_resolve_logo(info, fh_profile),
        }

    return {
        # Hero + KPI strip
        "sector":   sector,
        "industry": industry,
        "exchange": exchange,
        "cusip":    cusip,
        "mcap":     _stock_format_mcap(mcap_raw),
        "pe":       _stock_format_pe(pe_raw),
        "holders":  holders,
        # About panel (from company_about table or live fallback)
        **{k: about[k] for k in (
            "about_blurb", "about_ceo", "about_employees",
            "about_hq", "about_ipo", "about_website", "logo_url",
        )},
    }


async def _fetch_stock_overview(ticker: str) -> dict:
    """Fetch OHLCV + quote for a stock.  Returns rich context dict.

    Falls back to NVDA-shaped demo data if fetches fail (so the page still
    looks complete during local dev when the network is flaky).
    """
    try:
        from filings import market_data
        ohlcv = await to_heavy(market_data.get_stock_ohlcv, ticker.upper(), "1M")
    except Exception as exc:
        logger.warning("Stock OHLCV fetch failed for %s: %s", ticker, exc)
        ohlcv = None

    # News is fetched separately via _fetch_stock_news so it can use the
    # resolved company name from meta to filter related-but-off-topic
    # headlines (Broadcom-vs-Nvidia, etc.).
    news: list[dict] = []

    if not ohlcv or not ohlcv.get("ohlcv"):
        # Demo fallback (NVDA-shaped).
        return {
            "ticker":  ticker.upper(),
            "name":    "NVIDIA Corporation",
            "exchange": "NASDAQ",
            "cusip":    "67066G104",
            "price":    142.18,
            "chg_abs":  5.74,
            "chg_pct":  0.0421,
            "open":     "137.42",
            "high":     "143.51",
            "low":      "136.91",
            "close":    "142.18",
            "volume":   "284.6M",
            "avg_vol":  "212.4M",
            "mcap":     "$3.51T",
            "pe":       "68.4",
            "high_52":  "152.84",
            "low_52":   "86.62",
            "candles":  [],
            "news":     news or [],
            "is_mock":  True,
        }

    candles = ohlcv["ohlcv"]
    # Today's bar = last candle.  yfinance sometimes returns date-only-EOD;
    # use the latest row regardless.
    last = candles[-1] if candles else None
    if last and len(last) >= 6:
        _, o, h, l, c, v = last[:6]
    else:
        o = h = l = c = v = None

    # Compute *today's* change from the last two closes.  ohlcv["pct_change"]
    # is the full 1Y aggregate which we don't want to display as "today".
    today_chg_pct = None
    today_chg_abs = None
    if len(candles) >= 2:
        prev_close = candles[-2][4]
        last_close = candles[-1][4]
        if prev_close and last_close is not None:
            today_chg_abs = last_close - prev_close
            today_chg_pct = (last_close - prev_close) / prev_close

    # 52-week range from full history.
    highs = [row[2] for row in candles if len(row) >= 3 and row[2] is not None]
    lows  = [row[3] for row in candles if len(row) >= 4 and row[3] is not None]
    high_52 = max(highs) if highs else None
    low_52  = min(lows)  if lows  else None

    # Average volume — simple mean across the year.
    vols = [row[5] for row in candles if len(row) >= 6 and row[5] is not None]
    avg_vol = sum(vols) / len(vols) if vols else None

    return {
        "ticker":   ohlcv.get("ticker", ticker.upper()),
        "name":     ohlcv.get("name", ticker.upper()),
        "exchange": "NASDAQ",  # market_data doesn't carry this; default for now.
        "cusip":    "—",       # CUSIP not in OHLCV payload; needs SEC join.
        "price":    ohlcv.get("price"),
        "chg_pct":  today_chg_pct,
        "chg_abs":  today_chg_abs,
        "open":     _stock_format_price(o),
        "high":     _stock_format_price(h),
        "low":      _stock_format_price(l),
        "close":    _stock_format_price(c),
        "volume":   _stock_format_volume(v),
        "avg_vol":  _stock_format_volume(avg_vol),
        "mcap":     "—",       # Needs fundamentals.get_fundamentals join.
        "pe":       "—",
        "high_52":  _stock_format_price(high_52),
        "low_52":   _stock_format_price(low_52),
        "candles":  candles,
        "news":     news or [],
        "is_mock":  False,
    }


def _stock_kpi_strip(ctx: dict) -> list[dict]:
    """8-cell KPI strip — Open / High / Low / Volume / Avg Vol / Mkt Cap / P/E / 52W."""
    return [
        {"label": "Open",    "value": ctx["open"],                                      "delta": None,  "up": None},
        {"label": "High",    "value": ctx["high"],                                      "delta": "day", "up": None},
        {"label": "Low",     "value": ctx["low"],                                       "delta": "day", "up": None},
        {"label": "Volume",  "value": ctx["volume"],                                    "delta": None,  "up": None},
        {"label": "Avg Vol", "value": ctx["avg_vol"],                                   "delta": None,  "up": None},
        {"label": "Mkt Cap", "value": ctx["mcap"],                                      "delta": None,  "up": None},
        {"label": "P/E",     "value": ctx["pe"],                                        "delta": None,  "up": None},
        {"label": "52W H/L", "value": f"{ctx['high_52']} / {ctx['low_52']}",            "delta": None,  "up": None},
    ]


def _stock_chart_axis_labels(bars: list, period: str, n_ticks: int = 7) -> list:
    """Pick ``n_ticks`` evenly-spaced bars and format their timestamps.

    Format depends on the chart period so the axis reads correctly at
    every zoom level:

      1D → ``HH:MM`` (intraday minute markers, UTC for now)
      1W → ``%a %d`` (e.g. ``Mon 5``)
      1M → ``%b %d`` (e.g. ``Apr 5``)
      3M → ``%b %d`` (e.g. ``May 12``)
      1Y → ``%b`` w/ ``'YY`` suffix on first tick + any year crossover
      5Y → ``%Y`` (year only)

    Parent template / partial uses CSS flexbox (``justify-content:
    space-between``) to space the resulting strings evenly across the
    chart width -- matches the SVG's ``preserveAspectRatio="none"``
    stretch model so we don't have to compute pixel-perfect x positions.
    """
    if not bars:
        return []
    n = len(bars)
    n_ticks = min(max(2, n_ticks), n)
    if n == 1:
        indices = [0]
    else:
        indices = [round(i * (n - 1) / (n_ticks - 1)) for i in range(n_ticks)]

    labels: list[str] = []
    last_year: int | None = None
    for idx in indices:
        ts_ms = bars[idx].get("t", 0)
        if not ts_ms:
            labels.append("")
            continue
        dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        if period == "1D":
            label = dt.strftime("%H:%M")
        elif period == "1W":
            label = dt.strftime("%a %d").lstrip("0")
        elif period in ("1M", "3M"):
            label = f"{dt.strftime('%b')} {dt.day}"
        elif period == "5Y":
            label = str(dt.year)
        else:  # 1Y default
            label = dt.strftime("%b")
            # Year tag on first tick AND every year change so a chart
            # spanning a year boundary still reads unambiguously.
            if last_year is None or dt.year != last_year:
                label = f"{label} '{dt.year % 100:02d}"
        last_year = dt.year
        labels.append(label)
    return labels


def _stock_candlestick_paths(candles: list, period: str = "1Y",
                             vb_w: int = 800, vb_h: int = 280) -> dict:
    """Build SVG geometry for a candlestick + volume strip.

    Default viewBox is 800×280 (~2.86:1) — matches the stock-page hero's
    2-column container ratio (chart sits next to the news panel) so
    `preserveAspectRatio="none"` doesn't squish or stretch the candles.

    Returns dict with `bars` (per-candle drawing data), `axis_labels`
    (period-aware X-axis tick strings), and meta.  Each bar entry:
    {x, wick_y1, wick_y2, body_y, body_h, up, vol_h, t, o, h, l, c, v}.

    ``period`` (``1D``/``1W``/``1M``/``3M``/``1Y``/``5Y``) drives axis
    formatting -- bar geometry is period-agnostic.
    """
    if not candles or len(candles) < 5:
        return {"bars": [], "n": 0, "axis_labels": []}

    n = len(candles)
    # Sample down to ~60 bars for visual density.
    step = max(1, n // 60)
    sampled = candles[::step][:60]
    n_s = len(sampled)
    if n_s == 0:
        return {"bars": [], "n": 0, "axis_labels": []}

    highs = [r[2] for r in sampled if r[2] is not None]
    lows  = [r[3] for r in sampled if r[3] is not None]
    if not highs or not lows:
        return {"bars": [], "n": 0, "axis_labels": []}
    p_min = min(lows) - 2
    p_max = max(highs) + 2
    span = p_max - p_min
    if span <= 0:
        span = 1.0

    # Reserve bottom 60px for volume; price area = top 200px.
    price_top = 10
    price_bottom = 200
    inner_h = price_bottom - price_top

    def y_for(price: float) -> float:
        return price_bottom - (price - p_min) / span * inner_h

    # x-positioning: 12px left margin, 576px width.
    w_x = (vb_w - 24) / max(n_s - 1, 1)
    body_w = max(2, min(8, int(w_x * 0.6)))

    # Volume scaling — bottom strip is 60px tall.
    vols = [r[5] for r in sampled if r[5] is not None]
    v_max = max(vols) if vols else 1
    v_strip_top = 220
    v_strip_h = 50

    bars = []
    for i, row in enumerate(sampled):
        if len(row) < 6:
            continue
        ts, o, h, l, c, v = row[:6]
        if any(x is None for x in [o, h, l, c]):
            continue
        x = 12 + i * w_x
        up = c >= o
        bar_top = y_for(max(o, c))
        bar_bot = y_for(min(o, c))
        body_h = max(2, bar_bot - bar_top)
        vol_h = (v / v_max * v_strip_h) if v and v_max else 0
        bars.append({
            "x":       round(x, 1),
            "body_x":  round(x - body_w / 2, 1),
            "body_w":  body_w,
            "wick_y1": round(y_for(h), 1),
            "wick_y2": round(y_for(l), 1),
            "body_y":  round(bar_top, 1),
            "body_h":  round(body_h, 1),
            "up":      up,
            "vol_x":   round(x - body_w / 2, 1),
            "vol_y":   round(v_strip_top + v_strip_h - vol_h, 1),
            "vol_h":   round(vol_h, 1),
            # Raw OHLCV for the hover tooltip — kept separate from the SVG
            # geometry so the JSON payload stays readable.
            "t":       int(ts) if ts is not None else 0,
            "o":       round(float(o), 2),
            "h":       round(float(h), 2),
            "l":       round(float(l), 2),
            "c":       round(float(c), 2),
            "v":       int(v) if v is not None else 0,
        })

    return {
        "bars":        bars,
        "n":           n_s,
        "vb_w":        vb_w,
        "axis_labels": _stock_chart_axis_labels(bars, period),
    }


async def build_stock_data_bundle(fund_cache: dict, ticker: str) -> tuple[dict, dict]:
    """Build the cacheable stock-page bundle for *ticker*.

    ``fund_cache`` is the in-memory 13F dict (typically
    ``app.state.fund_cache``); used by the meta + ownership-funds
    fetchers.  Plumbing it explicitly (rather than via Request)
    keeps this callable from any context -- request handler, cron,
    admin endpoint.

    Returns ``(bundle, source_status)``:
      * ``bundle`` -- dict with keys ``overview``, ``meta``,
        ``sentiment``, ``ownership_funds``, ``ownership_insiders``,
        ``ownership_congress``, ``ownership_filings``, ``financials``,
        ``forecasts``, ``signals_aux``, ``signals_data`` (derived),
        ``kpis`` (derived).  Excluded: candlestick chart geometry
        (recomputed per-period from ``overview.candles``), watchlist
        state (per-user), shell context (per-request).
      * ``source_status`` -- per-source result map
        (``{"overview": "ok", "financials": "timeout", ...}``) for
        partial-data diagnostics.

    The canonical fanout for stock-page data: ``preview_stock`` calls
    this on cache miss; the warmer calls this for hot/warm tiers.
    """
    source_status: dict[str, str] = {}

    async def _bounded(coro, *, timeout: float, fallback, name: str):
        try:
            result = await asyncio.wait_for(coro, timeout=timeout)
            # `to_upstream` returns None when the circuit breaker is
            # open for a source -- treat that as "no data for now" and
            # render the fallback (same shape as a timeout).  Without
            # this guard the template would receive None where it
            # expects a dict.
            if result is None:
                source_status[name] = "circuit_open"
                return fallback
            source_status[name] = "ok"
            return result
        except asyncio.TimeoutError:
            logger.warning("Stock %s bundle: %s timed out after %.1fs",
                           ticker, name, timeout)
            source_status[name] = "timeout"
        except Exception as exc:
            logger.warning("Stock %s bundle: %s failed: %s", ticker, name, exc)
            source_status[name] = f"error:{type(exc).__name__}"
        return fallback

    payload, meta, sentiment_data, ownership_funds, ownership_insiders, \
        ownership_congress, ownership_filings, financials, forecasts, \
        signals_aux = await asyncio.gather(
        _bounded(_fetch_stock_overview(ticker),                      timeout=8.0,
                 fallback={"ticker": ticker.upper(), "name": ticker.upper(),
                           "exchange": "—", "cusip": "—", "price": None,
                           "chg_pct": None, "chg_abs": None,
                           "open": "—", "high": "—", "low": "—", "close": "—",
                           "volume": "—", "avg_vol": "—", "mcap": "—", "pe": "—",
                           "high_52": "—", "low_52": "—", "candles": [],
                           "news": [], "is_mock": True},
                 name="overview"),
        _bounded(_fetch_stock_meta(fund_cache, ticker),              timeout=6.0,
                 fallback={"sector": "—", "industry": "—", "exchange": "—",
                           "cusip": "—", "mcap": "—", "pe": "—", "holders": 0,
                           "about_blurb": "", "about_ceo": "—",
                           "about_employees": "—", "about_hq": "—",
                           "about_ipo": "—", "about_website": "—",
                           "logo_url": ""},
                 name="meta"),
        # Sentiment fans out to ApeWisdom -- route through the
        # per-source gate (2 concurrent calls + circuit breaker after
        # 3 timeouts in 60s).  Previously on `to_light` which used
        # the unbounded default pool and was the source of the
        # 2026-05-10 / -11 prod saturations.
        _bounded(to_upstream("apewisdom", _stock_build_sentiment, ticker), timeout=4.0,
                 fallback={"has_data": False, "score": 0, "score_str": "—",
                           "score_class": "", "mentions": "—", "rank_str": "",
                           "cohort": [], "note": "", "fill_pct": 0, "fill_left": 50},
                 name="sentiment"),
        _bounded(to_light(_stock_build_ownership_funds, fund_cache, ticker), timeout=2.0,
                 fallback={"rows": [], "total_count": 0, "total_value": "—",
                           "activity_chart": {"labels": [], "adds": [], "reduces": [],
                                              "adds_count": [], "reduces_count": []},
                           "quarters": []},
                 name="ownership_funds"),
        _bounded(to_heavy(_stock_build_ownership_insiders, ticker),  timeout=4.0,
                 fallback={"insiders": [], "quarters": [], "chart": None,
                           "per_insider_chart": {}, "insights": None,
                           "total_count": 0},
                 name="ownership_insiders"),
        _bounded(to_supabase(_stock_build_ownership_congress, ticker), timeout=3.0,
                 fallback={"rows": [], "total_count": 0,
                           "politicians": [], "party_breakdown": None,
                           "total_politicians": 0, "total_trades": 0},
                 name="ownership_congress"),
        _bounded(_stock_build_ownership_filings(ticker),             timeout=4.0,
                 fallback={"rows": []},
                 name="ownership_filings"),
        _bounded(to_heavy(_stock_build_financials, ticker),          timeout=6.0,
                 fallback={"has_data": False, "kpis": _STOCK_MOCK_FIN_KPIS,
                           "annual": None, "quarterly": None, "chart_data": {}},
                 name="financials"),
        _bounded(to_heavy(_stock_build_forecasts, ticker, None),     timeout=6.0,
                 fallback={"has_data": False, "ratings": _STOCK_FCT_RATINGS,
                           "ratings_total": sum(r[1] for r in _STOCK_FCT_RATINGS),
                           "analysts": _STOCK_FCT_ANALYSTS,
                           "rev_est": _STOCK_FCT_REV_EST,
                           "eps": _STOCK_FCT_EPS_REVISIONS,
                           "target_band": _STOCK_FCT_TARGET_BAND,
                           "now_pct": _STOCK_FCT_PRICE_PCT,
                           "consensus": "BUY",
                           "grouped": [], "current_price": None,
                           "earnings": {}, "est_eps": [], "est_revenue": []},
                 name="forecasts"),
        _bounded(_stock_build_signals_data(ticker),                  timeout=6.0,
                 fallback={"cnn": None, "finnhub": None, "apewisdom": None,
                           "alphavantage": None, "gt_keywords": None,
                           "gt_trend": None, "wt_tranco": None,
                           "wt_wikipedia": None, "short_interest": None,
                           "short_interest_history": []},
                 name="signals_aux"),
    )

    # Re-anchor the price tick on the analyst target distribution once the
    # live quote has landed (forecasts ran in parallel without it).
    cur_price = payload.get("price")
    if cur_price:
        forecasts["current_price"] = cur_price
    if forecasts.get("has_data") and cur_price:
        band = forecasts["target_band"]
        span = band["high"] - band["low"] or 1
        forecasts["now_pct"] = max(0.0, min(100.0,
            round((cur_price - band["low"]) / span * 100, 1)))

    # News -- runs serially after meta so the brand-name filter has access
    # to the resolved company name (drops Finnhub-related-tag noise).
    payload["news"] = await _bounded(
        _fetch_stock_news(ticker, payload.get("name") or meta.get("name") or ticker),
        timeout=8.0, fallback=[], name="news",
    )

    # Overlay real metadata onto payload so KPI strip + hero pills render
    # live values instead of em-dashes.
    payload["mcap"]     = meta["mcap"]
    payload["pe"]       = meta["pe"]
    if meta["exchange"] != "—":
        payload["exchange"] = meta["exchange"]
    if meta["cusip"] != "—":
        payload["cusip"] = meta["cusip"]

    # Derived: signals + KPIs.  Pure dict reductions; cheap but cached
    # so the page handler doesn't redo them on every render.
    signals_data = _stock_compute_signals(
        sentiment=sentiment_data, ownership_funds=ownership_funds,
        ownership_insiders=ownership_insiders, ownership_congress=ownership_congress,
        forecasts=forecasts, payload=payload, meta=meta,
        short_interest=signals_aux.get("short_interest"),
    )
    kpis = _stock_kpi_strip(payload)

    bundle = {
        "overview":           payload,
        "meta":               meta,
        "sentiment":          sentiment_data,
        "ownership_funds":    ownership_funds,
        "ownership_insiders": ownership_insiders,
        "ownership_congress": ownership_congress,
        "ownership_filings":  ownership_filings,
        "financials":         financials,
        "forecasts":          forecasts,
        "signals_aux":        signals_aux,
        "signals_data":       signals_data,
        "kpis":               kpis,
    }
    return bundle, source_status


def _format_news_items(raw_news: list) -> list[dict]:
    """Normalise news-payload field names for the right-rail panel.

    Sources (Finnhub vs market_data demo fallback) emit different
    keys; pick whichever's present, default to em-dash.
    """
    out = []
    for n in raw_news or []:
        out.append({
            "src":   n.get("src") or n.get("source") or n.get("publisher") or "—",
            "ago":   n.get("ago") or n.get("relative_time") or n.get("time_ago") or "",
            "title": n.get("title") or n.get("headline") or "",
            "url":   n.get("url") or "",
        })
    return out


def _build_stock_template_ctx(
    request: Request,
    bundle: dict,
    *,
    stock_watching: bool,
    signed_in: bool,
    shell_ctx: dict,
) -> dict:
    """Flatten the bundle into the wide ctx the stock template expects.

    Pure data transform from the stored bundle shape into the ~60 ctx
    keys the template consumes.  Includes per-render derivations
    (chart geometry from candles, SVG paths for the EPS chart,
    analyst-tick percentages relative to the target band).
    """
    payload  = bundle.get("overview")           or {}
    meta     = bundle.get("meta")               or {}
    sentiment_data    = bundle.get("sentiment")          or {}
    ownership_funds   = bundle.get("ownership_funds")    or {}
    ownership_insiders = bundle.get("ownership_insiders") or {}
    ownership_congress = bundle.get("ownership_congress") or {}
    ownership_filings  = bundle.get("ownership_filings")  or {}
    financials = bundle.get("financials")        or {}
    forecasts  = bundle.get("forecasts")         or {}
    signals_aux  = bundle.get("signals_aux")     or {}
    signals_data = bundle.get("signals_data")    or {}
    kpis         = bundle.get("kpis")            or []

    # Initial page render uses 1M (default range_chip selection);
    # range-chip clicks swap via /stock/{ticker}/chart/{p}.
    chart = _stock_candlestick_paths(payload.get("candles") or [], period="1M")
    news_items = _format_news_items(payload.get("news"))[:6]

    # Forecasts EPS chart geometry (server-rendered SVG line).
    eps_series = forecasts.get("eps") or _STOCK_FCT_EPS_REVISIONS
    eps_min = min(eps_series) - 1
    eps_max = max(eps_series) + 1
    eps_path = _stock_line_path(eps_series, eps_min, eps_max)
    eps_pts  = _stock_chart_points(eps_series, eps_min, eps_max)

    # Analyst price-target ticks for the Forecasts number-line.
    band = forecasts.get("target_band") or _STOCK_FCT_TARGET_BAND
    span = (band["high"] - band["low"]) or 1
    analyst_ticks = [
        {**a, "pct":  max(0.0, min(100.0,
                          round((a["target"] - band["low"]) / span * 100, 1))),
              "delta": round(a["target"] - a["prev"], 2)}
        for a in (forecasts.get("analysts") or [])
    ]

    return {
        "request":      request,
        **shell_ctx,    # Stock page doesn't highlight a sidebar item
        # Header
        "stock_ticker":   payload.get("ticker"),
        "stock_name":     payload.get("name"),
        "stock_exchange": payload.get("exchange"),
        "stock_cusip":    payload.get("cusip"),
        "stock_sector":   meta.get("sector"),
        "stock_industry": meta.get("industry"),
        "stock_mcap":     meta.get("mcap"),
        "stock_holders":  meta.get("holders"),
        "stock_logo":     meta.get("logo_url"),
        # About panel
        "about_blurb":     meta.get("about_blurb"),
        "about_ceo":       meta.get("about_ceo"),
        "about_employees": meta.get("about_employees"),
        "about_hq":        meta.get("about_hq"),
        "about_ipo":       meta.get("about_ipo"),
        "about_website":   meta.get("about_website"),
        # Retail sentiment panel (ApeWisdom-backed)
        "sentiment":      sentiment_data,
        "stock_price":    _stock_format_price(payload.get("price")),
        "stock_chg_pct":  payload.get("chg_pct"),
        "stock_chg_abs":  payload.get("chg_abs"),
        "stock_chg_up":   (payload.get("chg_pct") or 0) >= 0,
        "stock_close":    payload.get("close"),
        # Overview body
        "stock_kpi":      kpis,
        "stock_chart":    chart,
        "stock_news":     news_items,
        "stock_is_mock":  payload.get("is_mock", False),
        "stock_tab":      "Overview",
        # Financials tab
        "fin_kpis":        financials.get("kpis") or _STOCK_MOCK_FIN_KPIS,
        "fin_annual":      financials.get("annual"),
        "fin_quarterly":   financials.get("quarterly"),
        "fin_chart_data":  financials.get("chart_data") or {},
        "fin_has_data":    financials.get("has_data", False),
        # Ownership tab
        "own_funds":             ownership_funds.get("rows") or [],
        "own_funds_count":       ownership_funds.get("total_count", 0),
        "own_funds_value":       ownership_funds.get("total_value", "—"),
        "own_funds_chart":       ownership_funds.get("activity_chart") or {},
        "own_funds_quarters":    ownership_funds.get("quarters") or [],
        "own_insiders":             ownership_insiders.get("insiders") or [],
        "own_insiders_count":       ownership_insiders.get("total_count", 0),
        "own_insiders_quarters":    ownership_insiders.get("quarters") or [],
        "own_insiders_chart":       ownership_insiders.get("chart"),
        "own_insiders_per_chart":   ownership_insiders.get("per_insider_chart") or {},
        "own_insiders_insights":    ownership_insiders.get("insights"),
        "own_congress":          ownership_congress.get("rows") or [],
        "own_congress_count":    ownership_congress.get("total_count", 0),
        "own_congress_pols":     ownership_congress.get("politicians") or [],
        "own_congress_party":    ownership_congress.get("party_breakdown"),
        "own_congress_n_pols":   ownership_congress.get("total_politicians", 0),
        "own_congress_n_trades": ownership_congress.get("total_trades", 0),
        "own_filings":           ownership_filings.get("rows") or [],
        # Forecasts tab
        "fct_ratings":       forecasts.get("ratings") or _STOCK_FCT_RATINGS,
        "fct_ratings_total": forecasts.get("ratings_total", sum(r[1] for r in _STOCK_FCT_RATINGS)),
        "fct_analysts":      analyst_ticks,
        "fct_rev_est":       forecasts.get("rev_est") or _STOCK_FCT_REV_EST,
        "fct_eps_path":      eps_path,
        "fct_eps_pts":       eps_pts,
        "fct_target_low":    band["low"],
        "fct_target_high":   band["high"],
        "fct_now_pct":       forecasts.get("now_pct", _STOCK_FCT_PRICE_PCT),
        "fct_consensus":     forecasts.get("consensus", "BUY"),
        "fct_grouped":       forecasts.get("grouped") or [],
        "fct_current_price": forecasts.get("current_price"),
        "fct_earnings":      forecasts.get("earnings") or {},
        "fct_est_eps":       forecasts.get("est_eps") or [],
        "fct_est_revenue":   forecasts.get("est_revenue") or [],
        # Signals tab
        "sig_signals":         signals_data.get("signals") or [],
        "sig_composite":       signals_data.get("composite"),
        "sig_composite_score": signals_data.get("composite_score"),
        "sig_cnn":          signals_aux.get("cnn"),
        "sig_finnhub":      signals_aux.get("finnhub"),
        "sig_apewisdom":    signals_aux.get("apewisdom"),
        "sig_alphavantage": signals_aux.get("alphavantage"),
        "sig_gt_keywords":  signals_aux.get("gt_keywords"),
        "sig_gt_trend":     signals_aux.get("gt_trend"),
        "sig_wt_tranco":    signals_aux.get("wt_tranco"),
        "sig_wt_wikipedia": signals_aux.get("wt_wikipedia"),
        "sig_short_interest":         signals_aux.get("short_interest"),
        "sig_short_interest_history": signals_aux.get("short_interest_history") or [],
        # Watchlist state
        "stock_watching":       stock_watching,
        "stock_user_signed_in": signed_in,
    }


@router.get("/stock/{ticker}", response_class=HTMLResponse)
@_maybe_rate_limit("30/minute")
async def preview_stock(request: Request, ticker: str):
    """Stock detail.

    Reads the pre-aggregated bundle from ``stock_overview_cache`` if
    fresh; otherwise builds it live via :func:`build_stock_data_bundle`
    and schedules a cold-tier write back so the next request hits the
    cache.  Per-user state (watchlist) and per-render derivations
    (chart geometry, EPS SVG path, analyst ticks) are computed off
    the bundle at render time.

    Rate-limited at 30/minute/IP (slowapi) -- real users browse one
    ticker every 2-5s at most; sustained 30+/min is the crawler
    pattern that drove the cold-path thread leak.  Limit applies
    per-IP via ``X-Forwarded-For``.
    """
    fund_cache = _request_fund_cache(request)

    user = getattr(request.state, "user", None)
    user_id = (user or {}).get("sub") if user else None

    async def _watching() -> bool:
        if not user_id:
            return False
        try:
            from filings import supabase_cache
            return await asyncio.to_thread(
                supabase_cache.is_ticker_watched, user_id, ticker.upper(),
            )
        except Exception as exc:
            logger.debug("is_ticker_watched(%s) failed: %s", ticker, exc)
            return False

    async def _bundle_or_fresh() -> dict:
        cached = await asyncio.to_thread(stock_bundle.get_bundle, ticker)

        if cached and stock_bundle.is_fresh(cached["refreshed_at"], cached["tier"]):
            stock_bundle.record_hit()
            return cached["bundle"]

        # Stale-While-Revalidate: cached row exists, past tier TTL but
        # under MAX_STALE_AGE_S.  Serve the stale bundle immediately
        # (sub-second response) and refresh in background so the next
        # request gets fresh data.  This is the path that turns a
        # 6-second cold-rebuild into a 300ms response on the dominant
        # case (warmer permanently behind the universe of tickers).
        if (cached
                and cached.get("bundle")
                and not stock_bundle.is_too_stale(cached["refreshed_at"])):
            t_up = ticker.upper()
            # Two-level gating on the bg refresh:
            #   1. per-ticker debounce -- N concurrent stale requests
            #      for the same ticker fire only one refresh.
            #   2. global cap (`_SWR_MAX_CONCURRENT`) -- caps total
            #      concurrent bg refreshes so SWR can't pin the heavy
            #      pool the way the warmer used to.
            # If we hit either gate, just serve the stale bundle and
            # let the next request re-arm; worst case the warmer
            # picks the ticker up.
            if (t_up not in _swr_refreshing
                    and len(_swr_refreshing) < _SWR_MAX_CONCURRENT):
                _swr_refreshing.add(t_up)
                stock_bundle.record_bg_refresh()
                _track_bg(
                    _swr_refresh_bundle(ticker, fund_cache, cached["tier"]),
                    name=f"stock-bundle-swr:{ticker}",
                )
            stock_bundle.record_stale_served()
            return cached["bundle"]

        # True cold miss OR ancient (>MAX_STALE_AGE_S) cache: must
        # block on a synchronous rebuild.  This is the only path
        # where the user sees the full fanout latency.
        stock_bundle.record_miss(had_cached=cached is not None)

        # Backpressure: if the heavy pool is already saturated, don't
        # queue another ~10-call cold-path fanout behind it.  Serve
        # stale even past MAX_STALE_AGE_S (better than nothing); 503
        # only when there's genuinely nothing to serve.
        if is_heavy_saturated():
            stale_bundle = (cached or {}).get("bundle") or {}
            if stale_bundle:
                logger.debug(
                    "backpressure: serving stale bundle for %s (heavy pool full)",
                    ticker,
                )
                return stale_bundle
            raise HTTPException(
                status_code=503,
                detail="Data temporarily unavailable. Please try again shortly.",
            )

        bundle, source_status = await build_stock_data_bundle(fund_cache, ticker)
        # Preserve an existing tier classification when refreshing a
        # seeded row (hot/warm); default to cold for ad-hoc misses.
        # Without this, the request path would clobber a hot ticker
        # back to cold and the warmer would stop refreshing it.
        write_tier = cached["tier"] if cached else stock_bundle.COLD_TIER
        _track_bg(asyncio.to_thread(
            stock_bundle.set_bundle, ticker, bundle,
            tier=write_tier, source_status=source_status,
        ), name=f"stock-bundle-write:{ticker}")
        return bundle

    bundle, stock_watching, shell_ctx = await asyncio.gather(
        _bundle_or_fresh(),
        _watching(),
        _shell_context(request, ""),
    )

    ctx = _build_stock_template_ctx(
        request, bundle,
        stock_watching=stock_watching,
        signed_in=bool(user_id),
        shell_ctx=shell_ctx,
    )
    return templates.TemplateResponse("_redesign/stock.html", ctx)


_CHART_PERIODS = {"1D", "1W", "1M", "3M", "1Y", "5Y"}


@router.get("/stock/{ticker}/chart/{period}", response_class=HTMLResponse)
async def preview_stock_chart(request: Request, ticker: str, period: str):
    """Partial: just the candlestick + volume SVG bars + axis labels.

    Powers the range-chip click handler — JS swaps the inner HTML of
    ``.pp-stock-chart-body`` instead of refetching the whole page.
    """
    period_norm = period.upper() if period.upper() in _CHART_PERIODS else "1M"

    try:
        from filings import market_data
        ohlcv = await to_heavy(market_data.get_stock_ohlcv, ticker.upper(), period_norm)
    except Exception as exc:
        logger.warning("Stock OHLCV(%s, %s) failed: %s", ticker, period_norm, exc)
        ohlcv = None

    candles = (ohlcv or {}).get("ohlcv") or []
    chart = _stock_candlestick_paths(candles, period=period_norm)
    return templates.TemplateResponse(
        "_redesign/partials/stock_chart.html",
        {"request": request, "stock_chart": chart, "period": period_norm,
         "ticker": ticker.upper()},
    )
