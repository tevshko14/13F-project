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
import logging
import math
import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from filings.app_state import templates

logger = logging.getLogger(__name__)


def is_enabled() -> bool:
    """True when the preview router should be mounted.

    Read at import time in web.py — flipping the env var requires a
    web restart (which is the local-dev workflow anyway: ``preview_start``
    re-launches the process every time).
    """
    return os.environ.get("PP_REDESIGN_PREVIEW", "").lower() in ("1", "true", "yes")


router = APIRouter(prefix="/_v2", tags=["redesign-preview"])


# ─────────────────────────────────────────────────────────────────────────────
# Mock data — mirrors design_handoff_paperpanda/data.js so visual pages
# match the design canvas pixel-for-pixel.
# These get replaced with live data at the route-flip step.
# ─────────────────────────────────────────────────────────────────────────────


def _today_label() -> str:
    """Reproduce the topbar kicker format ('FRI MAY 2, 2026 · MARKETS OPEN')."""
    now = datetime.now(ZoneInfo("America/New_York"))
    return now.strftime("%a %b %-d, %Y").upper()


def _market_status() -> str:
    """Naive market-hours check — returns "MARKETS OPEN" / "MARKETS CLOSED".
    Matches the design's display, doesn't account for holidays."""
    now = datetime.now(ZoneInfo("America/New_York"))
    if now.weekday() >= 5:  # Saturday/Sunday
        return "MARKETS CLOSED"
    h, m = now.hour, now.minute
    open_time = (9, 30)
    close_time = (16, 0)
    is_open = (h, m) >= open_time and (h, m) < close_time
    return "MARKETS OPEN" if is_open else "MARKETS CLOSED"


# Sparkline series from data.js — small ~normalized arrays for the SVG path
SPARK = [
    0.32, 0.38, 0.41, 0.39, 0.45, 0.52, 0.48, 0.55, 0.6, 0.58, 0.62, 0.66,
    0.63, 0.7, 0.72, 0.68, 0.75, 0.78, 0.74, 0.81, 0.84, 0.8, 0.86, 0.83,
    0.88, 0.91, 0.87, 0.93,
]
SPARK_DOWN = [
    0.85, 0.82, 0.79, 0.81, 0.74, 0.72, 0.7, 0.68, 0.71, 0.65, 0.62, 0.6,
    0.58, 0.61, 0.54, 0.5, 0.52, 0.48, 0.45, 0.41, 0.43, 0.38, 0.34, 0.36,
    0.3, 0.28, 0.31, 0.25,
]


def _shell_context(active: str) -> dict:
    """Common context every redesign page needs for the app shell."""
    return {
        "nav_active": active,
        "today_label": _today_label(),
        "market_status": _market_status(),
        "panda_raised": 84,
        "panda_goal": 200,
        "panda_month": datetime.now().strftime("%b"),
        "panda_pct": 42,
        "user_initials": "JK",
        "notif_unread": 2,
        "insiders_count": 12,
        "congress_count": 3,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Index — links to every preview page so we can navigate the build.
# Acts as a contact sheet of all redesign pages while they're in progress.
# ─────────────────────────────────────────────────────────────────────────────


@router.get("", response_class=HTMLResponse)
async def preview_index(request: Request):
    """Preview index — lists every redesign route."""
    pages = [
        ("Home",      "/_v2/home"),
        ("Stock",     "/_v2/stock/AAPL"),
        ("Funds",     "/_v2/funds"),
        ("Screener",  "/_v2/screener"),
        ("Insiders",  "/_v2/insiders"),
        ("Congress",  "/_v2/congress"),
        ("Macro",     "/_v2/macro"),
        ("Retail",    "/_v2/retail"),
        ("Options",   "/_v2/options"),
        ("Profile",   "/_v2/profile"),
    ]
    return templates.TemplateResponse(
        "_redesign/_preview_index.html",
        {"request": request, "pages": pages, **_shell_context("Home")},
    )


# ─────────────────────────────────────────────────────────────────────────────
# Page routes — wired to real data through existing modules where possible,
# with mock-only fallbacks for sections that would otherwise be empty.
# ─────────────────────────────────────────────────────────────────────────────


# KPI strip: which 5 yfinance symbols feed the Home masthead, in display order.
# Each entry is (symbol, label, formatter).  The formatter takes the raw price
# and returns a display string matching the design — indices show "5,847.42",
# VIX shows "14.82", and ^TNX (the 10-Year Treasury yield) shows "4.214%".
_KPI_INDICES: list[tuple[str, str, str]] = [
    ("^GSPC", "S&P 500", "comma2"),
    ("^IXIC", "Nasdaq",  "comma2"),
    ("^DJI",  "Dow",     "comma2"),
    ("^VIX",  "VIX",     "two"),
    ("^TNX",  "10Y",     "yield3"),
]


def _format_kpi_value(price: float, kind: str) -> str:
    """Format a KPI value to match the design's exact rendering."""
    if price is None:
        return "—"
    if kind == "comma2":
        # Indices: thousands separator + 2 decimals → "5,847.42"
        return f"{price:,.2f}"
    if kind == "yield3":
        # 10-Year Treasury yield: 3 decimals + percent sign → "4.214%"
        return f"{price:.3f}%"
    if kind == "two":
        # VIX: just 2 decimals, no comma (always < 1000) → "14.82"
        return f"{price:.2f}"
    return str(price)


def _format_kpi_delta(pct_change: float | None) -> str:
    """Format the delta string (no leading sign — arrow conveys direction)."""
    if pct_change is None:
        return ""
    return f"{abs(pct_change) * 100:.2f}%"


def _daily_pct_from_history(history: list | None) -> float | None:
    """Return the most recent day's percent change as a fractional float
    (e.g. 0.0072 = +0.72%) from a [[epoch_ms, close], ...] history list.

    The KPI strip needs DAILY change (today close vs prior close), but
    get_index_market_data() returns 1Y history with a `pct_change` field
    computed over the full window — too long for the design's intent.
    """
    if not history or len(history) < 2:
        return None
    try:
        prev = float(history[-2][1])
        last = float(history[-1][1])
        if prev == 0:
            return None
        return (last - prev) / prev
    except (TypeError, ValueError, IndexError):
        return None


async def _fetch_kpi_strip() -> list[dict]:
    """Build the 5-cell KPI strip from real market data.

    Goes through filings.market_data.get_index_market_data() which already
    has L1 in-memory + L2 Supabase caching, so this is fast on warm cache.
    Daily percent change is recomputed from the tail of the 1Y history
    series (the row's own `pct_change` is full-window — wrong for the
    daily-tape framing of this strip).
    Fallback is the static design values so the page never breaks if
    yfinance / Supabase are unavailable.
    """
    fallback = [
        {"label": "S&P 500", "value": "5,847.42",  "delta": "0.72%", "up": True},
        {"label": "Nasdaq",  "value": "20,194.18", "delta": "0.45%", "up": True},
        {"label": "Dow",     "value": "42,233.71", "delta": "0.04%", "up": False},
        {"label": "VIX",     "value": "14.82",     "delta": "2.05%", "up": False},
        {"label": "10Y",     "value": "4.214%",    "delta": "0.48%", "up": True},
    ]
    try:
        from filings import market_data

        data = await asyncio.to_thread(market_data.get_index_market_data)
        if not data:
            return fallback
        items = []
        for sym, label, kind in _KPI_INDICES:
            row = data.get(sym)
            if not row:
                items.append(next((f for f in fallback if f["label"] == label), fallback[0]))
                continue
            price = row.get("price")
            daily_pct = _daily_pct_from_history(row.get("history"))
            items.append({
                "label": label,
                "value": _format_kpi_value(price, kind),
                "delta": _format_kpi_delta(daily_pct),
                "up":   (daily_pct is not None and daily_pct >= 0),
            })
        return items
    except Exception as exc:
        logger.warning("KPI strip live fetch failed, using design fallback: %s", exc)
        return fallback


def _build_intraday_chart() -> dict:
    """Reproduce the SVG path the JSX home page generates client-side.

    The JSX uses Math.sin(i*0.4) + Math.cos(i*0.9) - i*0.55 — a synthetic
    intraday price walk.  We compute the same path server-side so the
    rendered page matches the design canvas pixel-for-pixel.
    """
    pts = []
    for i in range(60):
        x = i * 10
        y = 110 + math.sin(i * 0.4) * 30 + math.cos(i * 0.9) * 10 - i * 0.55
        pts.append((x, y))

    line_d = " ".join(
        f"{'M' if i == 0 else 'L'}{x:.1f} {y:.1f}" for i, (x, y) in enumerate(pts)
    )
    # Closed area path — line + bottom-right + bottom-left + close
    area_d = f"{line_d} L 600 200 L 0 200 Z"
    return {"line": line_d, "area": area_d}


# Mock home page payload — mirrors window.PP_DATA in design_handoff/data.js
_HOME_TOP_MOVERS = [
    {"ticker": "NVDA", "name": "NVIDIA",     "last": "142.18", "pct":  0.0421, "vol": "284M",  "spark_series": SPARK},
    {"ticker": "TSLA", "name": "Tesla",      "last": "351.92", "pct":  0.0309, "vol": "98.2M", "spark_series": SPARK},
    {"ticker": "PLTR", "name": "Palantir",   "last":  "82.04", "pct":  0.0612, "vol": "62.1M", "spark_series": SPARK},
    {"ticker": "AAPL", "name": "Apple",      "last": "232.71", "pct": -0.0083, "vol": "44.8M", "spark_series": SPARK_DOWN},
    {"ticker": "META", "name": "Meta",       "last": "612.40", "pct":  0.0142, "vol": "12.7M", "spark_series": SPARK},
    {"ticker": "AMZN", "name": "Amazon",     "last": "226.08", "pct": -0.0051, "vol": "31.4M", "spark_series": SPARK_DOWN},
]

_HOME_FUND_FLOWS = [
    {"manager": "Warren Buffett",       "fund": "Berkshire Hathaway", "aum": "$312B",  "action": "ADDED",   "ticker": "OXY"},
    {"manager": "Bill Ackman",          "fund": "Pershing Square",    "aum": "$11.4B", "action": "NEW",     "ticker": "BAM"},
    {"manager": "Michael Burry",        "fund": "Scion Asset Mgmt",   "aum": "$83M",   "action": "REDUCED", "ticker": "JD"},
    {"manager": "David Einhorn",        "fund": "Greenlight Capital", "aum": "$1.6B",  "action": "ADDED",   "ticker": "GRBK"},
    {"manager": "Daniel Loeb",          "fund": "Third Point",        "aum": "$5.9B",  "action": "EXITED",  "ticker": "PCG"},
    {"manager": "David Tepper",         "fund": "Appaloosa",          "aum": "$6.2B",  "action": "ADDED",   "ticker": "BABA"},
]

_HOME_INSIDERS = [
    {"person": "Cook, Tim",        "role": "CEO", "ticker": "AAPL",  "action": "SELL", "value": "$52.1M"},
    {"person": "Huang, Jen-Hsun",  "role": "CEO", "ticker": "NVDA",  "action": "SELL", "value": "$17.0M"},
    {"person": "Musk, Elon",       "role": "CEO", "ticker": "TSLA",  "action": "BUY",  "value": "$17.6M"},
    {"person": "Pichai, Sundar",   "role": "CEO", "ticker": "GOOGL", "action": "SELL", "value": "$4.3M"},
    {"person": "Zuckerberg, Mark", "role": "CEO", "ticker": "META",  "action": "SELL", "value": "$23.2M"},
]

_HOME_CONGRESS = [
    {"person": "Pelosi, Nancy",    "party": "D", "chamber": "House",  "ticker": "NVDA",  "action": "BUY",  "size": "$1M-5M"},
    {"person": "Tuberville, T.",   "party": "R", "chamber": "Senate", "ticker": "AAPL",  "action": "BUY",  "size": "$50K-100K"},
    {"person": "Crenshaw, Dan",    "party": "R", "chamber": "House",  "ticker": "MSFT",  "action": "BUY",  "size": "$15K-50K"},
    {"person": "Khanna, Ro",       "party": "D", "chamber": "House",  "ticker": "GOOGL", "action": "SELL", "size": "$15K-50K"},
    {"person": "Bresnahan, Rob",   "party": "R", "chamber": "House",  "ticker": "AMZN",  "action": "BUY",  "size": "$1K-15K"},
]

_HOME_MACRO = [
    {"label": "Fed Funds",    "val": "4.25-4.50%", "chg": "—",      "note": "Mar FOMC"},
    {"label": "Core CPI YoY", "val": "3.1%",       "chg": "-0.1pp", "note": "Mar 2026"},
    {"label": "Unemployment", "val": "4.0%",       "chg": "+0.1pp", "note": "Apr 2026"},
    {"label": "M2 Supply",    "val": "$21.8T",     "chg": "+0.4%",  "note": "Mar 2026"},
]

_HOME_RETAIL_FEAT = {"ticker": "GME", "mentions": "4,218", "sentiment": 62}

# 4 retail rows below the featured one — sentiment in [-1,1].
# bar_pct + bar_color computed once so the template can stay simple.
_HOME_RETAIL_RAW = [
    {"ticker": "TSLA", "mentions": 3104, "sentiment":  0.18},
    {"ticker": "NVDA", "mentions": 2891, "sentiment":  0.71},
    {"ticker": "AMC",  "mentions": 1842, "sentiment":  0.04},
    {"ticker": "PLTR", "mentions": 1611, "sentiment":  0.55},
]


def _retail_rows():
    rows = []
    for r in _HOME_RETAIL_RAW:
        # Match the JSX: bar width = min(mentions/45, 100)%
        bar_pct = min(r["mentions"] / 45, 100)
        # Coral if sentiment in [0, 0.3), green if >=0.3, red if <0
        s = r["sentiment"]
        if s >= 0.3:
            color = "var(--pp-up)"
        elif s >= 0:
            color = "var(--pp-accent)"
        else:
            color = "var(--pp-down)"
        rows.append({**r, "bar_pct": bar_pct, "bar_color": color})
    return rows


@router.get("/home", response_class=HTMLResponse)
async def preview_home(request: Request):
    """Home page — masthead + real KPI strip + hero + 3 grid sections."""
    chart = _build_intraday_chart()
    kpi_items = await _fetch_kpi_strip()
    ctx = {
        "request": request,
        **_shell_context("Home"),

        # Masthead copy — kicker rendered uppercase via CSS.
        "mast_kicker":  "PaperPanda · Intelligence",
        "mast_h1":      "A sharper market dashboard for modern investors.",
        "mast_sub":     "Track 85 superinvestor funds, 201 members of Congress, and thousands of insider trades — powered by SEC EDGAR, STOCK Act filings, and Federal Reserve data.",

        # KPI strip — REAL data via market_data.get_index_market_data()
        # ^GSPC + ^IXIC + ^DJI + ^VIX + ^TNX (10Y Treasury yield).
        # Falls back to design values if Supabase / yfinance unavailable.
        "kpi_strip_items": kpi_items,

        # Hero chart
        "chart_path":       chart["line"],
        "chart_area":       chart["area"],
        "chart_change":     "41.86",
        "chart_change_pct": "0.72%",
        "chart_tag":        "5847",
        "chart_ohlcv": [
            ("OPEN",  "5,805.56"),
            ("HIGH",  "5,851.20"),
            ("LOW",   "5,798.14"),
            ("VOL",   "3.41B"),
        ],

        # Retail pulse (right side of hero)
        "retail_feat": _HOME_RETAIL_FEAT,
        "retail_rows": _retail_rows(),

        # Body data
        "top_movers": _HOME_TOP_MOVERS,
        "fund_flows": _HOME_FUND_FLOWS,
        "insiders":   _HOME_INSIDERS,
        "congress":   _HOME_CONGRESS,
        "macro":      _HOME_MACRO,
    }
    return templates.TemplateResponse("_redesign/home.html", ctx)
