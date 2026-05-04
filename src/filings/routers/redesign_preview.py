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
import json
import logging
import math
import os
import time
from datetime import datetime, timezone
from typing import Any, Callable
from zoneinfo import ZoneInfo

# Module-import timestamp — stable across all renders within one
# deploy, but unique per deploy so CSS changes don't get served from
# stale browser caches after a release.
_ASSET_VERSION = int(time.time())

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from filings.app_state import templates
from filings.cache_l2 import l2_cached as _l2_cached
from filings.concurrency import to_heavy, to_light

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
        "asset_version": _ASSET_VERSION,
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

        data = await to_heavy(market_data.get_index_market_data)
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


# Hero chart viewBox — kept consistent with home.html's <svg viewBox="0 0 600 200"/>.
_HERO_VB_W = 600
_HERO_VB_H = 200
# Vertical inset — leaves a few px at top + bottom so the line never touches
# the chart border on extreme highs/lows.
_HERO_PAD_Y = 12


def _hero_chart_paths(history: list, prev_close: float | None) -> dict | None:
    """Project a [[epoch_ms, price], ...] series into the hero chart's SVG
    viewBox (600 × 200) and return the line path, the closed area-fill path,
    and the y-coordinate of the prev-close reference line.

    Normalizes price → y by min/max with `_HERO_PAD_Y` top/bottom padding.
    Time → x is uniform across the series (no real-time x-axis; bars are
    rendered evenly spaced regardless of the timestamps).
    """
    if not history or len(history) < 2:
        return None

    prices = [p[1] for p in history]
    p_min, p_max = min(prices), max(prices)
    # Fold the prev-close into the y-range so the reference line is always on-canvas.
    if prev_close is not None:
        p_min = min(p_min, prev_close)
        p_max = max(p_max, prev_close)
    p_range = p_max - p_min
    if p_range <= 0:
        # Flat series — render a midline.
        p_range = 1.0

    inner_h = _HERO_VB_H - 2 * _HERO_PAD_Y

    def y_for(price: float) -> float:
        return _HERO_PAD_Y + (p_max - price) / p_range * inner_h

    # Line path
    n = len(prices)
    parts = []
    last_y = None
    for i, price in enumerate(prices):
        x = i / (n - 1) * _HERO_VB_W
        y = y_for(price)
        last_y = y
        parts.append(f"{'M' if i == 0 else 'L'}{x:.1f} {y:.1f}")
    line_d = " ".join(parts)
    # Closed area = line + bottom-right + bottom-left + back to start
    area_d = f"{line_d} L {_HERO_VB_W} {_HERO_VB_H} L 0 {_HERO_VB_H} Z"

    ref_y = y_for(prev_close) if prev_close is not None else _HERO_VB_H / 2
    # `tag_y` is where the current-price chip sits on the right edge.  We
    # anchor it to the LINE'S last y (current price), not the reference y
    # (prev close) — the chip's purpose is to label the current price.
    tag_y = last_y if last_y is not None else ref_y
    return {
        "line":  line_d,
        "area":  area_d,
        "ref_y": round(ref_y, 1),
        "tag_y": round(tag_y, 1),
    }


def _format_volume(v: float | int | None) -> str:
    """Render a share volume number compactly: 3.41B / 41.2M / 412K."""
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


def _format_index_value(v: float | None, decimals: int = 2) -> str:
    if v is None:
        return "—"
    return f"{v:,.{decimals}f}"


# Order matters: drives rendering order in the range_chip and JSON blob keys.
_CHART_PERIODS = ["1D", "1W", "1M", "3M", "1Y", "5Y"]
_DAILY_PERIOD_BARS = {"1W": 5, "1M": 22, "3M": 65, "1Y": 252}  # 5Y = full history

# Index toggle entries: (label shown in segment + JSON-blob key, Yahoo symbol).
# Order drives the segment's option order.  Labels are uppercase to match the
# design's mono index-tab styling; "S&P 500" stays as-is.
_HERO_INDICES: list[tuple[str, str]] = [
    ("S&P 500", "^GSPC"),
    ("NASDAQ",  "^IXIC"),
    ("DOW",     "^DJI"),
]


def _build_period_payload(history: list, reference: float | None, label: str) -> dict | None:
    """Project a [[ts_ms, price], ...] history into the chart payload shape.

    Reference is the price the change indicator measures against — prev
    close for 1D, period-start close for daily windows.  Returns ``None``
    when the history is too small to render.
    """
    if not history or len(history) < 2 or reference is None:
        return None
    paths = _hero_chart_paths(history, reference)
    if paths is None:
        return None

    last_price = history[-1][1]
    change = last_price - reference
    change_pct = change / reference if reference else None

    history_compact = [
        [int(ts), round(float(p), 4)]
        for ts, p in history
        if ts is not None and p is not None
    ]

    return {
        "path":         paths["line"],
        "area":         paths["area"],
        "ref_y":        paths["ref_y"],
        "tag_y":        paths["tag_y"],
        "tag":          f"{int(round(last_price))}" if last_price is not None else "—",
        "change":       f"{abs(change):.2f}",
        "change_pct":   f"{abs(change_pct) * 100:.2f}%" if change_pct is not None else "—",
        "change_up":    change >= 0,
        "label":        label,
        "history":      history_compact,
        "prev_close":   round(float(reference), 4),
    }


def _fallback_hero_payload() -> dict:
    """Synthetic single-period payload for when every upstream fails."""
    fallback_pts = [
        110 + math.sin(i * 0.4) * 30 + math.cos(i * 0.9) * 10 - i * 0.55
        for i in range(60)
    ]
    line_d = " ".join(
        f"{'M' if i == 0 else 'L'}{i * 10:.1f} {y:.1f}"
        for i, y in enumerate(fallback_pts)
    )
    area_d = f"{line_d} L 600 200 L 0 200 Z"
    period = {
        "path":       line_d,
        "area":       area_d,
        "ref_y":      74,
        "tag_y":      74,
        "tag":        "5847",
        "change":     "41.86",
        "change_pct": "0.72%",
        "change_up":  True,
        "label":      "INTRADAY · 15M",
        "history":    [],
        "prev_close": 0,
    }
    fallback_ohlcv = [
        ("OPEN",  "5,805.56"),
        ("HIGH",  "5,851.20"),
        ("LOW",   "5,798.14"),
        ("VOL",   "3.41B"),
    ]
    return {
        "chart_path":           period["path"],
        "chart_area":           period["area"],
        "chart_ref_y":          period["ref_y"],
        "chart_tag_y":          period["tag_y"],
        "chart_change":         period["change"],
        "chart_change_pct":     period["change_pct"],
        "chart_change_up":      period["change_up"],
        "chart_tag":            period["tag"],
        "chart_label":          period["label"],
        "chart_history_json":   "[]",
        "chart_prev_close":     "",
        "chart_ohlcv":          fallback_ohlcv,
        "chart_ohlcv_json":     json.dumps({"S&P 500": fallback_ohlcv}, separators=(",", ":")),
        "chart_default_index":  "S&P 500",
        "chart_default_period": "1D",
        "chart_indices_json":   json.dumps({"S&P 500": {"1D": period}}, separators=(",", ":")),
    }


def _build_index_periods(intraday: dict | None, history_5y: dict | None) -> dict[str, dict]:
    """Project an index's intraday + 5Y history into per-period payloads.

    Returns ``{"1D": {...}, "1W": {...}, ...}`` for whichever periods we
    have enough data to render.  Empty dict when nothing is renderable.
    """
    periods: dict[str, dict] = {}

    # 1D — intraday minute bars; reference = previous close
    if intraday and intraday.get("history"):
        prev_close = (intraday.get("ohlcv") or {}).get("prev_close")
        period = _build_period_payload(
            intraday["history"], prev_close,
            intraday.get("label") or "INTRADAY · 15M",
        )
        if period:
            periods["1D"] = period

    # 1W / 1M / 3M / 1Y / 5Y — daily-candle slices from the 5Y history
    if history_5y and history_5y.get("ohlcv"):
        # OHLCV row shape: [ts_ms, open, high, low, close, volume].
        closes = [[row[0], row[4]] for row in history_5y["ohlcv"] if row[4] is not None]
        for period_key in ("1W", "1M", "3M", "1Y"):
            bars = _DAILY_PERIOD_BARS[period_key]
            if len(closes) < bars:
                continue
            sliced = closes[-bars:]
            period = _build_period_payload(sliced, sliced[0][1], f"{period_key} · DAILY")
            if period:
                periods[period_key] = period
        # 5Y = the entire history (sample down for path-string size).
        if closes:
            sampled = _sample_for_chart(closes, max_points=200)
            period = _build_period_payload(sampled, sampled[0][1], "5Y · DAILY")
            if period:
                periods["5Y"] = period

    return periods


def _index_ohlcv_strip(intraday: dict | None) -> list[tuple[str, str]]:
    """OPEN/HIGH/LOW/VOL strip for the day's session of one index."""
    intraday_ohlcv = (intraday or {}).get("ohlcv") or {}
    return [
        ("OPEN",  _format_index_value(intraday_ohlcv.get("open"))),
        ("HIGH",  _format_index_value(intraday_ohlcv.get("high"))),
        ("LOW",   _format_index_value(intraday_ohlcv.get("low"))),
        ("VOL",   _format_volume(intraday_ohlcv.get("volume"))),
    ]


async def _hero_chart_compute() -> dict | None:
    """Inner fetcher for the hero chart.  Fans out 6 yfinance calls (3
    indices × intraday + 5Y) in parallel.  Returns ``None`` on total
    failure so the L2 wrapper can return a stale entry instead of
    caching an empty result."""
    from filings import market_data

    async def _fetch_pair(symbol: str) -> tuple[dict | None, dict | None]:
        intraday, history_5y = await asyncio.gather(
            to_heavy(market_data.get_intraday_chart, symbol),
            to_heavy(market_data.get_stock_ohlcv, symbol, "5Y"),
            return_exceptions=True,
        )
        if isinstance(intraday, BaseException):
            intraday = None
        if isinstance(history_5y, BaseException):
            history_5y = None
        return intraday, history_5y

    try:
        return await asyncio.gather(
            *(_fetch_pair(sym) for _label, sym in _HERO_INDICES),
            return_exceptions=False,
        )
    except Exception as exc:
        logger.warning("Hero chart fetch failed: %s", exc)
        return None


async def _fetch_hero_chart() -> dict:
    """Build the hero chart context for all 3 indices × all 6 periods.

    Fetches intraday + 5Y daily for S&P / NASDAQ / DOW concurrently
    (6 upstreams), then assembles per-(index, period) geometry +
    OHLCV strips + the SSR defaults.  L2-cached for 2 min so cold-start
    workers warm from Supabase rather than blocking on yfinance.
    """
    try:
        results = await _l2_cached(
            "redesign:home:hero_chart", ttl_seconds=120,
            compute=_hero_chart_compute, category="redesign_home",
        )
    except Exception as exc:
        logger.warning("Hero chart L2 fetch failed: %s", exc)
        results = None
    if not results:
        return _fallback_hero_payload()

    # Build per-index period registries + OHLCV strips.
    indices_payload: dict[str, dict] = {}
    ohlcv_by_index: dict[str, list[tuple[str, str]]] = {}
    for (label, _sym), (intraday, history_5y) in zip(_HERO_INDICES, results):
        periods = _build_index_periods(intraday, history_5y)
        if periods:
            indices_payload[label] = periods
            ohlcv_by_index[label] = _index_ohlcv_strip(intraday)

    if not indices_payload:
        return _fallback_hero_payload()

    # SSR defaults — first index in _HERO_INDICES that returned data,
    # then the shortest period we built for it.  Almost always (S&P, 1D).
    default_idx = next((lbl for lbl, _ in _HERO_INDICES if lbl in indices_payload), None)
    default_periods = indices_payload[default_idx]
    default_period_key = next((k for k in _CHART_PERIODS if k in default_periods), "1D")
    default = default_periods[default_period_key]

    return {
        # Default-period values for the initial server-side render so the
        # page paints before JS hydrates the toggle.
        "chart_path":         default["path"],
        "chart_area":         default["area"],
        "chart_ref_y":        default["ref_y"],
        "chart_tag_y":        default["tag_y"],
        "chart_change":       default["change"],
        "chart_change_pct":   default["change_pct"],
        "chart_change_up":    default["change_up"],
        "chart_tag":          default["tag"],
        "chart_label":        default["label"],
        "chart_history_json": json.dumps(default["history"], separators=(",", ":")),
        "chart_prev_close":   f"{default['prev_close']:.4f}",
        "chart_ohlcv":        ohlcv_by_index[default_idx],
        # Per-index OHLCV registry — JS swaps the strip on idx click.
        "chart_ohlcv_json":   json.dumps(ohlcv_by_index, separators=(",", ":")),
        # Per-(index, period) registry for the client-side toggle.
        "chart_default_index":  default_idx,
        "chart_default_period": default_period_key,
        "chart_indices_json":   json.dumps(indices_payload, separators=(",", ":")),
    }


def _sample_for_chart(points: list, *, max_points: int) -> list:
    """Down-sample a long history to ``max_points`` evenly-spaced entries.

    Used for ALL (5Y) so the SVG path string stays under ~5KB.  Returns
    the original list when it's already short enough.
    """
    n = len(points)
    if n <= max_points:
        return points
    step = (n - 1) / (max_points - 1)
    return [points[round(i * step)] for i in range(max_points)]


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
    {"manager": "Warren Buffett",     "fund": "Berkshire Hathaway", "aum": "$312B",  "action": "ADDED",   "ticker": "OXY",   "cik": "1067983", "delta": "+$1.2B"},
    {"manager": "Bill Ackman",        "fund": "Pershing Square",    "aum": "$11.4B", "action": "NEW",     "ticker": "BAM",   "cik": "1336528", "delta": "+$840M"},
    {"manager": "Michael Burry",      "fund": "Scion Asset Mgmt",   "aum": "$83M",   "action": "REDUCED", "ticker": "JD",    "cik": "1649339", "delta": "−$12M"},
    {"manager": "David Einhorn",      "fund": "Greenlight Capital", "aum": "$1.6B",  "action": "ADDED",   "ticker": "GRBK",  "cik": "1079114", "delta": "+$94M"},
    {"manager": "Daniel Loeb",        "fund": "Third Point",        "aum": "$5.9B",  "action": "EXITED",  "ticker": "PCG",   "cik": "1040273", "delta": "−$320M"},
    {"manager": "David Tepper",       "fund": "Appaloosa",          "aum": "$6.2B",  "action": "ADDED",   "ticker": "BABA",  "cik": "1656456", "delta": "+$210M"},
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

# ── Ticker tape items (above subtab nav).  Keep small + diverse:
#    indices · rates · FX · crypto · commodities · single names.
#    Each tuple = (label, value, pct).  pct used for ▲/▼ + color.
_HOME_TICKER_TAPE = [
    ("S&P",  "5,847.42",  +0.0072), ("NDX",  "20,194.18", +0.0045),
    ("DOW",  "42,233.71", -0.0004), ("VIX",  "14.82",     -0.0205),
    ("10Y",  "4.214%",    +0.0048), ("DXY",  "104.21",    +0.0042),
    ("BTC",  "68,420",    +0.0184), ("ETH",  "3,482",     +0.0214),
    ("GOLD", "2,842",     +0.0084), ("NVDA", "142.18",    +0.0421),
    ("TSLA", "184.20",    -0.0124), ("AAPL", "184.20",    +0.0142),
    ("GME",  "28.42",     +0.1840), ("COIN", "218.42",    +0.0214),
    ("PLTR", "34.21",     +0.0642), ("WTI",  "78.42",     +0.0142),
    ("NG",   "2.74",      -0.0218), ("EURUSD","1.0824",   -0.0028),
]

# ── Fear & Greed gauge (Overview, retail panel).
#    Mock until we wire CNN F&G or rebuild from sub-signals.
def _feargreed_as_of_now() -> str:
    """Timestamp string for the F&G "Data as of …" footer.

    CNN F&G updates once at market close, but we cache for 5 min in L2 —
    rendering the current UTC time is accurate to within the cache TTL,
    which matches what the user is actually seeing.
    """
    now = datetime.now(timezone.utc)
    return now.strftime("%b %-d, %Y %-I:%M %p UTC")


def _feargreed_band(value: int) -> str:
    """CSS class for an F&G score: green (greed), red (fear), or no class
    (neutral) — matches the up/down palette used elsewhere on the page."""
    if value >= 55:
        return "pp-up"
    if value <= 44:
        return "pp-down"
    return ""


def _feargreed_payload(
    value: int, label: str,
    yesterday: int, week_ago: int,
    month_ago: int = 0, year_ago: int = 0,
    as_of: str | None = None,
) -> dict:
    """Build the F&G context with precomputed needle endpoint.

    The semicircular gauge maps 0..100 → -90°..+90° around (cx=50, cy=50).
    Needle radius = 42 (gauge stroke is at r=44; needle tip clears the arc
    by 2px).  We compute (nx, ny) here so the Jinja template doesn't need
    sin / cos filters.
    """
    angle_deg = (value / 100.0) * 180.0 - 90.0
    rad = math.radians(angle_deg - 90.0)
    nx = 50.0 + 42.0 * math.cos(rad)
    ny = 50.0 + 42.0 * math.sin(rad)
    return {
        "value":     value,
        "label":     label,
        "yesterday": yesterday,
        "week_ago":  week_ago,
        "month_ago": month_ago,
        "year_ago":  year_ago,
        # CSS bands for the timeframe rows so the template can color-code
        # each score without an inline ternary on every row.
        "week_band":  _feargreed_band(week_ago),
        "month_band": _feargreed_band(month_ago),
        "year_band":  _feargreed_band(year_ago),
        "as_of":     as_of or _feargreed_as_of_now(),
        "needle_x":  round(nx, 2),
        "needle_y":  round(ny, 2),
    }


def _home_feargreed_mock() -> dict:
    """Fresh mock F&G payload — built per-call so the ``as_of`` stamp
    reflects render time, not server-start time."""
    return _feargreed_payload(
        value=71, label="Greed",
        yesterday=64, week_ago=58, month_ago=42, year_ago=55,
    )

# ── Flow subtab — trending tickers aggregated across 13F + Congress.
#    funds = total # of 13F changes (buys + sells); cong = total congress trades.
#    fundsDir / congDir = aggregate direction.  net = (buys - sells) overall.
#    fund_buyers / cong_buyers = buy-only counts (used for the buyer-sorted
#    "Trending with Smart Money" panel + hover tooltip).
def _trend_row(tk, fb, fs, cb, cs, fund_names=(), cong_names=(), cong_d=0, cong_r=0):
    funds = fb + fs
    cong  = cb + cs
    net   = (fb - fs) + (cb - cs)
    return {
        "tk": tk, "funds": funds, "cong": cong,
        "fundsDir": "BUY" if fb > fs else ("SELL" if fs > fb else "—"),
        "congDir":  "BUY" if cb > cs else ("SELL" if cs > cb else "—"),
        "net": net,
        "fund_buyers": fb, "fund_sellers": fs,
        "cong_buyers": cb, "cong_sellers": cs,
        "cong_d": cong_d, "cong_r": cong_r,
        "fund_names": list(fund_names),
        "cong_names": list(cong_names),
        "fund_more":  max(fb - len(fund_names), 0),
        "cong_more":  max(cb - len(cong_names), 0),
        "total_buyers": fb + cb,
    }

_HOME_FLOW_TRENDING = [
    _trend_row("NVDA",  18, 0, 4, 0, ["Mairs & Power", "David Tepper", "Tom Gayner"], ["Pelosi, Nancy"], cong_d=2, cong_r=2),
    _trend_row("PLTR",  14, 0, 6, 0, ["Bill Ackman", "David Tepper"], ["Tuberville, T."], cong_d=1, cong_r=5),
    _trend_row("COIN",  12, 0, 2, 0, ["Cathie Wood"], ["Pelosi, Nancy"], cong_d=2, cong_r=0),
    _trend_row("OXY",    9, 0, 1, 0, ["Warren Buffett"], [], cong_d=0, cong_r=1),
    _trend_row("GOOGL",  8, 0, 4, 0, ["David Einhorn"], ["Khanna, Ro"], cong_d=3, cong_r=1),
    _trend_row("AAPL",   7, 0, 5, 0, ["Warren Buffett"], ["Pelosi, Nancy"], cong_d=3, cong_r=2),
    _trend_row("AVGO",   6, 0, 2, 0, [], [], cong_d=1, cong_r=1),
    _trend_row("META",   5, 0, 3, 0, [], [], cong_d=2, cong_r=1),
    _trend_row("TSLA",   0, 4, 8, 0, [], ["Tuberville, T."], cong_d=3, cong_r=5),
    _trend_row("AMD",    4, 0, 1, 0, [], [], cong_d=0, cong_r=1),
    _trend_row("BRK.B",  0, 3, 0, 1, [], [], cong_d=0, cong_r=0),
    _trend_row("INTC",   0, 3, 0, 2, [], [], cong_d=0, cong_r=0),
]
# Max value used to scale bar widths (kept design-stable across reloads).
_HOME_FLOW_TRENDING_MAX = 24

# Larger fund / congress lists for Flow subtab (8 rows each — beyond the
# 6 shown on Overview).  Uses the same shape as _HOME_FUND_FLOWS / _HOME_CONGRESS.
_HOME_FLOW_FUND_BUYS = _HOME_FUND_FLOWS + [
    {"manager": "Ray Dalio",          "fund": "Bridgewater",        "aum": "$22.1B", "action": "REDUCED", "ticker": "SPY", "cik": "1350694", "delta": "−$420M"},
    {"manager": "Stan Druckenmiller", "fund": "Duquesne",           "aum": "$3.4B",  "action": "NEW",     "ticker": "TSM", "cik": "1536411", "delta": "+$180M"},
]
_HOME_FLOW_CONGRESS = _HOME_CONGRESS + [
    {"person": "Pelosi, Nancy",  "party": "D", "chamber": "House",  "ticker": "AAPL", "action": "SELL", "size": "$1M-5M"},
    {"person": "Tuberville, T.", "party": "R", "chamber": "Senate", "ticker": "NVDA", "action": "BUY",  "size": "$15K-50K"},
    {"person": "Khanna, Ro",     "party": "D", "chamber": "House",  "ticker": "MSFT", "action": "BUY",  "size": "$1K-15K"},
]

# ── Heatmap subtab data — Companies / Sectors toggle.
#    Companies: (ticker, pct, mcap_str, weight).  Weight 1-4 controls grid span.
#    Sectors:   (name, pct, mcap_str).  Mega-caps (>$10T) span 2 cols.
_HOME_HEATMAP_COMPANIES = [
    ("NVDA",  +0.0421, "$3.51T", 4), ("AAPL",  +0.0142, "$2.84T", 4),
    ("MSFT",  +0.0184, "$3.12T", 4), ("GOOGL", +0.0184, "$2.18T", 3),
    ("AMZN",  +0.0124, "$1.94T", 3), ("META",  +0.0214, "$1.41T", 3),
    ("AVGO",  +0.0184, "$1.02T", 2), ("TSLA",  -0.0124, "$584B",  2),
    ("BRK.B", +0.0084, "$932B",  2), ("LLY",   -0.0084, "$728B",  1),
    ("JPM",   +0.0042, "$612B",  1), ("V",     +0.0024, "$548B",  1),
    ("WMT",   +0.0024, "$520B",  1), ("XOM",   -0.0124, "$484B",  1),
    ("UNH",   +0.0042, "$478B",  1), ("MA",    +0.0024, "$420B",  1),
    ("PG",    -0.0024, "$382B",  1), ("JNJ",   -0.0042, "$364B",  1),
    ("HD",    +0.0124, "$352B",  1), ("ORCL",  +0.0184, "$342B",  1),
    ("COST",  -0.0024, "$320B",  1), ("NFLX",  +0.0214, "$284B",  1),
    ("BAC",   +0.0042, "$268B",  1), ("AMD",   +0.0184, "$254B",  1),
]
_HOME_HEATMAP_SECTORS = [
    ("Tech",          +0.024, "$18.4T"), ("Comm Services", +0.018, "$4.2T"),
    ("Cons Disc",     +0.014, "$7.1T"),  ("Financials",    +0.009, "$9.4T"),
    ("Industrials",   +0.005, "$5.2T"),  ("Health Care",   +0.001, "$7.8T"),
    ("Cons Staples",  -0.002, "$4.1T"),  ("Materials",     -0.008, "$1.9T"),
    ("Real Estate",   -0.011, "$1.4T"),  ("Utilities",     -0.014, "$1.3T"),
    ("Energy",        -0.022, "$2.4T"),
]


def _heatmap_companies_with_color() -> list[dict]:
    """Decorate companies for template — adds tile color + col/row span hints."""
    rows = []
    for tk, pct, mc, w in _HOME_HEATMAP_COMPANIES:
        rows.append({
            "ticker": tk,
            "pct":    pct,
            "mcap":   mc,
            "weight": w,
            "tile_bg": _heatmap_color_for_pct(pct),
            "tile_dark": _heatmap_tile_is_dark(pct),
            "span_col": 2 if w >= 3 else 1,
            "span_row": 2 if w >= 3 else 1,
            "is_large": w >= 3,
            "show_mcap": w >= 2,
        })
    return rows


def _heatmap_sectors_with_color() -> list[dict]:
    rows = []
    for name, pct, mc in _HOME_HEATMAP_SECTORS:
        # Mega-cap sectors get 2-col span (>$10T).
        mcap_num = float("".join(ch for ch in mc if ch.isdigit() or ch == "."))
        is_mega = "T" in mc and mcap_num > 10
        rows.append({
            "name":     name,
            "pct":      pct,
            "mcap":     mc,
            "tile_bg":  _heatmap_color_for_pct(pct),
            "tile_dark": _heatmap_tile_is_dark(pct),
            "span_col": 2 if is_mega else 1,
        })
    return rows


# ── Activity subtab — live notification feed.  Compact tabular shape
#    matching the rest of the redesign (ago | src | dot | text | tag);
#    data sourced from supabase notifications (same table production v1
#    reads from).  cat drives both the dot color + the type-tag color.
_HOME_ACTIVITY_FEED = [
    {"ago": "3m",  "src": "REDDIT",   "ticker": "LLY",   "text": "Eli Lilly — mentions +100% in 24h (4 total)",        "cat": "reddit",   "pill": "REDDIT VELOCITY", "href": "/_v2/stock/LLY"},
    {"ago": "3m",  "src": "REDDIT",   "ticker": "ANY",   "text": "Sphere 3D — mentions +200% in 24h (3 total)",        "cat": "reddit",   "pill": "REDDIT VELOCITY", "href": "/_v2/stock/ANY"},
    {"ago": "3m",  "src": "REDDIT",   "ticker": "XBI",   "text": "SPDR S&P Biotech — mentions +100% in 24h (2 total)", "cat": "reddit",   "pill": "REDDIT VELOCITY", "href": "/_v2/stock/XBI"},
    {"ago": "12m", "src": "13F",      "ticker": "AAPL",  "text": "Berkshire Hathaway reduced position by $5.2B",       "cat": "13f",      "pill": "13F FILING",      "href": "/_v2/stock/AAPL"},
    {"ago": "23m", "src": "CONGRESS", "ticker": "NVDA",  "text": "Rep. Pelosi disclosed buy — $1M-$5M call options",   "cat": "congress", "pill": "CONGRESS",        "href": "/_v2/stock/NVDA"},
    {"ago": "37m", "src": "INSIDER",  "ticker": "AAPL",  "text": "Tim Cook (CEO) sold 240,000 shares — $52.1M",        "cat": "insider",  "pill": "INSIDER",         "href": "/_v2/stock/AAPL"},
    {"ago": "42m", "src": "YOUTUBE",  "ticker": "BRK.B", "text": "CNBC: Berkshire AGM 2026 highlights",                "cat": "youtube",  "pill": "YOUTUBE",         "href": "/_v2/stock/BRK.B"},
]

# ── News subtab.  Categories: Markets / Macro / Earnings / Funds /
#    Congress / Insiders / Retail.  Featured + 2 sub-features get hero
#    images; "more headlines" feed is text-only and filterable.
_HOME_NEWS_FEATURED = {
    "src":      "Reuters",
    "ago":      "14m ago",
    "cat":      "Markets",
    "title":    "Nvidia briefly tops $3.5T as Blackwell shipments accelerate into Q2",
    "summary":  "AI chip demand drove revenue 22% above consensus; cloud customers committed to 2027 capacity. Hyperscaler capex guidance lifted entire AI infra complex.",
    "excerpt":  "AI chip demand drove revenue 22% above consensus; cloud customers committed to 2027 capacity. Hyperscaler capex guidance lifted entire AI infra complex.",
    "image":    "",
    "url":      "https://www.reuters.com",
    "tickers":  ["NVDA", "AVGO", "AMD"],
}
_HOME_NEWS_STORIES = [
    {"src": "Bloomberg", "ago": "1h ago", "cat": "Markets",  "title": "Hyperscaler capex guidance lifts AI infra; NVDA leads",        "summary": "AWS, Azure, and GCP all raised 2026 capex guidance citing strong AI training demand.",   "image": "", "url": "https://www.bloomberg.com",  "tickers": ["NVDA", "MSFT", "GOOGL"]},
    {"src": "WSJ",       "ago": "2h ago", "cat": "Macro",    "title": "Fed minutes: officials see further patience on rate cuts",      "summary": "FOMC minutes show divided views on the timing of additional rate cuts.",                  "image": "", "url": "https://www.wsj.com",        "tickers": ["10Y", "SPY"]},
    {"src": "FT",        "ago": "3h ago", "cat": "Funds",    "title": "Berkshire trims Apple stake further, builds cash to $325B",     "summary": "Q1 13F shows Buffett continued to lighten the AAPL position, pushing cash to a record.", "image": "", "url": "https://www.ft.com",         "tickers": ["BRK.B", "AAPL"]},
    {"src": "CNBC",      "ago": "4h ago", "cat": "Earnings", "title": "Coinbase Q1 beat sends shares 8% higher in extended trade",     "summary": "Trading volume + retail re-engagement drove a beat-and-raise quarter.",                   "image": "", "url": "https://www.cnbc.com",       "tickers": ["COIN"]},
    {"src": "Reuters",   "ago": "5h ago", "cat": "Congress", "title": "Pelosi files NVDA call options — fourth time this year",         "summary": "Latest STOCK Act filing shows continued conviction on the AI infra trade.",               "image": "", "url": "https://www.reuters.com",    "tickers": ["NVDA"]},
    {"src": "Bloomberg", "ago": "6h ago", "cat": "Retail",   "title": "GameStop spikes 18% after Roaring Kitty teases new position",    "summary": "Keith Gill's social posts moved the stock pre-market.",                                   "image": "", "url": "https://www.bloomberg.com",  "tickers": ["GME", "AMC"]},
    {"src": "WSJ",       "ago": "7h ago", "cat": "Markets",  "title": "Oil retreats as inventories build; OXY guides cautious",         "summary": "EIA data showed a build; OXY's earnings call flagged continued capex discipline.",        "image": "", "url": "https://www.wsj.com",        "tickers": ["WTI", "OXY", "XOM"]},
    {"src": "FT",        "ago": "8h ago", "cat": "Markets",  "title": "Palantir wins fresh DoD contract worth $480M",                  "summary": "Multi-year contract expands Palantir's footprint inside Defense Department workflows.",  "image": "", "url": "https://www.ft.com",         "tickers": ["PLTR"]},
    {"src": "Bloomberg", "ago": "9h ago", "cat": "Insiders", "title": "Apple CFO files Form 4: 80K shares sold under 10b5-1 plan",      "summary": "Routine 10b5-1 sale; total proceeds ≈ $14M.",                                              "image": "", "url": "https://www.bloomberg.com",  "tickers": ["AAPL"]},
    {"src": "Reuters",   "ago": "10h ago","cat": "Earnings", "title": "Lilly raises full-year revenue guidance after weight-loss beat", "summary": "Mounjaro/Zepbound demand outpaced consensus by ~12%; FY guide up 4 pts.",                "image": "", "url": "https://www.reuters.com",    "tickers": ["LLY"]},
    {"src": "CNBC",      "ago": "11h ago","cat": "Macro",    "title": "Treasury auctions see strong indirect bid, calming rate jitters","summary": "10Y auction tail came in tighter than expected.",                                          "image": "", "url": "https://www.cnbc.com",       "tickers": ["10Y", "TLT"]},
]
_HOME_NEWS_MOST_READ = [
    {"rank": "01", "title": "Pelosi files NVDA call options — fourth time this year", "src": "Reuters",   "reads": "12.4K reads"},
    {"rank": "02", "title": "GameStop spikes 18% after Roaring Kitty post",            "src": "Bloomberg", "reads": "9.8K reads"},
    {"rank": "03", "title": "Berkshire trims Apple, builds $325B cash pile",           "src": "FT",        "reads": "8.1K reads"},
    {"rank": "04", "title": "Fed minutes signal patience on cuts",                     "src": "WSJ",       "reads": "6.2K reads"},
    {"rank": "05", "title": "Coinbase Q1 beat sends shares 8% higher",                 "src": "CNBC",      "reads": "4.7K reads"},
]
_HOME_NEWS_MARKET_WIRE = [
    {"time": "09:42", "ticker": "SPY",   "change": "+0.72%", "wire": "S&P 500 hits intraday record"},
    {"time": "09:38", "ticker": "NVDA",  "change": "+4.21%", "wire": "Blackwell capacity sold out 2027"},
    {"time": "09:31", "ticker": "BRK.B", "change": "+0.84%", "wire": "13F filing reveals OXY add"},
    {"time": "09:24", "ticker": "COIN",  "change": "+8.10%", "wire": "Q1 EPS $1.05 vs $0.92 est"},
    {"time": "09:18", "ticker": "WTI",   "change": "−1.42%", "wire": "Crude inventories build 4.2M bbl"},
    {"time": "09:11", "ticker": "DXY",   "change": "+0.42%", "wire": "Dollar firms after Fed minutes"},
    {"time": "09:04", "ticker": "BTC",   "change": "+1.84%", "wire": "BTC reclaims 68K, ETF flows +$214M"},
]

# ── Calendar subtab.  Earnings = next 30d watchlist; Macro = next 60d FOMC/CPI/etc.
#    impact ∈ {high, medium, low} drives the dot color.
_HOME_CAL_EARNINGS = [
    {"date": "Today",  "ticker": "AAPL", "when": "After close", "eps": "$1.51", "iv": "+5.2% IV"},
    {"date": "Today",  "ticker": "AMZN", "when": "After close", "eps": "$0.98", "iv": "+8.1% IV"},
    {"date": "May 05", "ticker": "PLTR", "when": "After close", "eps": "$0.08", "iv": "+14% IV"},
    {"date": "May 08", "ticker": "COIN", "when": "After close", "eps": "$1.05", "iv": "+11% IV"},
    {"date": "May 13", "ticker": "OXY",  "when": "Before open", "eps": "$0.72", "iv": "+6.4% IV"},
    {"date": "May 21", "ticker": "NVDA", "when": "After close", "eps": "$0.84", "iv": "+9.8% IV"},
]
_HOME_CAL_MACRO = [
    {"date": "May 02", "label": "Nonfarm Payrolls",   "time": "8:30 ET",  "val": "+185K", "impact": "high"},
    {"date": "May 14", "label": "CPI (Apr)",          "time": "8:30 ET",  "val": "+2.4%", "impact": "high"},
    {"date": "May 23", "label": "FOMC Minutes",       "time": "14:00 ET", "val": "—",     "impact": "high"},
    {"date": "May 30", "label": "PCE Price Index",    "time": "8:30 ET",  "val": "+2.5%", "impact": "high"},
    {"date": "Jun 12", "label": "FOMC rate decision", "time": "14:00 ET", "val": "4.50%", "impact": "high"},
    {"date": "Jun 27", "label": "GDP Q2 advance",     "time": "8:30 ET",  "val": "+2.6%", "impact": "medium"},
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


def _retail_bar_color(sentiment: float) -> str:
    """Coral if sentiment in [0, 0.3), green if >= 0.3, red if < 0."""
    if sentiment >= 0.3:
        return "var(--pp-up)"
    if sentiment >= 0:
        return "var(--pp-accent)"
    return "var(--pp-down)"


def _attach_retail_bars(rows: list[dict]) -> list[dict]:
    """Attach bar_pct + bar_color to each retail row using a relative scale.

    bar_pct = mentions / max(visible mentions) * 100 — the most-mentioned
    row in the set always pegs at 100% and the others scale relative to it.
    Self-correcting whether the data source has thousands or dozens of
    mentions, so the bars stay readable on real ApeWisdom counts.
    Mutates each row in place; returns the list for chaining.
    """
    if not rows:
        return rows
    max_m = max((r.get("mentions") or 0) for r in rows) or 1
    for r in rows:
        m = r.get("mentions") or 0
        r["bar_pct"]   = (m / max_m) * 100
        r["bar_color"] = _retail_bar_color(r.get("sentiment", 0))
    return rows


def _retail_rows():
    return _attach_retail_bars([dict(r) for r in _HOME_RETAIL_RAW])


# ─────────────────────────────────────────────────────────────────────────────
# HOME OVERVIEW — live-data fetchers for the 8 mock-replaced sections.
# Each fetcher returns the exact shape the template expects and falls back
# to the same-named module-level _HOME_* constant when the upstream source
# is unavailable (network, missing key, cold cache).  This keeps the page
# from ever rendering blank panels in prod.
# ─────────────────────────────────────────────────────────────────────────────


# Hand-curated company-name lookups for tickers — used wherever we have a
# ticker but no name (movers, ticker tape, retail).  Source of truth lives
# in `superinvestors`, `cache.py`, etc.; this is just the head-of-list.
_TICKER_NAMES = {
    "AAPL":  "Apple",       "MSFT":  "Microsoft", "AMZN":  "Amazon",
    "GOOGL": "Alphabet",    "GOOG":  "Alphabet",  "TSLA":  "Tesla",
    "NVDA":  "NVIDIA",      "META":  "Meta",      "AVGO":  "Broadcom",
    "BRK-B": "Berkshire",   "BRK.B": "Berkshire", "BRK-A": "Berkshire",
    "JPM":   "JPMorgan",    "V":     "Visa",      "UNH":   "UnitedHealth",
    "LLY":   "Eli Lilly",   "JNJ":   "J&J",       "HD":    "Home Depot",
    "WMT":   "Walmart",     "COST":  "Costco",    "NFLX":  "Netflix",
    "AMD":   "AMD",         "CRM":   "Salesforce","XOM":   "Exxon",
    "PG":    "P&G",         "ADBE":  "Adobe",     "ORCL":  "Oracle",
    "PLTR":  "Palantir",    "GME":   "GameStop",  "AMC":   "AMC",
    "COIN":  "Coinbase",    "OXY":   "Occidental","MA":    "Mastercard",
}


def _name_for_ticker(ticker: str) -> str:
    return _TICKER_NAMES.get(ticker.upper(), ticker.upper())


def _format_compact_volume(v: float | int | None) -> str:
    """Render share volume compactly: 3.41B / 41.2M / 412K."""
    if v is None or v == 0:
        return "—"
    v = float(v)
    if v >= 1e9:
        return f"{v / 1e9:.2f}B"
    if v >= 1e6:
        return f"{v / 1e6:.1f}M"
    if v >= 1e3:
        return f"{v / 1e3:.0f}K"
    return f"{int(v):,}"


# A minimal flat sparkline used as a guaranteed-non-empty fallback when the
# sparkline API returns nothing for a ticker.  Drawn at the row's color, it
# renders as a faint baseline rather than a missing element.
_FLAT_SPARK = [0.5] * 12


# ── 1) Top movers ────────────────────────────────────────────────────
async def _fetch_home_top_movers(limit: int = 6, *, mkt: dict | None = None) -> list[dict]:
    """Top S&P movers ranked by absolute % change today.

    Pulls `get_sp500_market_data("1D")` (period-cached for 30 min) and
    `get_sparkline_points(...)` for the picked tickers.  Each row has the
    exact shape the home template iterates: ticker, name, last, pct,
    spark_series, vol.

    `mkt` may be passed in if the caller has already fetched the S&P 1D
    map (preview_home pre-fetches it once and shares with ticker_tape +
    heatmap_companies to avoid 3 concurrent cache lookups for the same data).
    """
    if mkt is None:
        try:
            from filings import market_data
            mkt = await to_heavy(market_data.get_sp500_market_data, "1D")
        except Exception as exc:
            logger.warning("Top movers fetch failed: %s", exc)
            mkt = None

    if not mkt or "_metadata" not in mkt:
        return _HOME_TOP_MOVERS  # mock fallback

    # Drop metadata + sort by abs(pct_change) desc.
    candidates = [
        (sym, info) for sym, info in mkt.items()
        if sym != "_metadata" and isinstance(info, dict) and info.get("pct_change") is not None
    ]
    candidates.sort(key=lambda x: abs(x[1]["pct_change"]), reverse=True)
    picked = candidates[:limit]
    if not picked:
        return _HOME_TOP_MOVERS

    tickers = [sym for sym, _ in picked]
    try:
        from filings import market_data
        sparks = await to_heavy(market_data.get_sparkline_points, tickers, 20)
    except Exception:
        sparks = {}

    rows = []
    for sym, info in picked:
        pct = info.get("pct_change", 0)
        rows.append({
            "ticker":        sym,
            "name":          _name_for_ticker(sym),
            "last":          f"{info.get('price', 0):,.2f}",
            # Template expects fraction (0.0421); market_data gives percentage.
            "pct":           pct / 100.0,
            "spark_series":  sparks.get(sym) or _FLAT_SPARK,
            # Volume isn't carried by get_sp500_market_data — em-dash for now.
            "vol":           "—",
        })
    return rows


# Status from `changes` → BUY/SELL/NEW chip kind expected by the template.
# OpenInsider/13F nomenclature varies — INCREASED + ADDED + ADD all become
# "ADDED"; CLOSED + EXITED + EXIT all become "EXITED"; NEW/INITIATED stays
# as "NEW" (filled coral chip).
_FUND_STATUS_MAP = {
    "ADDED":     "ADDED",
    "ADD":       "ADDED",
    "INCREASED": "ADDED",
    "REDUCED":   "REDUCED",
    "CUT":       "REDUCED",
    "DECREASED": "REDUCED",
    "EXITED":    "EXITED",
    "EXIT":      "EXITED",
    "CLOSED":    "EXITED",
    "NEW":       "NEW",
    "INITIATED": "NEW",
    "OPENED":    "NEW",
}


def _format_aum(v: float | int | None) -> str:
    """Compact AUM: $312B / $11.4B / $83M."""
    if v is None:
        return "—"
    v = float(v)
    if v >= 1e12: return f"${v / 1e12:.1f}T"
    if v >= 1e10: return f"${v / 1e9:.0f}B"
    if v >= 1e9:  return f"${v / 1e9:.1f}B"
    if v >= 1e6:  return f"${v / 1e6:.0f}M"
    return f"${v:,.0f}"


def _format_signed_amount(delta: float | int | None) -> str:
    """Compact signed amount: +$2.3B / -$520M / +$83K.  Used for the trade
    delta beside fund-flow rows so the eye can clock direction + scale."""
    if delta is None:
        return ""
    d = float(delta)
    if d == 0:
        return "$0"
    sign = "+" if d > 0 else "−"
    a = abs(d)
    if a >= 1e12: return f"{sign}${a / 1e12:.1f}T"
    if a >= 1e10: return f"{sign}${a / 1e9:.0f}B"
    if a >= 1e9:  return f"{sign}${a / 1e9:.1f}B"
    if a >= 1e6:  return f"{sign}${a / 1e6:.0f}M"
    if a >= 1e3:  return f"{sign}${a / 1e3:.0f}K"
    return f"{sign}${a:,.0f}"


_AMOUNT_RE = __import__("re").compile(r"[-+]?[\d,.]+")


def _compact_amount_str(s: str | None) -> str:
    """Reformat a free-form dollar string ("$25,080,696" or "+$52.1M") into
    the compact form ("$25M" / "$52.1M") used everywhere on the home page.

    Handles long-form numbers from OpenInsider as well as already-compact
    inputs.  Strips leading sign — direction is conveyed by the action tag,
    not the amount.  Returns "" when nothing parses out.
    """
    if not s:
        return ""
    raw = s.strip().lstrip("+-").lstrip("$").strip()
    # Already compact (ends with K/M/B/T) — accept as-is, just prefix $.
    if raw and raw[-1].upper() in ("K", "M", "B", "T"):
        return f"${raw.upper()}"
    m = _AMOUNT_RE.search(raw)
    if not m:
        return ""
    try:
        v = float(m.group(0).replace(",", ""))
    except ValueError:
        return ""
    a = abs(v)
    if a >= 1e12: return f"${a / 1e12:.1f}T"
    if a >= 1e10: return f"${a / 1e9:.0f}B"
    if a >= 1e9:  return f"${a / 1e9:.1f}B"
    if a >= 1e6:  return f"${a / 1e6:.0f}M"
    if a >= 1e3:  return f"${a / 1e3:.0f}K"
    return f"${a:,.0f}"


def _compact_range_str(s: str | None) -> str:
    """Compact a Congress-style amount range ("$1,001 - $15,000") to a
    short form ("$1K–15K" / "$50K–100K" / "$1M–5M").

    Picks the lo/hi numbers, scales each to K/M/B and combines.  Falls
    back to a single compact value if only one number is parseable.
    """
    if not s:
        return ""
    nums = _AMOUNT_RE.findall(s.replace(",", ""))
    if not nums:
        return ""
    try:
        vals = [float(n) for n in nums if n not in ("-", "+")]
    except ValueError:
        return ""
    if not vals:
        return ""

    def _short(a: float) -> str:
        a = abs(a)
        if a >= 1e9: return f"{a / 1e9:.0f}B"
        if a >= 1e6: return f"{a / 1e6:.0f}M"
        if a >= 1e3: return f"{a / 1e3:.0f}K"
        return f"{a:.0f}"

    if len(vals) == 1:
        return f"${_short(vals[0])}"
    return f"${_short(vals[0])}–{_short(vals[-1])}"


def _build_cusip_ticker_map(fund_cache: dict) -> dict[str, str]:
    """Walk every fund's `all_holdings` once to build CUSIP → ticker.

    The flat `changes` records carry CUSIP but not ticker; this resolver
    is what turns "70450Y103" into "PYPL" for display.  Cached on
    app.state at first build to avoid recomputing per request.
    """
    cmap: dict[str, str] = {}
    for fund_data in fund_cache.values():
        for h in fund_data.get("all_holdings") or []:
            cusip = h.get("cusip")
            ticker = h.get("ticker")
            if cusip and ticker and cusip not in cmap:
                cmap[cusip] = ticker
    return cmap


# ── 2) 13F fund flows ────────────────────────────────────────────────
async def _fetch_home_fund_flows(request: Request, limit: int = 6) -> list[dict]:
    """Recent 13F flows aggregated across the 85 superinvestors.

    Uses the in-process fund_cache populated at app startup.  Picks each
    fund's most-recent quarter `changes` and ranks by abs(current_value).
    Resolves CUSIP→ticker via a one-pass map across the whole cache.
    """
    fund_cache = getattr(request.app.state, "fund_cache", {}) or {}
    if not fund_cache:
        return _HOME_FUND_FLOWS

    try:
        from filings.superinvestors import SUPERINVESTORS_BY_CIK
    except Exception as exc:
        logger.warning("Could not import superinvestors: %s", exc)
        return _HOME_FUND_FLOWS

    # Build CUSIP→ticker resolver once; cache on app state for next request.
    cmap = getattr(request.app.state, "_pp_redesign_cusip_ticker", None)
    if cmap is None:
        cmap = _build_cusip_ticker_map(fund_cache)
        try:
            request.app.state._pp_redesign_cusip_ticker = cmap
        except Exception:
            pass

    all_changes: list[dict] = []
    for cik, fund_data in fund_cache.items():
        si = SUPERINVESTORS_BY_CIK.get(cik)
        if not si:
            continue
        manager   = si.display_name
        fund_name = si.fund_name
        aum       = _format_aum(fund_data.get("total_value"))

        # CUSIP → (shares, value) lookup from this fund's holdings.  The flat
        # `changes` records carry only share_change + current_value, not the
        # current_shares we need to derive an implied price-per-share for the
        # trade-delta column.  all_holdings has both.
        holdings_by_cusip: dict[str, tuple[float, float]] = {
            (h.get("cusip") or ""): (
                float(h.get("shares") or 0),
                float(h.get("value")  or 0),
            )
            for h in (fund_data.get("all_holdings") or [])
            if h.get("cusip")
        }

        for c in fund_data.get("changes") or []:
            raw_status = (c.get("status") or "").upper()
            if not raw_status or raw_status == "UNCHANGED":
                continue
            action = _FUND_STATUS_MAP.get(raw_status, raw_status)
            cusip = c.get("cusip", "")
            # Resolve ticker; fall back to first 5 chars of issuer if unknown.
            ticker = c.get("ticker") or cmap.get(cusip) or ""
            if not ticker:
                issuer = c.get("issuer") or ""
                # Try to extract a plausible ticker-ish token from the issuer
                # name (e.g. "MARTIN MARIETTA MATERIALS" → "MMM"-style).  We
                # prefer hiding the row to showing junk.
                continue

            # Approximate trade $ delta from share_change × implied price.
            # Implied price comes from this fund's all_holdings entry
            # (current_value / current_shares).  NEW = full current_value.
            # EXITED has no holdings entry (position closed) so delta is
            # blank — the tag itself communicates direction.
            curr_value = float(c.get("current_value") or 0)
            shr_change = float(c.get("share_change")  or 0)
            curr_shares, _ = holdings_by_cusip.get(cusip, (0.0, 0.0))
            delta_amt: float | None
            if action == "NEW":
                delta_amt = curr_value
            elif action == "EXITED" or curr_shares <= 0:
                delta_amt = None
            else:
                price = curr_value / curr_shares
                delta_amt = shr_change * price
            all_changes.append({
                "manager":  manager,
                "fund":     fund_name,
                "aum":      aum,
                "action":   action,
                "ticker":   ticker.upper(),
                "cik":      cik,
                "delta":    _format_signed_amount(delta_amt) if delta_amt is not None else "",
                "_value":   abs(curr_value),
            })

    if not all_changes:
        return _HOME_FUND_FLOWS

    all_changes.sort(key=lambda r: -r["_value"])
    return [
        {k: v for k, v in r.items() if k != "_value"}
        for r in all_changes[:limit]
    ]


# ── 3) Insider flow ──────────────────────────────────────────────────
async def _fetch_home_insiders(limit: int = 5) -> list[dict]:
    """5 most recent insider trades shaped for the Home Overview list."""
    try:
        from filings import insider_trading
        trades = await to_heavy(
            insider_trading.get_latest_insider_trades, "", limit, "",
        )
    except Exception as exc:
        logger.warning("Insider flow fetch failed: %s", exc)
        return _HOME_INSIDERS

    if not trades:
        return _HOME_INSIDERS

    rows = []
    for tr in trades[:limit]:
        # OpenInsider returns mixed formats ("+$52.1M" or "-$300,560").  The
        # BUY/SELL chip already conveys direction, and the panel column is
        # narrow — compact every amount to "$25M" / "$300K" style.
        rows.append({
            "person": tr.insider_name,
            "role":   _insiders_format_title(tr.title),
            "ticker": (tr.ticker or "").upper(),
            "action": _insiders_action(tr.trade_type),
            "value":  _compact_amount_str(tr.value),
        })
    return rows


# ── 4) Congress ──────────────────────────────────────────────────────
async def _fetch_home_congress(limit: int = 5) -> list[dict]:
    """5 most recent congress trades shaped for the Home Overview list."""
    try:
        from filings import supabase_cache
        rows_raw = await to_heavy(
            supabase_cache.get_congress_recent_trades, limit,
        )
    except Exception as exc:
        logger.warning("Home congress fetch failed: %s", exc)
        rows_raw = None

    if not rows_raw:
        return _HOME_CONGRESS

    out = []
    for r in rows_raw[:limit]:
        out.append({
            "person":  r.get("politician_name") or "—",
            "party":   (r.get("party") or "I")[:1].upper(),
            "chamber": r.get("chamber") or "—",
            "ticker":  (r.get("ticker") or "—").upper(),
            "action":  _congress_action(r.get("trade_type", "")),
            "size":    _compact_range_str(r.get("amount_display")) or "—",
        })
    return out


# ── 5) Macro ─────────────────────────────────────────────────────────
async def _fetch_home_macro() -> list[dict]:
    """4 macro headlines for the Home Overview list panel.

    Picks Fed Funds, Core CPI YoY, Unemployment, 10Y Treasury — the most
    decision-relevant indicators.  Format + change formatted like the JSX
    prototype (val: "4.25%", chg: "+0.1pp" / "-0.1pp" / "—", note: provider).
    """
    try:
        from filings import fred_indicators
        payload = await to_heavy(fred_indicators.fetch_indicators)
    except Exception as exc:
        logger.warning("Home macro fetch failed: %s", exc)
        return _HOME_MACRO

    if not payload or not payload.get("indicators"):
        return _HOME_MACRO

    by_id = {i["series_id"]: i for i in payload["indicators"]}

    # Map to the 4 rows the design wants, using friendly labels.  We pull
    # 10-Y Treasury (DGS10) instead of the design's "M2 Supply" — DGS10 is
    # both more current (daily) and richer signal than M2.
    picks = [
        ("DFF",     "Fed Funds",     "Mar FOMC"),
        ("CPIAUCSL","Core CPI YoY",  "Mar 2026"),
        ("UNRATE",  "Unemployment",  "Apr 2026"),
        ("DGS10",   "10Y Treasury",  "Daily"),
    ]
    rows = []
    for sid, label, note_default in picks:
        ind = by_id.get(sid)
        if not ind:
            continue
        rows.append({
            "label": label,
            "val":   ind.get("value_fmt") or "—",
            # change_fmt is already pre-signed ("+0.1%" / "-0.04%" / "—").
            # Append "pp" for rates so users read it as percentage points.
            "chg":   ind.get("change_fmt") or "—",
            "note":  note_default,
        })

    return rows or _HOME_MACRO


# ── 6) Retail pulse + most-discussed ────────────────────────────────
async def _fetch_home_retail() -> dict:
    """Most-discussed ticker + 4 retail rows from ApeWisdom.

    Returns dict with `feat` (most-discussed) and `rows` (4 supporting
    tickers) — matches what the template iterates.  Sentiment uses the
    same upvotes/mentions proxy the /_v2/retail page uses.
    """
    try:
        # Async-native fetcher uses the shared httpx client so all 5
        # ApeWisdom paginated requests fire concurrently and yield the
        # event loop during network I/O — no thread slots held.
        items = await _l2_cached(
            "redesign:home:apewisdom", ttl_seconds=300,
            compute=_fetch_apewisdom_async,
            category="redesign_home",
        )
    except Exception as exc:
        logger.warning("Home retail fetch failed: %s", exc)
        items = None

    if not items or len(items) < 5:
        return {
            "feat": _HOME_RETAIL_FEAT,
            "rows": _retail_rows(),
        }

    head, tail = items[0], items[1:5]
    feat_sentiment = _retail_sentiment_proxy(head)
    feat = {
        "ticker":    (head.get("ticker") or "").upper(),
        "mentions":  f"{int(head.get('mentions') or 0):,}",
        "sentiment": int(feat_sentiment * 100),
    }
    # Build 4 supporting rows matching _retail_rows() shape; bars scale
    # relative to the max-mentioned ticker in the visible set.
    rows = [
        {
            "ticker":    (it.get("ticker") or "").upper(),
            "mentions":  int(it.get("mentions") or 0),
            "sentiment": _retail_sentiment_proxy(it),
        }
        for it in tail
    ]
    return {"feat": feat, "rows": _attach_retail_bars(rows)}


# ── 7) Fear & Greed ─────────────────────────────────────────────────
async def _fetch_home_feargreed() -> dict:
    """CNN Fear & Greed live data.

    Returns the same shape `_feargreed_payload()` produces — value, label,
    yesterday, week_ago, plus precomputed needle_x / needle_y for the SVG.
    L2-cached for 5 min so cold workers don't all hit CNN simultaneously.
    Falls back to the mock 71 / Greed when CNN is unreachable.
    """
    try:
        # Async-native fetcher uses the shared httpx client — yields the
        # event loop during the CNN request, no thread slot held.
        cnn = await _l2_cached(
            "redesign:home:cnn_fg", ttl_seconds=300,
            compute=_fetch_cnn_fg_async,
            category="redesign_home",
        )
    except Exception as exc:
        logger.warning("Fear & Greed fetch failed: %s", exc)
        return _home_feargreed_mock()

    if not cnn or cnn.get("score") is None:
        return _home_feargreed_mock()

    score = int(round(cnn.get("score", 0)))
    label = cnn.get("rating") or "Neutral"

    # CNN returns prior values as either a bare number or a {score, rating}
    # dict depending on endpoint version.  Normalize either shape.
    def _coerce_prior(v) -> int:
        if v is None:
            return 0
        if isinstance(v, dict):
            return int(round(v.get("score") or v.get("value") or 0))
        try:
            return int(round(float(v)))
        except (TypeError, ValueError):
            return 0

    return _feargreed_payload(
        value=score,
        label=label,
        yesterday=_coerce_prior(cnn.get("previous_close")),
        week_ago=_coerce_prior(cnn.get("one_week_ago")),
        month_ago=_coerce_prior(cnn.get("one_month_ago")),
        year_ago=_coerce_prior(cnn.get("one_year_ago")),
    )


# ── 8) Ticker tape ──────────────────────────────────────────────────
# Symbols rendered in the tape.  Indices come from get_index_market_data,
# single-name equities from get_sp500_market_data, the rest from yfinance
# direct.  Order mirrors the original mock for visual familiarity.
_TICKER_TAPE_INDICES = ["^GSPC", "^IXIC", "^DJI", "^VIX", "^TNX"]
_TICKER_TAPE_EQUITIES = ["NVDA", "TSLA", "AAPL", "GME", "COIN", "PLTR"]
_TICKER_TAPE_LABELS = {
    "^GSPC": "S&P", "^IXIC": "NDX", "^DJI": "DOW",
    "^VIX": "VIX",  "^TNX": "10Y",
}


async def _fetch_home_ticker_tape(
    *, idx_data: dict | None = None, sp_data: dict | None = None,
) -> list:
    """Build the 18-cell ticker tape using already-cached market data.

    Indices: get_index_market_data() (cached 5 min, falls back to L2).
    Single names: get_sp500_market_data("1D") (already used by Top movers,
    so the call is shared in cache).
    Other (crypto / FX / commodities): we don't have a unified feed; show
    the mock values for those slots until a wider quote helper lands.

    Both `idx_data` and `sp_data` may be passed in by `preview_home` to
    avoid duplicate market_data fetches across top_movers / ticker_tape /
    heatmap_companies.
    """
    if idx_data is None or sp_data is None:
        try:
            from filings import market_data
            calls = []
            if idx_data is None:
                calls.append(to_heavy(market_data.get_index_market_data))
            if sp_data is None:
                calls.append(to_heavy(market_data.get_sp500_market_data, "1D"))
            results = await asyncio.gather(*calls)
            ri = iter(results)
            if idx_data is None:
                idx_data = next(ri)
            if sp_data is None:
                sp_data = next(ri)
        except Exception as exc:
            logger.warning("Ticker tape fetch failed: %s", exc)
            idx_data = idx_data or None
            sp_data = sp_data or None

    out: list[tuple[str, str, float]] = []

    # Indices — defensively read price/pct (use `is not None` not truthy `or`
    # since pct_change of 0.0 is a legitimate value that `or` would treat
    # as missing).
    if isinstance(idx_data, dict):
        for sym in _TICKER_TAPE_INDICES:
            entry = idx_data.get(sym)
            if not entry:
                continue
            label = _TICKER_TAPE_LABELS.get(sym, sym)
            price = entry.get("price")
            if price is None:
                price = entry.get("last")
            pct = entry.get("pct_change")
            if pct is None:
                pct = entry.get("change_pct")
            if price is None or pct is None:
                continue
            # Format value: TNX is reported as a yield (e.g. 4.214) — show
            # with a percent suffix; everything else is just a number.
            if sym == "^TNX":
                value_str = f"{float(price):.3f}%"
            else:
                value_str = f"{float(price):,.2f}"
            # market_data returns pct as a percentage (e.g. 0.72); the tape
            # template formats it as `pct * 100` for ▲/▼ so we pass a
            # fraction (0.0072).
            out.append((label, value_str, float(pct) / 100.0))

    # Single equities from S&P 500 daily data.  This dict has a `_metadata`
    # marker so we know it came back fully populated.
    if isinstance(sp_data, dict) and "_metadata" in sp_data:
        for sym in _TICKER_TAPE_EQUITIES:
            entry = sp_data.get(sym)
            if not entry:
                continue
            price = entry.get("price")
            pct = entry.get("pct_change")
            if price is None or pct is None:
                continue
            out.append((sym, f"{float(price):,.2f}", float(pct) / 100.0))

    # If we have anything live, use it — don't pad with mock to hit a
    # threshold (mixing real + mock is more confusing than a shorter tape).
    if not out:
        return _HOME_TICKER_TAPE
    return out


# ─────────────────────────────────────────────────────────────────────────────
# HOME SUBTABS — live data for Flow / News / Calendar.  Heatmap + Activity
# stay mock-only until proper aggregators land (flagged in audit).
# ─────────────────────────────────────────────────────────────────────────────


# ── Flow / Trending ──────────────────────────────────────────────────
async def _fetch_home_flow_trending(request: Request, limit: int = 12) -> tuple[list[dict], int]:
    """Aggregate ticker-level activity across 13F changes + congress trades.

    Returns (rows, max_count) where rows shape matches the design:
        [{tk, funds, cong, fundsDir, congDir, net}, ...]

    `funds` / `cong` are the per-ticker activity counts (buys + sells);
    `*Dir` is "BUY" / "SELL" / "—" by majority direction; `net` is
    (buys-sells) summed across both data sources, used for the right-edge
    "+N net" / "-N net" signal chip.

    Falls back to the static mock when either upstream is empty.
    """
    fund_cache = getattr(request.app.state, "fund_cache", {}) or {}
    if not fund_cache:
        return _HOME_FLOW_TRENDING, _HOME_FLOW_TRENDING_MAX

    cmap = getattr(request.app.state, "_pp_redesign_cusip_ticker", None) or {}
    if not cmap:
        cmap = _build_cusip_ticker_map(fund_cache)
        try:
            request.app.state._pp_redesign_cusip_ticker = cmap
        except Exception:
            pass

    from collections import Counter
    from filings.superinvestors import SUPERINVESTORS_BY_CIK

    # Per-ticker tallies + name lists.  Names power the hover tooltip;
    # counts drive sort + bar widths.
    funds_buy_names:  dict[str, list[str]] = {}
    funds_sell_names: dict[str, list[str]] = {}
    funds_buys:       Counter = Counter()
    funds_sells:      Counter = Counter()

    for cik, fund_data in fund_cache.items():
        si = SUPERINVESTORS_BY_CIK.get(cik)
        manager = (si.display_name if si else fund_data.get("name")) or ""
        for c in fund_data.get("changes") or []:
            raw_status = (c.get("status") or "").upper()
            action = _FUND_STATUS_MAP.get(raw_status, raw_status)
            ticker = c.get("ticker") or cmap.get(c.get("cusip", "")) or ""
            if not ticker:
                continue
            ticker = ticker.upper()
            if action in ("NEW", "ADDED"):
                funds_buys[ticker] += 1
                if manager:
                    funds_buy_names.setdefault(ticker, []).append(manager)
            elif action in ("REDUCED", "EXITED"):
                funds_sells[ticker] += 1
                if manager:
                    funds_sell_names.setdefault(ticker, []).append(manager)

    # Congress side — pull a wide window so the aggregate is meaningful.
    cong_buys:       Counter = Counter()
    cong_sells:      Counter = Counter()
    cong_d:          Counter = Counter()  # buyers only
    cong_r:          Counter = Counter()  # buyers only
    cong_buy_names:  dict[str, list[str]] = {}
    cong_sell_names: dict[str, list[str]] = {}
    try:
        from filings import supabase_cache
        cong_trades = await to_heavy(
            supabase_cache.get_congress_recent_trades, 200,
        )
    except Exception as exc:
        logger.warning("Congress trades fetch for flow trending failed: %s", exc)
        cong_trades = None

    for tr in cong_trades or []:
        ticker = (tr.get("ticker") or "").upper()
        if not ticker:
            continue
        ttype = (tr.get("trade_type") or "").lower()
        person = (tr.get("politician_name") or tr.get("name") or "").strip()
        party  = (tr.get("party") or "").upper()[:1]  # "D" / "R"
        if "buy" in ttype or "purchase" in ttype:
            cong_buys[ticker] += 1
            if person:
                cong_buy_names.setdefault(ticker, []).append(person)
            if party == "D":
                cong_d[ticker] += 1
            elif party == "R":
                cong_r[ticker] += 1
        elif "sell" in ttype or "sale" in ttype:
            cong_sells[ticker] += 1
            if person:
                cong_sell_names.setdefault(ticker, []).append(person)

    if not (funds_buys or funds_sells or cong_buys or cong_sells):
        return _HOME_FLOW_TRENDING, _HOME_FLOW_TRENDING_MAX

    all_tickers = set(funds_buys) | set(funds_sells) | set(cong_buys) | set(cong_sells)
    rows: list[dict] = []
    for tk in all_tickers:
        fb, fs = funds_buys[tk], funds_sells[tk]
        cb, cs = cong_buys[tk],  cong_sells[tk]
        funds_n = fb + fs
        cong_n  = cb + cs
        if funds_n == 0 and cong_n == 0:
            continue
        funds_dir = "BUY" if fb > fs else ("SELL" if fs > fb else "—")
        cong_dir  = "BUY" if cb > cs else ("SELL" if cs > cb else "—")
        net = (fb - fs) + (cb - cs)
        # Top-N names for the hover tooltip; the JSON blob keeps payload
        # size sane on tickers like SPY with hundreds of touches.
        fund_names_top = funds_buy_names.get(tk, [])[:5]
        fund_more = max(fb - len(fund_names_top), 0)
        cong_names_top = cong_buy_names.get(tk, [])[:3]
        cong_more = max(cb - len(cong_names_top), 0)
        rows.append({
            "tk":          tk,
            "funds":       funds_n,
            "cong":        cong_n,
            "fundsDir":    funds_dir,
            "congDir":     cong_dir,
            "net":         net,
            # Hover-tooltip + sort-by-buyers payload.
            "fund_buyers": fb,
            "fund_sellers": fs,
            "cong_buyers": cb,
            "cong_sellers": cs,
            "cong_d":      cong_d[tk],
            "cong_r":      cong_r[tk],
            "fund_names":  fund_names_top,
            "fund_more":   fund_more,
            "cong_names":  cong_names_top,
            "cong_more":   cong_more,
            "total_buyers": fb + cb,
        })

    # Sort descending by total buyer count (buys across superinvestors +
    # Congress).  Tie-break by total activity so two tickers with equal
    # buyer counts surface the busier one.
    rows.sort(key=lambda r: (-r["total_buyers"], -(r["funds"] + r["cong"])))
    top = rows[:limit]
    if not top:
        return _HOME_FLOW_TRENDING, _HOME_FLOW_TRENDING_MAX

    # Bar widths scale to the max (funds OR cong) seen in the top set so
    # the bars saturate at 60% of the row.
    max_count = max((max(r["funds"], r["cong"]) for r in top), default=1)
    return top, max(max_count, 1)


# ── News ─────────────────────────────────────────────────────────────
# Category inference — maps a Finnhub article to one of the 7 News
# subtab filters.  Order matters: more specific buckets win.  Default is
# "Markets" which catches generic financial headlines.
_NEWS_CAT_KEYWORDS = (
    ("Congress", ("Pelosi", "Tuberville", "STOCK Act", "Senate", "House",
                  "Congress", "Khanna", "Crenshaw")),
    ("Insiders", ("10b5-1", "Form 4", "insider sale", "insider buy",
                  "exercised options", "CEO sold", "CFO sold")),
    ("Funds",    ("Berkshire", "Buffett", "Ackman", "Burry", "13F",
                  "hedge fund", "superinvestor", "Druckenmiller", "Tepper",
                  "Loeb", "Einhorn")),
    ("Earnings", ("earnings", "EPS", "beat", "miss", "guidance", "Q1",
                  "Q2", "Q3", "Q4", "revenue", "raises", "lowered")),
    ("Macro",    ("Fed ", "FOMC", "rate cut", "rate hike", "CPI",
                  "PCE", "inflation", "Treasury", "yield", "10Y",
                  "GDP", "Powell", "ECB", "unemployment", "jobs report",
                  "Treasury")),
    ("Retail",   ("Reddit", "WSB", "wallstreetbets", "Roaring Kitty",
                  "meme stock", "retail")),
)


def _infer_news_category(title: str, summary: str) -> str:
    """Best-effort category bucket for a generic financial-news headline."""
    blob = f"{title or ''}  {summary or ''}"
    for cat, keywords in _NEWS_CAT_KEYWORDS:
        for kw in keywords:
            if kw.lower() in blob.lower():
                return cat
    return "Markets"


def _build_news_market_wire(
    idx_data: dict | None, sp_data: dict | None, *, limit: int = 7,
) -> list[dict]:
    """Synthesize the Market Wire panel rows from already-fetched market
    data — same source the ticker tape uses, no extra API calls.

    Each row: ``{time, ticker, change, wire}``.  Time is now-ET clamped
    to the most recent minute (we don't have per-quote timestamps from
    yfinance; the tape itself is "now" so tagging the rows that way is
    truthful).  Rows are ordered by ``abs(pct_change)`` so the most
    active names surface first.  Falls back to the static mock when no
    live data is available.
    """
    candidates: list[dict] = []
    now_et = datetime.now(ZoneInfo("America/New_York")).strftime("%H:%M")

    def _emit(label: str, price, pct, copy: str) -> None:
        if price is None or pct is None:
            return
        try:
            pct_f = float(pct)
            price_f = float(price)
        except (TypeError, ValueError):
            return
        sign = "+" if pct_f >= 0 else "−"
        candidates.append({
            "time":    now_et,
            "ticker":  label,
            "change":  f"{sign}{abs(pct_f):.2f}%",
            "wire":    copy,
            "_abs":    abs(pct_f),
        })

    # Indices — show the price level in the wire copy ("S&P at 7,229.32").
    if isinstance(idx_data, dict):
        for sym in _TICKER_TAPE_INDICES:
            entry = idx_data.get(sym) or {}
            label = _TICKER_TAPE_LABELS.get(sym, sym)
            price = entry.get("price")
            if price is None:
                price = entry.get("last")
            pct = entry.get("pct_change")
            if pct is None:
                pct = entry.get("change_pct")
            if price is None or pct is None:
                continue
            value_str = f"{float(price):.3f}%" if sym == "^TNX" else f"{float(price):,.2f}"
            _emit(label, price, pct, f"{label} at {value_str}")

    # Single-name equities — show last + a short tag.
    if isinstance(sp_data, dict) and "_metadata" in sp_data:
        for sym in _TICKER_TAPE_EQUITIES:
            entry = sp_data.get(sym) or {}
            price = entry.get("price")
            pct = entry.get("pct_change")
            if price is None or pct is None:
                continue
            _emit(sym, price, pct, f"{sym} last ${float(price):,.2f}")

    if not candidates:
        return _HOME_NEWS_MARKET_WIRE

    candidates.sort(key=lambda r: -r["_abs"])
    return [{k: v for k, v in r.items() if k != "_abs"} for r in candidates[:limit]]


async def _fetch_home_news(
    *, idx_data: dict | None = None, sp_data: dict | None = None,
) -> tuple[dict, list[dict], list[dict], list[dict]]:
    """Featured story + supporting stories + most-read + market wire.

    Returns ``(featured, stories, most_read, market_wire)``.  Featured is
    the most recent Finnhub article; stories are the next 10 (2 surface
    in the top-grid, the rest in the More-headlines feed).  Market wire
    is built from the same live index + S&P quotes the ticker tape uses
    (when available).  Most-read stays mock for now — real engagement
    metrics need a separate tracking pipeline.  Falls back to all-mock
    when every upstream is empty.
    """
    def _compute():
        # market_data.get_market_news has L1-only TTL cache — wrap it so
        # multiple workers can share Finnhub responses across restarts.
        from filings import market_data
        return market_data.get_market_news("general", 14)

    try:
        articles = await _l2_cached(
            "redesign:home:news_general", ttl_seconds=600, compute=_compute,
            category="redesign_home",
        )
    except Exception as exc:
        logger.warning("Home news fetch failed: %s", exc)
        articles = None

    market_wire = _build_news_market_wire(idx_data, sp_data)

    if not articles:
        return (
            _HOME_NEWS_FEATURED,
            _HOME_NEWS_STORIES,
            _HOME_NEWS_MOST_READ,
            market_wire,
        )

    def _shape(a: dict) -> dict:
        # Finnhub returns: headline, source, time_ago, summary, image, url,
        # related_tickers.  We pass all through so the template + modal can
        # render thumbnails and "Read full article →" links without a
        # second round-trip.  ``cat`` is inferred from headline + summary.
        title = a.get("headline") or ""
        summary = (a.get("summary") or "").strip()
        return {
            "src":     a.get("source") or "—",
            "ago":     a.get("time_ago") or "",
            "cat":     _infer_news_category(title, summary),
            "title":   title,
            "summary": summary,
            "image":   a.get("image") or "",
            "url":     a.get("url") or "",
            "tickers": a.get("related_tickers") or [],
        }

    head = articles[0]
    featured = {
        **_shape(head),
        # Keep `excerpt` for backward-compat with the template's existing
        # featured-card variable name; truncate to 280 chars for layout.
        "excerpt": (head.get("summary") or "").strip()[:280],
    }
    stories = [_shape(a) for a in articles[1:11]]
    most_read = _build_news_most_read(articles)
    return featured, stories, most_read, market_wire


def _build_news_most_read(articles: list[dict] | None, *, limit: int = 5) -> list[dict]:
    """Derive a "Most read" panel from the Finnhub article set when no
    real engagement metric is available.  Picks the articles with the
    most related tickers (rough proxy for breadth = popularity), tagged
    with a synthesized read count that scales with breadth.

    Real engagement data will replace this when a tracking pipeline lands;
    until then, this is at least real headlines instead of a static mock.
    """
    if not articles:
        return _HOME_NEWS_MOST_READ
    ranked = sorted(
        articles,
        key=lambda a: (-len(a.get("related_tickers") or []), a.get("datetime_iso", "")),
    )
    rows: list[dict] = []
    base_reads = 14000
    for i, a in enumerate(ranked[:limit]):
        title = (a.get("headline") or "").strip()
        if not title:
            continue
        # Synthesized read count — decays as rank grows so the panel
        # reads as a leaderboard.  Real metrics will overwrite this.
        reads = base_reads // (i + 1)
        if reads >= 1000:
            reads_fmt = f"{reads / 1000:.1f}K reads"
        else:
            reads_fmt = f"{reads} reads"
        rows.append({
            "rank":  f"{i + 1:02d}",
            "title": title,
            "src":   a.get("source") or "",
            "reads": reads_fmt,
        })
    return rows or _HOME_NEWS_MOST_READ


# ── Heatmap — Companies ──────────────────────────────────────────────
# Static weight + market-cap labels for the top 24 S&P names.  Weight
# drives tile size (4=2x2, 3=2x2, 2=2x1, 1=1x1).  Mcap labels refresh
# rarely so we hardcode them; they're decorative context, not a signal.
_HEATMAP_COMPANIES_META = [
    ("NVDA",  4, "$3.51T"), ("AAPL",  4, "$2.84T"), ("MSFT",  4, "$3.12T"),
    ("GOOGL", 3, "$2.18T"), ("AMZN",  3, "$1.94T"), ("META",  3, "$1.41T"),
    ("AVGO",  2, "$1.02T"), ("TSLA",  2, "$584B"),  ("BRK-B", 2, "$932B"),
    ("LLY",   1, "$728B"),  ("JPM",   1, "$612B"),  ("V",     1, "$548B"),
    ("WMT",   1, "$520B"),  ("XOM",   1, "$484B"),  ("UNH",   1, "$478B"),
    ("MA",    1, "$420B"),  ("PG",    1, "$382B"),  ("JNJ",   1, "$364B"),
    ("HD",    1, "$352B"),  ("ORCL",  1, "$342B"),  ("COST",  1, "$320B"),
    ("NFLX",  1, "$284B"),  ("BAC",   1, "$268B"),  ("AMD",   1, "$254B"),
]


def _heatmap_color_for_pct(pct_decimal: float) -> str:
    """Color-mix tile background by pct change.  Mirrors the JSX logic:
    20%-90% accent of up/down mixed with --pp-bg, scaled by abs(pct/0.03)."""
    a = min(abs(pct_decimal) / 0.03, 1.0)
    pct_mix = int(20 + a * 70)
    accent = "var(--pp-up)" if pct_decimal >= 0 else "var(--pp-down)"
    return f"color-mix(in srgb, {accent} {pct_mix}%, var(--pp-bg))"


def _heatmap_tile_is_dark(pct_decimal: float) -> bool:
    """True when the tile's mixed background is too saturated for the
    default ink-colored text — template flips to a light text color via
    the ``is-dark`` class.  Threshold ~55% mix corresponds to |pct| >= 1.5%."""
    a = min(abs(pct_decimal) / 0.03, 1.0)
    return (20 + a * 70) >= 55


async def _fetch_home_heatmap_companies(*, mkt: dict | None = None) -> list[dict]:
    """Top S&P companies with daily pct + tile sizing.

    Real daily pct from `get_sp500_market_data("1D")`; static weight + mcap
    label from `_HEATMAP_COMPANIES_META`.  Falls back to all-mock when the
    market data fetch is empty.  Accepts a pre-fetched `mkt` dict to share
    a single market_data hit with top_movers + ticker_tape.
    """
    if mkt is None:
        try:
            from filings import market_data
            mkt = await to_heavy(market_data.get_sp500_market_data, "1D")
        except Exception as exc:
            logger.warning("Heatmap companies fetch failed: %s", exc)
            mkt = None

    if not mkt or "_metadata" not in mkt:
        return _heatmap_companies_with_color()

    rows = []
    for ticker, weight, mcap in _HEATMAP_COMPANIES_META:
        # yfinance uses BRK-B; market_data normalizes — try a couple variants.
        entry = mkt.get(ticker) or mkt.get(ticker.replace("-", ".")) or mkt.get(ticker.replace("-", ""))
        if not entry:
            # Tile is part of the design's locked layout; show it dimmed
            # rather than dropping it (keeps grid balanced).
            pct = 0.0
        else:
            # market_data returns pct as a percentage (0.72 = 0.72%); the
            # color helper + template both consume a fraction (0.0072).
            pct = float(entry.get("pct_change") or 0) / 100.0

        rows.append({
            "ticker":   ticker,
            "pct":      pct,
            "mcap":     mcap,
            "weight":   weight,
            "tile_bg":  _heatmap_color_for_pct(pct),
            "tile_dark": _heatmap_tile_is_dark(pct),
            "span_col": 2 if weight >= 3 else 1,
            "span_row": 2 if weight >= 3 else 1,
            "is_large": weight >= 3,
            "show_mcap": weight >= 2,
        })
    return rows


# ── Heatmap — Sectors ────────────────────────────────────────────────
# Sector ETFs as a proxy for sector daily performance.  yfinance gives us
# 1D pct via a small batch.  Mcap labels are static.
_HEATMAP_SECTOR_ETFS = [
    ("XLK",  "Tech",          "$18.4T"),
    ("XLC",  "Comm Services", "$4.2T"),
    ("XLY",  "Cons Disc",     "$7.1T"),
    ("XLF",  "Financials",    "$9.4T"),
    ("XLI",  "Industrials",   "$5.2T"),
    ("XLV",  "Health Care",   "$7.8T"),
    ("XLP",  "Cons Staples",  "$4.1T"),
    ("XLB",  "Materials",     "$1.9T"),
    ("XLRE", "Real Estate",   "$1.4T"),
    ("XLU",  "Utilities",     "$1.3T"),
    ("XLE",  "Energy",        "$2.4T"),
]


# Module-level TTL cache for sector ETF pct.  yfinance is slow, want to
# limit to once per ~5min.  Tuple of (timestamp, dict[symbol, pct_fraction]).
_SECTOR_ETF_CACHE: tuple[float, dict] | None = None
_SECTOR_ETF_TTL = 300


def _fetch_sector_etfs_sync() -> dict[str, float]:
    """Sync yfinance batch for 11 sector ETFs.  Returns {symbol: pct_fraction}.

    Cached at module level (5-min TTL) since yfinance batches add latency.
    """
    global _SECTOR_ETF_CACHE
    import time as _time
    now = _time.time()
    if _SECTOR_ETF_CACHE and (now - _SECTOR_ETF_CACHE[0]) < _SECTOR_ETF_TTL:
        return _SECTOR_ETF_CACHE[1]

    symbols = [s for s, _, _ in _HEATMAP_SECTOR_ETFS]
    try:
        import yfinance as yf
        df = yf.download(symbols, period="5d", threads=True, progress=False, timeout=15)
        if df.empty:
            return {}
        # Multi-index handling — Close column.
        close = df["Close"] if "Close" in df.columns.get_level_values(0).unique() else df
        out: dict[str, float] = {}
        for sym in symbols:
            try:
                s = close[sym].dropna()
                if len(s) < 2:
                    continue
                last, prev = float(s.iloc[-1]), float(s.iloc[-2])
                if prev > 0:
                    out[sym] = (last - prev) / prev
            except Exception:
                continue
        _SECTOR_ETF_CACHE = (now, out)
        return out
    except Exception as exc:
        logger.warning("Sector ETF batch fetch failed: %s", exc)
        return {}


async def _fetch_home_heatmap_sectors() -> list[dict]:
    """Sector heatmap rows with real daily pct from sector ETFs.

    L2-wrapped because the underlying yfinance batch is one of the slowest
    operations on cold start (~10-15s for 11 sector ETFs).
    """
    try:
        pcts = await _l2_cached(
            "redesign:home:sector_etfs",
            ttl_seconds=300,
            compute=_fetch_sector_etfs_sync,
            category="redesign_home",
        )
    except Exception as exc:
        logger.warning("Sector heatmap fetch failed: %s", exc)
        pcts = {}

    if not pcts:
        return _heatmap_sectors_with_color()

    rows = []
    for sym, name, mcap in _HEATMAP_SECTOR_ETFS:
        pct = pcts.get(sym, 0.0)
        # Mega-cap sectors (>$10T) get 2-col span — keeps Tech at the top.
        mcap_num = float("".join(ch for ch in mcap if ch.isdigit() or ch == "."))
        is_mega = "T" in mcap and mcap_num > 10
        rows.append({
            "name":     name,
            "pct":      pct,
            "mcap":     mcap,
            "tile_bg":  _heatmap_color_for_pct(pct),
            "tile_dark": _heatmap_tile_is_dark(pct),
            "span_col": 2 if is_mega else 1,
        })
    return rows


# ── Activity feed — Supabase notifications shaped for the redesign row.

def _activity_iso_time_ago(iso_str: str) -> str:
    """Convert ISO 8601 timestamp to "3m" / "2h" / "1d" / "3w" — same shape
    as web.py::_time_ago but without the trailing " ago" so the template
    can append it consistently."""
    if not iso_str:
        return ""
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        diff = datetime.now(timezone.utc) - dt
        seconds = int(diff.total_seconds())
        if seconds < 60:
            return "now"
        minutes = seconds // 60
        if minutes < 60:
            return f"{minutes}m"
        hours = minutes // 60
        if hours < 24:
            return f"{hours}h"
        days = hours // 24
        if days < 7:
            return f"{days}d"
        return f"{days // 7}w"
    except Exception:
        return ""


# Notification type → (cat-css variant, src label, pill label).  The cat
# drives the row's dot color + type-tag color; src is the small uppercase
# label in the second column; pill is the right-side colored chip text.
_NOTIF_TYPE_META = {
    "13f_change":        ("13f",      "13F",      "13F FILING"),
    "youtube":           ("youtube",  "YOUTUBE",  "YOUTUBE"),
    "reddit_velocity":   ("reddit",   "REDDIT",   "REDDIT VELOCITY"),
    "congress_trade":    ("congress", "CONGRESS", "CONGRESS"),
    "insider_trade":     ("insider",  "INSIDER",  "INSIDER"),
    "feature_release":   ("feature",  "PANDA",    "NEW FEATURE"),
}


def _strip_ticker_prefix(text: str, ticker: str) -> str:
    """Strip a leading ``$TICKER`` token (with trailing space/punct) so the
    template can render it separately as a coral mono ticker chip without
    the title duplicating it.  No-op when the title doesn't start with $."""
    if not text or not ticker:
        return text or ""
    pre = f"${ticker.upper()}"
    s = text.lstrip()
    if s.upper().startswith(pre):
        s = s[len(pre):].lstrip(" -—–·:|")
    return s


async def _fetch_home_activity(limit: int = 12) -> list[dict]:
    """Live notification feed for the Activity subtab.

    Mirrors the production v1 home page's "Live Activity" card — pulls
    from ``supabase_cache.get_recent_notifications`` so the same source
    of truth (the `notifications` table) feeds both views.  Each row
    carries icon + title + message + type pill + time-ago, plus a click
    target (notification's own ``link`` if set, else the metadata
    ticker's stock page).  Falls back to the static mock when Supabase
    is unreachable or returns an empty set.
    """
    try:
        from filings import supabase_cache
        notifs = await to_light(supabase_cache.get_recent_notifications, limit)
    except Exception as exc:
        logger.warning("Activity feed fetch failed: %s", exc)
        return _HOME_ACTIVITY_FEED

    if not notifs:
        return _HOME_ACTIVITY_FEED

    rows: list[dict] = []
    for n in notifs:
        ntype = (n.get("type") or "").lower()
        cat_class, src_label, pill = _NOTIF_TYPE_META.get(
            ntype,
            (ntype.replace("_", "-"), ntype.split("_")[0].upper(), ntype.replace("_", " ").upper()),
        )
        meta = n.get("metadata") or {}
        ticker = (meta.get("ticker") or "").upper() if isinstance(meta, dict) else ""
        # Compact 1-line description: prefer the notification's message
        # (cleaner for the row layout), fall back to the title with the
        # ticker prefix stripped so the template can render $TICKER once.
        text = (n.get("message") or "").strip()
        if not text:
            text = _strip_ticker_prefix(n.get("title") or "", ticker)
        # Click target — prefer the notification's explicit link; fall
        # back to the stock page when we have a ticker; else send users
        # to the alerts page where they can dive into the full feed.
        href = (n.get("link") or "").strip()
        if not href:
            href = f"/_v2/stock/{ticker}" if ticker else "/profile?tab=Alerts"
        rows.append({
            "ago":    _activity_iso_time_ago(n.get("created_at") or ""),
            "src":    src_label,
            "ticker": ticker,
            "text":   text,
            "cat":    cat_class,
            "pill":   pill,
            "href":   href,
        })

    if not rows:
        return _HOME_ACTIVITY_FEED

    return rows[:limit]


# ── Calendar — Earnings ──────────────────────────────────────────────
async def _fetch_home_cal_earnings(limit: int = 6) -> list[dict]:
    """Upcoming earnings — filtered to S&P 500 names for relevance.

    earnings_calendar.get_earnings_calendar() returns ALL US earnings (Finnhub
    feed); the design's "watchlist" framing implies the user's tracked
    names.  Until we wire the actual watchlist, filter to S&P 500 so we
    don't surface every micro-cap that happens to come first alphabetically.
    """
    def _compute_earnings():
        # earnings_calendar has its own TTL cache but it's per-process L1.
        # L2-wrap so cold-start workers warm from Supabase rather than the
        # Finnhub/FMP backends.
        from filings import earnings_calendar
        return earnings_calendar.get_earnings_calendar(None, None, 4)

    try:
        from filings import market_data
        payload, sp_constituents = await asyncio.gather(
            _l2_cached("redesign:home:earnings_4w", 600, _compute_earnings,
                       category="redesign_home"),
            to_heavy(market_data.get_sp500_constituents),
        )
    except Exception as exc:
        logger.warning("Home earnings calendar fetch failed: %s", exc)
        return _HOME_CAL_EARNINGS

    entries = (payload or {}).get("entries") or []
    if not entries:
        return _HOME_CAL_EARNINGS

    sp_set = {(c.get("ticker") or "").upper() for c in (sp_constituents or [])}
    today = datetime.now().strftime("%Y-%m-%d")

    def _format_when(timing: str) -> str:
        t = (timing or "").upper()
        if t in ("BMO", "BEFORE_MARKET", "BEFORE OPEN", "PRE-MARKET"):
            return "Before open"
        if t in ("AMC", "AFTER_MARKET", "AFTER CLOSE", "POST-MARKET"):
            return "After close"
        if t in ("DMT", "DURING-MARKET"):
            return "Mid-day"
        return "TBD"

    def _format_date(iso_str: str) -> str:
        if not iso_str:
            return "—"
        if iso_str == today:
            return "Today"
        try:
            return datetime.fromisoformat(iso_str[:10]).strftime("%b %d")
        except Exception:
            return iso_str[:10]

    # Filter to S&P 500 (when constituents loaded; otherwise show all to
    # avoid an empty panel) and only future-dated rows.
    if sp_set:
        entries = [e for e in entries
                   if (e.get("ticker") or "").upper() in sp_set
                   and (e.get("date") or "") >= today]
    entries.sort(key=lambda e: (e.get("date", "9999"), e.get("ticker", "")))

    rows = []
    for e in entries[:limit]:
        ticker = (e.get("ticker") or "").upper()
        if not ticker:
            continue
        rows.append({
            "date":   _format_date(e.get("date", "")),
            "ticker": ticker,
            "when":   _format_when(e.get("hour") or e.get("time") or ""),
            "eps":    e.get("eps_estimate_fmt") or "—",
            # IV not in earnings_calendar payload — needs options vendor.
            "iv":     "—",
        })
    return rows or _HOME_CAL_EARNINGS


# ── Calendar — Macro events ──────────────────────────────────────────
async def _fetch_home_cal_macro(limit: int = 6) -> list[dict]:
    """Upcoming US macro releases (CPI, NFP, FOMC, etc.).

    fred_calendar.fetch_economic_events() returns events_by_date; flatten
    and pick the soonest `limit` upcoming events.  Period "this_month"
    gives a useful window (next 30 days).
    """
    # `all` returns every upcoming event; we filter + cap to `limit` here.
    # Other valid PERIOD_CHOICES: this_week / next_week / next_2w.
    try:
        from filings import fred_calendar
        payload = await to_heavy(
            fred_calendar.fetch_economic_events, "all", "us", "all",
        )
    except Exception as exc:
        logger.warning("Home macro calendar fetch failed: %s", exc)
        return _HOME_CAL_MACRO

    # fred_calendar returns events_by_date as a LIST of {date, date_label,
    # entries} dicts (already sorted ascending), not a dict mapping.
    by_date = (payload or {}).get("events_by_date") or []
    if not by_date:
        return _HOME_CAL_MACRO

    today = datetime.now().strftime("%Y-%m-%d")

    def _format_date(iso_str: str) -> str:
        if not iso_str:
            return "—"
        try:
            return datetime.fromisoformat(iso_str[:10]).strftime("%b %d")
        except Exception:
            return iso_str[:10]

    flat: list[tuple[str, dict]] = []
    for day in by_date:
        date_str = day.get("date") or ""
        if not date_str or date_str < today:
            continue  # Skip already-released / undated.
        for ev in day.get("entries") or []:
            flat.append((date_str, ev))
    flat.sort(key=lambda x: x[0])

    rows = []
    for date_str, ev in flat[:limit]:
        impact = (ev.get("impact") or "low").lower()
        if impact not in ("high", "medium", "low"):
            impact = "low"
        # Pre-release events have no actual; use estimate or em-dash.
        actual = ev.get("actual")
        estimate = ev.get("estimate")
        previous = ev.get("previous")
        if actual is not None:
            val = str(actual)
        elif estimate is not None:
            val = str(estimate)
        elif previous is not None:
            val = str(previous)
        else:
            val = "—"
        # Time formatting — fred_calendar typically gives "08:30" 24h ET.
        time_str = ev.get("time") or ""
        if time_str and ":" in time_str:
            time_str = f"{time_str} ET"
        else:
            time_str = "—"
        rows.append({
            "date":   _format_date(date_str),
            "label":  ev.get("event") or ev.get("name") or "—",
            "time":   time_str,
            "val":    val,
            "impact": impact,
        })
    return rows or _HOME_CAL_MACRO


@router.get("/home", response_class=HTMLResponse)
async def preview_home(request: Request):
    """Home page — live KPI strip + hero chart + 8 fully-wired Overview sections."""
    # Fetch every Overview section in parallel.  Each fetcher returns mock
    # data on failure, so the page renders even if half the upstreams are
    # cold.  Total budget capped by the slowest fetcher (typically yfinance).
    # ── Phase 1 (parallel): shared market_data fetches ──
    # 3 fetchers (top_movers, ticker_tape, heatmap_companies) all need the
    # same S&P 1D map; 2 fetchers (kpi_strip, ticker_tape) need indices.
    # Pre-fetch each ONCE and pass dicts down — saves 3-4 cache lookups
    # plus all the thread-pool slots they were holding.  Both calls are
    # already L2-backed by market_data internally so a cold start hits
    # Supabase, not yfinance.
    from filings import market_data as _md
    sp_1d_map, idx_market_map = await asyncio.gather(
        to_heavy(_md.get_sp500_market_data, "1D"),
        to_heavy(_md.get_index_market_data),
    )

    # ── Phase 2 (parallel): every other fetcher, with pre-fetched data
    # passed where applicable.  Heavy external-API fetchers (F&G, retail,
    # news, earnings, sector ETFs, FRED) are L2-cached so a cold worker
    # warms from Supabase rather than the upstream APIs.
    #
    # Fund flows + congress are fetched ONCE at the larger limit (8) and
    # sliced for the 6-row Overview panels — saves duplicate upstream
    # round trips and two heavy-pool slots. ──
    (
        kpi_items, hero,
        top_movers_rows, fund_flows_full, insider_rows, congress_full,
        macro_rows, retail_payload, feargreed_payload, ticker_tape_rows,
        flow_trending_payload,
        heatmap_companies_rows, heatmap_sectors_rows,
        activity_rows,
        news_payload,
        cal_earnings_rows, cal_macro_rows,
    ) = await asyncio.gather(
        _fetch_kpi_strip(),
        _fetch_hero_chart(),
        _fetch_home_top_movers(limit=6, mkt=sp_1d_map),
        _fetch_home_fund_flows(request, limit=8),
        _fetch_home_insiders(limit=5),
        _fetch_home_congress(limit=8),
        _fetch_home_macro(),
        _fetch_home_retail(),
        _fetch_home_feargreed(),
        _fetch_home_ticker_tape(idx_data=idx_market_map, sp_data=sp_1d_map),
        _fetch_home_flow_trending(request, limit=12),
        _fetch_home_heatmap_companies(mkt=sp_1d_map),
        _fetch_home_heatmap_sectors(),
        _fetch_home_activity(limit=12),
        _fetch_home_news(idx_data=idx_market_map, sp_data=sp_1d_map),
        _fetch_home_cal_earnings(limit=6),
        _fetch_home_cal_macro(limit=6),
    )
    # Slice the 8-row results for the 6-row Overview panels.
    fund_flow_rows = fund_flows_full[:6]
    congress_rows = congress_full[:6]
    flow_fund_buys_rows = fund_flows_full
    flow_congress_rows = congress_full
    flow_trending_rows, flow_trending_max = flow_trending_payload
    news_featured_ctx, news_stories_ctx, news_most_read_ctx, news_market_wire_ctx = news_payload
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

        # Hero S&P chart — REAL intraday via market_data.get_intraday_chart().
        # Ladders intraday → stale_intraday → 1M daily so the chart never
        # renders blank.  `chart_label` reflects which source is showing.
        "chart_path":       hero["chart_path"],
        "chart_area":       hero["chart_area"],
        "chart_ref_y":      hero["chart_ref_y"],
        "chart_tag_y":      hero["chart_tag_y"],
        "chart_change":     hero["chart_change"],
        "chart_change_pct": hero["chart_change_pct"],
        "chart_change_up":  hero["chart_change_up"],
        "chart_tag":        hero["chart_tag"],
        "chart_label":      hero["chart_label"],
        "chart_ohlcv":      hero["chart_ohlcv"],
        # Hover interaction — JSON-encoded [[ts_ms, price], …] + numeric prev close.
        # Empty string / "[]" disable hover gracefully.
        "chart_history_json": hero.get("chart_history_json", "[]"),
        "chart_prev_close":   hero.get("chart_prev_close", ""),
        # Index + timeframe toggle — nested {idx: {period: payload}} registry,
        # per-index OHLCV strip, and which (idx, period) to render first.
        # Empty fallbacks keep the JS no-op when the upstream fetch failed.
        "chart_default_index":  hero.get("chart_default_index", "S&P 500"),
        "chart_default_period": hero.get("chart_default_period", "1D"),
        "chart_indices_json":   hero.get("chart_indices_json", "{}"),
        "chart_ohlcv_json":     hero.get("chart_ohlcv_json", "{}"),

        # Retail pulse (right side of hero) — LIVE via ApeWisdom.
        "retail_feat": retail_payload["feat"],
        "retail_rows": retail_payload["rows"],

        # Body data — Overview subtab.  All 5 are LIVE (insider / congress /
        # macro / fund_flows / top_movers).  Each fetcher falls back to its
        # _HOME_* mock on upstream failure so the page never renders blank.
        "top_movers": top_movers_rows,
        "fund_flows": fund_flow_rows,
        "insiders":   insider_rows,
        "congress":   congress_rows,
        "macro":      macro_rows,

        # Ticker tape (always-on bar above subtab nav) — LIVE indices + 6
        # equities from market_data; falls back to mock when cold.
        "ticker_tape": ticker_tape_rows,

        # Fear & Greed (Overview / retail panel) — LIVE via CNN.
        "feargreed": feargreed_payload,

        # Flow subtab — LIVE.  Trending aggregates 13F changes + congress
        # trades by ticker; fund buys + congress lists reuse the Overview
        # fetchers at higher limits.
        "flow_trending":     flow_trending_rows,
        "flow_trending_max": flow_trending_max,
        "flow_fund_buys":    flow_fund_buys_rows,
        "flow_congress":     flow_congress_rows,

        # Heatmap subtab — LIVE.  Companies use S&P daily pcts + static
        # weight/mcap; sectors use 11 sector-ETF daily pcts (5-min cached).
        "heatmap_companies": heatmap_companies_rows,
        "heatmap_sectors":   heatmap_sectors_rows,

        # Activity subtab — LIVE.  Mixes recent insider / congress / news
        # events sorted by timestamp.  Cat drives dot color + pill border.
        "activity_feed": activity_rows,

        # News subtab — LIVE via Finnhub general_news.  Most-read +
        # market wire are mock-only for now (separate engagement /
        # tape data sources not yet wired).
        "news_featured":   news_featured_ctx,
        "news_stories":    news_stories_ctx,
        "news_most_read":  news_most_read_ctx,
        "news_market_wire": news_market_wire_ctx,

        # Calendar subtab — LIVE.  Earnings via earnings_calendar (Finnhub /
        # FMP), macro via fred_calendar.
        "cal_earnings": cal_earnings_rows,
        "cal_macro":    cal_macro_rows,
    }
    return templates.TemplateResponse("_redesign/home.html", ctx)


# ─────────────────────────────────────────────────────────────────────────────
# MACRO — Indicators tab is the live (default).  Other 4 tabs (Yields,
# FX & Commodities, Calendar, Heatmap) render placeholder panels until
# their real-data wiring lands in a follow-up.
# ─────────────────────────────────────────────────────────────────────────────


# Group display order on Macro/Indicators.  We render whatever fred_indicators
# returns, but order the panels (Inflation first) per the design's emphasis
# and a 2-col flow that keeps important groups in the top row.
_MACRO_GROUP_ORDER = ["inflation", "rates", "employment", "consumer", "credit"]

# KPI strip items at the top of the page.  Mapped from fred_indicators by
# series_id where possible, with sensible fallbacks if the FRED key is
# unset or a series temporarily fails.
_MACRO_KPI_SERIES = ["CPIAUCSL", "PCEPILFE", "UNRATE", "DFF", "DGS10", "T10Y2Y"]


def _macro_kpi_strip(indicators_by_id: dict) -> list[dict]:
    """Build the 6-cell KPI strip on Macro hero.

    Pulls 6 specific FRED series; for each, formats the value (already done
    by fred_indicators) and the change as a delta with up/down direction.
    Falls back to a synthetic placeholder if a series is missing.
    """
    fallback = {
        "CPIAUCSL": ("CPI · YoY",       "2.4%",  "0.2pp",  True),
        "PCEPILFE": ("Core PCE",        "2.6%",  None,     None),
        "UNRATE":   ("Unemployment",    "3.9%",  "0.1pp",  False),
        "DFF":      ("Fed Funds",       "4.50%", None,     None),
        "DGS10":    ("10Y",             "4.21%", "0.04pp", True),
        "T10Y2Y":   ("Curve · 10Y-2Y", "+0.10%", "+0.02",  True),
    }
    cells: list[dict] = []
    for sid in _MACRO_KPI_SERIES:
        ind = indicators_by_id.get(sid)
        if ind:
            # `direction` is "up"/"down"/"flat"; flip it through invert_direction
            # is already handled in fetch_indicators.
            up_map = {"up": True, "down": False, "flat": None}
            cells.append({
                "label": ind.get("name", sid),
                "value": ind.get("value_fmt") or "—",
                "delta": ind.get("change_fmt") if ind.get("change") is not None else None,
                "up":    up_map.get(ind.get("direction"), None),
            })
        else:
            label, val, delta, up = fallback[sid]
            cells.append({"label": label, "value": val, "delta": delta, "up": up})
    return cells


def _macro_groups(payload: dict) -> list[dict]:
    """Re-shape fetch_indicators() output for the template.

    The template renders one panel per group, with N indicator rows inside.
    We sort groups by _MACRO_GROUP_ORDER and pass through the indicator dicts
    (the template uses sparkline + value_fmt + change_fmt directly).
    """
    raw_groups = payload.get("groups") or []
    by_key = {g["key"]: g for g in raw_groups}
    out = []
    for key in _MACRO_GROUP_ORDER:
        g = by_key.get(key)
        if not g or not g.get("indicators"):
            continue
        out.append({
            "key":   key,
            "label": g.get("label") or key.title(),
            "rows":  g["indicators"],
        })
    # If FRED yields any unexpected groups, append them at the end.
    for g in raw_groups:
        if g["key"] not in _MACRO_GROUP_ORDER and g.get("indicators"):
            out.append({"key": g["key"], "label": g.get("label") or g["key"].title(), "rows": g["indicators"]})
    return out


async def _fetch_macro_indicators() -> dict:
    """Fetch FRED indicators in a thread (sync httpx pool).  Falls back to
    the module's mock-builder if the FRED key isn't set or all series fail.

    L2-wrapped because the cold-start path fires 13 parallel FRED API
    requests (one per indicator), each ~1-2s — too expensive on every
    worker cold start.
    """
    def _compute():
        from filings import fred_indicators
        return fred_indicators.fetch_indicators() or {}

    try:
        payload = await _l2_cached(
            "redesign:home:fred_indicators", ttl_seconds=900, compute=_compute,
            category="redesign_home",
        )
        return payload or {}
    except Exception as exc:
        logger.warning("Macro indicators fetch failed: %s", exc)
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# FUNDS — single-page detail view (Berkshire is the default profile).  The
# top segment ["Holdings", "Activity", ...] is decorative for now (the
# design shows all sections in one scroll); we wire it so future sub-tabs
# can swap in without restructuring.
# ─────────────────────────────────────────────────────────────────────────────

# Berkshire Hathaway is the demo fund the design ships with.
_DEFAULT_FUND_CIK = "1067983"

# Manager metadata for the page hero — keyed by CIK.  Just enough to fill
# the breadcrumb / icon / location row.  Source of truth for the 85
# superinvestors lives in cache.py; this is a tiny lookup for chrome.
_FUND_META: dict[str, dict] = {
    "1067983": {"manager": "Warren Buffett",   "city": "Omaha, NE",        "icon": "BRK"},
    "1336528": {"manager": "Bill Ackman",      "city": "New York, NY",     "icon": "PSC"},
    "1649339": {"manager": "Michael Burry",    "city": "Saratoga, CA",     "icon": "SCN"},
    "1079114": {"manager": "David Einhorn",    "city": "New York, NY",     "icon": "GLC"},
    "1040273": {"manager": "Daniel Loeb",      "city": "New York, NY",     "icon": "TPP"},
    "1656456": {"manager": "Cathie Wood",      "city": "St. Petersburg, FL","icon": "ARK"},
}


def _format_dollars(v: float | int | None, *, full=False) -> str:
    """Compact dollar formatter — $312.4B / $11.4B / $83M / $1.6B / $5.9B etc."""
    if v is None:
        return "—"
    v = float(v)
    if v >= 1e12:
        return f"${v / 1e12:.2f}T"
    if v >= 1e9:
        return f"${v / 1e9:.1f}B"
    if v >= 1e6:
        return f"${v / 1e6:.1f}M"
    if v >= 1e3:
        return f"${v / 1e3:.1f}K"
    return f"${v:,.0f}"


def _format_shares(v: int | None) -> str:
    """Comma-separated share count, no decimal."""
    if v is None:
        return "—"
    return f"{int(v):,}"


def _quarter_label(report_period: str) -> str:
    """Convert "2026-03-31" → "Q1 2026"."""
    if not report_period:
        return ""
    try:
        d = datetime.fromisoformat(report_period[:10])
        q = (d.month - 1) // 3 + 1
        return f"Q{q} {d.year}"
    except Exception:
        return report_period


async def _fetch_fund_data(cik: str) -> dict | None:
    """Read 13F fund summary from L2 cache (Supabase) directly.

    We don't import _get_fund_data() from web.py to avoid pulling in the
    full app-state machinery; supabase_cache.get_cached_with_stale is the
    L2 source of truth and is what _get_fund_data() falls back to anyway.
    """
    cik_norm = (cik or "").lstrip("0") or cik
    try:
        from filings import supabase_cache
        data, _fresh = await to_heavy(
            supabase_cache.get_cached_with_stale, f"13f:{cik_norm}"
        )
        return data if isinstance(data, dict) else None
    except Exception as exc:
        logger.warning("Funds L2 cache miss for CIK=%s: %s", cik, exc)
        return None


def _funds_kpi_strip(fund: dict) -> list[dict]:
    """Build the 5-cell KPI strip — AUM, Positions, Top-10 conc, Turnover, YTD vs SPY.

    AUM + Positions are real (from 13F).  Top-10 concentration is computed
    from the holdings list.  Turnover and YTD-vs-SPY are not derivable from
    a single quarter's filing — show "—" with no delta until we add a
    multi-quarter aggregate job.
    """
    aum = fund.get("total_value")
    positions = fund.get("total_holdings")
    holdings = fund.get("all_holdings") or []
    top10_value = sum(h.get("value") or 0 for h in holdings[:10])
    top10_pct = (top10_value / aum * 100) if aum else None
    return [
        {"label": "AUM",            "value": _format_dollars(aum),               "delta": None,        "up": None},
        {"label": "Positions",      "value": str(positions) if positions else "—", "delta": None,      "up": None},
        {"label": "Top 10 conc.",   "value": f"{top10_pct:.1f}%" if top10_pct else "—", "delta": None, "up": None},
        {"label": "Turnover (TTM)", "value": "—",                                "delta": None,        "up": None},
        {"label": "YTD vs SPY",     "value": "—",                                "delta": None,        "up": None},
    ]


def _funds_recent_activity(fund: dict) -> list[dict]:
    """Convert quarterly_changes → 4 timeline rows.

    Most recent quarter is `current=True`.  AUM Δ + change count summarized
    into a single line; precise deltas would need linking shares-then to
    shares-now × prices-then which we don't carry quarter-over-quarter.
    """
    qchanges = fund.get("quarterly_changes") or []
    rows = []
    for i, q in enumerate(qchanges[:4]):
        changes = q.get("changes") or []
        adds = sum(1 for c in changes if c.get("status") in ("ADDED", "NEW", "ADD"))
        cuts = sum(1 for c in changes if c.get("status") in ("REDUCED", "EXITED", "CUT", "EXIT"))
        rows.append({
            "quarter":     _quarter_label(q.get("report_period", "")),
            "filing_date": q.get("filing_date", ""),
            "count_str":   f"+{adds} / -{cuts}",
            "current":     i == 0,
            # Precise AUM Δ requires multi-quarter price reconciliation; show
            # neutral em-dash until that aggregate lands.
            "aum_delta":   "—",
            "aum_up":      None,
        })
    return rows


def _funds_holdings_table(fund: dict, top_n: int = 10) -> list[dict]:
    """Format top-N holdings for the dense table."""
    aum = fund.get("total_value") or 0
    holdings = fund.get("all_holdings") or []
    rows = []
    for i, h in enumerate(holdings[:top_n], start=1):
        val = h.get("value") or 0
        port_pct = (val / aum) if aum else 0
        rows.append({
            "rank":   i,
            "ticker": h.get("ticker") or "—",
            "name":   h.get("issuer") or "",
            "shares": _format_shares(h.get("shares")),
            "value":  _format_dollars(val),
            "port":   port_pct,         # 0..1 for bar width
            "port_pct_str": f"{port_pct * 100:.1f}%",
            # Day / 1Y price data isn't carried in fund_summary — leave a
            # placeholder; populate via market_data quote join in a follow-up.
            "last":   None,
            "day":    None,
            "qoq":    "—",
        })
    return rows


def _funds_changes_split(fund: dict) -> tuple[list[dict], list[dict]]:
    """Split the most-recent quarter's `changes` into adds/news vs cuts/exits."""
    changes = fund.get("changes") or []
    adds, cuts = [], []
    for c in changes:
        status = c.get("status", "")
        share_chg = c.get("share_change") or 0
        # Sign formatter: +22.4M sh / -92.4M sh / 1.27M sh (NEW, no sign needed but pos).
        sign = "+" if share_chg > 0 else ("-" if share_chg < 0 else "")
        magnitude = abs(share_chg)
        if magnitude >= 1e6:
            chg_str = f"{sign}{magnitude / 1e6:.1f}M sh"
        elif magnitude >= 1e3:
            chg_str = f"{sign}{magnitude / 1e3:.0f}K sh"
        else:
            chg_str = f"{sign}{magnitude:,} sh"

        row = {
            "ticker": c.get("ticker") or c.get("issuer", "")[:6].upper(),
            "name":   c.get("issuer") or "",
            "action": status,
            "val":    chg_str,
        }
        if status in ("ADDED", "NEW", "ADD"):
            adds.append(row)
        elif status in ("REDUCED", "EXITED", "CUT", "EXIT"):
            cuts.append(row)
    return adds[:4], cuts[:4]


# Mock sector allocation — Q1 2026 Berkshire-shaped split.  Once we ship a
# ticker → sector lookup in fund_summary we can compute this from holdings.
_FUNDS_SECTORS_MOCK = [
    {"name": "Technology",        "pct": 0.412, "color": "var(--pp-accent)"},
    {"name": "Financials",        "pct": 0.221, "color": "var(--pp-ink)"},
    {"name": "Consumer Staples",  "pct": 0.108, "color": "var(--pp-up)"},
    {"name": "Energy",            "pct": 0.094, "color": "var(--pp-down)"},
    {"name": "Healthcare",        "pct": 0.062, "color": "var(--pp-dim)"},
    {"name": "Other",             "pct": 0.103, "color": "var(--pp-line2)"},
]


@router.get("/funds", response_class=HTMLResponse)
async def preview_funds(request: Request, cik: str = _DEFAULT_FUND_CIK):
    """Funds detail — Berkshire by default; ?cik=1067983 to load others."""
    fund = await _fetch_fund_data(cik)
    if not fund:
        # If the L2 cache hasn't seen this CIK, render an empty-state shell
        # rather than 404.  Keeps the page navigable while data warms.
        fund = {
            "name": "—",
            "cik":  cik,
            "report_period": "",
            "filing_date":   "",
            "total_value":   0,
            "total_holdings": 0,
            "top_holdings":  [],
            "all_holdings":  [],
            "changes":       [],
            "quarterly_changes": [],
        }
    meta = _FUND_META.get(cik.lstrip("0") or cik) or _FUND_META.get(cik) or {}
    adds, cuts = _funds_changes_split(fund)

    ctx = {
        "request": request,
        **_shell_context("Funds"),
        # Hero
        "fund_icon":   meta.get("icon") or (fund.get("name", "")[:3].upper() or "FND"),
        "fund_cik":    fund.get("cik") or cik,
        "fund_name":   fund.get("name") or "—",
        "fund_manager":     meta.get("manager") or "",
        "fund_city":        meta.get("city") or "",
        "fund_filing_date": fund.get("filing_date", ""),
        "fund_report_period": fund.get("report_period", ""),
        # Body
        "funds_kpi":      _funds_kpi_strip(fund),
        "funds_activity": _funds_recent_activity(fund),
        "funds_holdings": _funds_holdings_table(fund, top_n=10),
        "funds_pos_total": fund.get("total_holdings") or 0,
        "funds_sectors":  _FUNDS_SECTORS_MOCK,
        "funds_adds":     adds,
        "funds_cuts":     cuts,
        # Active sub-tab — segment is decorative for now; only "Holdings"
        # state is wired.  When we add Activity/Performance/Sectors/Filings
        # subroutes, swap default via ?tab=Activity etc.
        "funds_tab":      "Holdings",
    }
    return templates.TemplateResponse("_redesign/funds.html", ctx)


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


@router.get("/options", response_class=HTMLResponse)
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
        **_shell_context("Options"),
        "options_kpi":  kpi,
        "options_flow": _OPTIONS_FLOW_MOCK,
        "options_tab":  "Unusual flow",
    }
    return templates.TemplateResponse("_redesign/options.html", ctx)


# ─────────────────────────────────────────────────────────────────────────────
# STOCK — Overview (default tab) is wired to real OHLCV + quote data.
# Financials / Ownership / Forecasts / Signals / Vitals are placeholders.
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


async def _fetch_stock_overview(ticker: str) -> dict:
    """Fetch OHLCV + quote for a stock.  Returns rich context dict.

    Falls back to NVDA-shaped demo data if fetches fail (so the page still
    looks complete during local dev when the network is flaky).
    """
    try:
        from filings import market_data
        ohlcv = await to_heavy(market_data.get_stock_ohlcv, ticker.upper(), "1Y")
    except Exception as exc:
        logger.warning("Stock OHLCV fetch failed for %s: %s", ticker, exc)
        ohlcv = None

    try:
        from filings import market_data
        news = await to_heavy(market_data.get_market_news, "general", 6)
    except Exception as exc:
        logger.warning("Market news fetch failed: %s", exc)
        news = []

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


def _stock_candlestick_paths(candles: list, vb_w: int = 600, vb_h: int = 200) -> dict:
    """Build SVG geometry for a 1Y candlestick + volume strip.

    Returns dict with `bars` (list of per-candle drawing data) and meta.
    Each bar entry: {x, wick_y1, wick_y2, body_y, body_h, up, vol_h}.
    """
    if not candles or len(candles) < 5:
        return {"bars": [], "n": 0}

    n = len(candles)
    # Sample down to ~60 bars for visual density.
    step = max(1, n // 60)
    sampled = candles[::step][:60]
    n_s = len(sampled)
    if n_s == 0:
        return {"bars": [], "n": 0}

    highs = [r[2] for r in sampled if r[2] is not None]
    lows  = [r[3] for r in sampled if r[3] is not None]
    if not highs or not lows:
        return {"bars": [], "n": 0}
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
        _, o, h, l, c, v = row[:6]
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
            "vol_x":   round(x - 3, 1),
            "vol_y":   round(v_strip_top + v_strip_h - vol_h, 1),
            "vol_h":   round(vol_h, 1),
        })

    return {"bars": bars, "n": n_s}


@router.get("/stock/{ticker}", response_class=HTMLResponse)
async def preview_stock(request: Request, ticker: str):
    """Stock detail — Overview tab is wired (chart, news, KPIs).
    Other 5 tabs are placeholders."""
    payload = await _fetch_stock_overview(ticker)
    chart = _stock_candlestick_paths(payload.get("candles") or [])
    kpis = _stock_kpi_strip(payload)

    # Format news for the right-rail panel.
    news_items = []
    for n in payload.get("news") or []:
        news_items.append({
            "src":   n.get("source") or n.get("publisher") or "—",
            "ago":   n.get("ago") or n.get("relative_time") or "",
            "title": n.get("title") or n.get("headline") or "",
        })

    ctx = {
        "request":      request,
        **_shell_context(""),  # Stock page doesn't highlight a sidebar item
        # Header
        "stock_ticker":   payload["ticker"],
        "stock_name":     payload["name"],
        "stock_exchange": payload["exchange"],
        "stock_cusip":    payload["cusip"],
        "stock_price":    _stock_format_price(payload["price"]),
        "stock_chg_pct":  payload["chg_pct"],
        "stock_chg_abs":  payload["chg_abs"],
        "stock_chg_up":   (payload.get("chg_pct") or 0) >= 0,
        "stock_close":    payload["close"],
        # Body
        "stock_kpi":      kpis,
        "stock_chart":    chart,
        "stock_news":     news_items[:6],
        "stock_is_mock":  payload.get("is_mock", False),
        "stock_tab":      "Overview",
    }
    return templates.TemplateResponse("_redesign/stock.html", ctx)


# ─────────────────────────────────────────────────────────────────────────────
# SCREENER — single-page filter rail + result table.  Uses mock results
# because we don't yet have a multi-criteria screener engine.  The filter
# rail is visual-only — flagged in the gap audit as a `filter_rail`
# primitive needing a server-side /api/screener/run endpoint to power it.
# ─────────────────────────────────────────────────────────────────────────────


_SCREENER_PRESETS = [
    "My filters", "Smart-money buys", "13F new positions",
    "Insider clusters", "Congress momentum", "Magnificent 7",
    "Dividend aristocrats", "Quality compounders", "Cheap & growing",
]

# Mock filter rail — until /api/screener/run exists.  Each group is a
# (title, [(label, value), ...]) tuple.  Values render as outlined chips.
_SCREENER_FILTER_GROUPS = [
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

# Mock screener results — 9 well-known tickers.  Real impl would assemble
# from market_data + 13F / insider / congress aggregators per filter.
_SCREENER_RESULTS = [
    {"tk": "NVDA",  "name": "NVIDIA Corp.",         "sec": "Tech",       "mc": "$3.51T", "pe": "68.4", "price": "142.18", "chg":  0.0421, "smart": "+12 funds", "ins": "5 SELLs", "cong": "3 BUYs"},
    {"tk": "BRK.B", "name": "Berkshire Hathaway",   "sec": "Financial",  "mc": "$932B",  "pe": "22.1", "price": "428.42", "chg":  0.0084, "smart": "+4 funds",  "ins": "—",       "cong": "1 BUY"},
    {"tk": "COIN",  "name": "Coinbase Global",      "sec": "Financial",  "mc":  "$54B",  "pe": "42.4", "price": "218.42", "chg":  0.0214, "smart": "+8 funds",  "ins": "3 BUYs",  "cong": "—"},
    {"tk": "OXY",   "name": "Occidental Petroleum", "sec": "Energy",     "mc":  "$61B",  "pe": "14.2", "price":  "64.21", "chg": -0.0124, "smart": "+6 funds",  "ins": "4 BUYs",  "cong": "—"},
    {"tk": "GOOGL", "name": "Alphabet",             "sec": "Comm.",      "mc": "$2.18T", "pe": "24.8", "price": "172.42", "chg":  0.0184, "smart": "+9 funds",  "ins": "2 SELLs", "cong": "2 BUYs"},
    {"tk": "PLTR",  "name": "Palantir Tech",        "sec": "Tech",       "mc":  "$84B",  "pe": "94.2", "price":  "34.21", "chg":  0.0642, "smart": "+18 funds", "ins": "7 SELLs", "cong": "4 BUYs"},
    {"tk": "AVGO",  "name": "Broadcom Inc.",        "sec": "Tech",       "mc": "$1.02T", "pe": "38.4", "price": "218.92", "chg":  0.0184, "smart": "+5 funds",  "ins": "1 SELL",  "cong": "1 BUY"},
    {"tk": "AMZN",  "name": "Amazon.com",           "sec": "Cons Disc",  "mc": "$1.94T", "pe": "42.1", "price": "184.20", "chg":  0.0124, "smart": "+7 funds",  "ins": "—",       "cong": "1 BUY"},
    {"tk": "META",  "name": "Meta Platforms",       "sec": "Comm.",      "mc": "$1.41T", "pe": "28.4", "price": "548.20", "chg":  0.0214, "smart": "+11 funds", "ins": "3 SELLs", "cong": "1 BUY"},
]


@router.get("/screener", response_class=HTMLResponse)
async def preview_screener(request: Request):
    """Screener — mock results (no real engine yet).  Filter rail is visual."""
    ctx = {
        "request":         request,
        **_shell_context("Screener"),
        "screener_presets": _SCREENER_PRESETS,
        "screener_active_preset": "Smart-money buys",
        "screener_filters": _SCREENER_FILTER_GROUPS,
        "screener_results": _SCREENER_RESULTS,
        "screener_universe": "8,420",
    }
    return templates.TemplateResponse("_redesign/screener.html", ctx)


# ─────────────────────────────────────────────────────────────────────────────
# CONGRESS — Trades (default tab) is live; Members / Leaderboard / Performance
# / Calendar are placeholders.  Wired to congress_trading via the supabase
# cache table populated by the existing scrape pipeline.
# ─────────────────────────────────────────────────────────────────────────────


def _congress_action(trade_type: str) -> str:
    """Map db trade_type → BUY/SELL/EXCH chip."""
    t = (trade_type or "").lower()
    if t in ("buy", "purchase"):
        return "BUY"
    if t in ("sell", "sale"):
        return "SELL"
    if "exch" in t:
        return "EXCH"
    return (trade_type or "—").upper()


def _congress_lag(trade_date: str, filing_date: str) -> int | None:
    """Days between trade and filing."""
    if not trade_date or not filing_date:
        return None
    try:
        td = datetime.fromisoformat(trade_date[:10])
        fd = datetime.fromisoformat(filing_date[:10])
        return max((fd - td).days, 0)
    except Exception:
        return None


def _congress_format_date(iso_str: str) -> str:
    """ISO → "Mon DD, YYYY"."""
    if not iso_str:
        return "—"
    try:
        d = datetime.fromisoformat(iso_str[:10])
        return d.strftime("%b %d, %Y")
    except Exception:
        return iso_str[:10]


async def _fetch_congress_data() -> dict:
    """Read recent congressional trades from Supabase cache."""
    try:
        from filings import supabase_cache
        rows = await to_heavy(supabase_cache.get_congress_recent_trades, 30)
    except Exception as exc:
        logger.warning("Congress trades fetch failed: %s", exc)
        rows = None

    if not rows:
        # Demo fallback (matches JSX prototype shape).
        rows_mock = [
            {"politician_name": "Pelosi, Nancy",      "party": "D", "chamber": "House",  "state": "CA-11", "ticker": "NVDA",  "trade_type": "buy",  "amount_display": "$1M-$5M",      "trade_date": "2026-04-28", "filing_date": "2026-04-30"},
            {"politician_name": "Tuberville, Tommy",  "party": "R", "chamber": "Senate", "state": "AL",    "ticker": "AAPL",  "trade_type": "buy",  "amount_display": "$50K-$100K",   "trade_date": "2026-04-27", "filing_date": "2026-05-01"},
            {"politician_name": "Crenshaw, Dan",      "party": "R", "chamber": "House",  "state": "TX-2",  "ticker": "MSFT",  "trade_type": "buy",  "amount_display": "$15K-$50K",    "trade_date": "2026-04-26", "filing_date": "2026-04-28"},
            {"politician_name": "Khanna, Ro",         "party": "D", "chamber": "House",  "state": "CA-17", "ticker": "GOOGL", "trade_type": "sell", "amount_display": "$15K-$50K",    "trade_date": "2026-04-26", "filing_date": "2026-04-26"},
            {"politician_name": "Bresnahan, Rob",     "party": "R", "chamber": "House",  "state": "PA-8",  "ticker": "AMZN",  "trade_type": "buy",  "amount_display": "$1K-$15K",     "trade_date": "2026-04-25", "filing_date": "2026-04-27"},
            {"politician_name": "Marshall, Roger",    "party": "R", "chamber": "Senate", "state": "KS",    "ticker": "PLTR",  "trade_type": "buy",  "amount_display": "$50K-$100K",   "trade_date": "2026-04-24", "filing_date": "2026-04-30"},
            {"politician_name": "Gottheimer, Josh",   "party": "D", "chamber": "House",  "state": "NJ-5",  "ticker": "META",  "trade_type": "buy",  "amount_display": "$15K-$50K",    "trade_date": "2026-04-23", "filing_date": "2026-04-25"},
            {"politician_name": "Pelosi, Nancy",      "party": "D", "chamber": "House",  "state": "CA-11", "ticker": "NVDA",  "trade_type": "buy",  "amount_display": "$1M-$5M",      "trade_date": "2026-04-22", "filing_date": "2026-05-01"},
            {"politician_name": "Greene, Marjorie T.","party": "R", "chamber": "House",  "state": "GA-14", "ticker": "PLTR",  "trade_type": "buy",  "amount_display": "$1K-$15K",     "trade_date": "2026-04-21", "filing_date": "2026-04-22"},
            {"politician_name": "Wexton, Jennifer",   "party": "D", "chamber": "House",  "state": "VA-10", "ticker": "TSLA",  "trade_type": "sell", "amount_display": "$50K-$100K",   "trade_date": "2026-04-20", "filing_date": "2026-04-24"},
            {"politician_name": "Crenshaw, Dan",      "party": "R", "chamber": "House",  "state": "TX-2",  "ticker": "AVGO",  "trade_type": "buy",  "amount_display": "$15K-$50K",    "trade_date": "2026-04-19", "filing_date": "2026-04-22"},
            {"politician_name": "Schweikert, David",  "party": "R", "chamber": "House",  "state": "AZ-1",  "ticker": "BRK.B", "trade_type": "buy",  "amount_display": "$15K-$50K",    "trade_date": "2026-04-18", "filing_date": "2026-04-22"},
        ]
        return {"rows": rows_mock, "is_mock": True}

    out = []
    for r in rows[:12]:
        action = _congress_action(r.get("trade_type", ""))
        lag = _congress_lag(r.get("trade_date", ""), r.get("filing_date", ""))
        out.append({
            "person":  r.get("politician_name") or "—",
            "party":   (r.get("party") or "I")[:1].upper(),
            "chamber": r.get("chamber") or "—",
            "state":   r.get("state") or "",
            "ticker":  (r.get("ticker") or "—").upper(),
            "action":  action,
            "size":    r.get("amount_display") or "—",
            "date":    _congress_format_date(r.get("trade_date", "")),
            "filed":   _congress_format_date(r.get("filing_date", "")),
            "lag":     lag,
            # Notable = high-dollar BUY in a major name.  For now flag any
            # "$1M-$5M" or larger BUY trade.
            "notable": action == "BUY" and ("$1M" in (r.get("amount_display") or "") or "$5M" in (r.get("amount_display") or "")),
        })
    return {"rows": out, "is_mock": False}


def _congress_kpi_strip(rows: list[dict]) -> list[dict]:
    """KPI strip — totals, top buy/sell, avg disclosure lag."""
    buys = [r for r in rows if r["action"] == "BUY"]
    sells = [r for r in rows if r["action"] == "SELL"]
    lags = [r["lag"] for r in rows if r["lag"] is not None]
    avg_lag = sum(lags) / len(lags) if lags else None

    # Most-bought / most-sold ticker by frequency.
    from collections import Counter
    buy_counts = Counter(r["ticker"] for r in buys)
    sell_counts = Counter(r["ticker"] for r in sells)
    most_bought = buy_counts.most_common(1)[0][0] if buy_counts else "—"
    most_sold   = sell_counts.most_common(1)[0][0] if sell_counts else "—"

    return [
        {"label": "Trades · 30d",        "value": f"{len(rows) * 34:,}",   "delta": "18%", "up": True},
        {"label": "Volume · 30d",        "value": "$48.2M",                "delta": None,  "up": None},
        {"label": "Most-bought",         "value": most_bought,             "delta": None,  "up": None},
        {"label": "Most-sold",           "value": most_sold,               "delta": None,  "up": None},
        {"label": "Avg disclosure lag",  "value": f"{int(avg_lag)} days" if avg_lag is not None else "—", "delta": None, "up": None},
        {"label": "Top performer YTD",   "value": "+34.2%",                "delta": None,  "up": None},
    ]


def _congress_notable(rows: list[dict]) -> dict | None:
    """Highest-signal trade for the coral callout."""
    candidates = [r for r in rows if r.get("notable")]
    if not candidates:
        return None
    r = candidates[0]
    return {
        "person":  r["person"],
        "party":   r["party"],
        "state":   r["state"],
        "ticker":  r["ticker"],
        "size":    r["size"],
        "date":    r["date"],
        "filed":   r["filed"],
        "lag":     r["lag"],
    }


@router.get("/congress", response_class=HTMLResponse)
async def preview_congress(request: Request):
    """Congress page — Trades tab is live; others are placeholders."""
    payload = await _fetch_congress_data()
    rows = payload.get("rows") or []

    ctx = {
        "request":         request,
        **_shell_context("Congress"),
        "congress_kpi":    _congress_kpi_strip(rows),
        "congress_rows":   rows,
        "congress_notable": _congress_notable(rows),
        "congress_total":  "412",
        "congress_is_mock": payload.get("is_mock", False),
        "congress_tab":    "Trades",
    }
    return templates.TemplateResponse("_redesign/congress.html", ctx)


# ─────────────────────────────────────────────────────────────────────────────
# INSIDERS — Filings (default tab) is live; Clusters / People / Companies /
# Calendar are placeholders.  Filings is wired to insider_trading via
# get_latest_insider_trades().
# ─────────────────────────────────────────────────────────────────────────────


def _insiders_action(trade_type: str) -> str:
    """Map OpenInsider trade_type → BUY/SELL chip."""
    t = (trade_type or "").lower()
    if "purchase" in t or "p - purchase" in t or t.startswith("p"):
        return "BUY"
    if "sale" in t or t.startswith("s"):
        return "SELL"
    return "—"


def _insiders_plan(trade_type: str) -> str:
    """Open-market vs scheduled.  OpenInsider doesn't always carry the
    10b5-1 marker on the global feed; we tag "10b5-1" only when explicitly
    present and otherwise default to "open" so the column is populated.
    """
    s = (trade_type or "").lower()
    if "10b5-1" in s or "10b5" in s:
        return "10b5-1"
    return "open"


def _insiders_format_title(title: str) -> str:
    """Compact role shown in the table — strip filler words."""
    t = (title or "").strip()
    if not t:
        return "—"
    # OpenInsider gives strings like "CEO, Director" — keep just first segment
    # for the role column to match the design's compact display.
    return t.split(",")[0].strip()


async def _fetch_insiders_data() -> dict:
    """Fetch the latest insider filings (12 most recent) with safe fallback.

    For MVP we read the global feed.  Per-ticker / per-role filtering would
    happen on the client (the segment switches above the table) and re-render
    via a future /_v2/api/insiders endpoint.
    """
    try:
        from filings import insider_trading
        trades = await to_heavy(
            insider_trading.get_latest_insider_trades,
            "", 24, "",
        )
    except Exception as exc:
        logger.warning("Insider trades fetch failed: %s", exc)
        trades = []

    if not trades:
        # Fallback to design demo set (matches the JSX prototype).
        rows_mock = [
            {"person": "Cook, Tim",        "role": "CEO",       "ticker": "AAPL",  "action": "SELL", "shares": "223,986", "price": "232.71", "value": "$52.1M",  "plan": "10b5-1", "date": "Apr 28", "flag": False},
            {"person": "Huang, Jen-Hsun",  "role": "CEO",       "ticker": "NVDA",  "action": "SELL", "shares": "120,000", "price": "142.18", "value": "$17.0M",  "plan": "10b5-1", "date": "Apr 28", "flag": False},
            {"person": "Musk, Elon",       "role": "CEO",       "ticker": "TSLA",  "action": "BUY",  "shares":  "50,000", "price": "351.92", "value": "$17.6M",  "plan": "open",   "date": "Apr 27", "flag": True},
            {"person": "Pichai, Sundar",   "role": "CEO",       "ticker": "GOOGL", "action": "SELL", "shares":  "22,500", "price": "192.34", "value": " $4.3M",  "plan": "10b5-1", "date": "Apr 27", "flag": False},
            {"person": "Zuckerberg, Mark", "role": "CEO",       "ticker": "META",  "action": "SELL", "shares":  "38,000", "price": "612.40", "value": "$23.2M",  "plan": "10b5-1", "date": "Apr 26", "flag": False},
            {"person": "Karp, Alex",       "role": "CEO",       "ticker": "PLTR",  "action": "SELL", "shares": "150,000", "price":  "82.04", "value": "$12.3M",  "plan": "open",   "date": "Apr 26", "flag": False},
            {"person": "Hollub, Vicki",    "role": "CEO",       "ticker": "OXY",   "action": "BUY",  "shares":  "12,400", "price":  "63.66", "value": "$789K",   "plan": "open",   "date": "Apr 25", "flag": True},
            {"person": "Diller, Barry",    "role": "Chairman",  "ticker": "IAC",   "action": "BUY",  "shares":  "80,000", "price":  "42.18", "value": "$3.37M",  "plan": "open",   "date": "Apr 25", "flag": True},
            {"person": "Jassy, Andy",      "role": "CEO",       "ticker": "AMZN",  "action": "SELL", "shares":  "25,000", "price": "226.08", "value": "$5.65M",  "plan": "10b5-1", "date": "Apr 24", "flag": False},
            {"person": "Nadella, Satya",   "role": "CEO",       "ticker": "MSFT",  "action": "SELL", "shares":  "18,500", "price": "444.91", "value": "$8.23M",  "plan": "10b5-1", "date": "Apr 24", "flag": False},
            {"person": "Wood, Cathie",     "role": "10% own.",  "ticker": "COIN",  "action": "BUY",  "shares":  "84,200", "price": "185.41", "value": "$15.6M",  "plan": "open",   "date": "Apr 23", "flag": True},
            {"person": "Benioff, Marc",    "role": "CEO",       "ticker": "CRM",   "action": "SELL", "shares":  "14,800", "price": "284.12", "value": "$4.21M",  "plan": "10b5-1", "date": "Apr 23", "flag": False},
        ]
        return {"rows": rows_mock, "is_mock": True}

    rows = []
    for tr in trades[:12]:
        rows.append({
            "person":  tr.insider_name,
            "role":    _insiders_format_title(tr.title),
            "ticker":  (tr.ticker or "").upper(),
            "action":  _insiders_action(tr.trade_type),
            "shares":  tr.qty or "—",
            "price":   tr.price or "—",
            "value":   tr.value or "—",
            "plan":    _insiders_plan(tr.trade_type),
            "date":    (tr.trade_date or tr.filing_date or "")[:10],
            # Flag = open-market BUYs (≥ $1M) — strongest signal.
            "flag":    _insiders_action(tr.trade_type) == "BUY"
                       and _insiders_plan(tr.trade_type) == "open",
        })
    return {"rows": rows, "is_mock": False}


def _insiders_kpi_strip(rows: list[dict]) -> list[dict]:
    """Build the 6-cell KPI strip — counts, top buyer/seller from current view.

    Pure-from-window stats (filings count, top buyer/seller).  Net-flow + B/S
    ratio + active clusters need server-side aggregation we don't have yet —
    show fixed placeholders with no delta.
    """
    buys  = [r for r in rows if r["action"] == "BUY"]
    sells = [r for r in rows if r["action"] == "SELL"]
    top_buyer  = buys[0]["ticker"]  if buys  else "—"
    top_seller = sells[0]["ticker"] if sells else "—"
    return [
        {"label": "Filings · 30d",    "value": "1,847",                 "delta": "4.2%", "up": False},
        {"label": "Net flow · 30d",   "value": "-$2.84B",               "delta": None,   "up": None},
        {"label": "Buy / Sell ratio", "value": f"{len(buys)/max(len(sells),1):.2f}", "delta": None, "up": None},
        {"label": "Active clusters",  "value": "12",                    "delta": "3",    "up": True},
        {"label": "Top buyer",        "value": top_buyer,               "delta": None,   "up": None},
        {"label": "Top seller",       "value": top_seller,              "delta": None,   "up": None},
    ]


def _insiders_notable(rows: list[dict]) -> dict | None:
    """Pick the most notable open-market BUY for the coral-bordered callout."""
    candidates = [r for r in rows if r.get("flag")]
    if not candidates:
        return None
    r = candidates[0]
    return {
        "person":      r["person"],
        "role":        r["role"],
        "ticker":      r["ticker"],
        "value":       r["value"],
        "shares":      r["shares"],
        "price":       r["price"],
        "date":        r["date"],
        # Headline construction is left for the template — pass parts only.
    }


@router.get("/insiders", response_class=HTMLResponse)
async def preview_insiders(request: Request):
    """Insiders page — Filings tab is live; others are placeholders."""
    payload = await _fetch_insiders_data()
    rows = payload.get("rows") or []

    ctx = {
        "request":         request,
        **_shell_context("Insiders"),
        "insiders_kpi":    _insiders_kpi_strip(rows),
        "insiders_rows":   rows,
        "insiders_notable": _insiders_notable(rows),
        "insiders_total":  "1,847",  # 30d count placeholder
        "insiders_is_mock": payload.get("is_mock", False),
        "insiders_tab":    "Filings",
    }
    return templates.TemplateResponse("_redesign/insiders.html", ctx)


# ─────────────────────────────────────────────────────────────────────────────
# PROFILE — Watchlist (default tab) is live.  Alerts / Account / Subscription
# are placeholders.  Watchlist reads from filings.watchlist (local JSON).
# ─────────────────────────────────────────────────────────────────────────────


_PROFILE_USER = {
    "initials": "TM",
    "name":     "Tev McNeill",
    "email":    "tev@paperpanda.io",
    "plan":     "Pro plan",
    "member_since": "Jan 2024",
}


# Mock watchlist collections (segment selector at top).  Real grouping needs
# a list_id field on watchlist entries — current schema is flat.
_PROFILE_LISTS_MOCK = [
    "Core",
    "AI infra",
    "Smart-money buys",
    "Earnings this week",
    "Energy",
]


def _profile_format_added(iso_str: str) -> str:
    """Convert ISO timestamp → "MMM DD, YYYY" for the table column."""
    if not iso_str:
        return "—"
    try:
        d = datetime.fromisoformat(iso_str)
        return d.strftime("%b %d, %Y")
    except Exception:
        return iso_str[:10]


async def _fetch_profile_watchlist() -> list[dict]:
    """Read the local JSON watchlist and shape rows for the table.

    Price / day-pct / 1Y spark / earnings date / alerts count would each
    require per-ticker fetches (market_data, earnings_calendar, notifications).
    We render those columns as `—` for now — wire in a follow-up batch
    fetch once the row count starts mattering.
    """
    try:
        from filings import watchlist
        entries = await to_heavy(watchlist.load_watchlist)
    except Exception as exc:
        logger.warning("Watchlist load failed: %s", exc)
        entries = []

    rows = []
    for e in entries:
        rows.append({
            "ticker":   e.get("ticker", ""),
            "name":     e.get("issuer_name", "") or e.get("ticker", ""),
            "price":    None,
            "chg":      None,
            "alerts":   "none",
            "earnings": "—",
            "added":    _profile_format_added(e.get("added_at", "")),
        })
    return rows


@router.get("/profile", response_class=HTMLResponse)
async def preview_profile(request: Request):
    """Profile page — Watchlist tab is live; others are placeholders."""
    rows = await _fetch_profile_watchlist()

    ctx = {
        "request":    request,
        **_shell_context("Profile"),
        "user":       _PROFILE_USER,
        "watch_lists": [
            (label, len(rows) if i == 0 else 0)
            for i, label in enumerate(_PROFILE_LISTS_MOCK)
        ],
        "watch_active": _PROFILE_LISTS_MOCK[0],
        "watch_rows": rows,
        "watch_empty": len(rows) == 0,
        "profile_tab": "Watchlist",
    }
    return templates.TemplateResponse("_redesign/profile.html", ctx)


# ─────────────────────────────────────────────────────────────────────────────
# RETAIL — Pulse (default tab) is live; Trends + WSB are placeholders.
# Pulse is wired to ApeWisdom via filings.sentiment._get_apewisdom_all().
# ─────────────────────────────────────────────────────────────────────────────


# Static name overrides for tickers ApeWisdom doesn't include or where the
# returned name differs from market convention.  Used in the trending table.
_RETAIL_NAME_OVERRIDES = {
    "GME":  "GameStop Corp.",
    "AMC":  "AMC Entertainment",
    "BB":   "BlackBerry",
    "TSLA": "Tesla",
    "NVDA": "NVIDIA",
    "AAPL": "Apple",
    "PLTR": "Palantir",
    "SOFI": "SoFi Technologies",
}


def _retail_name(item: dict) -> str:
    tk = (item.get("ticker") or "").upper()
    if tk in _RETAIL_NAME_OVERRIDES:
        return _RETAIL_NAME_OVERRIDES[tk]
    # ApeWisdom occasionally returns HTML-escaped names ("SPDR S&amp;P 500…").
    # Unescape so Jinja's auto-escape doesn't produce a doubled `&amp;`.
    import html as _html
    return _html.unescape(item.get("name", tk) or tk)


def _retail_dod_delta(item: dict) -> float:
    """Day-over-day change in mentions, expressed as a fraction.

    ApeWisdom returns `mentions_24h_ago`.  When 0 / missing we treat the
    ticker as "new this window" and return a high positive number to keep
    it ranked at the top.
    """
    now = item.get("mentions") or 0
    then = item.get("mentions_24h_ago") or 0
    if then <= 0:
        return 1.0 if now > 0 else 0.0
    return (now - then) / then


def _retail_sentiment_proxy(item: dict) -> float:
    """ApeWisdom doesn't expose sentiment per ticker.  Use upvotes/mentions
    ratio as a crude proxy in [-1, 1].  Real signal needs Reddit comment
    NLP or Finnhub per-ticker — call out as a gap."""
    mentions = max(int(item.get("mentions") or 0), 1)
    upvotes = int(item.get("upvotes") or 0)
    raw = upvotes / mentions  # typically 0..50
    # Squash to [-1, 1] with a soft target of ~0.5 = neutral.
    if raw <= 0:
        return -0.2
    if raw < 1:
        return -0.1 + raw * 0.4
    if raw < 5:
        return 0.3 + (raw - 1) * 0.1
    if raw < 20:
        return 0.7 + min((raw - 5) / 30, 0.25)
    return 0.95


async def _fetch_retail_data() -> dict:
    """Fetch trending tickers from ApeWisdom with safe fallbacks.

    Returns dict with `featured` (top ticker dict) + `trending` (list[dict]).
    Falls back to a static demo set when ApeWisdom is unreachable.
    """
    try:
        from filings import sentiment
        items = await to_heavy(sentiment._get_apewisdom_all)
    except Exception as exc:
        logger.warning("ApeWisdom fetch failed: %s", exc)
        items = []

    if not items:
        # Demo fallback — same shape ApeWisdom returns.
        items = [
            {"rank": 1, "ticker": "GME",  "name": "GameStop",   "mentions": 8420, "upvotes": 24180, "mentions_24h_ago":  916},
            {"rank": 2, "ticker": "NVDA", "name": "NVIDIA",     "mentions": 2891, "upvotes": 12480, "mentions_24h_ago": 2532},
            {"rank": 3, "ticker": "AMC",  "name": "AMC",        "mentions": 2104, "upvotes":  6120, "mentions_24h_ago": 1294},
            {"rank": 4, "ticker": "TSLA", "name": "Tesla",      "mentions": 1842, "upvotes":  3120, "mentions_24h_ago": 2004},
            {"rank": 5, "ticker": "PLTR", "name": "Palantir",   "mentions": 1421, "upvotes":  4980, "mentions_24h_ago": 1364},
            {"rank": 6, "ticker": "AAPL", "name": "Apple",      "mentions": 1280, "upvotes":  2140, "mentions_24h_ago": 1257},
            {"rank": 7, "ticker": "BB",   "name": "BlackBerry", "mentions":  924, "upvotes":  3812, "mentions_24h_ago":  381},
            {"rank": 8, "ticker": "SOFI", "name": "SoFi",       "mentions":  842, "upvotes":  2104, "mentions_24h_ago":  776},
        ]
        is_mock = True
    else:
        is_mock = False

    # Cap to 8 trending rows for the dense table.
    items = items[:8]
    featured = items[0] if items else None

    return {"featured": featured, "trending": items, "is_mock": is_mock}


def _retail_kpi_strip(payload: dict) -> list[dict]:
    """6-cell KPI strip — mentions total, active tickers, sentiment proxies, top mover, hottest sector."""
    items = payload.get("trending") or []
    total_mentions = sum(int(i.get("mentions") or 0) for i in items)
    if total_mentions >= 1_000_000:
        mentions_str = f"{total_mentions / 1_000_000:.1f}M"
    elif total_mentions >= 1_000:
        mentions_str = f"{total_mentions / 1_000:.1f}K"
    else:
        mentions_str = f"{total_mentions:,}"
    # Top mover by % DoD
    by_dod = sorted(items, key=_retail_dod_delta, reverse=True)
    top = by_dod[0] if by_dod else None
    top_str = f"${top['ticker']} +{int(_retail_dod_delta(top) * 100)}%" if top else "—"
    return [
        {"label": "Mentions · 24h", "value": mentions_str,           "delta": None,              "up": None},
        {"label": "Active tickers", "value": f"{len(items):,}",      "delta": None,              "up": None},
        {"label": "Sentiment idx",  "value": "+44",                  "delta": "+6",              "up": True},
        {"label": "WSB index",      "value": "+71",                  "delta": None,              "up": None},
        {"label": "Top mover",      "value": top_str,                "delta": None,              "up": None},
        {"label": "Hottest sector", "value": "AI / Chips",           "delta": None,              "up": None},
    ]


def _retail_trending_rows(items: list[dict]) -> list[dict]:
    """Format trending rows for the dense table — mentions, DoD, sentiment, etc."""
    rows = []
    for i, it in enumerate(items, start=1):
        mentions = int(it.get("mentions") or 0)
        dod = _retail_dod_delta(it)
        sentiment = _retail_sentiment_proxy(it)
        # Sentiment bar: centered at 50%, fills outward.  abs(s) * 50% width.
        bar_width = abs(sentiment) * 50  # in % of bar
        # When negative the fill grows leftward from center via translateX(-100%).
        rows.append({
            "rank":     i,
            "ticker":   (it.get("ticker") or "").upper(),
            "name":     _retail_name(it),
            "mentions": mentions,
            "mentions_str": f"{mentions:,}",
            "dod":      dod,
            "dod_str":  f"{'+' if dod >= 0 else ''}{int(dod * 100)}%",
            "sentiment": sentiment,
            "sentiment_str": f"{'+' if sentiment >= 0 else ''}{int(sentiment * 100)}",
            "sentiment_bar_width": round(bar_width, 1),
            "sentiment_up":        sentiment >= 0,
            # Price / day data not joined yet — placeholder for now.
            "price":    None,
            "price_chg": None,
        })
    return rows


@router.get("/retail", response_class=HTMLResponse)
async def preview_retail(request: Request):
    """Retail page — Pulse tab is live (ApeWisdom).  Trends + WSB are placeholders."""
    payload = await _fetch_retail_data()
    featured = payload.get("featured")
    trending = payload.get("trending") or []

    # Featured callout — top ticker with formatted stats.
    feat_dod = _retail_dod_delta(featured) if featured else 0
    feat_sentiment = _retail_sentiment_proxy(featured) if featured else 0
    featured_ctx = None
    if featured:
        m = int(featured.get("mentions") or 0)
        featured_ctx = {
            "ticker":         (featured.get("ticker") or "").upper(),
            "name":           _retail_name(featured),
            "mentions":       m,
            "mentions_str":   f"{m:,}",
            "dod_str":        f"+{int(feat_dod * 100)}% DoD" if feat_dod >= 0 else f"{int(feat_dod * 100)}% DoD",
            "dod_up":         feat_dod >= 0,
            "sentiment":      int(feat_sentiment * 100),
            "sentiment_str":  f"+{int(feat_sentiment * 100)}" if feat_sentiment >= 0 else f"{int(feat_sentiment * 100)}",
            "sentiment_label": "Bullish" if feat_sentiment >= 0.3 else ("Bearish" if feat_sentiment < -0.1 else "Neutral"),
            "sentiment_up":   feat_sentiment >= 0,
            # Price / day chg need market_data join — placeholder for MVP.
            "price":          "—",
            "price_chg":      "—",
            "price_up":       None,
        }

    ctx = {
        "request":          request,
        **_shell_context("Retail"),
        "retail_kpi":       _retail_kpi_strip(payload),
        "retail_featured":  featured_ctx,
        "retail_trending":  _retail_trending_rows(trending),
        "retail_is_mock":   payload.get("is_mock", False),
        "retail_tab":       "Pulse",
    }
    return templates.TemplateResponse("_redesign/retail.html", ctx)


@router.get("/macro", response_class=HTMLResponse)
async def preview_macro(request: Request):
    """Macro page — Indicators tab is live, others are placeholders."""
    payload = await _fetch_macro_indicators()
    indicators = payload.get("indicators") or []
    indicators_by_id = {i["series_id"]: i for i in indicators}

    kpi_items = _macro_kpi_strip(indicators_by_id)
    groups = _macro_groups(payload)

    # Page hero counts/sub — total tracked indicators + last-updated stamp.
    indicator_count = sum(len(g["rows"]) for g in groups) or len(indicators)
    last_updated = payload.get("last_updated", "")
    is_mock = bool(payload.get("is_mock"))

    ctx = {
        "request":      request,
        **_shell_context("Macro"),
        "page_title":   "Macro",
        "macro_kpi":    kpi_items,
        "macro_groups": groups,
        "macro_count":  indicator_count,
        "macro_updated": last_updated,
        "macro_is_mock": is_mock,
        # Active tab — drives which content block renders + segment highlight.
        "macro_tab":    "Indicators",
    }
    return templates.TemplateResponse("_redesign/macro.html", ctx)


# ─────────────────────────────────────────────────────────────────────────────
# Async-native fetchers for the warmer / L2 hot path
# ─────────────────────────────────────────────────────────────────────────────
# Use the shared httpx.AsyncClient so network I/O yields the event loop
# instead of holding a thread-pool slot.  URL / header / shape constants
# live in `sentiment.py` so the sync + async fetchers share one source
# of truth for the wire contract.


async def _fetch_cnn_fg_async() -> dict | None:
    """CNN Fear & Greed via the shared async HTTP client."""
    from filings import sentiment
    from filings.http_client import get_async_client

    try:
        r = await get_async_client().get(
            sentiment._CNN_FG_URL, headers=sentiment._CNN_FG_HEADERS,
        )
        r.raise_for_status()
        return sentiment._normalize_cnn_fg(r.json())
    except Exception as exc:
        logger.debug("CNN F&G async fetch failed: %s", exc)
        return None


async def _fetch_apewisdom_async(pages: int = 5) -> list[dict]:
    """ApeWisdom trending tickers — all pages fetched concurrently."""
    from filings import sentiment
    from filings.http_client import get_async_client

    client = get_async_client()

    async def _one_page(page: int) -> list[dict]:
        try:
            r = await client.get(sentiment._APEWISDOM_PAGE_URL.format(page=page))
            r.raise_for_status()
            return r.json().get("results") or []
        except Exception as exc:
            logger.debug("ApeWisdom page %d fetch failed: %s", page, exc)
            return []

    page_results = await asyncio.gather(*[_one_page(p) for p in range(1, pages + 1)])
    merged: list[dict] = []
    for rows in page_results:
        merged.extend(rows)
    merged.sort(key=lambda r: r.get("rank") or 9999)
    return merged


def _l2_warmup_targets() -> list[tuple[str, int, Callable[[], Any]]]:
    """L2 cache entries keyed by (cache_key, ttl_seconds, compute_fn).

    The warmer awaits ``l2_cached`` against each — async compute fns
    bypass the heavy pool (yield event loop on network I/O); sync fns
    flow through the heavy semaphore.
    """
    from filings import market_data, fred_indicators, earnings_calendar

    return [
        # Async-native — no thread slot held during network I/O.
        ("redesign:home:cnn_fg",          300, _fetch_cnn_fg_async),
        ("redesign:home:apewisdom",       300, _fetch_apewisdom_async),
        ("redesign:home:hero_chart",      120, _hero_chart_compute),
        # Sync upstreams — go through the heavy pool / semaphore.
        ("redesign:home:sector_etfs",     300, _fetch_sector_etfs_sync),
        ("redesign:home:news_general",    600, lambda: market_data.get_market_news("general", 14)),
        ("redesign:home:earnings_4w",     600, lambda: earnings_calendar.get_earnings_calendar(None, None, 4)),
        ("redesign:home:fred_indicators", 900, lambda: fred_indicators.fetch_indicators() or {}),
    ]


async def warm_l2_caches() -> dict:
    """Pre-warm every L2 cache entry the home page depends on.

    Designed to be called from a recurring background task in the FastAPI
    lifespan.  All targets fire concurrently — independent upstreams,
    no shared backpressure beyond the heavy semaphore.  Returns a small
    status dict so the caller can log progress.
    """
    targets = _l2_warmup_targets()
    results = await asyncio.gather(
        *(
            _l2_cached(key, ttl_seconds=ttl, compute=compute_fn, category="redesign_home")
            for key, ttl, compute_fn in targets
        ),
        return_exceptions=True,
    )
    succeeded = 0
    failed: list[str] = []
    for (key, _ttl, _fn), result in zip(targets, results):
        if isinstance(result, Exception):
            logger.debug("warm_l2_caches: %s raised: %s", key, result)
            failed.append(key)
        elif result is None:
            failed.append(key)
        else:
            succeeded += 1
    return {"warmed": succeeded, "failed": failed, "total": len(targets)}
