"""Home page (v2 redesign) + L2 warmup orchestrator.

Routes:
  * GET /                          -- the main home page (Markets dashboard)
  * GET /_pages                    -- dev-only index of all redesign pages
  * GET /api/home/heatmap          -- lazy partial: companies + sectors heatmap
  * GET /api/home/activity         -- lazy partial: activity feed
  * GET /api/home/calendar         -- lazy partial: earnings + macro calendar

Also owns:
  * ``warm_homepage_caches``       -- worker prefetch primer for home L2 reads
  * ``warm_l2_caches``             -- recurring L2-warmer task body
  * ``_l2_warmup_targets``         -- list of (key, ttl, compute) tuples
  * Async-native fetchers          -- ApeWisdom / CNN F&G shared HTTP path

The home page Phase-1 / Phase-2 gather fans out to 14 fetchers; every
one is wrapped in ``_bounded_call`` so a single slow upstream can't stall
the render.  Bundle data flows through L2 (Supabase-backed) with SWR --
warm hits are sub-second.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Callable, TypedDict
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from filings import supabase_cache
from filings.app_state import templates
from filings.cache_l2 import l2_cached as _l2_cached
from filings.concurrency import (
    to_heavy,
    to_supabase,
    to_upstream,
)
from filings.routers._redesign.helpers import (
    _bounded_call,
    _build_cusip_ticker_map,
    _compact_amount_str,
    _compact_range_str,
    _congress_action,
    _insiders_action,
    _insiders_format_title,
    _shell_context,
    _short_date,
    _time_ago_iso,
    GracefulRoute,
    SPARK,
    SPARK_DOWN,
)

# ── Payload contracts ────────────────────────────────────────────────
# TypedDict shapes for the two highest-traffic dict-shaped fallbacks on
# the homepage hot path.  Catches drift between the bounded fallback
# (returned on upstream failure) and the success payload — exactly the
# class of bug we hit when ``_fetch_hero_chart`` gained a new key and
# ``_fallback_hero_payload`` didn't, so degraded renders 500'd on the
# missing ``chart_history_json`` key.
#
# The list-shaped fallbacks (top_movers / fund_flows / insiders / etc.)
# can't carry TypedDict contracts — the elements are heterogeneous and
# the templates iterate with `.get()` defenses.  Their module-level
# constants serve as the shape reference.


class HeroChartPayload(TypedDict):
    """`/` hero S&P/NASDAQ/DOW chart — full server-side render bundle."""
    chart_path:           str
    chart_area:           str
    chart_ref_y:          float
    chart_tag_y:          float
    chart_change:         str
    chart_change_pct:     str
    chart_change_up:      bool
    chart_tag:            str
    chart_label:          str
    chart_history_json:   str
    chart_prev_close:     str
    chart_ohlcv:          list[tuple[str, str]]
    chart_ohlcv_json:     str
    chart_default_index:  str
    chart_default_period: str
    chart_indices_json:   str


class FearGreedPayload(TypedDict):
    """CNN Fear & Greed gauge payload — bounded fallback target."""
    value:      int
    label:      str
    yesterday:  int
    week_ago:   int
    month_ago:  int
    year_ago:   int
    week_band:  str
    month_band: str
    year_band:  str
    as_of:      str
    needle_x:   float
    needle_y:   float


logger = logging.getLogger(__name__)

router = APIRouter(route_class=GracefulRoute)


# ─────────────────────────────────────────────────────────────────────────────
# Index — links to every preview page so we can navigate the build.
# Acts as a contact sheet of all redesign pages while they're in progress.
# ─────────────────────────────────────────────────────────────────────────────


@router.get("/_pages", response_class=HTMLResponse)
async def preview_index(request: Request):
    """Dev-only page index — lists every redesign route."""
    pages = [
        ("Home",      "/"),
        ("Stock",     "/stock/AAPL"),
        ("Funds",     "/funds"),
        ("Screener",  "/screener"),
        ("Insiders",  "/insiders"),
        ("Congress",  "/congress"),
        ("Macro",     "/macro"),
        ("Retail",    "/retail"),
        ("Options",   "/options"),
        ("Profile",   "/profile"),
    ]
    return templates.TemplateResponse(
        "_redesign/_preview_index.html",
        {"request": request, "pages": pages, **(await _shell_context(request, "Home"))},
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


# Hero chart viewBox — 800×200 (~4:1) matches the typical full-width hero
# panel container ratio so `preserveAspectRatio="none"` doesn't squish lines.
_HERO_VB_W = 800
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


def _fallback_hero_payload() -> HeroChartPayload:
    """Synthetic single-period payload for when every upstream fails."""
    fallback_pts = [
        110 + math.sin(i * 0.4) * 30 + math.cos(i * 0.9) * 10 - i * 0.55
        for i in range(60)
    ]
    # Step matches the hero viewBox width: 60 evenly-spaced bars across `_HERO_VB_W`.
    _step_x = _HERO_VB_W / 60
    line_d = " ".join(
        f"{'M' if i == 0 else 'L'}{i * _step_x:.1f} {y:.1f}"
        for i, y in enumerate(fallback_pts)
    )
    area_d = f"{line_d} L {_HERO_VB_W} {_HERO_VB_H} L 0 {_HERO_VB_H} Z"
    period: dict = {
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
    fallback_ohlcv: list[tuple[str, str]] = [
        ("OPEN",  "5,805.56"),
        ("HIGH",  "5,851.20"),
        ("LOW",   "5,798.14"),
        ("VOL",   "3.41B"),
    ]
    return {
        "chart_path":           line_d,
        "chart_area":           area_d,
        "chart_ref_y":          74.0,
        "chart_tag_y":          74.0,
        "chart_change":         "41.86",
        "chart_change_pct":     "0.72%",
        "chart_change_up":      True,
        "chart_tag":            "5847",
        "chart_label":          "INTRADAY · 15M",
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


async def _hero_chart_compute() -> list[tuple[dict | None, dict | None]] | None:
    """Inner fetcher for the hero chart.  Fans out 6 yfinance calls (3
    indices × intraday + 5Y) in parallel.  Returns ``None`` on total
    failure so the L2 wrapper can return a stale entry instead of
    caching an empty result."""
    from filings import market_data

    async def _fetch_pair(symbol: str) -> tuple[dict | None, dict | None]:
        results = await asyncio.gather(
            to_heavy(market_data.get_intraday_chart, symbol),
            to_heavy(market_data.get_stock_ohlcv, symbol, "5Y"),
            return_exceptions=True,
        )
        intraday_raw, history_raw = results[0], results[1]
        intraday:   dict | None = None if isinstance(intraday_raw, BaseException) else intraday_raw
        history_5y: dict | None = None if isinstance(history_raw,  BaseException) else history_raw
        return intraday, history_5y

    try:
        return await asyncio.gather(
            *(_fetch_pair(sym) for _label, sym in _HERO_INDICES),
            return_exceptions=False,
        )
    except Exception as exc:
        logger.warning("Hero chart fetch failed: %s", exc)
        return None


async def _fetch_hero_chart() -> HeroChartPayload:
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
    # `indices_payload` was non-empty (guard above), and every key in it
    # came from `_HERO_INDICES`, so the `next` always finds a match —
    # the `cast` documents that for mypy.
    default_idx = next(lbl for lbl, _ in _HERO_INDICES if lbl in indices_payload)
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
    return now.strftime("%b %d %Y %-I:%M %p UTC")


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
) -> FearGreedPayload:
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


def _home_feargreed_mock() -> FearGreedPayload:
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
    {"ago": "3m",  "src": "REDDIT",   "ticker": "LLY",   "text": "Eli Lilly — mentions +100% in 24h (4 total)",        "cat": "reddit",   "pill": "REDDIT VELOCITY", "href": "/stock/LLY"},
    {"ago": "3m",  "src": "REDDIT",   "ticker": "ANY",   "text": "Sphere 3D — mentions +200% in 24h (3 total)",        "cat": "reddit",   "pill": "REDDIT VELOCITY", "href": "/stock/ANY"},
    {"ago": "3m",  "src": "REDDIT",   "ticker": "XBI",   "text": "SPDR S&P Biotech — mentions +100% in 24h (2 total)", "cat": "reddit",   "pill": "REDDIT VELOCITY", "href": "/stock/XBI"},
    {"ago": "12m", "src": "13F",      "ticker": "AAPL",  "text": "Berkshire Hathaway reduced position by $5.2B",       "cat": "13f",      "pill": "13F FILING",      "href": "/stock/AAPL"},
    {"ago": "23m", "src": "CONGRESS", "ticker": "NVDA",  "text": "Rep. Pelosi disclosed buy — $1M-$5M call options",   "cat": "congress", "pill": "CONGRESS",        "href": "/stock/NVDA"},
    {"ago": "37m", "src": "INSIDER",  "ticker": "AAPL",  "text": "Tim Cook (CEO) sold 240,000 shares — $52.1M",        "cat": "insider",  "pill": "INSIDER",         "href": "/stock/AAPL"},
    {"ago": "42m", "src": "YOUTUBE",  "ticker": "BRK.B", "text": "CNBC: Berkshire AGM 2026 highlights",                "cat": "youtube",  "pill": "YOUTUBE",         "href": "/stock/BRK.B"},
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


# `_compact_amount_str` + `_compact_range_str` moved to _redesign.helpers
# (shared by home + stock + congress).


# `_build_cusip_ticker_map` moved to _redesign.helpers (shared with funds).


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
            # Resolve ticker; skip the row if we can't (no robust way to
            # derive a ticker from "MARTIN MARIETTA MATERIALS"-style issuer
            # names — we prefer hiding the row to showing junk).
            ticker = c.get("ticker") or cmap.get(cusip) or ""
            if not ticker:
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
        # insider_trading.get_latest_insider_trades is Supabase-first
        # (hot table + cold archive; OpenInsider scrape is emergency
        # fallback only).  Route through to_supabase so it can't get
        # queued behind slow yfinance work on the heavy pool.
        trades = await to_supabase(
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
        rows_raw = await to_supabase(
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

    # `_retail_sentiment_proxy` lives with the /retail page module
    # (will move to home-shared helpers once home is extracted).
    # Lazy-import keeps this module free of import-cycle risk.
    from filings.routers._redesign.retail import _retail_sentiment_proxy

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
async def _fetch_home_feargreed() -> FearGreedPayload:
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
            float(price)  # validate; we format the raw `price` value below
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


async def warm_homepage_caches() -> None:
    """Pre-populate the Supabase-backed L2 caches the homepage reads.

    Called by the worker process during its market-data prefetch.
    Currently primes ``redesign:home:sector_etfs`` (otherwise lazily
    populated on first homepage hit -- which paid a 15s yfinance hit
    on a fresh deploy).

    Best-effort: each prime is independent; one failing doesn't block
    the others.  Returns nothing.
    """
    try:
        await _l2_cached(
            key="redesign:home:sector_etfs",
            ttl_seconds=300,
            compute=_fetch_sector_etfs_sync,
            category="redesign_home",
        )
    except Exception as exc:
        logger.debug("warm_homepage_caches: sector_etfs prime failed: %s", exc)


# ── Activity feed — Supabase notifications shaped for the redesign row.

def _activity_iso_time_ago(iso_str: str) -> str:
    """Bare-number variant of `_time_ago_iso` — template appends " ago" itself."""
    return _time_ago_iso(iso_str, suffix="", just_now="now")


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
        notifs = await to_supabase(supabase_cache.get_recent_notifications, limit)
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
        # to the notifications feed where they can dive into the stream.
        href = (n.get("link") or "").strip()
        if not href:
            href = f"/stock/{ticker}" if ticker else "/notifications"
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
        return _short_date(iso_str) or iso_str[:10]

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
        return _short_date(iso_str) or "—"

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


@router.get("/", response_class=HTMLResponse)
async def preview_home(request: Request):
    """Home page — live KPI strip + hero chart + 8 fully-wired Overview sections."""
    # Fetch every Overview section in parallel.  Each fetcher returns mock
    # data on failure, so the page renders even if half the upstreams are
    # cold.  Total budget capped by the slowest fetcher (typically yfinance).
    # ── Phase 1 (parallel): shared market_data fetches ──
    # 3 fetchers (top_movers, ticker_tape, heatmap_companies) all need the
    # same S&P 1D map; 2 fetchers (kpi_strip, ticker_tape) need indices.
    # Pre-fetch each ONCE and pass dicts down — saves 3-4 cache lookups
    # plus all the thread-pool slots they were holding.
    #
    # Both calls are gated through `to_upstream("yfinance")` so a slow
    # yfinance day trips the per-source circuit breaker for the worker
    # tier (web normally reads the Supabase-backed L2 these functions
    # populate first; yfinance is a deep fallback).  And each is wrapped
    # in `_bounded_call(timeout=3.0, fallback={})` so even if the L2
    # read AND the yfinance fallback both stall, the page renders in
    # <3s per section with em-dash markers instead of 500-ing on a 15s
    # global ceiling.
    from filings import market_data as _md
    sp_1d_map, idx_market_map = await asyncio.gather(
        _bounded_call(
            to_upstream("yfinance", _md.get_sp500_market_data, "1D"),
            timeout=3.0, fallback={}, name="home:sp500",
        ),
        _bounded_call(
            to_upstream("yfinance", _md.get_index_market_data),
            timeout=3.0, fallback={}, name="home:indices",
        ),
    )

    # ── Phase 2 (parallel): every other fetcher, with pre-fetched data
    # passed where applicable.  Heavy external-API fetchers (F&G, retail,
    # news, earnings, sector ETFs, FRED) are L2-cached so a cold worker
    # warms from Supabase rather than the upstream APIs.
    #
    # Fund flows + congress are fetched ONCE at the larger limit (8) and
    # sliced for the 6-row Overview panels — saves duplicate upstream
    # round trips and two heavy-pool slots. ──
    # Heatmap, Activity, Calendar panes are lazy-loaded on tab click via
    # `/api/home/{heatmap,activity,calendar}` (see partial handlers
    # below).  Skipping their 5 fetchers here saves ~5 thread slots and
    # ~95KB of HTML on the default Overview landing.
    #
    # Each fetcher is wrapped in `_bounded_call(timeout=4.0, fallback=…)`
    # so that ONE slow fetcher (cold-L2 hero chart blocked on yfinance,
    # FRED hiccup, etc.) can't drag the whole gather to its worst-case.
    # Each fallback matches the fetcher's own internal mock so the
    # template renders identically to an internal-error path.  Worst
    # case homepage budget: Phase 1 (3s) + Phase 2 (4s) ≈ 7s.
    #
    # Heavy fallbacks (hero payload, feargreed mock, news 4-tuple) are
    # passed as zero-arg callables so they only build on the failure
    # path — saves a few ms per happy-path request.
    (
        kpi_items, hero,
        top_movers_rows, fund_flows_full, insider_rows, congress_full,
        macro_rows, retail_payload, feargreed_payload, ticker_tape_rows,
        flow_trending_payload,
        news_payload,
    ) = await asyncio.gather(
        _bounded_call(
            _fetch_kpi_strip(),
            timeout=4.0, fallback=[], name="home:kpi",
        ),
        _bounded_call(
            _fetch_hero_chart(),
            timeout=4.0, fallback=_fallback_hero_payload, name="home:hero",
        ),
        _bounded_call(
            _fetch_home_top_movers(limit=6, mkt=sp_1d_map),
            timeout=4.0, fallback=_HOME_TOP_MOVERS, name="home:top_movers",
        ),
        _bounded_call(
            _fetch_home_fund_flows(request, limit=8),
            timeout=4.0, fallback=_HOME_FUND_FLOWS, name="home:fund_flows",
        ),
        _bounded_call(
            _fetch_home_insiders(limit=5),
            timeout=4.0, fallback=_HOME_INSIDERS, name="home:insiders",
        ),
        _bounded_call(
            _fetch_home_congress(limit=8),
            timeout=4.0, fallback=_HOME_CONGRESS, name="home:congress",
        ),
        _bounded_call(
            _fetch_home_macro(),
            timeout=4.0, fallback=_HOME_MACRO, name="home:macro",
        ),
        _bounded_call(
            _fetch_home_retail(),
            timeout=4.0,
            fallback=lambda: {"feat": _HOME_RETAIL_FEAT, "rows": _retail_rows()},
            name="home:retail",
        ),
        _bounded_call(
            _fetch_home_feargreed(),
            timeout=4.0, fallback=_home_feargreed_mock, name="home:feargreed",
        ),
        _bounded_call(
            _fetch_home_ticker_tape(idx_data=idx_market_map, sp_data=sp_1d_map),
            timeout=4.0, fallback=_HOME_TICKER_TAPE, name="home:ticker_tape",
        ),
        _bounded_call(
            _fetch_home_flow_trending(request, limit=12),
            timeout=4.0,
            fallback=(_HOME_FLOW_TRENDING, _HOME_FLOW_TRENDING_MAX),
            name="home:flow_trending",
        ),
        _bounded_call(
            _fetch_home_news(idx_data=idx_market_map, sp_data=sp_1d_map),
            timeout=4.0,
            fallback=lambda: (
                _HOME_NEWS_FEATURED, _HOME_NEWS_STORIES,
                _HOME_NEWS_MOST_READ,
                _build_news_market_wire(idx_market_map, sp_1d_map),
            ),
            name="home:news",
        ),
    )
    # Lazy-loaded panes get empty defaults; the partial endpoints render
    # the real content into the page on first tab activation.
    heatmap_companies_rows: list = []
    heatmap_sectors_rows:   list = []
    activity_rows:          list = []
    cal_earnings_rows:      list = []
    cal_macro_rows:         list = []
    # Slice the 8-row results for the 6-row Overview panels.
    fund_flow_rows = fund_flows_full[:6]
    congress_rows = congress_full[:6]
    flow_fund_buys_rows = fund_flows_full
    flow_congress_rows = congress_full
    flow_trending_rows, flow_trending_max = flow_trending_payload
    news_featured_ctx, news_stories_ctx, news_most_read_ctx, news_market_wire_ctx = news_payload
    ctx = {
        "request": request,
        **(await _shell_context(request, "Home")),

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
# Home page LAZY-LOADED PANE PARTIALS
# Each route renders just one subtab's HTML, fetched on first tab click.
# Saves ~5 thread slots and ~95KB on the default Overview landing.
# ─────────────────────────────────────────────────────────────────────────────


@router.get("/api/home/heatmap", response_class=HTMLResponse)
async def preview_home_heatmap_partial(request: Request):
    """Lazy-loaded Heatmap pane — companies + sectors grids.

    ``to_heavy`` raises ``TimeoutError`` on hung yfinance fetches -- the
    fetchers below have their own internal fallbacks, but this top-level
    call needs ``_bounded_call`` so a saturated yfinance pool can't 500
    the partial.  Empty dict triggers the fetchers' mock-fallback path.
    """
    from filings import market_data as _md
    sp_1d_map = await _bounded_call(
        to_heavy(_md.get_sp500_market_data, "1D"),
        timeout=8.0, fallback={}, name="heatmap:sp500",
    )
    companies, sectors = await asyncio.gather(
        _bounded_call(
            _fetch_home_heatmap_companies(mkt=sp_1d_map),
            timeout=10.0, fallback=[], name="heatmap:companies",
        ),
        _bounded_call(
            _fetch_home_heatmap_sectors(),
            timeout=10.0, fallback=[], name="heatmap:sectors",
        ),
    )
    return templates.TemplateResponse(
        "_redesign/partials/home_heatmap.html",
        {
            "request": request,
            "heatmap_companies": companies,
            "heatmap_sectors":   sectors,
        },
    )


@router.get("/api/home/activity", response_class=HTMLResponse)
async def preview_home_activity_partial(request: Request):
    """Lazy-loaded Activity pane — live activity feed."""
    activity_rows = await _bounded_call(
        _fetch_home_activity(limit=12),
        timeout=6.0, fallback=[], name="activity",
    )
    return templates.TemplateResponse(
        "_redesign/partials/home_activity.html",
        {"request": request, "activity_feed": activity_rows},
    )


@router.get("/api/home/calendar", response_class=HTMLResponse)
async def preview_home_calendar_partial(request: Request):
    """Lazy-loaded Calendar pane — earnings + macro events."""
    cal_earnings, cal_macro = await asyncio.gather(
        _bounded_call(
            _fetch_home_cal_earnings(limit=6),
            timeout=6.0, fallback=[], name="cal_earnings",
        ),
        _bounded_call(
            _fetch_home_cal_macro(limit=6),
            timeout=6.0, fallback=[], name="cal_macro",
        ),
    )
    return templates.TemplateResponse(
        "_redesign/partials/home_calendar.html",
        {
            "request": request,
            "cal_earnings": cal_earnings,
            "cal_macro":    cal_macro,
        },
    )

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


