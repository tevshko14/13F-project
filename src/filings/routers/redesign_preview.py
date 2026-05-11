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
import json
import re
import logging
import math
import os
import time
from datetime import datetime, timezone
from typing import Any, Callable
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse

from filings import supabase_cache
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

# Shared helpers extracted from this file -- see _redesign/helpers.py.
# We re-import the names locally so the routes still defined in this
# file (home/stock/funds/etc., before they're broken out into sub-
# modules of their own) keep working unchanged.  ``is_enabled`` is
# imported for re-export to ``web.py`` (which does
# ``redesign_preview.is_enabled()`` to gate router mounting).
from filings.routers._redesign.helpers import (
    _bounded,
    _bounded_call,
    _build_cusip_ticker_map,
    _congress_action,
    _insiders_action,
    _insiders_format_title,
    _maybe_rate_limit,
    _request_fund_cache,
    _shell_context,
    _short_date,
    SPARK,
    SPARK_DOWN,
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


def _fallback_hero_payload() -> dict:
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
    """Lazy-loaded Heatmap pane — companies + sectors grids."""
    from filings import market_data as _md
    sp_1d_map = await to_heavy(_md.get_sp500_market_data, "1D")
    companies, sectors = await asyncio.gather(
        _fetch_home_heatmap_companies(mkt=sp_1d_map),
        _fetch_home_heatmap_sectors(),
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
    activity_rows = await _fetch_home_activity(limit=12)
    return templates.TemplateResponse(
        "_redesign/partials/home_activity.html",
        {"request": request, "activity_feed": activity_rows},
    )


@router.get("/api/home/calendar", response_class=HTMLResponse)
async def preview_home_calendar_partial(request: Request):
    """Lazy-loaded Calendar pane — earnings + macro events."""
    cal_earnings, cal_macro = await asyncio.gather(
        _fetch_home_cal_earnings(limit=6),
        _fetch_home_cal_macro(limit=6),
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

router.include_router(_support_routes.router)
router.include_router(_profile_watchlist_routes.router)
router.include_router(_insiders_routes.router)
router.include_router(_notifications_routes.router)
router.include_router(_congress_routes.router)
router.include_router(_macro_routes.router)
router.include_router(_retail_routes.router)
router.include_router(_funds_routes.router)
