"""Macro page (v2 redesign).

The biggest of the extracted feature modules -- 7 tabs each with its
own payload builder + chart geometry:

  * Indicators -- FRED-backed series + KPI strip + featured charts
  * Yields     -- treasury curve + key spreads + debt-to-GDP panel
  * FX & Commodities -- frankfurter pairs + commodity tape
  * Events Calendar  -- economic_calendar week grid + just-released
  * Heatmap    -- shares the home heatmap helpers (lazy-imported below)
  * Earnings   -- earnings_scorecard donut + trend + KPI strip
  * Performance -- market_breadth ad-line + momentum chart
  * Sentiment  -- L2-only Google Trends with bg warmer
  * Volatility -- CBOE put/call + VIX term + SKEW

Every tab payload runs in parallel under a single `_bounded()` budget
so one slow upstream can't stall the whole render.
"""

from __future__ import annotations

import asyncio
import functools
import logging
import math
from datetime import datetime, timedelta
from typing import TypedDict

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from filings import supabase_cache, warmer as _warmer
from filings.app_state import templates
from filings.concurrency import to_heavy, to_supabase
from filings.routers._redesign.helpers import (
    _bounded,
    _bounded_call,
    _shell_context,
    _short_date,
    GracefulRoute,
)


# ─────────────────────────────────────────────────────────────────────────────
# TypedDict contracts for the per-tab payloads.
#
# Each ``_v2_*_payload`` function returns a dict matching one of these
# types; each ``_v2_*_empty`` function returns the SAME type with empty
# / zero values.  mypy enforces that both implementations stay in sync
# -- so the next time someone adds a key the template references, the
# linter catches the missing-fallback-key bug at PR time, not in prod.
#
# Nested values (e.g. ``vol.pc``, ``earn.donut_eps``) are typed as
# ``dict[str, Any]`` for now -- the chart builders (`_pc_ratio_chart`,
# `_earn_donut`, etc.) each carry their own ``{"have_data": False}``
# sentinel + payload keys.  Templates branch on ``.have_data`` for the
# nested dicts; we don't need TypedDict precision two levels deep.
# ─────────────────────────────────────────────────────────────────────────────


class VolatilityPayload(TypedDict):
    """`/macro` Volatility tab — Put/Call · VIX · SKEW."""
    have_data: bool
    current_pc: str
    pc_types: dict
    pc:        dict
    vix:       dict
    skew:      dict


class EventsCalendarPayload(TypedDict):
    """`/macro` Events Calendar tab — week grid + just-released."""
    have_data:        bool
    events_by_date:   list
    week_grid:        list
    just_released:    list
    metrics:          dict
    countdown:        dict | None
    kpi_strip:        list
    current_period:   str
    current_country:  str
    current_impact:   str
    periods:          dict
    countries:        dict
    impact_choices:   dict
    is_mock:          bool


class MacroEarningsPayload(TypedDict):
    """`/macro` Earnings tab — scorecard + donuts + trend chart."""
    have_data:       bool
    metrics:         dict
    results:         list
    results_total:   int
    kpi_strip:       list
    donut_eps:       dict
    donut_rev:       dict
    trend:           dict
    current_index:   str
    current_quarter: str
    current_sector:  str
    quarters:        list
    indices:         dict
    sectors:         list | dict


class MacroCalendarPayload(TypedDict):
    """`/macro` Earnings Calendar sub-pane — week grid + just-reported."""
    have_data:      bool
    metrics:        dict
    upcoming:       list
    just_reported:  list
    kpi_strip:      list
    week_grid:      list
    current_index:  str
    current_period: str
    indices:        dict
    periods:        dict


class MacroPerformancePayload(TypedDict):
    """`/macro` Performance tab — breadth + ad-line + momentum chart."""
    have_data:           bool
    metrics:             dict
    advance_pct:         float
    decline_pct:         float
    unchanged_pct:       float
    status:              dict
    above_50d:           dict
    sector_breadth:      list
    top_gainers:         list
    top_losers:          list
    divergence:          dict | None
    momentum:            dict
    kpi_strip:           list
    current_index:       str
    current_period:      str
    indices:             dict
    periods:             dict
    data_period_label:   str
    data_index_name:     str
    data_as_of:          str

logger = logging.getLogger(__name__)

router = APIRouter(route_class=GracefulRoute)


# ── Indicators tab ───────────────────────────────────────────────────

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

    Strict L2 — fred_indicators is warmed at the warm tier (4 min)
    via the warmer registry.  Request path reads from Supabase only.
    """
    try:
        from filings import warmer
        payload = await warmer.read_via_l2("redesign:home:fred_indicators")
        return payload or {}
    except Exception as exc:
        logger.warning("Macro indicators fetch failed: %s", exc)
        return {}


# ── Yields tab ────────────────────────────────────────────────────────────

# Tenor mapping: treasury_data uses "1 Mo"/"10 Yr"; the design speaks in
# "1M"/"10Y" tokens.  Fixed order so the curve renders left → right with
# the natural maturity progression.
_YIELD_TENOR_ORDER: list[tuple[str, str]] = [
    ("1 Mo", "1M"), ("3 Mo", "3M"), ("6 Mo", "6M"),
    ("1 Yr", "1Y"), ("2 Yr", "2Y"), ("3 Yr", "3Y"),
    ("5 Yr", "5Y"), ("7 Yr", "7Y"), ("10 Yr", "10Y"),
    ("20 Yr", "20Y"), ("30 Yr", "30Y"),
]


def _yields_curve_payload(curve_data: dict | None) -> dict:
    """Convert treasury_data.get_yield_curve() output → SVG-ready chart payload."""
    if not curve_data:
        return {"have_data": False, "rows": [], "line_d": "", "ticks": []}

    yields = curve_data.get("yields") or {}
    rows = []
    for source_key, label in _YIELD_TENOR_ORDER:
        if source_key in yields:
            rows.append({"tenor": label, "yield": yields[source_key]})

    if len(rows) < 3:
        return {"have_data": False, "rows": [], "line_d": "", "ticks": []}

    # ViewBox sized 1500×240 to match the typical full-width container's
    # natural aspect ratio (~6:1) so `preserveAspectRatio="none"` becomes a
    # near-identity transform — no horizontal stretch / no warped slopes.
    width, height = 1500.0, 240.0
    pad_top, pad_bot = 15.0, 25.0
    pad_x = 40.0
    plot_h = height - pad_top - pad_bot
    plot_w = width - 2 * pad_x

    vals = [r["yield"] for r in rows]
    lo, hi = min(vals), max(vals)
    rng = hi - lo if hi > lo else 1
    pad = rng * 0.15
    lo, hi = lo - pad, hi + pad
    rng = hi - lo if hi > lo else 1

    pts: list[tuple[float, float]] = []
    for i, r in enumerate(rows):
        x = pad_x + (i / max(len(rows) - 1, 1)) * plot_w
        y = pad_top + (1 - (r["yield"] - lo) / rng) * plot_h
        pts.append((round(x, 1), round(y, 1)))

    line_d = " ".join(("M" if i == 0 else "L") + f"{x} {y}" for i, (x, y) in enumerate(pts))
    ticks = [{"label": r["tenor"], "x": p[0]} for r, p in zip(rows, pts)]
    dots  = [{"x": p[0], "y": p[1]} for p in pts]

    # Per-tenor hover payload — JSON-serialized for the mousemove handler
    # so the tooltip can show {tenor, yield} on hover.
    chart_history = [
        {"x": p[0], "y": p[1], "tenor": r["tenor"], "yield_pct": r["yield"],
         "yield_str": f"{r['yield']:.2f}%"}
        for r, p in zip(rows, pts)
    ]

    return {
        "have_data": True,
        "rows":      [{"tenor": r["tenor"], "yield_str": f"{r['yield']:.2f}%"} for r in rows],
        "line_d":    line_d,
        "dots":      dots,
        "ticks":     ticks,
        "as_of":     curve_data.get("date") or "",
        "inverted":  bool(curve_data.get("inverted")),
        "vb_width":  width,
        "vb_height": height,
        "chart_history": chart_history,
    }


def _yields_spreads(curve_data: dict | None) -> list[dict]:
    """Compute key spreads from the yield curve."""
    if not curve_data:
        return []
    y = curve_data.get("yields") or {}

    def _g(*keys):
        for k in keys:
            if k in y:
                return y[k]
        return None

    pairs = [
        ("2s10s",  "10Y - 2Y · recession watch", _g("10 Yr"), _g("2 Yr")),
        ("3M-10Y", "10Y - 3M · curve slope",     _g("10 Yr"), _g("3 Mo")),
        ("5s30s",  "30Y - 5Y · long-end shape",  _g("30 Yr"), _g("5 Yr")),
        ("2s30s",  "30Y - 2Y · cycle pulse",     _g("30 Yr"), _g("2 Yr")),
    ]
    rows = []
    for label, desc, long_y, short_y in pairs:
        if long_y is None or short_y is None:
            continue
        spread = long_y - short_y
        rows.append({
            "name":     label,
            "desc":     desc,
            "value_pp": spread,
            "value_str": f"{spread:+.2f}%",
            "inverted": spread < 0,
        })
    return rows


# ── Indicators tab — featured charts (v1 enhancement) ──────────────────────
#
# v1 mocks up Fed Funds Rate, CPI YoY, and Unemployment as full-width
# featured charts above the compact indicator rows.  We re-use the FRED
# sparkline data already on each indicator dict (24 obs), shape it into
# SVG path/bar geometry, and let the template drop in the result.

_INDICATOR_FEATURED_PALETTE = {
    "DFF":      {"color": "#a855f7", "kind": "line"},   # Fed Funds — purple
    "CPIAUCSL": {"color": "#f59e0b", "kind": "line"},   # CPI — amber
    "UNRATE":   {"color": "#10b981", "kind": "bar"},    # Unemployment — green bars
}


def _indicator_featured_chart(ind: dict) -> dict | None:
    """Build SVG-ready geometry for a single featured indicator chart."""
    if not ind:
        return None
    series = ind.get("sparkline") or []
    if len(series) < 2:
        return None

    sid    = ind.get("series_id", "")
    style  = _INDICATOR_FEATURED_PALETTE.get(sid, {"color": "var(--pp-accent)", "kind": "line"})
    kind   = style["kind"]
    color  = style["color"]

    vbw, vbh = 1500, 220
    pad_x, pad_y = 50, 26
    plot_w = vbw - pad_x * 2
    plot_h = vbh - pad_y * 2

    s_min, s_max = min(series), max(series)
    rng = (s_max - s_min) or abs(s_max) * 0.05 or 1
    # Pad range so the line doesn't touch the chart edge.
    pad_v = rng * 0.08
    s_min -= pad_v
    s_max += pad_v
    rng = s_max - s_min or 1
    n = len(series)

    def _x(i): return pad_x + (plot_w * i / max(n - 1, 1))
    def _y(v): return pad_y + plot_h * (1 - (v - s_min) / rng)

    payload: dict = {
        "have_data":   True,
        "series_id":   sid,
        "name":        ind.get("name", sid),
        "value_fmt":   ind.get("value_fmt") or "—",
        "change_fmt":  ind.get("change_fmt") or "—",
        "direction":   ind.get("direction", "neutral"),
        "vb_w":        vbw,
        "vb_h":        vbh,
        "color":       color,
        "kind":        kind,
        "y_ticks":     [],
    }

    unit_fmt = "%" if (ind.get("value_fmt") or "").endswith("%") else ""
    if kind == "bar":
        bar_pad = 4
        bar_w   = max(6.0, (plot_w - bar_pad * (n + 1)) / max(n, 1))
        bars: list[dict] = []
        hover_points: list[dict] = []
        for i, v in enumerate(series):
            h = (v - s_min) / rng * plot_h
            x = pad_x + bar_pad + i * (bar_w + bar_pad)
            bar_top_y = pad_y + plot_h - h
            bars.append({
                "x": round(x, 2), "y": round(bar_top_y, 2),
                "w": round(bar_w, 2), "h": round(h, 2),
            })
            # Anchor the hover dot to the bar's top-center.
            hover_points.append({
                "x":     round(x + bar_w / 2, 2),
                "y":     round(bar_top_y, 2),
                "label": f"#{i + 1}",  # FRED sparkline doesn't carry per-point dates
                "value": f"{v:.2f}{unit_fmt}",
            })
        payload["bars"] = bars
        payload["hover_points"] = hover_points
    else:
        d_parts = []
        hover_points = []
        for i, v in enumerate(series):
            d_parts.append(f"{'M' if i == 0 else 'L'}{_x(i):.2f} {_y(v):.2f}")
            hover_points.append({
                "x":     round(_x(i), 2),
                "y":     round(_y(v), 2),
                "label": f"Period {i + 1} of {n}",
                "value": f"{v:.2f}{unit_fmt}",
            })
        payload["path_d"] = " ".join(d_parts)
        payload["hover_points"] = hover_points

    # 4 y-ticks for grid lines.
    for j in range(4):
        v = s_min + rng * (1 - j / 3)
        payload["y_ticks"].append({
            "y":     round(pad_y + plot_h * j / 3, 2),
            "label": f"{v:.2f}",
        })

    return payload


def _indicator_featured_charts(indicators_by_id: dict) -> list[dict]:
    """Return the curated 3-up featured-chart row for the Indicators tab."""
    out: list[dict] = []
    for sid in ("DFF", "CPIAUCSL", "UNRATE"):
        ind = indicators_by_id.get(sid)
        chart = _indicator_featured_chart(ind) if ind else None
        if chart:
            out.append(chart)
    return out


# ── Yields tab — National Debt panel (v1 enhancement) ──────────────────────
#
# Pulls daily total public debt outstanding from Treasury Fiscal Data
# (treasury_data.get_debt_data) and shapes it into a bar-chart-ready
# payload — last ~30 daily values, value labels, latest figure in trillions.

def _debt_panel_payload(debt: dict | None) -> dict:
    """Reshape treasury_data.get_debt_data() for the v2 National Debt panel."""
    if not debt or not isinstance(debt, dict):
        return {"have_data": False}
    data = debt.get("data") or []
    if not data:
        return {"have_data": False}

    latest = debt.get("latest") or data[-1]
    latest_t = debt.get("latest_trillions") or round(float(latest["debt"]) / 1e12, 2)

    # Cap at last 30 daily entries for a readable bar chart.
    rows = data[-30:]
    debts = [float(r["debt"]) for r in rows]
    d_max = max(debts)

    bars: list[dict]          = []
    hover_points: list[dict]  = []
    n = len(rows)
    bar_pad = 4  # gap between bars in viewBox units
    vbw, vbh = 1500, 240
    plot_h   = vbh - 30        # leave room for axis labels
    bar_w    = max(8.0, (vbw - bar_pad * (n + 1)) / max(n, 1))
    for i, r in enumerate(rows):
        debt_t = round(float(r["debt"]) / 1e12, 2)
        h      = (float(r["debt"]) / d_max) * plot_h
        x      = bar_pad + i * (bar_w + bar_pad)
        bar_top_y = plot_h - h
        bars.append({
            "x":      round(x, 2), "y": round(bar_top_y, 2),
            "w":      round(bar_w, 2), "h": round(h, 2),
            "date":   r["date"][5:],   # "MM-DD" — full year is implicit in latest
            "debt_t": debt_t,
        })
        hover_points.append({
            "x":     round(x + bar_w / 2, 2),
            "y":     round(bar_top_y, 2),
            "label": r["date"],
            "value": f"${debt_t}T",
        })

    # X-axis ticks every ~6 bars.
    step = max(1, n // 6)
    x_ticks: list[dict] = []
    for i in range(0, n, step):
        x_ticks.append({
            "x": round(bar_pad + i * (bar_w + bar_pad) + bar_w / 2, 2),
            "label": rows[i]["date"][5:],
        })

    return {
        "have_data":    True,
        "latest_t":     latest_t,
        "latest_date":  latest.get("date", ""),
        "vb_w":         vbw,
        "vb_h":         vbh,
        "plot_h":       plot_h,
        "bars":         bars,
        "x_ticks":      x_ticks,
        "hover_points": hover_points,
    }


# ── FX & Commodities tab ──────────────────────────────────────────────────

# Display order + symbol translation for the FX panel.
_FX_DISPLAY_ORDER: list[tuple[str, str, str]] = [
    ("EUR", "EURUSD", "Euro / US Dollar"),
    ("GBP", "GBPUSD", "British Pound / US Dollar"),
    ("JPY", "USDJPY", "US Dollar / Japanese Yen"),
    ("CAD", "USDCAD", "US Dollar / Canadian Dollar"),
    ("AUD", "AUDUSD", "Australian Dollar"),
    ("CHF", "USDCHF", "US Dollar / Swiss Franc"),
    ("CNY", "USDCNY", "US Dollar / Chinese Yuan"),
    ("MXN", "USDMXN", "US Dollar / Mexican Peso"),
]


def _fx_panel_rows(fx_payload: dict | None) -> list[dict]:
    """Build the FX panel rows from frankfurter latest + sparkline data.

    Frankfurter quotes per-USD (USD→FX), so EUR/GBP/AUD get inverted to
    show the conventional FX/USD pair.  Day delta is derived from the
    last two points of the sparkline series when available.
    """
    if not fx_payload:
        return []
    latest_obj = fx_payload.get("latest") or {}
    rates = latest_obj.get("rates") or []
    sparklines = fx_payload.get("sparklines") or {}

    by_code = {r["code"]: r for r in rates}
    out: list[dict] = []
    for code, sym, name in _FX_DISPLAY_ORDER:
        rec = by_code.get(code)
        if not rec:
            continue
        usd_per_one = rec.get("inverse")    # 1 EUR = X USD
        per_usd     = rec.get("rate")       # 1 USD = X EUR
        # Convention: EUR/GBP/AUD use FX/USD form; the rest USD/FX.
        invert = code in ("EUR", "GBP", "AUD")
        value = usd_per_one if invert else per_usd
        if value is None:
            continue
        # Sparkline series — value-side aligned with display direction.
        ts = sparklines.get(code) or []
        series: list[float] = []
        for p in ts:
            r = p.get("rate")
            if r is None:
                continue
            v = (1.0 / r) if invert else r
            series.append(v)
        d_pct = None
        if len(series) >= 2 and series[-2]:
            d_pct = (series[-1] - series[-2]) / series[-2]
        out.append({
            "sym":   sym,
            "name":  name,
            "value": value,
            "value_str": f"{value:,.4f}" if value < 100 else f"{value:,.2f}",
            "delta_pct": d_pct,
            "delta_str": (f"{d_pct * 100:+.2f}%" if d_pct is not None else "—"),
            "up":    (d_pct >= 0) if d_pct is not None else None,
            "spark": series[-30:] if series else [],
        })
    return out


# Commodity universe — uses get_index_market_data() output for the four
# yfinance front-month futures we already cache, plus the spot Treasury
# yield index for "10Y" — same data plumbing the Home page uses.
_COMMODITY_SYMBOLS: list[tuple[str, str, str, str]] = [
    ("CL=F", "WTI",   "Crude oil",     "$/bbl"),
    ("GC=F", "Gold",  "Gold",          "$/oz"),
    ("SI=F", "Silver","Silver",        "$/oz"),
    ("NG=F", "NatGas","Natural gas",   "$/MMBtu"),
]


# ── FX charts grid + exchange-rates table (v1 enhancements) ─────────────
#
# `get_fx_dashboard()` already returns 30-day timeseries for EUR/GBP/JPY/CNY.
# We pre-compute SVG path geometry for the 4-up mini-chart grid and a flat
# rates table so the template stays declarative.

# Pair colors — match v1's mini-chart palette so the macro page reads as
# the same tool as the v1 page during the transition window.
_FX_PAIR_COLORS: dict[str, str] = {
    "EUR": "#3b82f6",     # blue
    "GBP": "#a855f7",     # purple
    "JPY": "#ef4444",     # red
    "CNY": "#f59e0b",     # amber
}


def _fx_chart_grid(fx_payload: dict | None) -> list[dict]:
    """Build 4-up mini-chart payload from frankfurter sparklines.

    Each series is `[{date, rate}, ...]` ascending — convert to SVG path
    coords scaled to a 0..vbw / 0..vbh viewBox so the template just drops
    the d= string into a path.
    """
    if not fx_payload:
        return []
    sparklines = fx_payload.get("sparklines") or {}

    out: list[dict] = []
    vbw, vbh = 600, 220
    pad_x, pad_y = 30, 22
    plot_w = vbw - pad_x * 2
    plot_h = vbh - pad_y * 2

    for code in ("EUR", "GBP", "JPY", "CNY"):
        series = sparklines.get(code) or []
        if len(series) < 2:
            continue
        rates = [float(p["rate"]) for p in series]
        s_min, s_max = min(rates), max(rates)
        # Pad y-range slightly so the line doesn't touch the chart edge.
        rng = (s_max - s_min) or s_max * 0.001 or 1
        s_min -= rng * 0.05
        s_max += rng * 0.05
        rng = s_max - s_min or 1
        n = len(rates)

        def _x(i, n=n): return pad_x + (plot_w * i / max(n - 1, 1))
        def _y(v):       return pad_y + plot_h * (1 - (v - s_min) / rng)

        d_parts: list[str] = []
        hover_points: list[dict] = []
        rate_fmt = "{:.4f}" if max(rates) < 10 else "{:.2f}"
        for i, r in enumerate(rates):
            x_v = _x(i)
            y_v = _y(r)
            d_parts.append(f"{'M' if i == 0 else 'L'}{x_v:.2f} {y_v:.2f}")
            hover_points.append({
                "x":     round(x_v, 2),
                "y":     round(y_v, 2),
                "label": series[i]["date"],
                "value": rate_fmt.format(r),
            })
        path_d = " ".join(d_parts)

        # Y-axis ticks (4 evenly-spaced).
        y_ticks = []
        for j in range(4):
            v = s_min + rng * (1 - j / 3)
            y_ticks.append({
                "y":     round(pad_y + plot_h * j / 3, 2),
                "label": f"{v:.4f}" if v < 10 else f"{v:.2f}",
            })

        # X-axis ticks every ~5 days.
        step = max(1, n // 6)
        x_ticks = []
        for i in range(0, n, step):
            x_ticks.append({"x": round(_x(i), 2), "label": series[i]["date"][5:]})

        out.append({
            "code":     code,
            "label":    f"USD/{code}",
            "color":    _FX_PAIR_COLORS.get(code, "var(--pp-accent)"),
            "hover_points": hover_points,
            "vb_w":     vbw,
            "vb_h":     vbh,
            "path_d":   path_d,
            "y_ticks":  y_ticks,
            "x_ticks":  x_ticks,
            "days":     n,
        })
    return out


def _fx_rates_table(fx_payload: dict | None) -> dict:
    """Flatten the latest rates dict into a sortable rows list."""
    if not fx_payload:
        return {"have_data": False, "rows": [], "as_of": ""}
    latest = (fx_payload.get("latest") or {})
    rates  = latest.get("rates") or []
    if not rates:
        return {"have_data": False, "rows": [], "as_of": ""}

    rows = []
    for r in rates:
        per_usd     = r.get("rate")
        usd_per_one = r.get("inverse")
        if per_usd is None:
            continue
        rows.append({
            "code":       r["code"],
            "name":       r.get("name", r["code"]),
            "per_usd":    f"{per_usd:.4f}",
            "usd_per":    f"{usd_per_one:.4f}" if usd_per_one is not None else "—",
        })
    return {
        "have_data": bool(rows),
        "rows":      rows,
        "as_of":     latest.get("date", ""),
    }


def _commodities_panel_rows(index_payload: dict | None) -> list[dict]:
    """Pluck commodity rows out of the existing index_market_data feed."""
    if not index_payload or not isinstance(index_payload, dict):
        return []
    out: list[dict] = []
    for sym_yf, label, name, unit in _COMMODITY_SYMBOLS:
        rec = index_payload.get(sym_yf)
        if not isinstance(rec, dict):
            continue
        price = rec.get("price")
        pct   = rec.get("pct_change")    # already in %
        spark = rec.get("spark") or []
        out.append({
            "sym":   label,
            "name":  name,
            "unit":  unit,
            "value": price,
            "value_str": (f"{price:,.2f}" if isinstance(price, (int, float)) else "—"),
            "delta_pct": (pct / 100.0 if isinstance(pct, (int, float)) else None),
            "delta_str": (f"{pct:+.2f}%" if isinstance(pct, (int, float)) else "—"),
            "up":    (pct >= 0) if isinstance(pct, (int, float)) else None,
            # Spark from index_market_data is already 0–1 normalized 20pts.
            "spark": spark,
        })
    return out


# ── Shared payload helpers ───────────────────────────────────────────────
#
# A few primitives used by every tab's payload helper.  They were
# extracted from inline copies inside Earnings, Events Calendar, and
# Performance once the third one was found.  Keeping them here so the
# next tab to land doesn't reinvent the wheel.


def _xy_mappers(
    n: int,
    *,
    vbw: int, vbh: int, pad_x: int, pad_y: int,
    s_min: float, s_max: float,
):
    """Build (x_fn, y_fn, plot_w, plot_h) coordinate mappers for an SVG chart.

    Replaces the inline ``def _x(i): ...; def _y(v): ...`` closures that
    previously appeared in ~7 chart helpers.  ``s_min``/``s_max`` define
    the value range; the caller is responsible for any padding.  Empty
    or zero-range series degrade to a midline rather than dividing by 0.
    """
    plot_w = vbw - pad_x * 2
    plot_h = vbh - pad_y * 2
    rng    = (s_max - s_min) or 1
    def _x(i: int) -> float:
        return pad_x + (plot_w * i / max(n - 1, 1))
    def _y(v: float) -> float:
        return pad_y + plot_h * (1 - (v - s_min) / rng)
    return _x, _y, plot_w, plot_h


def _svg_line_path(points, *, prec: int = 2) -> str:
    """Convert ``[(x, y), (x, y), ...]`` into an SVG path d= string.

    Folds the ~7 hand-rolled `M{x} {y}L{x} {y}...` joins scattered across
    the chart helpers into one place.  ``prec`` controls coord rounding —
    used at 2 everywhere; left configurable for future high-density paths.
    """
    fmt = f"{{:.{prec}f}}"
    return " ".join(
        f"{'M' if i == 0 else 'L'}{fmt.format(x)} {fmt.format(y)}"
        for i, (x, y) in enumerate(points)
    )


def _kpi(label: str, value, delta: str = "", up: bool | None = None) -> dict:
    """Build one cell of a `kpi_strip` macro payload.

    `value` is coerced to ``str`` so callers can pass ints / floats / None
    without sprinkling `str(...)` at every call site.
    """
    return {
        "label": label,
        "value": "—" if value is None else str(value),
        "delta": delta,
        "up":    up,
    }


def _week_grid_mon_fri(
    items_by_date: list[dict],
    *,
    flag_fn=None,
) -> list[dict]:
    """Pivot a `[{date, entries, ...}, ...]` list into a Mon-Fri week grid.

    Anchors on the first item's date, backs up to that Monday, emits 5
    cells (Sat/Sun dropped — neither earnings nor macro releases land
    on weekends).  `flag_fn(entries) -> bool` adds an optional `has_flag`
    field so calendars can highlight days with high-impact items, large
    SI presence, etc.

    Returns ``[]`` on empty/malformed input rather than raising — used in
    a render-and-show-placeholder context.
    """
    if not items_by_date:
        return []
    by_date = {d["date"]: d for d in items_by_date if isinstance(d, dict) and d.get("date")}
    try:
        anchor = datetime.strptime(items_by_date[0]["date"], "%Y-%m-%d").date()
    except (KeyError, ValueError, TypeError):
        return []

    monday    = anchor - timedelta(days=anchor.weekday())
    today_iso = datetime.now().strftime("%Y-%m-%d")
    cells: list[dict] = []
    for i in range(5):
        d   = monday + timedelta(days=i)
        iso = d.strftime("%Y-%m-%d")
        rec = by_date.get(iso) or {}
        entries = rec.get("entries") or []
        cell = {
            "iso":      iso,
            "dow":      d.strftime("%a").upper(),
            "label":    d.strftime("%b %d"),
            "entries":  entries,
            "is_today": iso == today_iso,
        }
        if flag_fn is not None:
            cell["has_flag"] = bool(flag_fn(entries))
        cells.append(cell)
    return cells


# ── Events Calendar tab (v1 enhancements) ────────────────────────────────
#
# Wraps `economic_calendar.fetch_economic_events` (the v1 helper that
# already returns events_by_date + metrics + countdown_target) and reshapes
# the output for the v2 Calendar pane.  Adds KPI strip, Mon-Fri week grid,
# and a Just-Released list — same data the v1 page renders.


def _events_just_released(events_by_date: list[dict], limit: int = 10) -> list[dict]:
    """Flatten + sort released events newest-first for the Just-Released table."""
    out: list[dict] = []
    for day in events_by_date or []:
        for ev in (day.get("entries") or []):
            if ev.get("is_released"):
                out.append(ev)
    # Newest first (most recently dated/timed)
    out.sort(key=lambda e: (e.get("date", ""), e.get("time", "")), reverse=True)
    return out[:limit]


def _v2_events_calendar_empty() -> EventsCalendarPayload:
    """Empty-shape fallback for the events-calendar tab.

    Matches the success-path return of :func:`_v2_events_calendar_payload`
    so the macro template can iterate ``events.periods``, ``events.kpi_strip``,
    etc. unconditionally without hitting Jinja UndefinedError when the
    bound fires.  Built lazily so the upstream-constant imports happen
    only on the failure path.
    """
    from filings import economic_calendar
    return {
        "have_data":       False,
        "events_by_date":  [],
        "week_grid":       [],
        "just_released":   [],
        "metrics":         {},
        "countdown":       None,
        "kpi_strip":       [
            _kpi("Total Events", 0, ""),
            _kpi("High Impact",  0, "market-moving"),
            _kpi("Released",     0, "actuals in"),
            _kpi("Upcoming",     0, "still pending"),
        ],
        "current_period":  "this_week",
        "current_country": "us",
        "current_impact":  "all",
        "periods":         economic_calendar.PERIOD_CHOICES,
        "countries":       economic_calendar.COUNTRY_CHOICES,
        "impact_choices":  economic_calendar.IMPACT_CHOICES,
        "is_mock":         False,
    }


async def _v2_events_calendar_payload(
    period: str = "this_week",
    country: str = "us",
    impact_filter: str = "all",
) -> EventsCalendarPayload:
    """Fetch + reshape economic events for the v2 Calendar tab."""
    from filings import economic_calendar

    if period not in economic_calendar.PERIOD_CHOICES:
        period = "this_week"
    if country not in economic_calendar.COUNTRY_CHOICES:
        country = "us"
    if impact_filter not in economic_calendar.IMPACT_CHOICES:
        impact_filter = "all"

    bundle = await to_heavy(
        economic_calendar.fetch_economic_events, period, country, impact_filter,
    )
    if not bundle:
        # Upstream returned nothing -- match the same complete-shape
        # contract the bounded() fallback uses so the template can
        # always iterate `events.periods`, `events.kpi_strip`, etc.
        return _v2_events_calendar_empty()

    events_by_date = bundle.get("events_by_date") or []
    metrics        = bundle.get("metrics") or {}

    return {
        "have_data":        bool(events_by_date),
        "events_by_date":   events_by_date,
        "week_grid":        _week_grid_mon_fri(
            events_by_date,
            flag_fn=lambda es: any(e.get("impact") == "high" for e in es),
        ),
        "just_released":    _events_just_released(events_by_date),
        "metrics":          metrics,
        "countdown":        bundle.get("countdown_target"),
        "kpi_strip":        [
            _kpi("Total Events", metrics.get("total_events", 0), bundle.get("period_label", "")),
            _kpi("High Impact",  metrics.get("high_impact_count", 0), "market-moving"),
            _kpi("Released",     metrics.get("released_count", 0), "actuals in"),
            _kpi("Upcoming",     metrics.get("upcoming_count", 0), "still pending"),
        ],
        "current_period":   period,
        "current_country":  country,
        "current_impact":   impact_filter,
        "periods":          economic_calendar.PERIOD_CHOICES,
        "countries":        economic_calendar.COUNTRY_CHOICES,
        "impact_choices":   economic_calendar.IMPACT_CHOICES,
        "is_mock":          bool(bundle.get("is_mock")),
    }


# ── Macro Calendar tab — high-impact upcoming releases ────────────────────

async def _macro_calendar_rows(*, top_n: int = 12) -> list[dict]:
    try:
        from filings import economic_calendar
        bundle = await to_heavy(economic_calendar.fetch_economic_events, "all", "us", "all")
    except Exception as exc:
        logger.warning("Macro calendar fetch failed: %s", exc)
        return []
    if not bundle:
        return []
    days = bundle.get("events_by_date") or []
    out: list[dict] = []
    for day in days:
        if not isinstance(day, dict):
            continue
        date_iso = day.get("date") or ""
        for ev in (day.get("entries") or []):
            out.append({
                "d":          _short_date(date_iso) or date_iso[:10],
                "raw_date":   date_iso[:10],
                "evt":        ev.get("event") or "—",
                "when":       (ev.get("time") or "—") + (" ET" if ev.get("time") else ""),
                "impact":     (ev.get("impact") or "low").lower(),
                "consensus":  ev.get("estimate_fmt") or "—",
                "prior":      ev.get("previous_fmt") or "—",
                "actual":     ev.get("actual_fmt") or "—",
                "released":   bool(ev.get("is_released")),
            })
    # Sort high-impact first within each date to surface the most-watched events.
    impact_rank = {"high": 0, "medium": 1, "low": 2}
    out.sort(key=lambda r: (r.get("raw_date", ""), impact_rank.get(r["impact"], 9)))
    # Prefer high+medium impact; fall back to all if not enough.
    headliners = [r for r in out if r["impact"] in ("high", "medium")]
    rows = (headliners or out)[:top_n]
    return rows


# ── Heatmap tab — sector grid + global indices ────────────────────────────

# GICS sector labels → palette (same colours as Funds page sectors).
_HEATMAP_SECTOR_LABELS: dict[str, str] = {
    "Information Technology": "Tech",
    "Communication Services": "Comm",
    "Consumer Discretionary": "Cons Disc",
    "Financials":             "Financials",
    "Industrials":            "Industrials",
    "Health Care":            "Health Care",
    "Consumer Staples":       "Cons Staples",
    "Materials":              "Materials",
    "Real Estate":            "Real Estate",
    "Utilities":              "Utilities",
    "Energy":                 "Energy",
}


def _heatmap_sectors(*, period: str = "1D") -> list[dict]:
    """Aggregate S&P 500 daily moves by GICS sector — average ticker
    pct_change weighted by uniform weight (the design renders the average
    daily move per sector, not market-cap-weighted, since we don't carry
    per-ticker market cap on the hot path)."""
    try:
        from filings import market_data
    except Exception:
        return []
    sp_data = market_data.get_sp500_market_data(period) or {}
    constituents = market_data.get_sp500_constituents() or []
    ticker_to_sector = {(c.get("ticker") or "").upper(): c.get("sector") or "" for c in constituents}

    from collections import defaultdict
    by_sector: dict[str, list[float]] = defaultdict(list)
    for tk, rec in sp_data.items():
        if not isinstance(rec, dict) or tk == "_metadata":
            continue
        sector = ticker_to_sector.get(tk)
        pct = rec.get("pct_change")
        if not sector or not isinstance(pct, (int, float)):
            continue
        by_sector[sector].append(pct)

    rows: list[dict] = []
    for sector, label in _HEATMAP_SECTOR_LABELS.items():
        if sector not in by_sector or not by_sector[sector]:
            continue
        avg = sum(by_sector[sector]) / len(by_sector[sector])
        rows.append({
            "name":   label,
            "sector": sector,
            "delta":  avg / 100.0,        # 0..1 fraction
            "delta_str": f"{avg:+.2f}%",
            "n":      len(by_sector[sector]),
        })

    # Sort biggest-up → biggest-down so the grid reads as a hot/cold gradient.
    rows.sort(key=lambda r: r["delta"], reverse=True)
    return rows


def _heatmap_global_indices(index_payload: dict | None) -> list[dict]:
    """Build the global-indices table from the cached index_market_data feed."""
    if not isinstance(index_payload, dict):
        return []
    universe = [
        ("US",   "^GSPC", "S&P 500"),
        ("US",   "^IXIC", "Nasdaq"),
        ("US",   "^DJI",  "Dow Jones"),
        ("US",   "^RUT",  "Russell 2000"),
        ("Vol",  "^VIX",  "VIX"),
    ]
    rows: list[dict] = []
    for region, sym, name in universe:
        rec = index_payload.get(sym)
        if not isinstance(rec, dict):
            continue
        price = rec.get("price")
        pct = rec.get("pct_change")     # already in %
        rows.append({
            "region":    region,
            "name":      name,
            "price_str": (f"{price:,.2f}" if isinstance(price, (int, float)) else "—"),
            "today_str": (f"{pct:+.2f}%" if isinstance(pct, (int, float)) else "—"),
            "today_up":  (pct >= 0) if isinstance(pct, (int, float)) else None,
            "spark":     rec.get("spark") or [],
        })
    return rows


# ── Earnings tab — Scorecard sub-pane ────────────────────────────────────
#
# Reuses v1's `earnings_scorecard.fetch_earnings_data` + `fetch_historical_
# beat_rates` so a pre-warmed L2 row from the v1 endpoint serves the v2
# render too.  Shape function below converts the raw helper output into
# the lightweight payload the template consumes (KPI strip, two donuts,
# trend chart geometry, filterable results table).

_EARN_DONUT_R    = 60.0
_EARN_DONUT_C    = 2 * math.pi * _EARN_DONUT_R     # circumference
_EARN_TREND_VBW  = 1500
_EARN_TREND_VBH  = 360
_EARN_TREND_PADX = 60
_EARN_TREND_PADY = 40


def _earn_donut(beats: int, misses: int, inline: int) -> dict:
    """Build a donut chart payload — three arcs sized by share of total."""
    total = beats + misses + inline
    if total <= 0:
        return {"have_data": False, "beats": 0, "misses": 0, "inline": 0,
                "beat_pct": 0, "miss_pct": 0, "inline_pct": 0,
                "circumference": _EARN_DONUT_C, "r": _EARN_DONUT_R,
                "beat_dash": 0, "miss_dash": 0, "inline_dash": 0,
                "miss_offset": 0, "inline_offset": 0}
    beat_pct  = beats  / total * 100
    miss_pct  = misses / total * 100
    inl_pct   = inline / total * 100
    beat_dash = (beat_pct / 100) * _EARN_DONUT_C
    miss_dash = (miss_pct / 100) * _EARN_DONUT_C
    inl_dash  = (inl_pct  / 100) * _EARN_DONUT_C
    # SVG dashes draw clockwise from the 12-o'clock anchor (rotated -90°).
    # Each subsequent arc starts where the previous one left off; we flip
    # the sign because dash-offset is direction-reversed.
    return {
        "have_data": True,
        "beats": beats, "misses": misses, "inline": inline, "total": total,
        "beat_pct": round(beat_pct, 1),
        "miss_pct": round(miss_pct, 1),
        "inline_pct": round(inl_pct, 1),
        "circumference": round(_EARN_DONUT_C, 2),
        "r": _EARN_DONUT_R,
        "beat_dash": round(beat_dash, 2),
        "miss_dash": round(miss_dash, 2),
        "inline_dash": round(inl_dash, 2),
        "miss_offset":   round(-beat_dash, 2),
        "inline_offset": round(-(beat_dash + miss_dash), 2),
    }


def _earn_trend_chart(trend: list[dict]) -> dict:
    """Build SVG-ready geometry for the beat-rate vs. market-reaction trend.

    Two overlaid lines (EPS beat %, Rev beat %) on a 0-100 left axis and a
    series of bars (avg market reaction %) on a right axis centred at 0.
    Path strings + axis tick labels are pre-computed so the template stays
    declarative — no math in Jinja.
    """
    rows = [r for r in (trend or []) if isinstance(r, dict)]
    if not rows:
        return {"have_data": False}

    n = len(rows)
    vbw, vbh = _EARN_TREND_VBW, _EARN_TREND_VBH
    pad_x, pad_y = _EARN_TREND_PADX, _EARN_TREND_PADY
    plot_w = vbw - pad_x * 2
    plot_h = vbh - pad_y * 2

    def _x(i: int) -> float:
        if n == 1:
            return vbw / 2
        return pad_x + (plot_w * i / (n - 1))

    def _y_pct(p: float) -> float:                     # 0..100 → top..bottom
        return pad_y + (plot_h * (1 - max(0, min(100, p)) / 100))

    # Bars (avg_price_change): -10..+10 default, expanded to fit data.
    rxn_vals = [float(r.get("avg_price_change") or 0) for r in rows]
    rxn_max  = max(abs(min(rxn_vals)), abs(max(rxn_vals)), 5.0) * 1.15
    rxn_zero = vbh / 2

    bar_w = max(8, plot_w / max(n, 1) * 0.18)

    eps_pts: list[tuple[float, float]] = []
    rev_pts: list[tuple[float, float]] = []
    bars:    list[dict]                = []
    quarters: list[str]                = []
    hover_points: list[dict]           = []
    for i, r in enumerate(rows):
        x   = _x(i)
        eps = float(r.get("eps_beat_rate") or 0)
        rev = float(r.get("rev_beat_rate") or 0)
        rxn = float(r.get("avg_price_change") or 0)
        eps_pts.append((x, _y_pct(eps)))
        rev_pts.append((x, _y_pct(rev)))
        # Map rxn onto vertical span, anchored at zero-line.
        bar_h = abs(rxn) / rxn_max * (plot_h / 2)
        bar_y = rxn_zero - bar_h if rxn >= 0 else rxn_zero
        bars.append({
            "x": round(x - bar_w / 2, 2), "y": round(bar_y, 2),
            "w": round(bar_w, 2), "h": round(bar_h, 2),
            "up": rxn >= 0, "rxn_str": f"{rxn:+.2f}%",
        })
        quarters.append(r.get("quarter", ""))
        hover_points.append({
            "x":     round(x, 2),
            "y":     round(_y_pct(eps), 2),    # anchor on EPS line (primary)
            "label": r.get("quarter", ""),
            "rows":  [
                {"label": "EPS Beat", "value": f"{eps:.1f}%", "color": "var(--pp-accent)"},
                {"label": "Rev Beat", "value": f"{rev:.1f}%", "color": "var(--pp-ink)"},
                {"label": "Reaction", "value": f"{rxn:+.2f}%",
                 "color": "var(--pp-up)" if rxn >= 0 else "var(--pp-down)"},
            ],
        })

    return {
        "have_data": True,
        "vb_w": vbw, "vb_h": vbh,
        "pad_x": pad_x, "pad_y": pad_y,
        "plot_w": plot_w, "plot_h": plot_h,
        "eps_path": _svg_line_path(eps_pts),
        "rev_path": _svg_line_path(rev_pts),
        "eps_dots": [{"x": round(x, 2), "y": round(y, 2),
                      "v": rows[i].get("eps_beat_rate")} for i, (x, y) in enumerate(eps_pts)],
        "rev_dots": [{"x": round(x, 2), "y": round(y, 2),
                      "v": rows[i].get("rev_beat_rate")} for i, (x, y) in enumerate(rev_pts)],
        "bars":         bars,
        "quarters":     quarters,
        "hover_points": hover_points,
        "y_ticks":      [0, 25, 50, 75, 100],
        "y_pct":        [{"y": round(_y_pct(p), 2), "label": f"{p}%"} for p in (0, 25, 50, 75, 100)],
        "rxn_zero":     round(rxn_zero, 2),
        "rxn_max":      round(rxn_max, 2),
    }


def _earn_kpi_strip(metrics: dict) -> list[dict]:
    """Map _compute_metrics() → 4-cell KPI strip.

    Always returns a 4-cell list (em-dashed when metrics is empty) so
    the template's ``earn.kpi_strip`` iteration is a stable contract --
    callers can rely on a non-empty strip regardless of upstream state.
    """
    if not metrics:
        return [
            _kpi("EPS Beat Rate",       "—", "no data"),
            _kpi("Revenue Beat Rate",   "—", "no data"),
            _kpi("Dual Beats",          "—", "no data"),
            _kpi("Avg Market Reaction", "—", "next-day price"),
        ]
    eps_rate = metrics.get("eps_beat_rate", 0)
    rev_rate = metrics.get("rev_beat_rate", 0)
    rxn      = metrics.get("avg_price_change", 0)
    return [
        _kpi("EPS Beat Rate",       f"{eps_rate:.1f}%",
             f"{metrics.get('eps_beats', 0)} beats / {metrics.get('total', 0)}", eps_rate >= 70),
        _kpi("Revenue Beat Rate",   f"{rev_rate:.1f}%",
             f"{metrics.get('rev_beats', 0)} beats / {metrics.get('rev_total', metrics.get('total', 0))}",
             rev_rate >= 60),
        _kpi("Dual Beats",          metrics.get("dual_beats", 0),
             f"of {metrics.get('total', 0)} reporting"),
        _kpi("Avg Market Reaction", f"{rxn:+.2f}%", "next-day price", rxn >= 0),
    ]


def _v2_macro_earnings_empty() -> MacroEarningsPayload:
    """Empty-shape fallback matching :func:`_v2_macro_earnings_payload`."""
    from filings import earnings_scorecard
    return {
        "have_data":       False,
        "metrics":         {},
        "results":         [],
        "results_total":   0,
        "kpi_strip":       _earn_kpi_strip({}),
        "donut_eps":       _earn_donut(0, 0, 0),
        "donut_rev":       _earn_donut(0, 0, 0),
        "trend":           _earn_trend_chart([]),
        "current_index":   "all",
        "current_quarter": "",
        "current_sector":  "",
        "quarters":        [],
        "indices":         earnings_scorecard.INDEX_CHOICES,
        "sectors":         earnings_scorecard.SECTORS,
    }


async def _v2_macro_earnings_payload(
    index: str, quarter: str | None, sector: str | None,
) -> MacroEarningsPayload:
    """Fetch + reshape Earnings Scorecard data for the v2 Earnings tab."""
    from filings import earnings_scorecard

    if index not in earnings_scorecard.INDEX_CHOICES:
        index = "all"
    quarter = quarter or None
    sector  = sector  or None
    if sector and sector not in earnings_scorecard.SECTORS:
        sector = None

    data, trend = await asyncio.gather(
        to_heavy(earnings_scorecard.fetch_earnings_data, index, quarter, sector),
        to_heavy(earnings_scorecard.fetch_historical_beat_rates, index),
    )
    metrics  = data.get("metrics") or {}
    results  = data.get("results") or []
    quarters = earnings_scorecard.get_available_quarters()

    # Sort results by abs(price_change) descending so the user sees the
    # biggest reactions first; rows without a price_change drop to the bottom.
    def _sort_key(r: dict) -> tuple[int, float]:
        pc = r.get("price_change")
        return (0, -abs(float(pc))) if isinstance(pc, (int, float)) else (1, 0.0)
    results_sorted = sorted(results, key=_sort_key)

    return {
        "have_data":       bool(metrics.get("total", 0) > 0),
        "metrics":         metrics,
        "results":         results_sorted[:60],         # cap rendered rows
        "results_total":   len(results_sorted),
        "kpi_strip":       _earn_kpi_strip(metrics),
        "donut_eps":       _earn_donut(
            metrics.get("eps_beats", 0),
            metrics.get("eps_misses", 0),
            metrics.get("eps_inline", 0),
        ),
        "donut_rev":       _earn_donut(
            metrics.get("rev_beats", 0),
            metrics.get("rev_misses", 0),
            metrics.get("rev_inline", 0),
        ),
        "trend":           _earn_trend_chart(trend),
        "current_index":   index,
        "current_quarter": data.get("quarter") or (quarters[0] if quarters else ""),
        "current_sector":  sector or "",
        "quarters":        quarters,
        "indices":         earnings_scorecard.INDEX_CHOICES,
        "sectors":         earnings_scorecard.SECTORS,
    }


# ── Earnings tab — Calendar sub-pane ─────────────────────────────────────

def _v2_macro_calendar_empty() -> MacroCalendarPayload:
    """Empty-shape fallback matching :func:`_v2_macro_calendar_payload`."""
    from filings import earnings_scorecard
    return {
        "have_data":      False,
        "metrics":        {},
        "upcoming":       [],
        "just_reported":  [],
        "kpi_strip":      [
            _kpi("Reporting",   0, ""),
            _kpi("Before Open", 0, "BMO releases"),
            _kpi("After Close", 0, "AMC releases"),
            _kpi("SI Holdings", 0, "tracked names"),
        ],
        "week_grid":      [],
        "current_index":  "all",
        "current_period": "this_week",
        "indices":        earnings_scorecard.INDEX_CHOICES,
        "periods":        earnings_scorecard.CALENDAR_PERIODS,
    }


async def _v2_macro_calendar_payload(
    request: Request, index: str, period: str,
) -> MacroCalendarPayload:
    """Fetch + reshape Earnings Calendar data for the v2 Earnings tab."""
    from filings import earnings_scorecard
    from filings.client import build_ticker_ownership_map

    if index not in earnings_scorecard.INDEX_CHOICES:
        index = "all"
    if period not in earnings_scorecard.CALENDAR_PERIODS:
        period = "this_week"

    # Reuse the cached app.state ownership map (built lazily, invalidated
    # by fund_cache id change).  Same pattern as web._get_ownership_map.
    fund_cache = getattr(request.app.state, "fund_cache", {}) or {}
    ownership_map: dict[str, list[str]] = {}
    if fund_cache:
        cached = getattr(request.app.state, "_ownership_map", None)
        cache_id = id(fund_cache)
        if cached is not None and cached[0] == cache_id:
            ownership_map = cached[1]
        else:
            try:
                from filings.superinvestors import SUPERINVESTORS_BY_CIK
                ownership_map = build_ticker_ownership_map(fund_cache, SUPERINVESTORS_BY_CIK)
                request.app.state._ownership_map = (cache_id, ownership_map)
            except Exception:
                ownership_map = {}

    si_tickers = set(ownership_map.keys())

    data = await to_heavy(
        earnings_scorecard.fetch_earnings_calendar, index, period, si_tickers,
    )

    # Important: `fetch_earnings_calendar` caches the result dict.  Mutating
    # `entry["si_names"]` directly would pollute the cached payload — the
    # next caller with a stale fund_cache would see the previous map.  Build
    # a shallow-copied list so the cache stays clean.
    raw_upcoming = data.get("upcoming") or []
    if ownership_map:
        upcoming = []
        for date_group in raw_upcoming:
            entries = date_group.get("entries") or []
            new_entries = [
                ({**e, "si_names": ownership_map[e["symbol"]]}
                 if e.get("symbol") in ownership_map else e)
                for e in entries
            ]
            upcoming.append({**date_group, "entries": new_entries})
    else:
        upcoming = raw_upcoming

    metrics = data.get("metrics") or {}
    return {
        "have_data":      bool(upcoming),
        "metrics":        metrics,
        "upcoming":       upcoming,
        "just_reported":  data.get("just_reported") or [],
        "kpi_strip":      [
            _kpi("Reporting",   metrics.get("reporting_count", 0), data.get("period_label", "")),
            _kpi("Before Open", metrics.get("bmo_count", 0),       "BMO releases"),
            _kpi("After Close", metrics.get("amc_count", 0),       "AMC releases"),
            _kpi("SI Holdings", metrics.get("si_reporting_count", 0), "tracked names"),
        ],
        "week_grid":      _week_grid_mon_fri(upcoming),
        "current_index":  index,
        "current_period": period,
        "indices":        earnings_scorecard.INDEX_CHOICES,
        "periods":        earnings_scorecard.CALENDAR_PERIODS,
    }


# ── Performance tab — market breadth ─────────────────────────────────────
#
# Wraps `market_breadth.fetch_breadth_data` and `fetch_ad_line_history`
# (both already L1 30 min / L2 6 hr cached inside the helper) so warm hits
# are sub-second.  The shape function below precomputes everything the
# template needs: KPI strip, sector bars, top movers list, and the SVG
# geometry for the dual-axis A/D-vs-index momentum chart.

_PERF_VBW = 1500
_PERF_VBH = 360


def _perf_status(advance_pct: float) -> dict:
    """Map advance % → status pill (Bullish / Neutral / Bearish)."""
    if advance_pct >= 60:
        return {"label": "Bullish", "tone": "up"}
    if advance_pct <= 40:
        return {"label": "Bearish", "tone": "down"}
    return {"label": "Neutral", "tone": "dim"}


def _perf_momentum_chart(ad_line: dict | None) -> dict:
    """Dual-axis line chart: cumulative A/D vs index price (last 60 trading days).

    Accepts ``None`` (handled identically to an empty dict) so callers can
    pass the raw result of ``to_heavy(market_breadth.fetch_ad_line_history)``
    without an extra guard.
    """
    if not ad_line:
        return {"have_data": False}
    dates = ad_line.get("dates") or []
    ads   = ad_line.get("cumulative_ad") or []
    pxs   = ad_line.get("index_prices") or []
    if not dates or len(dates) < 5:
        return {"have_data": False}

    n = min(60, len(dates))
    dates = dates[-n:]
    ads   = ads[-n:]
    pxs   = pxs[-n:]

    # A/D line and index price share the x axis but each has its own y
    # range — build two separate (_x, _y) factories from `_xy_mappers`.
    ad_min, ad_max = min(ads), max(ads)
    px_clean = [p for p in pxs if isinstance(p, (int, float))]
    px_min   = min(px_clean) if px_clean else 0.0
    px_max   = max(px_clean) if px_clean else 1.0

    _x, _y_ad, _, _ = _xy_mappers(n, vbw=_PERF_VBW, vbh=_PERF_VBH, pad_x=50, pad_y=30,
                                   s_min=ad_min, s_max=ad_max)
    _,  _y_px, _, _ = _xy_mappers(n, vbw=_PERF_VBW, vbh=_PERF_VBH, pad_x=50, pad_y=30,
                                   s_min=px_min, s_max=px_max)

    ad_pts = [(_x(i), _y_ad(v)) for i, v in enumerate(ads)]
    px_pts = [(_x(i), _y_px(p)) for i, p in enumerate(pxs)
              if isinstance(p, (int, float))]

    # Sample x-axis labels every ~6 trading days.
    step = max(1, n // 6)
    x_labels = []
    for i in range(0, n, step):
        try:
            d = datetime.strptime(dates[i], "%Y-%m-%d")
            x_labels.append({"x": round(_x(i), 2), "label": d.strftime("%b %d")})
        except ValueError:
            continue

    # Multi-line hover: each x position carries the A/D and price values.
    # Y-anchor is the A/D point so the hover dot sits on the primary line.
    ad_color   = "var(--pp-accent)"
    index_name = ad_line.get("index_name", "Index")
    px_color   = "var(--pp-ink)"
    hover_points: list[dict] = []
    for i, ad_v in enumerate(ads):
        rows = [{"label": "A/D line", "value": f"{ad_v:+.0f}", "color": ad_color}]
        p = pxs[i] if i < len(pxs) else None
        if isinstance(p, (int, float)):
            rows.append({"label": index_name, "value": f"{p:,.2f}", "color": px_color})
        hover_points.append({
            "x":     round(_x(i), 2),
            "y":     round(_y_ad(ad_v), 2),
            "label": dates[i],
            "rows":  rows,
        })

    return {
        "have_data": True,
        "vb_w": _PERF_VBW, "vb_h": _PERF_VBH,
        "ad_path": _svg_line_path(ad_pts),
        "px_path": _svg_line_path(px_pts),
        "x_labels": x_labels,
        "hover_points": hover_points,
        "index_name": index_name,
        "n_days": n,
    }


def _v2_macro_performance_empty() -> MacroPerformancePayload:
    """Empty-shape fallback matching :func:`_v2_macro_performance_payload`."""
    from filings import market_breadth
    return {
        "have_data":         False,
        "metrics":           {},
        "advance_pct":       0.0,
        "decline_pct":       0.0,
        "unchanged_pct":     0.0,
        "status":            _perf_status(0.0),
        "above_50d":         {},
        "sector_breadth":    [],
        "top_gainers":       [],
        "top_losers":        [],
        "divergence":        None,
        "momentum":          _perf_momentum_chart(None),
        "kpi_strip":         [
            _kpi("Up / Down",       0, "0 : 0", None),
            _kpi("Advancers",       0, "0.0%",  None),
            _kpi("Decliners",       0, "0.0%",  None),
            _kpi("Above 50-day MA", "—", "", None),
        ],
        "current_index":     "sp500",
        "current_period":    "1d",
        "indices":           market_breadth.INDEX_CHOICES,
        "periods":           market_breadth.PERIOD_CHOICES,
        "data_period_label": "",
        "data_index_name":   "",
        "data_as_of":        "",
    }


async def _v2_macro_performance_payload(index: str, period: str) -> MacroPerformancePayload:
    """Fetch + reshape Market Breadth data for the v2 Performance tab."""
    from filings import market_breadth

    if index not in market_breadth.INDEX_CHOICES:
        index = "sp500"
    if period not in market_breadth.PERIOD_CHOICES:
        period = "1d"

    data, ad_line = await asyncio.gather(
        to_heavy(market_breadth.fetch_breadth_data, index, period),
        to_heavy(market_breadth.fetch_ad_line_history, index),
    )
    divergence = market_breadth.detect_divergence(ad_line) if ad_line else None
    metrics = data.get("metrics") or {}
    above   = data.get("above_50d") or {}
    movers  = data.get("top_movers") or {}

    advance_pct = float(metrics.get("advance_pct") or 0)
    decline_pct = float(metrics.get("decline_pct") or 0)
    status      = _perf_status(advance_pct)

    # Up-vs-down ratio bar segments — leftover slice fills with "unchanged".
    unchanged_pct = max(0.0, 100 - advance_pct - decline_pct)

    # Enrich each sector breadth row with the full {up, down, unchanged}
    # split (the v1 helper only emits `up` + `total`).  Surfacing the
    # decline + unchanged counts lets the bar chart render a 3-segment
    # stacked fill and the hover tooltip show the full distribution.
    sector_breadth: list[dict] = []
    for s in (data.get("sector_breadth") or []):
        up_n    = int(s.get("up") or 0)
        down_n  = int(s.get("down") or 0)
        total_n = int(s.get("total") or 0)
        unc_n   = max(0, total_n - up_n - down_n)
        denom   = max(total_n, 1)
        sector_breadth.append({
            **s,
            "down":          down_n,
            "unchanged":     unc_n,
            "down_pct":      round(down_n / denom * 100, 1),
            "unchanged_pct": round(unc_n  / denom * 100, 1),
        })

    return {
        "have_data":      bool(metrics.get("total", 0) > 0),
        "metrics":        metrics,
        "advance_pct":    round(advance_pct, 1),
        "decline_pct":    round(decline_pct, 1),
        "unchanged_pct":  round(unchanged_pct, 1),
        "status":         status,
        "above_50d":      above,
        "sector_breadth": sector_breadth,
        "top_gainers":    (movers.get("gainers") or [])[:5],
        "top_losers":     (movers.get("losers")  or [])[:5],
        "divergence":     divergence,
        "momentum":       _perf_momentum_chart(ad_line),
        "kpi_strip":      [
            _kpi("Up / Down",   metrics.get("breadth_ratio", 0),
                 f"{metrics.get('advances', 0)} : {metrics.get('declines', 0)}",
                 advance_pct >= 50),
            _kpi("Advancers",   metrics.get("advances", 0), f"{advance_pct:.1f}%", up=True),
            _kpi("Decliners",   metrics.get("declines", 0), f"{decline_pct:.1f}%", up=False),
            _kpi("Above 50-day MA",
                 (f"{above.get('pct', 0):.1f}%" if above else None),
                 (f"{above.get('above', 0)} of {above.get('total', 0)}" if above else ""),
                 (above.get("pct", 0) >= 50) if above else None),
        ],
        "current_index":  index,
        "current_period": period,
        "indices":        market_breadth.INDEX_CHOICES,
        "periods":        market_breadth.PERIOD_CHOICES,
        "data_period_label": data.get("period_label", ""),
        "data_index_name":   data.get("index_name", ""),
        "data_as_of":        data.get("as_of", ""),
    }


# ── Sentiment tab — Google Trends category dashboard ──────────────────────
#
# Reuses `google_trends.MACRO_CATEGORIES` (8 themed groups × ~4 keywords)
# and `fetch_macro_trends(category)` which already L1-caches for 24h per
# category.  We additionally wrap the whole 8-category fetch in L2 with a
# 12h TTL so pytrends rate-limits don't dominate the cold path.

def _sentiment_chart(payload: dict | None) -> dict:
    """Build a multi-line SVG payload for one category's interest-over-time."""
    if not payload or not payload.get("data"):
        return {"have_data": False}

    data     = payload["data"]
    keywords = payload.get("keywords") or []
    if not keywords or len(data) < 2:
        return {"have_data": False}

    # Flatten — each keyword gets its own series of (idx, value).
    series_by_kw: dict[str, list[float]] = {kw: [] for kw in keywords}
    for pt in data:
        vals = pt.get("values") or {}
        for kw in keywords:
            series_by_kw[kw].append(float(vals.get(kw, 0)))

    n = len(data)
    vbw, vbh = 600, 160
    # Google Trends scale is 0..100 by definition — fixed range here.
    _x, _y, _, _ = _xy_mappers(n, vbw=vbw, vbh=vbh, pad_x=8, pad_y=8,
                                s_min=0.0, s_max=100.0)

    # Per-keyword palette — keep tiny + recyclable across cards.
    kw_colors = ["#3b82f6", "#a855f7", "#ef4444", "#f59e0b", "#10b981"]

    lines: list[dict] = []
    for i, kw in enumerate(keywords):
        points = [(_x(j), _y(v)) for j, v in enumerate(series_by_kw[kw])]
        lines.append({
            "kw":      kw,
            "color":   kw_colors[i % len(kw_colors)],
            "path_d":  _svg_line_path(points),
            "avg":     payload.get("averages", {}).get(kw, 0),
        })

    # Multi-line hover: one entry per x-position, each carrying a row per
    # keyword series so the tooltip shows all values at the hover point.
    hover_points = []
    for j in range(n):
        hover_points.append({
            "x":     round(_x(j), 2),
            "label": data[j].get("date", ""),
            "rows":  [
                {"label": kw,
                 "value": str(int(series_by_kw[kw][j])),
                 "color": kw_colors[k % len(kw_colors)]}
                for k, kw in enumerate(keywords)
            ],
        })

    # First/last date labels for the x-axis.
    first_d = data[0].get("date", "")
    last_d  = data[-1].get("date", "")

    return {
        "have_data":    True,
        "vb_w":         vbw,
        "vb_h":         vbh,
        "lines":        lines,
        "hover_points": hover_points,
        "first_date":   first_d,
        "last_date":    last_d,
        "n_points":     n,
    }


_SENTIMENT_L2_KEY = "redesign:macro:sentiment:3m"
_SENTIMENT_L2_TTL = 43200      # 12h — Google Trends data updates daily
# Tracks whether a background warmup is already running so concurrent page
# loads don't all kick off their own slow fetch.
_sentiment_warming: bool = False
_sentiment_warm_lock = asyncio.Lock()


def _sentiment_compute_sync() -> dict:
    """Sequential per-category fetch — pytrends rate-limits globally so
    parallelism inside the same process won't help and risks 429s.

    Returns ``{}`` (falsy) when zero categories returned real data so the
    L2 wrapper skips the writeback and the next page load tries again.
    """
    from filings import google_trends
    out: dict[str, dict] = {}
    successes = 0
    for cat in google_trends.MACRO_CATEGORIES.keys():
        try:
            payload = google_trends.fetch_macro_trends(cat, timeframe="today 3-m")
        except Exception as exc:
            logger.warning("sentiment fetch %s failed: %s", cat, exc)
            payload = None
        if payload and payload.get("data"):
            successes += 1
            out[cat] = payload
        else:
            out[cat] = {}
    if successes == 0:
        return {}
    logger.info("sentiment compute: %d/%d categories returned data", successes, len(out))
    return out


async def _sentiment_warm_l2() -> None:
    """Background task: run the slow compute once and write to L2.

    Bypasses L2 read so a poisoned cache row (e.g. from a 429-only run)
    can be self-healing — we always recompute and overwrite when this
    warmer fires.  Guarded by `_sentiment_warming` so multiple in-flight
    page loads don't fan out a herd of identical Google Trends fetches.
    """
    global _sentiment_warming
    async with _sentiment_warm_lock:
        if _sentiment_warming:
            return
        _sentiment_warming = True
    try:
        payload = await to_heavy(_sentiment_compute_sync)
        if payload and isinstance(payload, dict):
            usable = sum(
                1 for v in payload.values()
                if isinstance(v, dict) and v.get("data")
            )
            if usable > 0:
                # Write to L2 directly (skipping the read path).  Same
                # category as redesign_home so ops queries stay simple.
                # ``to_supabase(fn, *args)`` forwards positionally only --
                # ``ttl_seconds`` is `set_cached`'s 4th positional param,
                # NOT a kwarg to ``to_supabase`` (mypy caught this).
                await to_supabase(
                    supabase_cache.set_cached,
                    _SENTIMENT_L2_KEY, "macro", payload, _SENTIMENT_L2_TTL,
                )
                logger.info("sentiment warmer: L2 row written (%d/%d categories)",
                            usable, len(payload))
            else:
                logger.warning("sentiment warmer: compute returned 0 usable categories — "
                               "skipping L2 write so the next request retries")
        else:
            logger.warning("sentiment warmer: compute returned %s (empty/None)",
                           type(payload).__name__)
    except Exception as exc:
        logger.warning("sentiment warmer raised: %s", exc)
    finally:
        async with _sentiment_warm_lock:
            _sentiment_warming = False


async def _v2_sentiment_payload() -> dict:
    """Reshape Google Trends macro data for the v2 Sentiment tab.

    Hot path: L2 cache hit (~10ms).  Cold path: returns the placeholder
    state immediately and kicks off a background warmer; the next page
    load will see the populated L2 row.  This avoids blocking page
    render on a 30-40s Pytrends fetch that's rate-limited globally.
    """
    from filings import google_trends

    categories = list(google_trends.MACRO_CATEGORIES.keys())

    # Try L2 first; do NOT pass through to compute (we don't want to
    # block the page render on it).  Cold miss returns None; we then
    # kick off the background warmer.  We also treat a cached row that
    # contains zero usable categories as a miss (poison from a 429-only
    # warm — better to retry than serve placeholders forever).
    raw: dict | None = None
    try:
        result = await to_supabase(
            supabase_cache.get_cached_with_stale, _SENTIMENT_L2_KEY,
        )
        cached, _is_fresh = result if result is not None else (None, False)
        if cached and isinstance(cached, dict):
            usable = sum(
                1 for v in cached.values()
                if isinstance(v, dict) and v.get("data")
            )
            if usable > 0:
                raw = cached
    except Exception as exc:
        logger.debug("sentiment L2 read failed: %s", exc)

    if raw is None:
        # Fire-and-forget background warmer — first user pays the cache
        # miss as a placeholder render; the next request gets real data.
        asyncio.create_task(_sentiment_warm_l2())

    cards: list[dict] = []
    for cat in categories:
        kws = google_trends.MACRO_CATEGORIES[cat]
        payload = (raw or {}).get(cat) or {}
        chart = _sentiment_chart(payload)
        cards.append({
            "category":  cat,
            "keywords":  kws,
            "chart":     chart,
            "have_data": chart.get("have_data", False),
        })

    return {
        "have_data":   any(c["have_data"] for c in cards),
        "cards":       cards,
        "categories":  categories,
        "is_warming":  raw is None,    # surfaced to template for "warming up" copy
    }


# ── Volatility tab — Put/Call Ratio · VIX term structure · SKEW Index ────
#
# All three feeds come from `cboe_data` — Put/Call ratios from CBOE CSVs,
# VIX term structure + SKEW from yfinance.  We fan out concurrent
# to_heavy() calls and shape each into chart-ready geometry.

_PC_RATIO_TYPES = {"total": "Total", "index": "Index", "equity": "Equity"}


def _pc_ratio_chart(rows: list[dict], ratio_type: str) -> dict:
    """Build SVG-ready payload for the Put/Call ratio panel."""
    if not rows or len(rows) < 5:
        return {"have_data": False, "ratio_type": ratio_type}

    ratios = [float(r.get("ratio") or 0) for r in rows if r.get("ratio")]
    if not ratios:
        return {"have_data": False, "ratio_type": ratio_type}

    current = ratios[-1]
    s_min, s_max = min(ratios), max(ratios)
    rng = (s_max - s_min) or 1
    # Pad y-range so the line doesn't touch the edge.
    s_min -= rng * 0.05
    s_max += rng * 0.05
    rng = s_max - s_min

    n = len(rows)
    vbw, vbh = 1500, 280
    _x, _y, _, _ = _xy_mappers(n, vbw=vbw, vbh=vbh, pad_x=50, pad_y=24,
                                s_min=s_min, s_max=s_max)
    valid = [(i, r) for i, r in enumerate(rows) if r.get("ratio") is not None]
    points = [(_x(i), _y(float(r["ratio"]))) for i, r in valid]
    path_d = _svg_line_path(points)
    hover_points = [
        {"x": round(_x(i), 2), "y": round(_y(float(r["ratio"])), 2),
         "label": r["date"], "value": f"{float(r['ratio']):.2f}"}
        for i, r in valid
    ]

    # 1.0 threshold dashed line (bearish/bullish split).
    threshold_y = round(_y(1.0), 2)

    # Y-ticks in 0.3 increments rounded to a friendly grid.
    y_ticks = [{"y": round(_y(v), 2), "label": f"{v:.1f}"}
               for v in (0, 0.3, 0.6, 0.9, 1.2, 1.5, 1.8, 2.1)
               if s_min <= v <= s_max]

    # X-axis ticks every ~6 weeks.
    step = max(1, n // 6)
    x_ticks = [{"x": round(_x(i), 2), "label": rows[i]["date"]}
               for i in range(0, n, step)]

    # Signal: P/C > 1.0 = bearish (more puts), < 0.7 = bullish (more calls).
    if current >= 1.0:
        signal = {"label": "BEARISH", "tone": "down"}
    elif current <= 0.7:
        signal = {"label": "BULLISH", "tone": "up"}
    else:
        signal = {"label": "NEUTRAL", "tone": "dim"}

    return {
        "have_data":   True,
        "ratio_type":  ratio_type,
        "ratio_label": _PC_RATIO_TYPES.get(ratio_type, ratio_type.title()),
        "current":     round(current, 2),
        "signal":      signal,
        "vb_w":        vbw,
        "vb_h":        vbh,
        "path_d":      path_d,
        "threshold_y": threshold_y,
        "y_ticks":     y_ticks,
        "x_ticks":     x_ticks,
        "hover_points": hover_points,
        "first_date":  rows[0]["date"],
        "last_date":   rows[-1]["date"],
    }


def _vix_term_payload(term: dict | None) -> dict:
    """Reshape get_vix_term_structure() into a chart payload."""
    if not term or not term.get("tenors"):
        return {"have_data": False}

    tenors = term["tenors"]
    if len(tenors) < 2:
        return {"have_data": False}

    values = [float(t["value"]) for t in tenors]
    s_min, s_max = min(values), max(values)
    rng = (s_max - s_min) or 1
    pad = rng * 0.15
    s_min -= pad
    s_max += pad
    rng = s_max - s_min

    n = len(tenors)
    vbw, vbh = 600, 220
    _x, _y, _, _ = _xy_mappers(n, vbw=vbw, vbh=vbh, pad_x=50, pad_y=28,
                                s_min=s_min, s_max=s_max)
    points = [(_x(i), _y(v)) for i, v in enumerate(values)]
    dots   = [{"x": round(p[0], 2), "y": round(p[1], 2),
               "label": tenors[i]["label"], "value": values[i]}
              for i, p in enumerate(points)]
    hover_points = [
        {"x": round(p[0], 2), "y": round(p[1], 2),
         "label": tenors[i]["label"], "value": f"{values[i]:.2f}"}
        for i, p in enumerate(points)
    ]
    return {
        "have_data": True,
        "spot":      term.get("spot"),
        "state":     (term.get("state") or "").upper(),
        "updated":   term.get("updated", ""),
        "vb_w":      vbw,
        "vb_h":      vbh,
        "path_d":    _svg_line_path(points),
        "dots":      dots,
        "hover_points": hover_points,
    }


def _skew_chart(rows: list[dict]) -> dict:
    """Build SVG-ready payload for the CBOE SKEW index panel."""
    if not rows or len(rows) < 5:
        return {"have_data": False}

    values = [float(r.get("value") or 0) for r in rows if r.get("value")]
    if not values:
        return {"have_data": False}

    s_min, s_max = min(values), max(values)
    rng = (s_max - s_min) or 1
    s_min -= rng * 0.05
    s_max += rng * 0.05
    rng = s_max - s_min

    n = len(rows)
    vbw, vbh = 1500, 220
    _x, _y, _, _ = _xy_mappers(n, vbw=vbw, vbh=vbh, pad_x=50, pad_y=24,
                                s_min=s_min, s_max=s_max)
    points  = [(_x(i), _y(float(r["value"]))) for i, r in enumerate(rows)]
    hover_points = [
        {"x": round(_x(i), 2), "y": round(_y(float(r["value"])), 2),
         "label": r["date"], "value": f"{float(r['value']):.2f}"}
        for i, r in enumerate(rows)
    ]

    # SKEW > 130 = elevated tail risk (per CBOE).
    threshold_y = round(_y(130.0), 2) if s_min <= 130 <= s_max else None

    step = max(1, n // 6)
    x_ticks = [{"x": round(_x(i), 2), "label": rows[i]["date"]}
               for i in range(0, n, step)]

    return {
        "have_data":   True,
        "current":     round(values[-1], 2),
        "vb_w":        vbw,
        "vb_h":        vbh,
        "path_d":      _svg_line_path(points),
        "threshold_y": threshold_y,
        "x_ticks":     x_ticks,
        "hover_points": hover_points,
        "first_date":  rows[0]["date"],
        "last_date":   rows[-1]["date"],
    }


def _v2_volatility_empty() -> VolatilityPayload:
    """Empty-shape fallback matching :func:`_v2_volatility_payload`.

    Note: ``pc`` / ``vix`` / ``skew`` use the chart builders' own
    ``{"have_data": False}`` shape so the template can branch on
    ``vol.pc.have_data`` etc. without crashing on missing keys.
    """
    return {
        "have_data":  False,
        "current_pc": "total",
        "pc_types":   _PC_RATIO_TYPES,
        "pc":         _pc_ratio_chart([], "total"),
        "vix":        _vix_term_payload(None),
        "skew":       _skew_chart([]),
    }


async def _v2_volatility_payload(ratio_type: str = "total") -> VolatilityPayload:
    """Fetch + reshape Put/Call · VIX · SKEW for the v2 Volatility tab."""
    from filings import cboe_data

    if ratio_type not in _PC_RATIO_TYPES:
        ratio_type = "total"

    pc_rows, vix_term, skew_rows = await asyncio.gather(
        to_heavy(cboe_data.get_put_call_ratio, ratio_type),
        to_heavy(cboe_data.get_vix_term_structure),
        to_heavy(cboe_data.get_skew_index),
    )

    return {
        "have_data":   bool(pc_rows or vix_term or skew_rows),
        "current_pc":  ratio_type,
        "pc_types":    _PC_RATIO_TYPES,
        "pc":          _pc_ratio_chart(pc_rows or [], ratio_type),
        "vix":         _vix_term_payload(vix_term),
        "skew":        _skew_chart(skew_rows or []),
    }


# Tab keys allowed in `?tab=` deep-links — anything else falls back to the
# default first tab.  Lower-cased for robustness against URL casing.
# `calendar` is kept as an alias because that was the v2 slug before the
# tab was renamed to "Events Calendar"; dropping it would break old
# bookmarks / deep links from the redesign warmup period.
_MACRO_TABS = {
    "indicators":         "Indicators",
    "yields":             "Yields",
    "fx-and-commodities": "FX & Commodities",
    "events-calendar":    "Events Calendar",
    "calendar":           "Events Calendar",   # legacy alias
    "heatmap":            "Heatmap",
    "earnings":           "Earnings",
    "performance":        "Performance",
    "sentiment":          "Sentiment",
    "volatility":         "Volatility",
}


@router.get("/macro", response_class=HTMLResponse)
async def preview_macro(
    request: Request,
    tab: str = "indicators",
    # Earnings tab params
    earn_index: str = "all",
    earn_quarter: str = "",
    earn_sector: str = "",
    earn_view: str = "scorecard",
    # Earnings calendar (sub-pane) params
    cal_index: str = "all",
    cal_period: str = "this_week",
    # Performance tab params
    perf_index: str = "sp500",
    perf_period: str = "1d",
    # Indicators tab params
    ind_group: str = "all",
    # Events Calendar tab params
    ev_period: str = "this_week",
    ev_country: str = "us",
    ev_impact: str = "all",
    # Calendar sub-view selector (week | list)
    ev_view: str = "week",
    # Volatility tab params
    pc_type: str = "total",
):
    """Macro page — 7 tabs render real data on the initial load.

    Each tab payload is a parallel `bounded()` fetch sharing one budget
    so one slow upstream can't stall the whole render.  Reuses v1
    helpers (with their existing L2 caches) wherever possible.
    """
    bounded = functools.partial(_bounded, page="Macro page")

    from filings import treasury_data as _treasury_data
    from filings import frankfurter   as _frankfurter

    # Lazy-loaded (cut from this gather, fetched via /api/macro/{X} on
    # first tab activation): Volatility (12 s), Heatmap (12 s + 8 s),
    # Earnings + ecal (10 s + 8 s), Performance (10 s).  Initial paint
    # max bounded = max(indicators 8s, yields 6s, fx 6s, events 6s,
    # debt 6s, calendar 5s, sentiment 4s, idx 2s) = 8 s ceiling,
    # typical <500 ms with warm L2.
    (
        payload, yield_curve, fx_payload, idx_payload, calendar_rows,
        events_payload, debt_payload, sentiment_payload,
    ) = await asyncio.gather(
        bounded(_fetch_macro_indicators(),                          timeout=8.0, fallback={},   name="indicators"),
        bounded(to_heavy(_treasury_data.get_yield_curve),           timeout=6.0, fallback=None, name="yield_curve"),
        bounded(to_heavy(_frankfurter.get_fx_dashboard),            timeout=6.0, fallback=None, name="fx"),
        bounded(_warmer.read_via_l2("redesign:home:index_market"),  timeout=2.0, fallback=None, name="index_md"),
        bounded(_macro_calendar_rows(top_n=12),                     timeout=5.0, fallback=[],   name="calendar"),
        bounded(_v2_events_calendar_payload(ev_period, ev_country, ev_impact),
                timeout=6.0,  fallback=_v2_events_calendar_empty, name="events"),
        bounded(to_heavy(_treasury_data.get_debt_data),             timeout=6.0, fallback=None, name="debt"),
        # Sentiment is L2-only on the request path — slow Google Trends
        # fetch runs in a background task, so this should always be ~10ms.
        bounded(_v2_sentiment_payload(),
                timeout=4.0,  fallback={"have_data": False, "cards": [],
                                        "categories": [], "is_warming": True}, name="sentiment"),
    )

    indicators = payload.get("indicators") or []
    indicators_by_id = {i["series_id"]: i for i in indicators}
    kpi_items = _macro_kpi_strip(indicators_by_id)
    groups = _macro_groups(payload)
    indicator_count = sum(len(g["rows"]) for g in groups) or len(indicators)

    active_tab = _MACRO_TABS.get((tab or "").lower(), "Indicators")
    earn_view  = "calendar" if (earn_view or "").lower() == "calendar" else "scorecard"
    ev_view    = "list"     if (ev_view  or "").lower() == "list"     else "week"

    # Group filter for Indicators tab — "all" plus FRED group keys.
    valid_groups = {g["key"] for g in groups} | {"all"}
    ind_group = ind_group if ind_group in valid_groups else "all"

    ctx = {
        "request":      request,
        **(await _shell_context(request, "Macro")),
        "page_title":   "Macro",
        "macro_kpi":    kpi_items,
        "macro_groups": groups,
        "macro_count":  indicator_count,
        "macro_updated": payload.get("last_updated", ""),
        "macro_is_mock": bool(payload.get("is_mock")),
        # Indicators tab — featured charts + group filter:
        "macro_featured":  _indicator_featured_charts(indicators_by_id),
        "macro_ind_group": ind_group,
        # Tab payloads:
        "yields_curve":     _yields_curve_payload(yield_curve),
        "yields_spreads":   _yields_spreads(yield_curve),
        "yields_debt":      _debt_panel_payload(debt_payload),
        "fx_rows":          _fx_panel_rows(fx_payload),
        "fx_charts":        _fx_chart_grid(fx_payload),
        "fx_table":         _fx_rates_table(fx_payload),
        "commodity_rows":   _commodities_panel_rows(idx_payload),
        "macro_calendar":   calendar_rows,
        # heatmap_companies / heatmap_sectors / heatmap_indices are
        # fetched via /api/macro/heatmap on tab activation — no need
        # to pass them through the main route context.
        "macro_tab":        active_tab,
        "events":           events_payload,
        "ev_view":          ev_view,
        # Lazy-loaded tab params — pass through so each tab's
        # data-pane-fetch URL carries the user's sub-selection.
        "vol_pc_type":      pc_type,
        "earn_index":       earn_index,
        "earn_quarter":     earn_quarter,
        "earn_sector":      earn_sector,
        "earn_view":        earn_view,
        "cal_index":        cal_index,
        "cal_period":       cal_period,
        "perf_index":       perf_index,
        "perf_period":      perf_period,
        "sentiment":        sentiment_payload,
    }
    return templates.TemplateResponse("_redesign/macro.html", ctx)


@router.get("/api/macro/volatility", response_class=HTMLResponse)
async def preview_macro_volatility_partial(
    request: Request, pc_type: str = "total",
):
    """Lazy-loaded Volatility pane — Put/Call ratio + VIX term + SKEW.

    Default ``pc_type=total`` reads from warmer-managed L2 + LKG (sub-
    500 ms typical, real data even during yfinance degradation).
    Non-default pc_type (index / equity, rare) pays a cold fetch
    through bounded() with the 12 s ceiling.
    """
    if pc_type == "total":
        volatility_payload = await _warmer.read_via_l2("redesign:macro:volatility")
        volatility_payload = volatility_payload or _v2_volatility_empty()
    else:
        volatility_payload = await _bounded_call(
            _v2_volatility_payload(pc_type),
            timeout=12.0, fallback=_v2_volatility_empty, name="volatility",
        )
    return templates.TemplateResponse(
        "_redesign/partials/macro_volatility.html",
        {"request": request, "vol": volatility_payload},
    )


@router.get("/api/macro/earnings", response_class=HTMLResponse)
async def preview_macro_earnings_partial(
    request: Request,
    earn_index: str = "all",
    earn_quarter: str = "",
    earn_sector: str = "",
    earn_view: str = "scorecard",
    cal_index: str = "all",
    cal_period: str = "this_week",
):
    """Lazy-loaded Earnings pane — Scorecard + Calendar sub-tabs.

    Scorecard default (all / current quarter / all sectors) reads
    warmer-managed L2 + LKG.  Non-default Scorecard params + the
    Calendar sub-tab pay cold-fetch (Calendar isn't warmable — needs
    request.app.state for fund_cache).
    """
    is_default_scorecard = (
        earn_index == "all" and not earn_quarter and not earn_sector
    )
    if is_default_scorecard:
        earnings_task = _warmer.read_via_l2("redesign:macro:earnings_default")
    else:
        earnings_task = _bounded_call(
            _v2_macro_earnings_payload(earn_index, earn_quarter or None, earn_sector or None),
            timeout=10.0, fallback=_v2_macro_earnings_empty, name="earnings",
        )
    earnings_payload, calendar_payload = await asyncio.gather(
        earnings_task,
        _bounded_call(
            _v2_macro_calendar_payload(request, cal_index, cal_period),
            timeout=8.0,  fallback=_v2_macro_calendar_empty, name="ecal",
        ),
    )
    earnings_payload = earnings_payload or _v2_macro_earnings_empty()
    earn_view_norm = "calendar" if (earn_view or "").lower() == "calendar" else "scorecard"
    return templates.TemplateResponse(
        "_redesign/partials/macro_earnings.html",
        {
            "request":   request,
            "earn":      earnings_payload,
            "ecal":      calendar_payload,
            "earn_view": earn_view_norm,
        },
    )


@router.get("/api/macro/performance", response_class=HTMLResponse)
async def preview_macro_performance_partial(
    request: Request,
    perf_index: str = "sp500",
    perf_period: str = "1d",
):
    """Lazy-loaded Performance pane — market breadth.

    Default (sp500 / 1d) reads warmer-managed L2 + LKG.  Other
    index/period combos pay cold-fetch.
    """
    if perf_index == "sp500" and perf_period == "1d":
        performance_payload = await _warmer.read_via_l2("redesign:macro:performance_default")
        performance_payload = performance_payload or _v2_macro_performance_empty()
    else:
        performance_payload = await _bounded_call(
            _v2_macro_performance_payload(perf_index, perf_period),
            timeout=10.0, fallback=_v2_macro_performance_empty, name="performance",
        )
    return templates.TemplateResponse(
        "_redesign/partials/macro_performance.html",
        {"request": request, "perf": performance_payload},
    )


@router.get("/api/macro/heatmap", response_class=HTMLResponse)
async def preview_macro_heatmap_partial(request: Request):
    """Lazy-loaded Heatmap pane — Companies grid + Sectors grid +
    Global regions table.

    Reads warmer-managed L2 + LKG.  The warmer's compute fn fans out
    the three sub-fetches together so they share one TTL window.
    """
    bundle = await _warmer.read_via_l2("redesign:macro:heatmap")
    bundle = bundle or {}
    return templates.TemplateResponse(
        "_redesign/partials/macro_heatmap.html",
        {
            "request":           request,
            "heatmap_companies": bundle.get("heatmap_companies", []),
            "heatmap_sectors":   bundle.get("heatmap_sectors", []),
            "heatmap_indices":   bundle.get("heatmap_indices", []),
        },
    )
