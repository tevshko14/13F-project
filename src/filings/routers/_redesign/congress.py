"""Congress page (v2 redesign).

One route -- ``/congress`` -- with three tabs (Congress / Holdings /
Activity), wired through the v1 ``congress_trading`` module so we get
the same chamber visualisations, trade-frequency leaderboard, and
net-worth leaderboard v1 already proved out.

Activity tab honours ``?timeframe=&chamber=&party=`` query params.
Filter state is server-rendered (links carry the active values) -- no
client JS state to keep in sync.

This module dropped ~400 LOC of dead helpers (``_build_member_aggregates``,
``_members_panel_data``, ``_leaderboard_panel_data``, ``_performance_party_breakdown``,
``_performance_sectors``, ``_calendar_panel_data``, ``_ticker_hints_for_event``,
``_index_member_meta``, ``_party_letter``, ``_trade_in_window``,
``_CALENDAR_TICKER_HINTS``) from the prior architecture iteration that
ended up using v1's ``_ct.prepare_*`` helpers instead.
"""

from __future__ import annotations

import asyncio
import bisect
import functools
import logging
import math
from datetime import datetime

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from filings.app_state import templates
from filings.concurrency import to_heavy, to_light, to_supabase
from filings.routers._redesign.helpers import (
    _bounded,
    _congress_action,
    _format_compact_dollars,
    _nice_axis_step,
    _shell_context,
)

logger = logging.getLogger(__name__)

router = APIRouter()


# ── Trade-row helpers ─────────────────────────────────────────────────


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
        return d.strftime("%b %d %Y")
    except Exception:
        return iso_str[:10]


def _amount_midpoint(low, high) -> float:
    """Midpoint of an OGE amount-range disclosure (low/high are both in $)."""
    try:
        if low is None and high is None:
            return 0.0
        if low is None:
            return float(high)
        if high is None:
            return float(low)
        return (float(low) + float(high)) / 2.0
    except (TypeError, ValueError):
        return 0.0


def _format_signed_compact_dollars(v: float | int | None) -> str:
    """Compact dollar formatter that preserves sign — used for cumulative
    flow series where negative values are meaningful (cum sells > buys)."""
    if v is None:
        return "—"
    sign = "+" if v >= 0 else "−"
    a = abs(float(v))
    if a >= 1e9: return f"{sign}${a / 1e9:.1f}B"
    if a >= 1e6: return f"{sign}${a / 1e6:.1f}M"
    if a >= 1e3: return f"{sign}${a / 1e3:.0f}K"
    return f"{sign}${a:,.0f}"


# ── Data fetchers ─────────────────────────────────────────────────────


async def _fetch_congress_data() -> dict:
    """Read recent congressional trades from Supabase cache."""
    try:
        from filings import supabase_cache
        rows = await to_supabase(supabase_cache.get_congress_recent_trades, 60)
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


async def _fetch_congress_wider_window(months: int = 6, limit: int = 5000) -> list[dict]:
    """Slim trade list from the last N months — used for KPI aggregates,
    Members, Leaderboard, Performance, sectors.

    Supabase-direct: routes through ``to_supabase`` so a slow heavy pool
    can't queue this.
    """
    try:
        from filings import supabase_cache
        rows = await to_supabase(
            supabase_cache.get_congress_trades_recent_months, months, limit,
        )
    except Exception as exc:
        logger.warning("Congress wider-window fetch failed: %s", exc)
        return []
    return rows or []


async def _fetch_congress_members() -> list[dict]:
    """Member profile rows — name/party/chamber/state/net_worth."""
    try:
        from filings import supabase_cache
        rows = await to_supabase(supabase_cache.get_all_congress_members)
    except Exception as exc:
        logger.warning("Congress members fetch failed: %s", exc)
        return []
    return rows or []


# ── Page-level helpers ────────────────────────────────────────────────


def _congress_kpi_strip(stats: dict) -> list[dict]:
    """KPI strip — six headline stats consumed by the standard `kpi_strip`
    macro (matches v1's stats banner).  Source: ``stats`` produced by
    :func:`congress_trading.prepare_congress_page_data`.
    """
    start = (stats.get("date_range_start") or "")[:4]
    end   = (stats.get("date_range_end") or "")[:4]
    date_range = f"{start}–{end}" if start and end else "—"
    return [
        {"label": "Politicians", "value": f"{stats.get('total_members', 0):,}",  "delta": None, "up": None},
        {"label": "Trades",      "value": f"{stats.get('total_trades', 0):,}",   "delta": None, "up": None},
        {"label": "Stocks",      "value": f"{stats.get('unique_tickers', 0):,}", "delta": None, "up": None},
        {"label": "House",       "value": str(stats.get("house_count", 0)),      "delta": None, "up": None},
        {"label": "Senate",      "value": str(stats.get("senate_count", 0)),     "delta": None, "up": None},
        {"label": "Date range",  "value": date_range,                            "delta": None, "up": None},
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


# ── Performance tab — Congress vs SPY ─────────────────────────────────


async def _performance_index_chart(wider_rows: list[dict]) -> dict:
    """Build a Congress index vs SPY chart payload from the last 6 months.

    Congress series = cumulative net buy/sell weighted by amount midpoint
    (a directional volume curve, not a return); the design spec calls for
    a relative line shape, which this approximates without a price-join.
    SPY series uses the real ^GSPC daily close pulled from market_data.
    """
    try:
        from filings import market_data
        idx = await to_heavy(market_data.get_index_market_data)
    except Exception as exc:
        logger.warning("Performance: SPY fetch failed: %s", exc)
        idx = {}

    spy = idx.get("^GSPC") if isinstance(idx, dict) else None
    spy_history = (spy or {}).get("history") or []   # [[epoch_ms, close], ...]
    spy_pts: list[tuple[float, float]] = []          # (epoch_ms, close)
    if spy_history:
        spy_pts = [(p[0], p[1]) for p in spy_history if isinstance(p, (list, tuple)) and len(p) >= 2]

    # Congress directional flow: walk trades sorted oldest→newest, accumulate
    # signed net volume (BUY = +, SELL = −) so the series traces sentiment.
    sorted_trades = sorted(
        [t for t in wider_rows if t.get("trade_date")],
        key=lambda t: t.get("trade_date", "")[:10],
    )
    cong_pts: list[tuple[float, float]] = []
    cum = 0.0
    for t in sorted_trades:
        ttype = (t.get("trade_type") or "").lower()
        sign = 1 if ttype in ("buy", "purchase") else (-1 if ttype in ("sell", "sale") else 0)
        if sign == 0:
            continue
        v = _amount_midpoint(t.get("amount_low"), t.get("amount_high"))
        cum += sign * v
        try:
            d = datetime.fromisoformat(t["trade_date"][:10])
            epoch_ms = d.timestamp() * 1000
            cong_pts.append((epoch_ms, cum))
        except Exception:
            continue

    # Trim SPY history to roughly the same window as the trade data.
    if cong_pts and spy_pts:
        start = cong_pts[0][0]
        spy_pts = [p for p in spy_pts if p[0] >= start]

    def _series_to_path(pts: list[tuple[float, float]], width: float, height: float, pad_top: float = 12.0):
        """Returns (line_d, fill_d, screen_pts) where screen_pts is the same
        list re-projected into viewBox coordinates so the JS hover layer can
        snap to it."""
        if not pts or len(pts) < 2:
            return "", "", []
        xs = [p[0] for p in pts]
        ys = [p[1] for p in pts]
        x_min, x_max = min(xs), max(xs)
        y_min, y_max = min(ys), max(ys)
        xr = x_max - x_min if x_max > x_min else 1
        yr = y_max - y_min if y_max > y_min else 1
        plot_h = height - pad_top - 4
        out: list[str] = []
        screen: list[tuple[float, float, float, float]] = []
        for i, (x, y) in enumerate(pts):
            sx = (x - x_min) / xr * width
            sy = pad_top + (1 - (y - y_min) / yr) * plot_h
            out.append(("M" if i == 0 else "L") + f"{sx:.1f} {sy:.1f}")
            screen.append((round(sx, 1), round(sy, 1), x, y))
        line = " ".join(out)
        fill = f"{line} L {width:.1f} {height:.1f} L 0 {height:.1f} Z"
        return line, fill, screen

    # ViewBox 1500×220 — matches the typical full-width container ratio
    # so `preserveAspectRatio="none"` doesn't horizontally stretch slopes.
    width, height = 1500.0, 220.0
    cong_line, cong_fill, cong_screen = _series_to_path(cong_pts, width, height)
    spy_line, _spy_fill, spy_screen   = _series_to_path(spy_pts, width, height)

    # Build hover history aligned to the cong series (the user-relevant one);
    # for each cong point, look up the SPY value at the nearest timestamp.
    chart_history: list[dict] = []
    if cong_screen and spy_screen:
        # Index spy by timestamp for nearest-lookup.
        spy_by_ts = sorted(spy_screen, key=lambda p: p[2])
        spy_ts    = [p[2] for p in spy_by_ts]

        def _nearest_spy(ts: float):
            i = bisect.bisect_left(spy_ts, ts)
            if i <= 0:                 return spy_by_ts[0]
            if i >= len(spy_by_ts):    return spy_by_ts[-1]
            a, b = spy_by_ts[i - 1], spy_by_ts[i]
            return a if abs(a[2] - ts) <= abs(b[2] - ts) else b

        # SPY uses % return (real prices); Congress uses raw cumulative
        # dollar flow (cum starts at 0 so a % change off the first move is
        # nonsensical).
        spy_base  = spy_screen[0][3]  if spy_screen[0][3]  else 0.0

        for sx, sy, ts, cong_v in cong_screen:
            spy_pt = _nearest_spy(ts)
            spy_pct = ((spy_pt[3] - spy_base) / spy_base * 100) if spy_base else 0.0
            try:
                date_str = datetime.fromtimestamp(ts / 1000).strftime("%b %d %Y")
            except Exception:
                date_str = ""
            chart_history.append({
                "x":        sx,
                "cong_y":   sy,
                "spy_y":    spy_pt[1],
                "date":     date_str,
                "cong_str": _format_signed_compact_dollars(cong_v),
                "spy_str":  f"{spy_pct:+.1f}%",
            })

    # SPY YTD baseline percent.
    spy_ytd_pct: float | None = None
    if spy_history:
        # Find first close on/after Jan 1 of the current year.
        try:
            year = datetime.now().year
            ytd_target = datetime(year, 1, 1).timestamp() * 1000
            ytd_pt = next((p for p in spy_history if p[0] >= ytd_target), None)
            last_pt = spy_history[-1]
            if ytd_pt and last_pt and ytd_pt[1] > 0:
                spy_ytd_pct = (last_pt[1] - ytd_pt[1]) / ytd_pt[1] * 100
        except Exception:
            pass

    # Congress YTD net flow (sign of cumulative buy bias, ytd window).
    ytd_str = f"{datetime.now().year}-01-01"
    ytd_buys = sum(
        _amount_midpoint(t.get("amount_low"), t.get("amount_high"))
        for t in wider_rows
        if (t.get("trade_type") or "").lower() in ("buy", "purchase")
        and (t.get("trade_date") or "") >= ytd_str
    )
    ytd_sells = sum(
        _amount_midpoint(t.get("amount_low"), t.get("amount_high"))
        for t in wider_rows
        if (t.get("trade_type") or "").lower() in ("sell", "sale")
        and (t.get("trade_date") or "") >= ytd_str
    )
    cong_buy_bias_pct: float | None = None
    if (ytd_buys + ytd_sells) > 0:
        cong_buy_bias_pct = (ytd_buys - ytd_sells) / (ytd_buys + ytd_sells) * 100

    return {
        "have_data": bool(cong_line and spy_line),
        "cong_line": cong_line,
        "cong_fill": cong_fill,
        "spy_line":  spy_line,
        "vb_width":  width,
        "vb_height": height,
        "chart_history": chart_history,
        "ytd_buys": _format_compact_dollars(ytd_buys),
        "ytd_sells": _format_compact_dollars(ytd_sells),
        "cong_buy_bias_str": f"{cong_buy_bias_pct:+.0f}%" if cong_buy_bias_pct is not None else "—",
        "cong_buy_bias_up":  (cong_buy_bias_pct >= 0) if cong_buy_bias_pct is not None else None,
        "spy_ytd_str":  f"{spy_ytd_pct:+.1f}%" if spy_ytd_pct is not None else "—",
        "spy_ytd_up":   (spy_ytd_pct >= 0) if spy_ytd_pct is not None else None,
    }


# ── Holdings tab — chart geometry helpers ────────────────────────────


def _congress_vbar_geometry(
    items: list[dict],
    value_key: str,
    *,
    vb_w: float = 1500.0,
    vb_h: float = 260.0,
    pad_top: float = 18.0,
    pad_bot: float = 30.0,
    pad_left: float = 64.0,
    bar_gap: float = 12.0,
    signed: bool = False,
) -> dict:
    """Vertical bar-chart geometry for Trending / Recent Momentum.

    `signed=False` (Trending): all bars grow up from the x-axis; y-range is
    [0, max].  `signed=True` (Momentum): bars grow up for positive values
    and down for negative ones; y-range is [-max_abs, +max_abs] with a
    zero line at the midpoint.

    Each item must have `value_key` populated.  Returns SVG-ready bar
    payload + grid lines + y-axis labels.
    """
    if not items:
        return {"have_data": False, "bars": [], "grid_ys": [], "y_labels": []}

    plot_w = vb_w - pad_left
    plot_h = vb_h - pad_top - pad_bot
    n = len(items)
    bar_w = max(8.0, (plot_w - bar_gap * (n + 1)) / n)

    values = [it.get(value_key, 0) or 0 for it in items]
    if signed:
        max_abs = max((abs(v) for v in values), default=1) or 1
        step = _nice_axis_step(max_abs * 2, target_steps=4)
        rng_top = math.ceil(max_abs / step) * step
        rng_bot = -rng_top
    else:
        max_v = max(values, default=1) or 1
        step = _nice_axis_step(max_v, target_steps=4)
        rng_top = math.ceil(max_v / step) * step
        rng_bot = 0

    def _y_for(v: float) -> float:
        return pad_top + (1.0 - (v - rng_bot) / (rng_top - rng_bot)) * plot_h

    zero_y = _y_for(0)

    bars: list[dict] = []
    for i, it in enumerate(items):
        v = it.get(value_key, 0) or 0
        x = pad_left + bar_gap + i * (bar_w + bar_gap)
        if v >= 0:
            y_top, y_bot = _y_for(v), zero_y
        else:
            y_top, y_bot = zero_y, _y_for(v)
        bars.append({
            **it,
            "x":      round(x, 1),
            "y":      round(y_top, 1),
            "w":      round(bar_w, 1),
            "h":      round(max(y_bot - y_top, 1.0), 1),
            "label_x": round(x + bar_w / 2, 1),
            "is_pos": v >= 0,
        })

    y_labels: list[dict] = []
    grid_ys: list[float] = []
    v = rng_bot
    while v <= rng_top + step / 2:
        y_pos = round(_y_for(v), 1)
        y_labels.append({"label": f"{int(v):,}" if v == int(v) else f"{v:.1f}", "y": y_pos})
        grid_ys.append(y_pos)
        v += step

    return {
        "have_data": True,
        "bars":      bars,
        "y_labels":  y_labels,
        "grid_ys":   grid_ys,
        "vb_width":  vb_w,
        "vb_height": vb_h,
        "zero_y":    round(zero_y, 1),
        "left_pad":  pad_left,
        "signed":    signed,
    }


def _congress_trending_chart(trending: list[dict], top_n: int = 15) -> dict:
    """Bar chart of top-N stocks by unique buyer count (last 6 months).

    Wraps :func:`_congress_vbar_geometry` and forwards every item field
    (ticker, name, add_count, democrat, republican, top_traders) through
    to the bar so the JS hover handler can pull rich data straight from
    the rendered DOM.
    """
    return _congress_vbar_geometry(trending[:top_n], "add_count", vb_h=260.0)


def _congress_momentum_chart(momentum: list[dict], top_n: int = 15) -> dict:
    """Bar chart of recent-momentum tickers (top-N by net = buys - sells).

    Mirrors the v1 chart: positive bars grow up (green), negative bars
    grow down (red), anchored to a zero line.  Hover surfaces buys / sells /
    net / active traders.
    """
    return _congress_vbar_geometry(momentum[:top_n], "net", vb_h=280.0, signed=True)


# ── Activity tab filter constraints ──────────────────────────────────


_CONGRESS_ACT_TIMEFRAMES = ("1W", "1M", "3M", "ALL")
_CONGRESS_ACT_CHAMBERS   = ("all", "house", "senate")
_CONGRESS_ACT_PARTIES    = ("all", "democrat", "republican")


# ── Route ─────────────────────────────────────────────────────────────


@router.get("/congress", response_class=HTMLResponse)
async def preview_congress(
    request: Request,
    view:      str = "congress",
    timeframe: str = "ALL",
    chamber:   str = "all",
    party:     str = "all",
):
    """Congress page — three tabs (Congress / Holdings / Activity), wired
    through the v1 ``congress_trading`` module so we get the same chamber
    visualisations, trade-frequency leaderboard, and net-worth leaderboard
    that v1 already proved out.

    Activity tab honours `?timeframe=&chamber=&party=` query params.  Filter
    state is server-rendered (links carry the active values) — no client JS
    state to keep in sync.
    """
    if view not in ("congress", "holdings", "activity"):
        view = "congress"
    if timeframe not in _CONGRESS_ACT_TIMEFRAMES: timeframe = "ALL"
    if chamber.lower() not in _CONGRESS_ACT_CHAMBERS: chamber = "all"
    if party.lower() not in _CONGRESS_ACT_PARTIES:     party   = "all"

    bounded = functools.partial(_bounded, page="Congress page")

    payload, wider_rows, members = await asyncio.gather(
        bounded(_fetch_congress_data(),                timeout=4.0, fallback={"rows": [], "is_mock": False}, name="recent_trades"),
        bounded(_fetch_congress_wider_window(6, 5000), timeout=6.0, fallback=[],                              name="wider_trades"),
        bounded(_fetch_congress_members(),             timeout=4.0, fallback=[],                              name="members"),
    )
    rows = payload.get("rows") or []

    # v1 page-data orchestrator gives us chamber_viz / trade_frequency /
    # net_worth_leaderboard / stats / trending / consensus / momentum /
    # activity in one call.  We re-run consensus with a deeper top_n for
    # the "All Congressional Holdings" table (page_data caps it at 10) and
    # re-run activity with the user-selected filter set.
    from filings import congress_trading as _ct
    page_data, holdings_table, activity = await asyncio.gather(
        to_light(_ct.prepare_congress_page_data,
                 members or [], wider_rows or [], wider_rows or []),
        to_light(_ct.prepare_congress_consensus,
                 wider_rows or [], members or [], 20),
        to_light(_ct.prepare_congress_activity,
                 wider_rows or [], timeframe, chamber, party, 200),
    )

    # Pre-format dollar amounts for the Activity tab — Jinja templates don't
    # have access to our Python helpers as filters, so we hand them strings
    # ready to render.  Keeps the template free of formatting math.
    _astats = activity.get("stats") or {}
    activity["stats_fmt"] = {
        "net_dollar_flow": _format_signed_compact_dollars(_astats.get("net_dollar_flow", 0)),
        "buy_value":       _format_compact_dollars(_astats.get("total_buy_value", 0)),
        "sell_value":      _format_compact_dollars(_astats.get("total_sell_value", 0)),
    }
    for c in activity.get("clusters") or []:
        c["net_flow_fmt"] = _format_signed_compact_dollars(c.get("net_flow", 0))

    perf_chart = await bounded(
        _performance_index_chart(wider_rows or []),
        timeout=5.0,
        fallback={"have_data": False},
        name="perf_chart",
    )

    ctx = {
        "request":         request,
        **(await _shell_context(request, "Congress")),
        "congress_view":   view,
        "congress_kpi":    _congress_kpi_strip(page_data.get("stats", {})),
        "congress_rows":   rows,
        "congress_notable": _congress_notable(rows),
        "congress_total":  f"{len(wider_rows):,}" if wider_rows else "—",
        "congress_is_mock": payload.get("is_mock", False),
        # v1-derived payloads — chamber_viz drives the House/Senate dot
        # grids; trade_frequency + net_worth_leaderboard back the chart
        # toggle on the Congress tab.
        "chamber_viz":     page_data.get("chamber_viz", {}),
        "trade_frequency": page_data.get("trade_frequency", []),
        "net_worth_leaderboard": page_data.get("net_worth_leaderboard", []),
        "congress_stats":  page_data.get("stats", {}),
        # Holdings tab payloads — raw lists + chart geometry derived from them.
        "trending":         page_data.get("trending", []),
        "consensus":        page_data.get("consensus", []),
        "momentum":         page_data.get("momentum", []),
        "trending_chart":   _congress_trending_chart(page_data.get("trending", [])),
        "momentum_chart":   _congress_momentum_chart(page_data.get("momentum", [])),
        "holdings_table":   holdings_table,
        # Activity tab — filtered server-side, with active filter state echoed
        # back so the pill links can render their is-active class.
        "activity":          activity,
        "activity_timeframe": timeframe,
        "activity_chamber":   chamber,
        "activity_party":     party,
        # Performance chart (kept — used elsewhere)
        "perf_chart":      perf_chart,
    }
    return templates.TemplateResponse("_redesign/congress.html", ctx)
