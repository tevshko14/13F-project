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
from datetime import datetime, timedelta, timezone
from typing import Any, Callable
from zoneinfo import ZoneInfo

# Module-import timestamp — stable across all renders within one
# deploy, but unique per deploy so CSS changes don't get served from
# stale browser caches after a release.
_ASSET_VERSION = int(time.time())

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from filings import supabase_cache
from filings import stock_bundle
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


def is_placeholders_enabled() -> bool:
    """Whether placeholder pages (screener, options) are mounted.

    Set ``PP_PLACEHOLDERS=1`` in local dev to browse them; leave unset
    in production so the v2 placeholder routes are NEVER registered and
    the v1 handlers (real features) serve those URLs instead.
    """
    return os.environ.get("PP_PLACEHOLDERS", "").lower() in ("1", "true", "yes")


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


router = APIRouter(prefix="", tags=["redesign"])


def _request_fund_cache(request: Request) -> dict:
    """Read ``app.state.fund_cache``, defaulting to an empty dict.

    Centralised so the same ``getattr(request.app.state, ...) or {}``
    dance isn't repeated across handlers (it appears 7+ times in this
    file pre-helper).  Returns the live cache dict by reference --
    callers must not mutate.
    """
    return getattr(request.app.state, "fund_cache", {}) or {}


# Strong-reference set for fire-and-forget background tasks owned by
# this router.  Tasks add themselves via :func:`_track_bg` and remove
# themselves on completion; without this they'd be GC'd mid-flight by
# asyncio (which only weakly tracks pending tasks).
_bg_tasks: set[asyncio.Task] = set()


def _track_bg(coro, *, name: str) -> asyncio.Task:
    """Spawn *coro* as a fire-and-forget task with a strong reference.

    Used for "do this side-effect, don't make the user wait" work --
    e.g. populating the stock-bundle cache after a request-path miss.
    Exceptions are logged via the coroutine itself; this wrapper just
    keeps the task alive until completion.
    """
    task = asyncio.create_task(coro, name=name)
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)
    return task


# ─────────────────────────────────────────────────────────────────────────────
# Mock data — mirrors design_handoff_paperpanda/data.js so visual pages
# match the design canvas pixel-for-pixel.
# These get replaced with live data at the route-flip step.
# ─────────────────────────────────────────────────────────────────────────────


def _today_label() -> str:
    """Topbar kicker — canonical 'MAY 06 2026' (uppercase MMM DD YYYY)."""
    now = datetime.now(ZoneInfo("America/New_York"))
    return now.strftime("%b %d %Y").upper()


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


_SHELL_NOTIF_WINDOW_HOURS   = 24                # fallback "fresh" window for first-time visitors
_SHELL_NOTIF_BADGE_CAP      = 99                # avoid 4-digit badge overflow
_SHELL_NOTIF_COOKIE         = "pp-notif-seen"   # per-browser "last viewed notifications" timestamp
_SHELL_NOTIF_COOKIE_MAX_AGE = 60 * 60 * 24 * 90 # 90 days
_SHELL_PANDA_GOAL_CENTS     = 20_000            # $200/month — same goal as v1 widget


def _initials_from_name(name: str) -> str:
    """Two-letter initials from a display name; empty when unavailable."""
    if not name:
        return ""
    parts = [p for p in re.split(r"\s+", name.strip()) if p]
    if not parts:
        return ""
    if len(parts) == 1:
        return parts[0][:2].upper()
    return (parts[0][:1] + parts[-1][:1]).upper()


async def _shell_context(request: Request, active: str) -> dict:
    """Common context every redesign page needs for the app shell.

    Live values (replacing prior hardcodes):
      - ``notif_unread`` — count of notifications in the last 24h.  ``0`` when
        empty so the badge collapses (template already conditional on truthy).
      - ``panda_raised/goal/pct/month`` — real Stripe donation totals via
        ``supabase_cache.get_monthly_raised_cents``.  Goal stays a config
        knob (`_SHELL_PANDA_GOAL_CENTS`).
      - ``user_initials`` — derived from the signed-in user's profile
        display name; empty for guests (template falls to the "PP" default).

    Per-source try/excepts so a Supabase blip can't 500 every page render.
    """
    today = datetime.now()

    # Notifications — count notifications created since the user last
    # visited /_v2/notifications.  We persist that visit timestamp in the
    # `pp-notif-seen` cookie so the badge accumulates across pages and
    # collapses to 0 right after a visit.  First-time visitors (no cookie)
    # fall back to a 24h "fresh" window so they see something on day one.
    seen_iso = ""
    try:
        seen_iso = (request.cookies.get(_SHELL_NOTIF_COOKIE) or "").strip()
        # Validate: parsing it must succeed.  A bad/expired value falls
        # through to the 24h window so the badge never silently sticks.
        if seen_iso:
            datetime.fromisoformat(seen_iso.replace("Z", "+00:00"))
    except Exception:
        seen_iso = ""
    if not seen_iso:
        seen_iso = (datetime.now(timezone.utc)
                    - timedelta(hours=_SHELL_NOTIF_WINDOW_HOURS)).isoformat()

    notif_unread: int | str = 0
    try:
        from filings import supabase_cache
        count, _latest = await to_light(supabase_cache.get_bell_state, seen_iso)
        if count > _SHELL_NOTIF_BADGE_CAP:
            notif_unread = f"{_SHELL_NOTIF_BADGE_CAP}+"
        elif count > 0:
            notif_unread = count
    except Exception as exc:
        logger.debug("shell: bell state failed: %s", exc)

    # Panda Fund — Stripe-backed monthly donation total.  Falls back to 0/0
    # cleanly when the row is missing so the widget shows "$0 / $200 · May".
    panda_raised, panda_goal, panda_pct = 0, _SHELL_PANDA_GOAL_CENTS // 100, 0
    try:
        from filings import supabase_cache
        cents = await to_light(supabase_cache.get_monthly_raised_cents,
                               today.strftime("%Y-%m"))
        if cents and cents > 0:
            panda_raised = min(cents // 100, panda_goal)
            panda_pct    = min(100, round(panda_raised / panda_goal * 100))
    except Exception as exc:
        logger.debug("shell: panda fund failed: %s", exc)

    # Avatar initials — only when signed in.  Profile carries display_name;
    # fall back to email's local-part initial; otherwise empty so the
    # template's `default("PP")` kicks in.
    user_initials = ""
    profile = getattr(request.state, "profile", None) if hasattr(request, "state") else None
    if isinstance(profile, dict):
        user_initials = _initials_from_name(profile.get("display_name") or "")
        if not user_initials and profile.get("email"):
            user_initials = (profile["email"][:1] or "").upper()

    return {
        "nav_active":     active,
        "today_label":    _today_label(),
        "market_status":  _market_status(),
        "panda_raised":   panda_raised,
        "panda_goal":     panda_goal,
        "panda_month":    today.strftime("%b"),
        "panda_pct":      panda_pct,
        "user_initials":  user_initials,
        "notif_unread":   notif_unread,
        "asset_version":  _ASSET_VERSION,
        "is_authed":      isinstance(profile, dict) and bool(profile),
        # Local-only flag: drives whether placeholder pages (Screener,
        # Options) show up in the sidenav.  Set ``PP_PLACEHOLDERS=1`` in
        # local dev; unset in production.
        "show_placeholder_nav": is_placeholders_enabled(),
    }


async def _bounded(coro, *, timeout: float, fallback, name: str, page: str = "page"):
    """Wrap an awaitable so a slow upstream can't stall the whole render.

    `page` shows up in the warning log so the timing-out source is easy to
    spot when several routes share the same upstream.
    """
    try:
        return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError:
        logger.warning("%s: %s timed out (>%ss)", page, name, timeout)
        return fallback
    except Exception as exc:
        logger.warning("%s: %s failed: %s", page, name, exc)
        return fallback


def _short_date(iso: str) -> str:
    """Repeated 'YYYY-MM-DD…' → 'Mon DD YYYY' formatter used by every
    calendar parser and the activity feed.

    Returns the canonical product date format (MMM DD YYYY).  Year-less
    "MMM DD" callers should switch to ``filings.dates_format.format_date_short``.
    """
    from filings.dates_format import format_date
    return format_date(iso, fallback=iso[:10] if iso else "")


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
    # Heatmap, Activity, Calendar panes are lazy-loaded on tab click via
    # `/api/home/{heatmap,activity,calendar}` (see partial handlers
    # below).  Skipping their 5 fetchers here saves ~5 thread slots and
    # ~95KB of HTML on the default Overview landing.
    (
        kpi_items, hero,
        top_movers_rows, fund_flows_full, insider_rows, congress_full,
        macro_rows, retail_payload, feargreed_payload, ticker_tape_rows,
        flow_trending_payload,
        news_payload,
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
        _fetch_home_news(idx_data=idx_market_map, sp_data=sp_1d_map),
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

# Manager metadata for the page hero — keyed by CIK.  Auto-derived from
# the canonical superinvestor list with a small per-CIK overlay for cities
# (city/HQ data isn't carried in superinvestors.py yet — manual overrides
# for the most-visited funds; everything else falls back to the fund_name).
_FUND_CITY_OVERRIDES: dict[str, str] = {
    "1067983": "Omaha, NE",            # Berkshire
    "1336528": "New York, NY",         # Pershing Square
    "1649339": "Saratoga, CA",         # Scion
    "1079114": "New York, NY",         # Greenlight
    "1040273": "New York, NY",         # Third Point
    "1656456": "Miami Beach, FL",      # Appaloosa (David Tepper)
    "1061768": "Boston, MA",           # Baupost
    "1135730": "New York, NY",         # Coatue
    "1423053": "Miami, FL",            # Citadel
    "1167483": "New York, NY",         # Tiger Global
    "1647251": "London, UK",           # TCI
    "1166559": "Seattle, WA",          # Gates Foundation
    "1418814": "San Francisco, CA",    # ValueAct
    "1037389": "East Setauket, NY",    # Renaissance
    "949509":  "Los Angeles, CA",      # Oaktree
    "1350694": "Westport, CT",         # Bridgewater
    "1061165": "Greenwich, CT",        # Lone Pine
    "1103804": "Greenwich, CT",        # Viking
    "934639":  "Dallas, TX",           # Maverick
    "1345471": "New York, NY",         # Trian
    "1358706": "Boston, MA",           # Abrams Capital
    "921669":  "New York, NY",         # Icahn
    "200217":  "San Francisco, CA",    # Dodge & Cox
    "1536411": "New York, NY",         # Duquesne
}


def _icon_from_fund_name(fund_name: str) -> str:
    """Derive a 3-letter chrome icon from a fund name.

    "Berkshire Hathaway" → "BRK"
    "Pershing Square"    → "PSC"
    "Scion Asset Mgmt"   → "SCN"
    Falls back to first 3 alphas if no decent split.
    """
    if not fund_name:
        return "FND"
    words = [w for w in re.split(r"[^A-Za-z]+", fund_name) if w]
    if not words:
        return "FND"
    if len(words) == 1:
        return words[0][:3].upper()
    # Take first letter of first word + 2 from second-most-distinctive word
    first = words[0]
    second = words[1] if len(words) > 1 else ""
    candidate = (first[:1] + second[:2]).upper() if second else first[:3].upper()
    return candidate or "FND"


def _build_fund_meta() -> dict[str, dict]:
    """Auto-derive page-hero metadata for every superinvestor on file.

    Reads from filings.superinvestors.SUPERINVESTORS_BY_CIK so the lookup
    automatically grows with the master list.
    """
    try:
        from filings.superinvestors import SUPERINVESTORS_BY_CIK
    except Exception:
        return {}
    meta: dict[str, dict] = {}
    for cik, info in SUPERINVESTORS_BY_CIK.items():
        meta[cik] = {
            "manager": info.display_name or info.fund_name or "",
            "city":    _FUND_CITY_OVERRIDES.get(cik, ""),
            "icon":    _icon_from_fund_name(info.fund_name or info.display_name),
        }
    return meta


_FUND_META: dict[str, dict] = _build_fund_meta()


def _format_dollars(v: float | int | None, *, full=False) -> str:
    """Dollar formatter — compact ($312.4B / $11.4B / $83M) by default,
    or fully unabbreviated comma-separated ($312,400,000,000) when
    ``full=True``.  Used in places (the All Holdings table) where the
    user expects to see exact dollar values, not the rounded compact form.
    """
    if v is None:
        return "—"
    v = float(v)
    if full:
        return f"${v:,.0f}"
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


def _funds_kpi_strip(fund: dict, history: list[dict] | None = None) -> list[dict]:
    """Build the KPI strip from real data only — every cell is derived from
    the 13F payload + AUM history.  No placeholder em-dash KPIs."""
    aum = fund.get("total_value")
    positions = fund.get("total_holdings")
    holdings = fund.get("all_holdings") or []
    qchanges = fund.get("quarterly_changes") or []

    top10_value = sum(h.get("value") or 0 for h in holdings[:10])
    top10_pct = (top10_value / aum * 100) if aum else None

    # New positions opened this quarter (from the most-recent quarter's diff).
    new_positions_q = sum(
        1 for c in (qchanges[0].get("changes") or []) if (c.get("status") or "").upper() in ("NEW", "NEWLY ADDED")
    ) if qchanges else 0

    # 4-quarter rolling turnover proxy: sum of |share_change| / sum(current_shares)
    # across the last 4 quarters.  A real Turnover-TTM needs full position
    # tracking, but this rolls-up the same trades the table already shows.
    chg_4q  = sum(abs(c.get("share_change") or 0)
                  for q in qchanges[:4] for c in (q.get("changes") or []))
    held_4q = sum(int(h.get("shares") or 0) for h in holdings) or 1
    turnover_pct = (chg_4q / held_4q * 100) if chg_4q else None

    # AUM QoQ delta from the history series (last two points).
    qoq_pct = None
    qoq_up = None
    if history and len(history) >= 2:
        prev = history[-2].get("total_value") or 0
        curr = history[-1].get("total_value") or 0
        if prev > 0:
            qoq_pct = (curr - prev) / prev * 100
            qoq_up  = qoq_pct >= 0

    return [
        {"label": "AUM",            "value": _format_dollars(aum),
         "delta": (f"{qoq_pct:+.1f}% QoQ" if qoq_pct is not None else None),
         "up":    qoq_up},
        {"label": "Positions",      "value": str(positions) if positions else "—",
         "delta": (f"+{new_positions_q} new" if new_positions_q else None),
         "up":    (True if new_positions_q else None)},
        {"label": "Top 10 conc.",   "value": (f"{top10_pct:.1f}%" if top10_pct else "—"),
         "delta": None, "up": None},
        {"label": "Turnover · 4q",  "value": (f"{turnover_pct:.1f}%" if turnover_pct is not None else "—"),
         "delta": None, "up": None},
    ]


def _funds_concentration_donut(fund: dict) -> dict:
    """v1's Portfolio Concentration donut — top 10 holdings as wedges + "Other"
    bucket for the long tail.  Returns a payload the template walks to render
    an SVG donut + legend."""
    aum = fund.get("total_value") or 0
    holdings = fund.get("all_holdings") or []
    if not aum or not holdings:
        return {"have_data": False, "wedges": [], "legend": []}

    palette = [
        "#3b82f6",  # blue (AAPL-style anchor)
        "#22c55e",  # green
        "#f97316",  # orange
        "#a855f7",  # purple
        "#ef4444",  # red
        "#14b8a6",  # teal
        "#eab308",  # yellow
        "#8b5cf6",  # violet
        "#94a3b8",  # slate
        "#ec4899",  # pink
    ]
    top10 = holdings[:10]
    rest_value = sum(h.get("value") or 0 for h in holdings[10:])

    wedges: list[dict] = []
    legend: list[dict] = []
    cumulative = 0.0  # 0..1 fraction along the donut path.

    cx, cy, r_outer, r_inner = 100.0, 100.0, 80.0, 50.0

    def _arc(start_frac: float, end_frac: float, color: str) -> str:
        """Return an SVG path for a donut wedge between two fractional angles."""
        import math
        a0 = -math.pi / 2 + start_frac * 2 * math.pi
        a1 = -math.pi / 2 + end_frac   * 2 * math.pi
        large = 1 if (end_frac - start_frac) > 0.5 else 0
        x0o, y0o = cx + r_outer * math.cos(a0), cy + r_outer * math.sin(a0)
        x1o, y1o = cx + r_outer * math.cos(a1), cy + r_outer * math.sin(a1)
        x0i, y0i = cx + r_inner * math.cos(a0), cy + r_inner * math.sin(a0)
        x1i, y1i = cx + r_inner * math.cos(a1), cy + r_inner * math.sin(a1)
        return (
            f"M {x0o:.2f} {y0o:.2f} "
            f"A {r_outer} {r_outer} 0 {large} 1 {x1o:.2f} {y1o:.2f} "
            f"L {x1i:.2f} {y1i:.2f} "
            f"A {r_inner} {r_inner} 0 {large} 0 {x0i:.2f} {y0i:.2f} Z"
        )

    for i, h in enumerate(top10):
        val = h.get("value") or 0
        if not val:
            continue
        frac = val / aum
        color = palette[i % len(palette)]
        wedges.append({"d": _arc(cumulative, cumulative + frac, color), "color": color})
        legend.append({
            "ticker": (h.get("ticker") or "—").upper(),
            "pct":    f"{frac * 100:.1f}%",
            "color":  color,
        })
        cumulative += frac

    if rest_value > 0:
        frac = rest_value / aum
        color = "var(--pp-line2)"
        wedges.append({"d": _arc(cumulative, cumulative + frac, color), "color": color})
        legend.append({"ticker": "Other", "pct": f"{frac * 100:.1f}%", "color": color})

    return {"have_data": True, "wedges": wedges, "legend": legend, "viewbox": "0 0 200 200"}


def _funds_position_changes_quarters(fund: dict) -> list[dict]:
    """v1's Position Changes — emit one entry per quarter with its formatted
    change rows ready to render.  Used by the Activity tab quarter selector."""
    qchanges = fund.get("quarterly_changes") or []
    out: list[dict] = []
    for q in qchanges:
        rows: list[dict] = []
        for c in (q.get("changes") or []):
            status = (c.get("status") or "").upper()
            share_chg = c.get("share_change") or 0
            cur_shares = c.get("current_shares") or 0
            prev_shares = c.get("previous_shares") or 0
            sign = "+" if share_chg > 0 else ("-" if share_chg < 0 else "")
            magnitude = abs(share_chg)
            if magnitude >= 1e6:
                chg_str = f"{sign}{magnitude / 1e6:.1f}M"
            elif magnitude >= 1e3:
                chg_str = f"{sign}{magnitude / 1e3:.0f}K"
            else:
                chg_str = f"{sign}{magnitude:,}" if magnitude else "—"
            pct_str = "—"
            if prev_shares > 0:
                pct = share_chg / prev_shares * 100
                pct_str = f"{pct:+.1f}%"
            elif status in ("NEW", "NEWLY ADDED"):
                pct_str = "New"
            rows.append({
                "ticker":     c.get("ticker") or (c.get("issuer", "")[:6].upper() or "—"),
                "name":       c.get("issuer") or "",
                "action":     status,
                "share_chg":  chg_str,
                "share_chg_up": share_chg > 0,
                "pct_str":    pct_str,
                "pct_up":     (share_chg > 0) if share_chg else None,
                "shares_now": _format_shares(cur_shares),
                "value_now":  _format_dollars(c.get("current_value")),
            })
        rows.sort(key=lambda r: abs((r.get("share_chg_up") or False) and 1 or -1), reverse=False)
        out.append({
            "label":       _quarter_label(q.get("report_period", "")),
            "report_date": q.get("report_period", ""),
            "filing_date": q.get("filing_date", ""),
            "trade_count": len(rows),
            "rows":        rows,
        })
    return out


# ── Capital Deployed (v1 module) — 13F equity + cash & equivalents ───────

async def _funds_capital_deployed(cik: str) -> dict:
    """Fetch the cached deployment metrics for one CIK.

    Reads from the same Supabase rows the v1 page uses
    (``aum_data.load_all_deployment_data`` / ``deployment:{cik}``).
    """
    cik_norm = (cik or "").lstrip("0") or cik
    try:
        from filings import supabase_cache
        cached, _fresh = await to_light(supabase_cache.get_cached_with_stale, f"deployment:{cik_norm}")
    except Exception as exc:
        logger.warning("Capital deployed fetch failed for CIK=%s: %s", cik, exc)
        return {}
    return cached if isinstance(cached, dict) else {}


def _funds_capital_panel(deployment: dict, fund: dict) -> dict:
    """Format the deployment payload into a 4-cell capital panel.

    Falls back to the in-fund 13F value when the deployment row is missing
    so the page still surfaces the 13F equity number every CIK has.
    """
    raum = (deployment or {}).get("raum")
    thirteenf = (deployment or {}).get("thirteenf_value") or fund.get("total_value")
    exact_cash = (deployment or {}).get("exact_cash")
    est_non_eq = (deployment or {}).get("estimated_non_equity")
    deployment_ratio = (deployment or {}).get("deployment_ratio")
    cash_period = (deployment or {}).get("exact_cash_period")
    data_source = (deployment or {}).get("data_source")

    cash_value = exact_cash if exact_cash is not None else est_non_eq
    cash_label = "Cash · 10-K/Q" if exact_cash is not None else (
        "Est. non-equity" if est_non_eq is not None else "Cash & Equiv"
    )
    cash_sub = (
        f"as of {cash_period}" if exact_cash is not None and cash_period
        else ("AUM gap (RAUM − 13F)" if est_non_eq is not None else None)
    )

    cells = [
        {"label": "13F Equity", "value": _format_dollars(thirteenf),
         "sub":   "from 13F-HR holdings"},
        {"label": cash_label,   "value": _format_dollars(cash_value),
         "sub":   cash_sub},
    ]
    if raum:
        cells.append({"label": "RAUM",
                      "value": _format_dollars(raum),
                      "sub":   "Form ADV reported AUM"})
    if deployment_ratio is not None:
        cells.append({
            "label": "Deployment ratio",
            "value": f"{deployment_ratio * 100:.1f}%",
            "sub":   "13F / RAUM",
        })

    return {
        "have_data": bool(thirteenf),
        "cells":     cells,
        "data_source": data_source,
    }


# ── SEC Filings list (Filings tab, mirrors stock-page pattern) ──────────

# Form-type → human-readable description.  Match by prefix so variants
# (10-K/A, 13F-HR/A, etc.) inherit the same kind.
_FILING_KIND_PREFIX: list[tuple[str, str]] = [
    ("13F",   "Quarterly holdings"),
    ("10-K",  "Annual report"),
    ("10-Q",  "Quarterly report"),
    ("8-K",   "Material event"),
    ("DEF ",  "Proxy statement"),
    ("SC 13",       "Beneficial ownership"),
    ("SCHEDULE 13", "Beneficial ownership"),
    ("S-1",   "Registration"),
    ("S-3",   "Registration"),
    ("S-4",   "Registration"),
    ("4",     "Insider transaction"),
    ("3",     "Insider transaction"),
    ("5",     "Insider transaction"),
    ("144",   "Notice of sale"),
]


def _filing_kind(form: str) -> str:
    f = (form or "").upper()
    for prefix, label in _FILING_KIND_PREFIX:
        if f.startswith(prefix):
            return label
    return "Filing"


def _filing_row_from_df(row, cik_norm: str) -> dict:
    """Convert one row of `EntityFilings.to_pandas()` into the template shape."""
    form = str(row.get("form", "") or "").strip()
    filing_date = str(row.get("filing_date", "") or "")
    accession = str(row.get("accession_number", "") or "").strip()
    acc_clean = accession.replace("-", "")
    url = (
        f"https://www.sec.gov/Archives/edgar/data/{cik_norm}/{acc_clean}/" if acc_clean
        else f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik_norm}&type={form}"
    )
    return {
        "form":         form,
        "kind":         _filing_kind(form),
        "filing_date":  filing_date,
        "filing_label": _short_date(filing_date) or filing_date,
        "accession":    accession,
        "url":          url,
    }


async def _funds_filings_list(cik: str, *, limit: int = 25) -> list[dict]:
    """List the most-recent SEC filings for *cik* via edgartools.

    L2-cached (24h) — filings only land a few times per quarter.
    """
    cik_norm = (cik or "").lstrip("0") or cik

    def _compute() -> list[dict]:
        try:
            # filings.client sets the SEC User-Agent identity at import-time
            # via set_identity() — required before any edgartools call.
            from filings import client as _client  # noqa: F401
            from edgar import Company
        except Exception:
            return []
        try:
            company = Company(int(cik_norm))
            # `.to_pandas()` sidesteps the per-attribute pyarrow access that
            # raises 'ChunkedArray has no as_py' under newer pyarrow.
            df = company.get_filings().to_pandas().head(limit)
        except Exception as exc:
            logger.warning("Funds filings: edgar load failed for CIK=%s: %s", cik, exc)
            return []
        return [_filing_row_from_df(row, cik_norm) for _, row in df.iterrows()]

    return await _l2_cached(
        key=f"funds:filings:{cik_norm}:v1",
        ttl_seconds=24 * 3600,
        compute=_compute,
        category="funds_filings",
    ) or []


def _qoq_pct_for_holding(h: dict, changes_by_cusip: dict[str, dict]) -> float | None:
    """Resolve the QoQ share-count change for a holding.

    Looks up `changes` (most-recent quarter's diff vs prior) by cusip; the
    `share_change` is signed.  Returns the percentage change relative to
    the prior quarter's share count, or None if unchanged / unknown.
    """
    cusip = h.get("cusip")
    if not cusip:
        return None
    rec = changes_by_cusip.get(cusip)
    if not rec:
        return None
    share_chg = rec.get("share_change") or 0
    prev = rec.get("previous_shares") or 0
    if prev <= 0 or share_chg == 0:
        # NEW positions report previous_shares=0; show as "NEW" via the tag below.
        return None
    return float(share_chg) / float(prev)


def _funds_holdings_table(
    fund: dict,
    top_n: int = 10,
    *,
    prices_by_ticker: dict[str, dict] | None = None,
    spark_by_ticker:  dict[str, list[float]] | None = None,
) -> list[dict]:
    """Format top-N holdings for the dense table.

    `prices_by_ticker` shape: {"AAPL": {"price": 232.71, "pct_change": -0.83}, ...}
    `spark_by_ticker`  shape: {"AAPL": [0.32, 0.38, ...], ...}
    Both are optional — pass None to render placeholders.
    """
    aum = fund.get("total_value") or 0
    holdings = fund.get("all_holdings") or []
    prices_by_ticker = prices_by_ticker or {}
    spark_by_ticker = spark_by_ticker or {}

    # cusip → {share_change, previous_shares, status}.  Pulled from the
    # most-recent quarter's `changes` list.  Used for the QoQ Δ column.
    changes_by_cusip: dict[str, dict] = {}
    qchanges = fund.get("quarterly_changes") or []
    if qchanges:
        for c in (qchanges[0].get("changes") or []):
            cusip = c.get("cusip")
            if cusip:
                changes_by_cusip[cusip] = c

    rows = []
    for i, h in enumerate(holdings[:top_n], start=1):
        val = h.get("value") or 0
        port_pct = (val / aum) if aum else 0
        ticker = (h.get("ticker") or "").upper() or "—"

        # QoQ — preferred resolution: actual share_change pct vs prior shares.
        qoq_pct = _qoq_pct_for_holding(h, changes_by_cusip)
        rec = changes_by_cusip.get(h.get("cusip") or "")
        is_new = bool(rec and rec.get("status") in ("NEW", "NEWLY ADDED"))
        if is_new:
            qoq_str = "NEW"
            qoq_kind = "new"          # gets the coral "NEW" chip
        elif qoq_pct is None:
            qoq_str = "—"
            qoq_kind = "neutral"
        else:
            qoq_str = f"{'+' if qoq_pct > 0 else ''}{qoq_pct * 100:.1f}%"
            qoq_kind = "added" if qoq_pct > 0 else "reduced"

        price_rec = prices_by_ticker.get(ticker) or {}
        last_v = price_rec.get("price")
        day_v  = price_rec.get("pct_change")  # already in % (e.g. -0.83)
        spark_series = spark_by_ticker.get(ticker)

        rows.append({
            "rank":   i,
            "ticker": ticker,
            "name":   h.get("issuer") or "",
            "shares": _format_shares(h.get("shares")),
            "value":  _format_dollars(val),
            "port":   port_pct,         # 0..1 for bar width
            "port_pct_str": f"{port_pct * 100:.1f}%",
            "qoq":         qoq_str,
            "qoq_kind":    qoq_kind,    # "added" | "reduced" | "new" | "neutral"
            "last":        f"{last_v:.2f}" if isinstance(last_v, (int, float)) else None,
            "day":         f"{day_v:+.2f}%" if isinstance(day_v, (int, float)) else None,
            "day_up":      (day_v >= 0) if isinstance(day_v, (int, float)) else None,
            "spark":       spark_series,
            "spark_up":    bool(spark_series and len(spark_series) > 1 and spark_series[-1] >= spark_series[0]),
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


# GICS sector → palette mapping for the allocation panel.  Values are CSS
# variable references so light/dark mode swap automatically.  "Other" /
# anything missing collapses to a neutral border colour.
_FUNDS_SECTOR_COLORS: dict[str, str] = {
    "Information Technology": "var(--pp-accent)",
    "Communication Services": "var(--pp-ink)",
    "Financials":             "var(--pp-up)",
    "Health Care":            "var(--pp-down)",
    "Consumer Discretionary": "var(--pp-dim)",
    "Consumer Staples":       "var(--pp-up)",
    "Energy":                 "var(--pp-down)",
    "Industrials":            "var(--pp-ink)",
    "Materials":              "var(--pp-dim2)",
    "Utilities":              "var(--pp-line2)",
    "Real Estate":            "var(--pp-line2)",
    "Other":                  "var(--pp-line2)",
}


def _build_ticker_sector_map() -> dict[str, str]:
    """Build a ticker → GICS-sector lookup from S&P 500 + NASDAQ 100 data.

    Cached implicitly by `market_data.get_sp500_constituents` (24h memory).
    Returns roughly 500 tickers mapped — enough to cover ~80% of any
    superinvestor's top holdings; the rest fall under "Other".
    """
    try:
        from filings import market_data
    except Exception:
        return {}
    out: dict[str, str] = {}
    for source in (
        market_data.get_sp500_constituents() or [],
        market_data.get_nasdaq100_constituents() or [],
    ):
        for c in source:
            t = (c.get("ticker") or "").upper()
            sec = c.get("sector") or ""
            if t and sec and t not in out:
                out[t] = sec
    return out


def _funds_sectors_breakdown(fund: dict) -> list[dict]:
    """Aggregate holdings by GICS sector for the allocation panel.

    Tickers we can't resolve roll into "Other" so the bars still sum to ~100%.
    Returns at most 6 rows ordered by allocation (largest first).
    """
    holdings = fund.get("all_holdings") or []
    aum = fund.get("total_value") or 0
    if not holdings or not aum:
        return []
    ticker_to_sector = _build_ticker_sector_map()
    by_sector: dict[str, float] = {}
    for h in holdings:
        ticker = (h.get("ticker") or "").upper()
        val = h.get("value") or 0
        if not val:
            continue
        sector = ticker_to_sector.get(ticker, "Other")
        by_sector[sector] = by_sector.get(sector, 0.0) + float(val)
    # Pull "Other" out of the named buckets so the long-tail rollup doesn't
    # create a duplicate row when the cap (>5) trips.
    other_pct = (by_sector.pop("Other", 0.0) / aum) if aum else 0.0
    rows = [
        {
            "name":  name,
            "pct":   total / aum,
            "color": _FUNDS_SECTOR_COLORS.get(name, "var(--pp-line2)"),
        }
        for name, total in by_sector.items()
    ]
    rows.sort(key=lambda r: r["pct"], reverse=True)
    # Cap at 5 named rows + 1 "Other" — long tail beyond 5 rolls into Other.
    if len(rows) > 5:
        other_pct += sum(r["pct"] for r in rows[5:])
        rows = rows[:5]
    if other_pct > 0:
        rows.append({"name": "Other", "pct": other_pct, "color": _FUNDS_SECTOR_COLORS["Other"]})
    return rows


def _funds_holdings_market_join(holdings: list[dict]) -> tuple[dict[str, dict], dict[str, list[float]]]:
    """Fetch current prices + sparkline series for the given holdings.

    Synchronous (uses market_data's in-memory caches).  Wrap calls in
    `to_heavy` from the route for cold paths where these may pull from
    yfinance.  Always returns dicts (possibly empty) so callers can safely
    pass the results into `_funds_holdings_table`.
    """
    tickers = sorted({(h.get("ticker") or "").upper() for h in holdings if h.get("ticker")})
    if not tickers:
        return {}, {}
    try:
        from filings import market_data
    except Exception:
        return {}, {}

    # 1. Day-percent + price for each S&P 500 holding (covers ~80% of typical
    #    13F top-10s; the rest fall back to current_prices_batch).
    sp500 = market_data.get_sp500_market_data("1D") or {}
    prices_by_ticker: dict[str, dict] = {
        t: {"price": v.get("price"), "pct_change": v.get("pct_change")}
        for t, v in sp500.items()
        if isinstance(v, dict) and t in tickers
    }

    # 2. Fill in missing prices via current-prices batch (no day-pct, just last).
    missing = [t for t in tickers if t not in prices_by_ticker]
    if missing:
        try:
            extra = market_data.get_current_prices_batch(missing) or {}
            for t, p in extra.items():
                prices_by_ticker[t] = {"price": p, "pct_change": None}
        except Exception:
            pass

    # 3. Normalized sparkline points (~1 month of close data; column header
    #    relabelled "1M" since that's all we cache for free today).
    spark_by_ticker: dict[str, list[float]] = {}
    try:
        spark_by_ticker = market_data.get_sparkline_points(tickers, num_points=24) or {}
    except Exception:
        pass

    return prices_by_ticker, spark_by_ticker


# ── AUM history (multi-quarter) ────────────────────────────────────────────
# Reconstructs total_value per historical 13F-HR by walking edgartools
# directly.  Cached in L2 with 24h TTL — the underlying filings are
# immutable once filed, so a long TTL is safe.

_FUNDS_AUM_HISTORY_TTL_SECONDS = 24 * 3600
_FUNDS_AUM_HISTORY_QUARTERS = 12   # ~3 years of quarterly cadence


def _fund_aum_history_compute(cik: str) -> list[dict]:
    """Synchronous: pull last N 13F-HRs via edgartools and total each one.

    Returns list of {report_period, filing_date, total_value} ordered
    oldest → newest.  Empty list if edgar can't reach the filer.

    `tf.total_value` triggers a pyarrow incompatibility in some edgartools
    versions ("ChunkedArray has no attribute as_py"); we compute the total
    directly from the holdings frame's Value column instead, which is the
    same number ``ThirteenF.total_value`` would yield.
    """
    try:
        from edgar import Company, ThirteenF
        from filings.client import _detect_multiplier
    except Exception:
        return []

    try:
        company = Company(int(cik))
        filings = company.get_filings(form="13F-HR", amendments=False)
    except Exception as exc:
        logger.warning("AUM history: edgar load failed for CIK=%s: %s", cik, exc)
        return []

    if not filings:
        return []

    n = min(len(filings), _FUNDS_AUM_HISTORY_QUARTERS)
    out: list[dict] = []
    for i in range(n):
        try:
            tf = ThirteenF(filings[i])
            holdings_df = tf.holdings
            if holdings_df is None or len(holdings_df) == 0:
                continue
            mult = _detect_multiplier(holdings_df)
            total = int(holdings_df["Value"].sum()) * mult
            if not total:
                continue
            out.append({
                "report_period": str(tf.report_period),
                "filing_date":   str(tf.filing_date),
                "total_value":   total,
            })
        except Exception as exc:
            logger.debug("AUM history: skipping filing %d for CIK=%s: %s", i, cik, exc)
            continue
    out.sort(key=lambda r: r["report_period"])
    return out


async def _fund_aum_history(cik: str) -> list[dict]:
    """L2-cached AUM history series for the funds page.

    Cache key: ``funds:aum_history:{cik_norm}:v1``.  Stale data is fine —
    historical 13Fs don't change once filed.
    """
    cik_norm = (cik or "").lstrip("0") or cik
    return await _l2_cached(
        key=f"funds:aum_history:{cik_norm}:v1",
        ttl_seconds=_FUNDS_AUM_HISTORY_TTL_SECONDS,
        compute=lambda: _fund_aum_history_compute(cik),
        category="funds_aum_history",
    )


def _nice_axis_step(rng: float, target_steps: int = 4) -> float:
    """Pick a 'nice' step size (1/2/5 × 10^k) that splits *rng* into ~target_steps."""
    if rng <= 0:
        return 1.0
    import math
    raw = rng / max(target_steps, 1)
    mag = 10 ** math.floor(math.log10(raw))
    n = raw / mag
    if   n < 1.5: nice = 1.0
    elif n < 3.5: nice = 2.0
    elif n < 7.5: nice = 5.0
    else:         nice = 10.0
    return nice * mag


def _format_dollars_compact(v: float) -> str:
    """Tight $XB / $XM string used by chart axis labels (no trailing zeros)."""
    av = abs(v)
    if av >= 1e12: out, suf = v / 1e12, "T"
    elif av >= 1e9:  out, suf = v / 1e9,  "B"
    elif av >= 1e6:  out, suf = v / 1e6,  "M"
    elif av >= 1e3:  out, suf = v / 1e3,  "K"
    else:            return f"${v:,.0f}"
    txt = f"{out:.1f}".rstrip("0").rstrip(".")
    return f"${txt}{suf}"


def _funds_aum_chart_payload(history: list[dict]) -> dict:
    """Convert AUM history into the data the SVG template needs.

    Produces SVG point pairs + nice-rounded y-axis labels in a 600×220
    viewBox.  The y-axis is anchored to round dollar values (e.g. $250B /
    $300B / $350B for Berkshire) so the absolute scale is visible — the
    earlier auto-normalized version made every fund's curve look the
    same shape regardless of magnitude.  The latest point's QoQ delta is
    precomputed so the panel header can colourize it.
    """
    if not history:
        return {
            "have_data": False,
            "points":     [],
            "ticks":      [],
            "y_labels":   [],
            "grid_ys":    [],
            "qoq_str":    "",
            "qoq_up":     None,
            "fill_d":     "",
            "line_d":     "",
        }

    # ViewBox sized 1500×240 to match the typical full-width container's
    # natural ratio (~6:1).  With `preserveAspectRatio="none"` the SVG
    # stretches to fill the container; if the viewBox aspect ratio matches,
    # that stretch is near-identity and circles stay round / slopes accurate.
    width, height = 1500.0, 240.0
    pad_top, pad_bot = 16.0, 12.0
    plot_h = height - pad_top - pad_bot
    n = len(history)

    vals = [r["total_value"] for r in history]
    lo_raw, hi_raw = min(vals), max(vals)
    rng = hi_raw - lo_raw if hi_raw > lo_raw else 1.0

    # Round y-axis bounds to a "nice" step so labels read $250B / $300B /
    # $350B instead of $258.5B / $303.4B / $348.2B.
    step = _nice_axis_step(rng, target_steps=4)
    import math
    lo = math.floor(lo_raw / step) * step
    hi = math.ceil (hi_raw / step) * step
    if hi == lo:
        hi = lo + step
    plot_rng = hi - lo

    def _y_for(v: float) -> float:
        return pad_top + (1.0 - (v - lo) / plot_rng) * plot_h

    points: list[tuple[float, float]] = []
    for i, v in enumerate(vals):
        x = (i / max(n - 1, 1)) * width
        points.append((round(x, 1), round(_y_for(v), 1)))

    line_d = " ".join(("M" if i == 0 else "L") + f"{x} {y}" for i, (x, y) in enumerate(points))
    fill_d = f"{line_d} L {width:.1f} {height:.1f} L 0 {height:.1f} Z"

    # Y-axis labels at every nice step between lo and hi.  Cap at 5 rows
    # so we don't crowd a small panel.
    y_labels: list[dict] = []
    grid_ys:  list[float] = []
    v = lo
    while v <= hi + step / 2:
        y_labels.append({
            "label": _format_dollars_compact(v),
            "y":     round(_y_for(v), 1),
        })
        grid_ys.append(round(_y_for(v), 1))
        v += step
    if len(y_labels) > 5:
        # Keep first / mid / last when too many.
        y_labels = [y_labels[0], y_labels[len(y_labels) // 2], y_labels[-1]]
        grid_ys  = [g["y"] for g in y_labels]

    # X-axis tick labels — ~6 evenly-spaced (incl. first + last).
    target_ticks = 6
    step_x = max(1, (n - 1) // (target_ticks - 1)) if n > 1 else 1
    tick_idxs = sorted(set([0] + list(range(0, n, step_x)) + [n - 1]))
    ticks = [{
        "label": _quarter_label(history[i]["report_period"]).replace(" 20", " '"),
        "x":     points[i][0],
    } for i in tick_idxs]

    # QoQ delta from the last two points.
    qoq_str = ""
    qoq_up: bool | None = None
    if n >= 2:
        delta = vals[-1] - vals[-2]
        sign = "+" if delta >= 0 else "-"
        if abs(delta) >= 1e9:
            qoq_str = f"{sign}${abs(delta) / 1e9:.1f}B QoQ"
        elif abs(delta) >= 1e6:
            qoq_str = f"{sign}${abs(delta) / 1e6:.0f}M QoQ"
        else:
            qoq_str = f"{sign}${abs(delta):,.0f} QoQ"
        qoq_up = delta >= 0

    # Per-point hover payload — quarter label + value + QoQ delta vs prior
    # point (None for the first row).  Serialized to JSON in the template
    # so the mousemove handler can look up the nearest point in O(1).
    chart_history: list[dict] = []
    for i, (h, p) in enumerate(zip(history, points)):
        prev_v = vals[i - 1] if i > 0 else None
        delta_pct = None
        if prev_v and prev_v > 0:
            delta_pct = (vals[i] - prev_v) / prev_v * 100
        chart_history.append({
            "x":          p[0],
            "y":          p[1],
            "value":      vals[i],
            "value_str":  _format_dollars_compact(vals[i]),
            "quarter":    _quarter_label(h["report_period"]),
            "filed":      h.get("filing_date", ""),
            "delta_pct":  delta_pct,
        })

    return {
        "have_data":     True,
        "points":        [{"x": x, "y": y} for x, y in points],
        "ticks":         ticks,
        "y_labels":      y_labels,
        "grid_ys":       grid_ys,
        "qoq_str":       qoq_str,
        "qoq_up":        qoq_up,
        "fill_d":        fill_d,
        "line_d":        line_d,
        "vb_width":      width,
        "vb_height":     height,
        "chart_history": chart_history,
    }


def _funds_recent_activity_with_aum(fund: dict, history: list[dict]) -> list[dict]:
    """Activity timeline with real AUM Δ when we have multi-quarter history.

    Pairs each `quarterly_changes` entry with the corresponding AUM-then
    record (matched by report_period).  When two consecutive AUM points
    are available, fills the Δ; otherwise leaves it as an em-dash.
    """
    qchanges = fund.get("quarterly_changes") or []
    history = history or []
    by_period = {h["report_period"]: h["total_value"] for h in history}
    history_periods = [h["report_period"] for h in history]

    rows = []
    for i, q in enumerate(qchanges[:4]):
        changes = q.get("changes") or []
        adds = sum(1 for c in changes if c.get("status") in ("ADDED", "NEW", "ADD"))
        cuts = sum(1 for c in changes if c.get("status") in ("REDUCED", "EXITED", "CUT", "EXIT"))

        period = q.get("report_period", "")
        aum_str, aum_up = "—", None
        if period in by_period:
            aum_now = by_period[period]
            try:
                idx = history_periods.index(period)
                if idx > 0:
                    prev = history[idx - 1]["total_value"]
                    if prev > 0:
                        delta_pct = (aum_now - prev) / prev * 100
                        sign = "+" if delta_pct >= 0 else ""
                        aum_str = f"{sign}{delta_pct:.1f}%"
                        aum_up = delta_pct >= 0
            except ValueError:
                pass

        rows.append({
            "quarter":     _quarter_label(period),
            "filing_date": q.get("filing_date", ""),
            "count_str":   f"+{adds} / -{cuts}",
            "current":     i == 0,
            "aum_delta":   aum_str,
            "aum_up":      aum_up,
        })
    return rows


_FUND_TABS = ("Portfolio", "Activity", "Performance", "Sectors", "Filings")


def _funds_index_rows(request: Request) -> list[dict]:
    """Build the sortable summary row for every cached superinvestor.

    Sources from `request.app.state.fund_cache` (populated at app startup;
    invalidated only when the cache reference changes).  Each row carries
    everything the index template renders — manager + fund name, portfolio
    value, holdings count, top-5 tickers + their stored logo IDs, filing
    date — sized to the v2 design token system (no extra fetches).
    """
    fund_cache = getattr(request.app.state, "fund_cache", {}) or {}
    if not fund_cache:
        return []
    try:
        from filings.superinvestors import SUPERINVESTORS_BY_CIK
    except Exception:
        return []

    rows: list[dict] = []
    for cik, info in SUPERINVESTORS_BY_CIK.items():
        cached = fund_cache.get(cik)
        if not cached:
            continue
        # `top_holdings` was dropped from the in-memory cache slim; read
        # the same prefix off `all_holdings` (value-sorted desc).
        top_tickers = [
            h.get("ticker") for h in (cached.get("all_holdings") or [])[:5]
            if h.get("ticker")
        ]
        rows.append({
            "cik":            cik,
            "manager":        info.display_name or info.fund_name or "—",
            "fund_name":      cached.get("name") or info.fund_name or "—",
            "value":          float(cached.get("total_value") or 0),
            "value_str":      _format_dollars(cached.get("total_value")),
            "holdings_n":     int(cached.get("total_holdings") or 0),
            "top_tickers":    top_tickers,
            "filing_date":    cached.get("filing_date", ""),
            "report_period":  cached.get("report_period", ""),
            "icon":           _icon_from_fund_name(cached.get("name") or info.fund_name or ""),
            "href":           f"/funds/{cik}",
        })
    rows.sort(key=lambda r: r["value"], reverse=True)
    return rows


# ── Funds index payload helpers ──────────────────────────────────────────
#
# Four sub-tabs (Funds / Holdings / Activity / Capital Deployed) all
# render against the same in-process ``fund_cache``.  Each helper below
# transforms the raw cache into the shape the corresponding pane needs.
# Mirrors v1's `/funds?view=…` page but with the v2 design system.


def _funds_holdings_consensus(grand_portfolio, n: int = 10) -> list[dict]:
    """Top-N stocks by holder count — drives the Consensus Leaders bar chart."""
    out: list[dict] = []
    max_holders = max((e.num_holders for e in grand_portfolio[:n]), default=1)
    for e in grand_portfolio[:n]:
        out.append({
            "ticker":          e.ticker or (e.cusip or "")[:6],
            "name":            e.issuer_name or "",
            "num_holders":     e.num_holders,
            "combined_value":  e.combined_value,
            "combined_value_str": _format_dollars(e.combined_value),
            "avg_weight":      round(e.avg_weight or 0, 1),
            "top_holders":     (e.holders or [])[:3],
            "bar_pct":         round(e.num_holders / max(max_holders, 1) * 100, 1),
        })
    return out


def _funds_holdings_momentum(most_added, n: int = 15) -> list[dict]:
    """Recent quarter momentum — top stocks added by superinvestors."""
    out: list[dict] = []
    max_adds = max((m.get("add_count", 0) for m in most_added[:n]), default=1)
    for m in most_added[:n]:
        out.append({
            "ticker":     m.get("ticker") or (m.get("cusip", "")[:6]),
            "name":       m.get("issuer_name", ""),
            "add_count":  m.get("add_count", 0),
            "adders":     [a for a in (m.get("adders") or [])[:5]],
            "value_str":  _format_dollars(m.get("total_value", 0)),
            "bar_pct":    round(m.get("add_count", 0) / max(max_adds, 1) * 100, 1),
        })
    return out


def _funds_index_holdings_table(grand_portfolio, top_n: int = 100) -> list[dict]:
    """Top-N stocks sorted by holder count — drives the funds-index All
    Holdings table.

    NB: distinct from the older `_funds_holdings_table` helper used by
    the fund detail page; renamed here to avoid the same shadow that
    collided `_funds_capital_deployed`.  ``pct_of_aggregate`` comes
    pre-computed off each entry from `client.build_grand_portfolio`,
    so no aggregate sum needed at this layer."""
    rows: list[dict] = []
    for i, e in enumerate(grand_portfolio[:top_n], start=1):
        rows.append({
            "rank":                i,
            "ticker":              e.ticker or (e.cusip or "")[:6],
            "name":                e.issuer_name or "",
            "num_holders":         e.num_holders,
            "combined_value":      e.combined_value,
            "combined_value_str":  _format_dollars(e.combined_value, full=True),
            "pct_of_aggregate":    round(e.pct_of_aggregate or 0, 2),
            "top_holders":         (e.holders or [])[:3],
            "more_holders":        max(0, len(e.holders or []) - 3),
        })
    return rows


_ACTIVITY_STATUS = {
    "NEW":      ("BUY",  True),
    "ADDED":    ("ADD",  True),
    "INCREASED": ("ADD", True),
    "REDUCED":  ("TRIM", False),
    "DECREASED": ("TRIM", False),
    "EXITED":   ("SELL", False),
    "SOLD":     ("SELL", False),
    "BOUGHT":   ("BUY",  True),
}


def _classify_action(raw_status: str) -> tuple[str, bool]:
    """Map raw 13F status → (display label, is_buy)."""
    return _ACTIVITY_STATUS.get((raw_status or "").upper(), (raw_status or "—", False))


_FUNDS_ACTIVITY_TOP_N = 50


def _funds_activity_consensus(fund_cache: dict, superinvestors_by_cik: dict,
                              *, cusip_to_ticker: dict | None = None) -> dict:
    """Aggregate every 13F change across all funds, grouped by ticker.

    Returns ``{moves: list, summary: dict}`` where each move row carries
    buyer/seller counts, net dollar flow, sentiment label, and the full
    per-fund detail list (for the expand-row pattern).

    `changes` rows often carry only a CUSIP (no ticker); resolve via the
    cross-fund CUSIP→ticker map so the consensus group keys collapse on
    the same security instead of fragmenting across CUSIP prefixes.

    Caller can pass a pre-built ``cusip_to_ticker`` map (e.g. the one
    cached on ``app.state._pp_redesign_cusip_ticker``) to skip the
    rebuild — saves ~50-150ms on warm Activity-tab hits.

    Truncates to the top ``_FUNDS_ACTIVITY_TOP_N`` tickers in Python
    BEFORE per-ticker trade sorting, so we don't sort ~5,950 throwaway
    trade lists that the template will discard via slice anyway.
    """
    if cusip_to_ticker is None:
        cusip_to_ticker = _build_cusip_ticker_map(fund_cache or {})

    by_ticker: dict[str, dict] = {}
    total_buy = 0.0
    total_sell = 0.0
    total_activities = 0

    for cik, fund_data in (fund_cache or {}).items():
        si = superinvestors_by_cik.get(cik)
        if not si:
            continue
        for c in (fund_data.get("changes") or []):
            raw = (c.get("status") or "").upper()
            if not raw or raw == "UNCHANGED":
                continue
            cusip = c.get("cusip", "") or ""
            ticker = c.get("ticker") or cusip_to_ticker.get(cusip)
            # No ticker resolution → skip (un-traded names crowd the list
            # without value; users wouldn't recognise raw CUSIP prefixes).
            if not ticker:
                continue
            action, is_buy = _classify_action(raw)
            value = float(c.get("current_value") or 0)
            shares = float(c.get("share_change") or 0)
            total_activities += 1
            if is_buy:
                total_buy += value
            else:
                total_sell += value

            slot = by_ticker.setdefault(ticker, {
                "ticker":      ticker,
                "issuer":      c.get("issuer", ""),
                "buy_count":   0,
                "sell_count":  0,
                "net_flow":    0.0,
                "trades":      [],
            })
            slot["buy_count"]  += 1 if is_buy else 0
            slot["sell_count"] += 0 if is_buy else 1
            # Net flow uses signed direction so dollars cancel within each ticker
            slot["net_flow"]   += value if is_buy else -value
            slot["trades"].append({
                "fund_name":    si.display_name,
                "cik":          cik,
                "action":       action,
                "is_buy":       is_buy,
                "share_change": shares,
                "value":        value,
                "value_str":    _format_dollars(value),
                "share_change_str": f"{shares:+,.0f}" if shares else "—",
                "raw_status":   raw,
            })

    # Cap to the top-N tickers BEFORE per-ticker trade sorting + sentiment
    # decoration.  ~6,000 raw tickers but only `_FUNDS_ACTIVITY_TOP_N` ever
    # render — sorting trade lists for the other ~5,950 was pure waste.
    total_consensus = len(by_ticker)
    moves_raw = sorted(
        by_ticker.values(),
        key=lambda m: -(m["buy_count"] + m["sell_count"]),
    )[:_FUNDS_ACTIVITY_TOP_N]

    moves: list[dict] = []
    for m in moves_raw:
        # Per-fund detail list sorted by absolute dollar move so the
        # biggest mover appears first when the user expands the row.
        m["trades"].sort(key=lambda t: -abs(t["value"]))
        if m["net_flow"] > 0:
            sentiment = ("BULLISH", "up")
        elif m["net_flow"] < 0:
            sentiment = ("BEARISH", "down")
        else:
            sentiment = ("NEUTRAL", "dim")
        m["sentiment_label"] = sentiment[0]
        m["sentiment_tone"]  = sentiment[1]
        m["net_flow_str"]    = (
            ("+" if m["net_flow"] >= 0 else "−") + _format_dollars(abs(m["net_flow"])).lstrip("$")
        )
        m["activity_n"]      = m["buy_count"] + m["sell_count"]
        moves.append(m)

    net_flow = total_buy - total_sell
    summary = {
        "value_sentiment":  "BULLISH" if net_flow > 0 else ("BEARISH" if net_flow < 0 else "NEUTRAL"),
        "value_tone":       "up" if net_flow > 0 else ("down" if net_flow < 0 else "dim"),
        "net_flow":         net_flow,
        "net_flow_str":     ("+" if net_flow >= 0 else "−") + _format_dollars(abs(net_flow)).lstrip("$"),
        "buying_str":       _format_dollars(total_buy),
        "selling_str":      _format_dollars(total_sell),
        "consensus_count":  total_consensus,
        "total_activities": total_activities,
    }
    return {"moves": moves, "summary": summary}


def _funds_capital_deployed_rows(deployment_data: dict) -> list[dict]:
    """Sortable rows for the funds-index Capital Deployed pane.

    NB: distinct from the older `_funds_capital_deployed(cik)` helper a
    few hundred lines up, which fetches per-fund deployment for the
    detail page.  Same domain, different consumer; renamed here to
    avoid the shadow that broke `/_v2/funds/{cik}` after this batch.

    Wraps `aum_data.build_deployment_leaderboard` and shapes each entry
    into the row format the template wants — formatted dollar strings,
    deployed-pct progress bar, source pill, and a stable sort key.
    """
    if not deployment_data:
        return []
    from filings import aum_data
    leaderboard = aum_data.build_deployment_leaderboard(deployment_data)

    rows: list[dict] = []
    for i, e in enumerate(leaderboard, start=1):
        # `deployment_ratio` is stored as a fraction (e.g. 0.46 = 46%);
        # multiply for the percentage display.
        ratio_raw = e.get("deployment_ratio")
        ratio_pct = round(float(ratio_raw) * 100, 1) if ratio_raw is not None else None
        # Deployed-bar tone: green if mostly deployed (≥ 60%), amber middle,
        # red when most cash sits outside the 13F equity book.
        if ratio_pct is None:
            tone = "dim"
        elif ratio_pct >= 60:
            tone = "up"
        elif ratio_pct >= 30:
            tone = "warn"
        else:
            tone = "down"

        cash = e.get("exact_cash") or e.get("estimated_non_equity")
        rows.append({
            "rank":               i,
            "cik":                e.get("cik", ""),
            "manager":            e.get("display_name") or e.get("fund_name") or "—",
            "fund_name":          e.get("fund_name") or "—",
            "raum":               e.get("raum") or 0,
            "raum_str":           _format_dollars(e.get("raum")),
            "thirteenf_value":    e.get("thirteenf_value") or 0,
            "thirteenf_value_str": _format_dollars(e.get("thirteenf_value")),
            "deployment_pct":     ratio_pct,
            "deployment_tone":    tone,
            "cash":               cash,
            "cash_str":           _format_dollars(cash) if cash else "—",
            "data_source":        (e.get("data_source") or "").upper(),
        })
    return rows


@router.get("/funds", response_class=HTMLResponse)
async def preview_funds_index(request: Request, view: str = "Funds"):
    """Funds index — 4 sub-panes (Funds list / Holdings / Activity /
    Capital Deployed).  Lands here when a user clicks "Funds" in the side
    nav.  Each fund row deep-links into ``/_v2/funds/{cik}``.

    All four panes render against the in-process ``fund_cache`` +
    ``deployment_cache`` populated at app startup — zero upstream fetches
    on this route, so warm hits are sub-200 ms across all sub-tabs.
    """
    valid_views = {"Funds", "Holdings", "Activity", "Capital Deployed"}
    # Accept legacy lowercase v1 view values so old bookmarks land on the
    # right tab (e.g. /funds?view=holdings → Holdings).
    legacy_alias = {
        "funds": "Funds", "holdings": "Holdings",
        "activity": "Activity", "deployment": "Capital Deployed",
    }
    if view not in valid_views:
        view = legacy_alias.get(view.lower(), "Funds")

    rows = _funds_index_rows(request)
    fund_cache       = getattr(request.app.state, "fund_cache", {}) or {}
    deployment_cache = getattr(request.app.state, "deployment_cache", {}) or {}

    cache_age = ""
    if fund_cache:
        try:
            from filings import cache as _cache_mod
            cache_age = _cache_mod.get_cache_age_str(fund_cache)
        except Exception:
            cache_age = ""

    # Holdings + Activity panes are now lazy-loaded on tab click via
    # `/api/funds-index/holdings` and `/api/funds-index/activity` (see
    # partial handlers below).  We only fetch their data here when the
    # user lands directly on `?view=Holdings` or `?view=Activity` — that
    # way the default Funds landing skips ~890ms of aggregation work and
    # ~750KB of HTML render.
    grand_portfolio: list = []
    most_added:      list = []
    activity_payload: dict = {"moves": [], "summary": {}}

    if fund_cache and view in ("Holdings", "Activity"):
        try:
            holdings_data, activity_data = await _fetch_funds_panes(
                request, fund_cache,
                need_holdings=(view == "Holdings"),
                need_activity=(view == "Activity"),
            )
            grand_portfolio, most_added = holdings_data
            activity_payload = activity_data or activity_payload
        except Exception as exc:
            logger.warning("Funds index aggregation failed: %s", exc)

    ctx = {
        "request":       request,
        **(await _shell_context(request, "Funds")),
        "page_title":    "Funds",
        # Tab navigation
        "funds_index_tabs":  ["Funds", "Holdings", "Activity", "Capital Deployed"],
        "funds_index_view":  view,
        # Funds pane (sortable list)
        "funds_index":   rows,
        "funds_count":   len(rows),
        "funds_cache_age": cache_age,
        # Holdings pane — only populated on direct ?view=Holdings link.
        "funds_consensus":    _funds_holdings_consensus(grand_portfolio, n=10),
        "funds_momentum":     _funds_holdings_momentum(most_added, n=15),
        "funds_holdings_all": _funds_index_holdings_table(grand_portfolio, top_n=100),
        # Activity pane — only populated on direct ?view=Activity link.
        "funds_activity_moves":   activity_payload.get("moves", []),
        "funds_activity_summary": activity_payload.get("summary", {}),
        # Capital Deployed pane (cheap — reads cached deployment_cache directly)
        "funds_capital_rows": _funds_capital_deployed_rows(deployment_cache),
    }
    return templates.TemplateResponse("_redesign/funds_index.html", ctx)


async def _fetch_funds_panes(
    request: Request,
    fund_cache: dict,
    *,
    need_holdings: bool,
    need_activity: bool,
) -> tuple[tuple[list, list], dict | None]:
    """Run only the aggregations needed for the requested panes.

    Returns ``((grand_portfolio, most_added), activity_payload_or_None)``.
    `build_grand_portfolio` and `build_most_added_table` are memoized on
    ``id(fund_cache)``; `_funds_activity_consensus_sync` likewise after
    the Wave 1 memoization pass.  So sequential `view=Holdings` then
    `view=Activity` requests share their respective memoized results.
    """
    from filings import client, market_data
    from filings.superinvestors import SUPERINVESTORS_BY_CIK

    tasks = []
    if need_holdings:
        tasks.append(asyncio.to_thread(client.build_grand_portfolio,
                                       fund_cache, SUPERINVESTORS_BY_CIK))
        tasks.append(asyncio.to_thread(market_data.build_most_added_table,
                                       fund_cache, SUPERINVESTORS_BY_CIK))
    if need_activity:
        tasks.append(asyncio.to_thread(_funds_activity_consensus_sync,
                                       request, fund_cache, SUPERINVESTORS_BY_CIK))

    results = await asyncio.gather(*tasks) if tasks else []
    i = 0
    grand_portfolio: list = []
    most_added:      list = []
    activity_payload: dict | None = None
    if need_holdings:
        grand_portfolio = results[i]; i += 1
        most_added      = results[i]; i += 1
    if need_activity:
        activity_payload = results[i]
    return (grand_portfolio, most_added), activity_payload


@router.get("/api/funds-index/holdings", response_class=HTMLResponse)
async def preview_funds_holdings_partial(request: Request):
    """Lazy-loaded Holdings pane — fetched by the funds-index page on
    first activation of the Holdings tab.  Renders just the partial
    template (no app shell)."""
    fund_cache = getattr(request.app.state, "fund_cache", {}) or {}
    grand_portfolio: list = []
    most_added:      list = []
    if fund_cache:
        try:
            (grand_portfolio, most_added), _ = await _fetch_funds_panes(
                request, fund_cache, need_holdings=True, need_activity=False,
            )
        except Exception as exc:
            logger.warning("Funds holdings partial failed: %s", exc)
    return templates.TemplateResponse(
        "_redesign/partials/funds_holdings.html",
        {
            "request": request,
            "funds_consensus":    _funds_holdings_consensus(grand_portfolio, n=10),
            "funds_momentum":     _funds_holdings_momentum(most_added, n=15),
            "funds_holdings_all": _funds_index_holdings_table(grand_portfolio, top_n=100),
        },
    )


@router.get("/api/funds-index/activity", response_class=HTMLResponse)
async def preview_funds_activity_partial(request: Request):
    """Lazy-loaded Activity pane — fetched on first Activity-tab click."""
    fund_cache = getattr(request.app.state, "fund_cache", {}) or {}
    activity_payload: dict = {"moves": [], "summary": {}}
    if fund_cache:
        try:
            _, activity_payload = await _fetch_funds_panes(
                request, fund_cache, need_holdings=False, need_activity=True,
            )
            activity_payload = activity_payload or {"moves": [], "summary": {}}
        except Exception as exc:
            logger.warning("Funds activity partial failed: %s", exc)
    return templates.TemplateResponse(
        "_redesign/partials/funds_activity.html",
        {
            "request": request,
            "funds_activity_moves":   activity_payload.get("moves", []),
            "funds_activity_summary": activity_payload.get("summary", {}),
        },
    )


_funds_activity_consensus_memo: tuple[int, dict] | None = None


def _funds_activity_consensus_sync(request: Request,
                                    fund_cache: dict,
                                    superinvestors_by_cik: dict) -> dict:
    """Sync wrapper around `_funds_activity_consensus` for ``to_thread``.

    Memoized on ``id(fund_cache)`` — the fund cache is rebuilt as a new
    dict object on each sweep, so identity is a reliable invalidation
    key and matches the pattern used by `build_grand_portfolio` and
    `build_most_added_table`.  Uses the request-scoped CUSIP→ticker map
    cached on ``app.state._pp_redesign_cusip_ticker``.
    """
    global _funds_activity_consensus_memo
    cache_id = id(fund_cache)
    if _funds_activity_consensus_memo and _funds_activity_consensus_memo[0] == cache_id:
        return _funds_activity_consensus_memo[1]

    cmap = getattr(request.app.state, "_pp_redesign_cusip_ticker", None)
    if cmap is None:
        cmap = _build_cusip_ticker_map(fund_cache)
        try:
            request.app.state._pp_redesign_cusip_ticker = cmap
        except Exception:
            pass
    result = _funds_activity_consensus(fund_cache, superinvestors_by_cik, cusip_to_ticker=cmap)
    _funds_activity_consensus_memo = (cache_id, result)
    return result


@router.get("/funds/detail", response_class=HTMLResponse)
async def preview_fund_detail_legacy(
    request: Request,
    cik: str = _DEFAULT_FUND_CIK,
    tab: str = "Portfolio",
):
    """Legacy `?cik=` deep-link entry point.

    Registered BEFORE the `/funds/{cik}` catch-all so FastAPI matches the
    exact path first — otherwise ``cik="detail"`` would leak into the
    detail handler and blow up int parsing downstream.

    Old internal links carrying `/_v2/funds?cik=…` were replaced with the
    canonical `/_v2/funds/{cik}` path; this route covers any external /
    bookmarked links that still point at the query-string form.
    """
    return await _render_fund_detail(request, cik=cik, tab=tab)


@router.get("/funds/{cik}", response_class=HTMLResponse)
async def preview_fund_detail_by_cik(
    request: Request,
    cik: str,
    tab: str = "Portfolio",
):
    """Funds detail — pretty-URL form `/_v2/funds/{cik}`.

    Delegates to `_render_fund_detail` so the legacy `?cik=` path keeps
    working for any external links cached during the redesign window.
    """
    return await _render_fund_detail(request, cik=cik, tab=tab)


async def _render_fund_detail(request: Request, *, cik: str, tab: str):
    """Funds detail — 5 tabs (Portfolio / Activity / Performance / Sectors /
    Filings).  ``cik`` selects the fund; ``tab`` deep-links the active pane."""
    if tab not in _FUND_TABS:
        tab = "Portfolio"
    bounded = functools.partial(_bounded, page="Funds page")

    fund = await _fetch_fund_data(cik)
    if not fund:
        fund = {
            "name": "—", "cik": cik, "report_period": "", "filing_date": "",
            "total_value": 0, "total_holdings": 0,
            "top_holdings": [], "all_holdings": [],
            "changes": [], "quarterly_changes": [],
        }

    meta = _FUND_META.get(cik.lstrip("0") or cik) or _FUND_META.get(cik) or {}
    adds, cuts = _funds_changes_split(fund)
    top10 = (fund.get("all_holdings") or [])[:10]

    history, market_join, deployment, filings = await asyncio.gather(
        bounded(_fund_aum_history(cik),                        timeout=8.0,
                 fallback=[],          name="aum_history"),
        bounded(to_heavy(_funds_holdings_market_join, top10),  timeout=4.0,
                 fallback=({}, {}),    name="holdings_market_join"),
        bounded(_funds_capital_deployed(cik),                  timeout=3.0,
                 fallback={},          name="capital_deployed"),
        bounded(_funds_filings_list(cik, limit=25),            timeout=8.0,
                 fallback=[],          name="filings"),
    )
    # _l2_cached returns None on hard compute failure; normalise.
    history = history or []
    prices_by_ticker, spark_by_ticker = market_join or ({}, {})

    aum_chart = _funds_aum_chart_payload(history)
    activity  = _funds_recent_activity_with_aum(fund, history)

    ctx = {
        "request": request,
        **(await _shell_context(request, "Funds")),
        # Hero
        "fund_icon":          meta.get("icon") or _icon_from_fund_name(fund.get("name", "")),
        "fund_cik":           fund.get("cik") or cik,
        "fund_name":          fund.get("name") or "—",
        "fund_manager":       meta.get("manager") or "",
        "fund_city":          meta.get("city") or "",
        "fund_filing_date":   fund.get("filing_date", ""),
        "fund_report_period": fund.get("report_period", ""),
        "fund_quarter_label": _quarter_label(fund.get("report_period", "")),
        # Tabs
        "fund_tabs":     list(_FUND_TABS),
        "funds_tab":     tab,
        # Portfolio tab payloads
        "funds_kpi":           _funds_kpi_strip(fund, history),
        "funds_concentration": _funds_concentration_donut(fund),
        "funds_holdings":      _funds_holdings_table(
            fund, top_n=10,
            prices_by_ticker=prices_by_ticker,
            spark_by_ticker=spark_by_ticker,
        ),
        "funds_pos_total":     fund.get("total_holdings") or 0,
        # Activity tab payloads
        "funds_activity":      activity,
        "funds_quarter_diffs": _funds_position_changes_quarters(fund),
        "funds_adds":          adds,
        "funds_cuts":          cuts,
        # Performance tab payloads
        "funds_aum_chart":     aum_chart,
        "funds_capital":       _funds_capital_panel(deployment, fund),
        # Sectors tab payload
        "funds_sectors":       _funds_sectors_breakdown(fund),
        # Filings tab payload
        "funds_filings":       filings,
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

    # ── Institutional ownership: count of distinct super-fund holdings. ──
    fund_cache = getattr(request.app.state, "fund_cache", {}) or {}
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
        return await to_light(ca.get_row, t_up)

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
                    await to_light(ca.upsert_row, row)
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
        _bounded(to_light(_stock_build_sentiment, ticker),           timeout=4.0,
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
        _bounded(to_light(_stock_build_ownership_congress, ticker),  timeout=3.0,
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
async def preview_stock(request: Request, ticker: str):
    """Stock detail.

    Reads the pre-aggregated bundle from ``stock_overview_cache`` if
    fresh; otherwise builds it live via :func:`build_stock_data_bundle`
    and schedules a cold-tier write back so the next request hits the
    cache.  Per-user state (watchlist) and per-render derivations
    (chart geometry, EPS SVG path, analyst ticks) are computed off
    the bundle at render time.
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
        stock_bundle.record_miss(had_cached=cached is not None)
        bundle, source_status = await build_stock_data_bundle(fund_cache, ticker)
        # Preserve an existing tier classification when refreshing a
        # seeded row (hot/warm); default to cold for ad-hoc misses.
        # Without this, the request path would clobber a hot ticker
        # back to cold and the warmer would stop refreshing it.
        write_tier = cached["tier"] if cached else stock_bundle.COLD_TIER
        # Fire-and-forget the cache write so we don't pay the Supabase
        # round-trip on the user's response after they already paid for
        # the live fanout.  Failures inside `set_bundle` are logged and
        # swallowed; lifespan tracks the task so it isn't GC'd mid-flight.
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
        to_heavy(insider_trading.get_latest_insider_trades, "", 200, ""),
        to_heavy(supabase_cache.get_congress_trades_recent_months, months, 5000),
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
        return d.strftime("%b %d %Y")
    except Exception:
        return iso_str[:10]


async def _fetch_congress_data() -> dict:
    """Read recent congressional trades from Supabase cache."""
    try:
        from filings import supabase_cache
        rows = await to_heavy(supabase_cache.get_congress_recent_trades, 60)
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


def _format_compact_dollars(v: float | int | None) -> str:
    """Compact dollar formatter for KPI / leaderboard cells."""
    if not v or v <= 0:
        return "—"
    v = float(v)
    if v >= 1e9:
        return f"${v / 1e9:.1f}B"
    if v >= 1e6:
        return f"${v / 1e6:.1f}M"
    if v >= 1e3:
        return f"${v / 1e3:.0f}K"
    return f"${v:,.0f}"


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


def _trade_in_window(trade_date: str, days: int) -> bool:
    """True if a trade's ISO date lies within the last `days` days."""
    if not trade_date:
        return False
    try:
        d = datetime.fromisoformat(trade_date[:10])
        return (datetime.now() - d).days <= days
    except Exception:
        return False


async def _fetch_congress_wider_window(months: int = 6, limit: int = 5000) -> list[dict]:
    """Slim trade list from the last N months — used for KPI aggregates,
    Members, Leaderboard, Performance, sectors."""
    try:
        from filings import supabase_cache
        rows = await to_heavy(
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
        rows = await to_heavy(supabase_cache.get_all_congress_members)
    except Exception as exc:
        logger.warning("Congress members fetch failed: %s", exc)
        return []
    return rows or []


def _index_member_meta(members: list[dict]) -> dict[str, dict]:
    """member_id → {party, chamber, state_abbr, full_name, net_worth_estimate}."""
    out: dict[str, dict] = {}
    for m in members:
        mid = m.get("member_id")
        if not mid:
            continue
        out[mid] = {
            "name":      m.get("full_name") or "",
            "party":     m.get("party") or "Independent",
            "chamber":   m.get("chamber") or "",
            "state":     m.get("state_abbr") or "",
            "district":  m.get("district") or "",
            "net_worth": m.get("net_worth_estimate") or 0,
        }
    return out


def _party_letter(party: str) -> str:
    """Normalise to 'D' / 'R' / 'I' for the dot."""
    p = (party or "").lower()
    if p.startswith("d"):
        return "D"
    if p.startswith("r"):
        return "R"
    return "I"


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


# ── Members tab — per-member aggregate cards ───────────────────────────────

def _build_member_aggregates(
    wider_rows: list[dict],
    member_index: dict[str, dict],
    *,
    window_days: int = 30,
) -> list[dict]:
    """Aggregate member-level activity from the wider trade window.

    The "window" is anchored to the latest filing_date in the data (not
    wall-clock now) so the cards stay populated even when the scrape
    pipeline lags by weeks.

    Returns one row per active member with:
        trades_window, volume_window, top_ticker, last_trade_date,
        spark_series (12 weekly buckets of trade frequency), party, chamber.
    """
    from collections import Counter, defaultdict

    # Anchor to the most recent filing_date.
    filing_dates = sorted(
        (t.get("filing_date") for t in wider_rows if t.get("filing_date")),
        reverse=True,
    )
    if filing_dates:
        try:
            ref = datetime.fromisoformat(filing_dates[0][:10])
        except Exception:
            ref = datetime.now()
    else:
        ref = datetime.now()

    def _days_before_ref(iso: str) -> int | None:
        if not iso:
            return None
        try:
            return (ref - datetime.fromisoformat(iso[:10])).days
        except Exception:
            return None

    by_member: dict[str, dict] = {}
    for t in wider_rows:
        mid = t.get("member_id") or t.get("politician_name") or "—"
        if mid not in by_member:
            by_member[mid] = {
                "member_id":   mid,
                "name":        t.get("politician_name") or "—",
                "party":       _party_letter(t.get("party") or ""),
                "chamber":     t.get("chamber") or "",
                "state":       t.get("state") or "",
                "tickers":     Counter(),
                "trades_total": 0,
                "trades_window": 0,
                "volume_window": 0.0,
                "last_trade_date": "",
                "weekly_counts": defaultdict(int),
            }
        agg = by_member[mid]
        agg["trades_total"] += 1
        ticker = (t.get("ticker") or "").upper()
        if ticker:
            agg["tickers"][ticker] += 1

        td = t.get("trade_date", "")
        if td and td > agg["last_trade_date"]:
            agg["last_trade_date"] = td

        # Window check uses filing_date (when public) anchored to ref.
        days_in = _days_before_ref(t.get("filing_date") or td)
        if days_in is not None and 0 <= days_in <= window_days:
            agg["trades_window"] += 1
            agg["volume_window"] += _amount_midpoint(t.get("amount_low"), t.get("amount_high"))

        # Weekly bucket (last 12 weeks) for the sparkline — also ref-anchored.
        if days_in is not None and 0 <= days_in < 84:
            weeks_ago = days_in // 7
            agg["weekly_counts"][11 - weeks_ago] += 1

    rows = []
    for mid, agg in by_member.items():
        if agg["trades_total"] == 0:
            continue
        # Backfill from member_index when present (party/chamber/state may be
        # missing/abbreviated on raw trade rows).
        meta = member_index.get(mid, {})
        party = _party_letter(meta.get("party") or agg["party"])
        chamber = meta.get("chamber") or agg["chamber"]
        state = meta.get("state") or agg["state"]

        spark = [agg["weekly_counts"].get(i, 0) for i in range(12)]
        top = agg["tickers"].most_common(1)
        rows.append({
            "member_id":      mid,
            "name":           agg["name"],
            "party":          party,
            "chamber":        chamber,
            "state":          state,
            "trades_window":  agg["trades_window"],
            "volume_window":  agg["volume_window"],
            "volume_str":     _format_compact_dollars(agg["volume_window"]),
            "top_ticker":     top[0][0] if top else "—",
            "last_trade":     _congress_format_date(agg["last_trade_date"]),
            "spark":          spark,
            "spark_up":       sum(spark[6:]) >= sum(spark[:6]),
            "trades_total":   agg["trades_total"],
            "net_worth":      meta.get("net_worth", 0),
        })

    return rows


def _members_panel_data(
    wider_rows: list[dict],
    member_index: dict[str, dict],
    *,
    top_n: int = 12,
) -> dict:
    """Build the Members tab payload — sort by 30d trade count then volume."""
    members = _build_member_aggregates(wider_rows, member_index, window_days=30)
    members.sort(key=lambda m: (m["trades_window"], m["volume_window"]), reverse=True)
    return {
        "rows":      members[:top_n],
        "total":     len([m for m in members if m["trades_window"] > 0]),
    }


# ── Leaderboard — ranked by activity (volume) with win-rate bar ────────────

def _leaderboard_panel_data(
    wider_rows: list[dict],
    member_index: dict[str, dict],
    *,
    top_n: int = 10,
) -> dict:
    """Rank members by 6-month trade volume (proxy for activity).

    win_rate is computed naively from BUY/SELL split (a SELL is "negative
    conviction") without forward-return data, which lives in
    congress_trades_prices and isn't yet read on the hot path.  The bar
    still gives a meaningful "buyer vs seller" signal.
    """
    members = _build_member_aggregates(wider_rows, member_index, window_days=180)
    # We need BUY / SELL split per member — re-walk wider_rows for that.
    from collections import defaultdict
    splits: dict[str, dict] = defaultdict(lambda: {"buys": 0, "sells": 0})
    for t in wider_rows:
        mid = t.get("member_id") or t.get("politician_name") or "—"
        ttype = (t.get("trade_type") or "").lower()
        if ttype in ("buy", "purchase"):
            splits[mid]["buys"] += 1
        elif ttype in ("sell", "sale"):
            splits[mid]["sells"] += 1

    out = []
    for m in members:
        s = splits.get(m["member_id"], {"buys": 0, "sells": 0})
        total = s["buys"] + s["sells"]
        # Buy-bias as a proxy "win-rate" stand-in until forward returns wire up.
        buy_pct = (s["buys"] / total) if total else 0.0
        out.append({
            **m,
            "buys": s["buys"],
            "sells": s["sells"],
            "total": total,
            "buy_pct": buy_pct,
        })

    out.sort(key=lambda r: r["volume_window"], reverse=True)
    for i, r in enumerate(out[:top_n], start=1):
        r["rank"] = i
    return {
        "podium": out[:3],
        "rows":   out[:top_n],
        "total":  len(out),
    }


# ── Performance tab — Congress vs SPY + by-party + sectors ────────────────

def _performance_party_breakdown(wider_rows: list[dict]) -> list[dict]:
    """Aggregate trade volume + count + buy-bias by party."""
    from collections import defaultdict
    by_party: dict[str, dict] = defaultdict(lambda: {
        "members": set(), "buys": 0, "sells": 0, "volume": 0.0,
    })
    for t in wider_rows:
        p = _party_letter(t.get("party") or "")
        bucket = by_party[p]
        if t.get("member_id"):
            bucket["members"].add(t["member_id"])
        ttype = (t.get("trade_type") or "").lower()
        if ttype in ("buy", "purchase"):
            bucket["buys"] += 1
        elif ttype in ("sell", "sale"):
            bucket["sells"] += 1
        bucket["volume"] += _amount_midpoint(t.get("amount_low"), t.get("amount_high"))

    name_for = {"D": "Democrats", "R": "Republicans", "I": "Independent"}
    rows = []
    for code, agg in by_party.items():
        total = agg["buys"] + agg["sells"]
        if total == 0:
            continue
        buy_pct = agg["buys"] / total if total else 0.0
        rows.append({
            "code":    code,
            "name":    name_for.get(code, code),
            "members": len(agg["members"]),
            "trades":  total,
            "volume":  _format_compact_dollars(agg["volume"]),
            "buy_pct": buy_pct,
            "buy_pct_str": f"{buy_pct * 100:.0f}%",
        })
    # Order: D, R, I
    order = {"D": 0, "R": 1, "I": 2}
    rows.sort(key=lambda r: order.get(r["code"], 99))
    return rows


def _performance_sectors(wider_rows: list[dict]) -> list[dict]:
    """Volume-weighted sector allocation for last-6mo trades."""
    try:
        from filings import market_data
    except Exception:
        return []
    sp = market_data.get_sp500_constituents() or []
    nq = market_data.get_nasdaq100_constituents() or []
    ticker_to_sector: dict[str, str] = {}
    for source in (sp, nq):
        for c in source:
            t = (c.get("ticker") or "").upper()
            sec = c.get("sector") or ""
            if t and sec and t not in ticker_to_sector:
                ticker_to_sector[t] = sec

    from collections import defaultdict
    by_sector: dict[str, float] = defaultdict(float)
    total_vol = 0.0
    for t in wider_rows:
        ticker = (t.get("ticker") or "").upper()
        if not ticker:
            continue
        sector = ticker_to_sector.get(ticker, "Other")
        v = _amount_midpoint(t.get("amount_low"), t.get("amount_high"))
        if v > 0:
            by_sector[sector] += v
            total_vol += v

    if total_vol <= 0:
        return []

    palette = {
        "Information Technology": "var(--pp-accent)",
        "Communication Services": "var(--pp-ink)",
        "Financials":             "var(--pp-up)",
        "Health Care":            "var(--pp-down)",
        "Consumer Discretionary": "var(--pp-dim)",
        "Consumer Staples":       "var(--pp-up)",
        "Energy":                 "var(--pp-down)",
        "Industrials":            "var(--pp-ink)",
        "Materials":              "var(--pp-dim2)",
        "Utilities":              "var(--pp-line2)",
        "Real Estate":            "var(--pp-line2)",
        "Other":                  "var(--pp-line2)",
    }
    other_share = by_sector.pop("Other", 0.0)
    rows = sorted(
        ({"name": n, "pct": v / total_vol, "color": palette.get(n, "var(--pp-line2)")}
         for n, v in by_sector.items()),
        key=lambda r: r["pct"], reverse=True,
    )
    if len(rows) > 5:
        other_share += sum(r["pct"] * total_vol for r in rows[5:])
        rows = rows[:5]
    if other_share > 0:
        rows.append({"name": "Other", "pct": other_share / total_vol, "color": palette["Other"]})
    return rows


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
            import bisect
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


# ── Calendar tab — economic events as proxy for "session days" ────────────

# Big macro events that move sectors — for the ticker-tag column.
_CALENDAR_TICKER_HINTS: dict[str, list[str]] = {
    "cpi":              ["TIPS", "TLT", "SPY"],
    "ppi":              ["TIPS", "TLT"],
    "non-farm":         ["SPY", "QQQ"],
    "nonfarm":          ["SPY", "QQQ"],
    "unemployment":     ["SPY", "TLT"],
    "fomc":             ["SPY", "TLT", "GLD"],
    "fed":              ["SPY", "TLT"],
    "rate decision":    ["SPY", "TLT"],
    "gdp":              ["SPY", "QQQ"],
    "pce":              ["TIPS", "TLT"],
    "retail sales":     ["XRT", "SPY"],
    "housing":          ["XHB", "ITB"],
    "oil":              ["XLE", "USO"],
    "consumer confidence": ["SPY", "XLY"],
}


def _ticker_hints_for_event(event_name: str) -> list[str]:
    name = (event_name or "").lower()
    for key, tickers in _CALENDAR_TICKER_HINTS.items():
        if key in name:
            return tickers
    return []


async def _calendar_panel_data(*, top_n: int = 12) -> dict:
    """Wire the Calendar tab to the existing economic_calendar feed.

    `events_by_date` is a list of {date, date_label, entries: [events]}
    grouped by ISO date — not a dict — so we walk the list and flatten.
    """
    try:
        from filings import economic_calendar
        bundle = await to_heavy(economic_calendar.fetch_economic_events, "all", "us", "all")
    except Exception as exc:
        logger.warning("Congress calendar: economic_calendar fetch failed: %s", exc)
        bundle = None

    if not bundle:
        return {"rows": [], "total": 0, "is_mock": True}

    days = bundle.get("events_by_date") or []
    is_mock = bool(bundle.get("is_mock"))

    flat: list[dict] = []
    for day in days:
        if not isinstance(day, dict):
            continue
        date_iso = day.get("date") or ""
        for ev in (day.get("entries") or []):
            evt_name = ev.get("event") or "—"
            flat.append({
                "d":        _short_date(date_iso) or date_iso[:10],
                "when":     ev.get("time") or "—",
                "evt":      evt_name,
                "impact":   (ev.get("impact") or "low").lower(),
                "tickers":  _ticker_hints_for_event(evt_name),
                "raw_date": date_iso[:10],
            })

    flat.sort(key=lambda r: r.get("raw_date", ""))
    return {"rows": flat[:top_n], "total": len(flat), "is_mock": is_mock}


# ── Congress Holdings tab — chart geometry helpers ───────────────────────────


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
        import math
        rng_top = math.ceil(max_abs / step) * step
        rng_bot = -rng_top
    else:
        max_v = max(values, default=1) or 1
        step = _nice_axis_step(max_v, target_steps=4)
        import math
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


_CONGRESS_ACT_TIMEFRAMES = ("1W", "1M", "3M", "ALL")
_CONGRESS_ACT_CHAMBERS   = ("all", "house", "senate")
_CONGRESS_ACT_PARTIES    = ("all", "democrat", "republican")


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


# ─────────────────────────────────────────────────────────────────────────────
# INSIDERS — two tabs: Filings (filtered recent trades) and Clusters
# (per-ticker rollups).  Both are wired to OpenInsider via insider_trading.
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


# Coarse role bucket used by the Filings tab role filter.  Maps every
# OpenInsider title segment to one of the chip values.  Anything that
# doesn't match a named bucket falls into "Other" so the filter still
# accounts for it without dropping rows from "All".
def _insiders_role_key(title: str) -> str:
    """Map a raw OpenInsider title to one of the role-filter buckets:
    'CEO', 'CFO', 'Director', '10pct', or 'Other'."""
    t = (title or "").lower()
    if not t:
        return "Other"
    # Title strings can list multiple roles, e.g. "Pres & CEO, Director" —
    # match the most senior-looking bucket first.
    if "ceo" in t or "chief executive" in t:
        return "CEO"
    if "cfo" in t or "chief financial" in t:
        return "CFO"
    if "10%" in t or "10 percent" in t or "ten percent" in t or "10 pct" in t:
        return "10pct"
    if "director" in t:
        return "Director"
    return "Other"


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


def _insiders_kpi_strip_empty() -> list[dict]:
    """KPI strip when no upstream trade data is available — every cell em-dashed."""
    return [
        {"label": "Filings",          "value": "—", "delta": None, "up": None},
        {"label": "Net flow",         "value": "—", "delta": None, "up": None},
        {"label": "Buy / Sell ratio", "value": "—", "delta": None, "up": None},
        {"label": "Active clusters",  "value": "—", "delta": None, "up": None},
        {"label": "Top buyer",        "value": "—", "delta": None, "up": None},
        {"label": "Top seller",       "value": "—", "delta": None, "up": None},
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


# ── Insider tab builders — Clusters ──────────────────────────────────────


async def _fetch_insider_trades_wide(count: int = 200) -> list:
    """Pull a wider window of insider trades for the new aggregation tabs.

    Reuses the same scraper/Supabase layer as the Filings tab fetcher, just
    with a larger count budget.  Returns the raw ``InsiderTrade`` list so
    each builder can roll up by ticker / insider / sector independently.
    """
    try:
        from filings import insider_trading
        return await to_heavy(
            insider_trading.get_latest_insider_trades, "", count, "",
        ) or []
    except Exception as exc:
        logger.warning("Insider wide-window fetch failed: %s", exc)
        return []


def _insider_value_dollars(trade) -> float:
    """Parse the OpenInsider ``value`` string into an absolute dollar float."""
    try:
        from filings.insider_trading import parse_dollar_value
    except Exception:
        return 0.0
    try:
        return abs(parse_dollar_value(trade.value))
    except Exception:
        return 0.0


def _insider_is_buy(trade) -> bool:
    """OpenInsider trade_type → bool BUY / SELL."""
    return "Purchase" in (trade.trade_type or "")


def _insiders_clusters_panel(trades: list, top_n: int = 4) -> list[dict]:
    """Group trades by ticker; surface tickers with 3+ insiders all trading
    the same direction inside the window.  Returns the densest 4 cards by
    aggregate dollar volume — matches the design's 2x2 grid."""
    from collections import defaultdict

    by_ticker: dict[str, dict] = {}
    for tr in trades:
        if not tr.ticker:
            continue
        key = tr.ticker.upper()
        if key not in by_ticker:
            by_ticker[key] = {
                "ticker":   key,
                "name":     tr.company_name or "",
                "buys":     [],
                "sells":    [],
                "buy_value":  0.0,
                "sell_value": 0.0,
            }
        bucket = by_ticker[key]
        v = _insider_value_dollars(tr)
        if _insider_is_buy(tr):
            bucket["buys"].append(tr)
            bucket["buy_value"] += v
        else:
            bucket["sells"].append(tr)
            bucket["sell_value"] += v

    clusters: list[dict] = []
    for key, b in by_ticker.items():
        # Cluster = ≥3 distinct insiders trading the same direction.
        if len(b["buys"]) >= 3:
            members = b["buys"]
            direction = "BUY"
            volume = b["buy_value"]
        elif len(b["sells"]) >= 3:
            members = b["sells"]
            direction = "SELL"
            volume = b["sell_value"]
        else:
            continue

        # Dedupe by insider name; keep the largest individual trade per person.
        per_person: dict[str, dict] = {}
        for m in members:
            person = m.insider_name or "—"
            mv = _insider_value_dollars(m)
            existing = per_person.get(person)
            if existing is None or mv > existing["v_raw"]:
                per_person[person] = {
                    "p":  person,
                    "r":  _shorten_role((m.title or "").split(",")[0].strip()),
                    "v":  _format_compact_dollars(mv),
                    "v_raw": mv,
                    "d":  (m.trade_date or m.filing_date or "")[:10],
                }
        members_list = sorted(per_person.values(), key=lambda x: x["v_raw"], reverse=True)[:6]

        clusters.append({
            "ticker":     key,
            "name":       b["name"],
            "direction":  direction,
            "count":      len(per_person),
            "value":      _format_compact_dollars(volume),
            "_volume":    volume,        # sort key — not consumed by the template
            "members":    members_list,
        })

    clusters.sort(key=lambda c: (c["count"], c["_volume"]), reverse=True)
    return clusters[:top_n]


def _shorten_role(title: str) -> str:
    """Compact role label — preserves CEO/CFO/Director, abbreviates the rest."""
    if not title:
        return "—"
    t = title.strip()
    aliases = {
        "Chief Executive Officer":  "CEO",
        "Chief Financial Officer":  "CFO",
        "Chief Operating Officer":  "COO",
        "Chief Technology Officer": "CTO",
        "Chief Legal Officer":      "CLO",
        "Chief Marketing Officer":  "CMO",
        "President":                "President",
        "Director":                 "Director",
    }
    for long_, short_ in aliases.items():
        if long_.lower() in t.lower():
            return short_
    if "10%" in t or "Owner" in t:
        return "10% own."
    return t[:14]


def _net_format(net: float) -> str:
    if not net:
        return "—"
    sign = "+" if net > 0 else "-"
    return f"{sign}{_format_compact_dollars(abs(net))}"


def _insiders_kpi_strip_real(trades: list) -> list[dict]:
    """Real KPI strip from the active trade window. Empty data → every cell
    em-dashed; we never emit fake fallback numbers."""
    from collections import Counter
    if not trades:
        return _insiders_kpi_strip_empty()

    buys = [t for t in trades if _insider_is_buy(t)]
    sells = [t for t in trades if not _insider_is_buy(t)]
    buy_v = sum(_insider_value_dollars(t) for t in buys)
    sell_v = sum(_insider_value_dollars(t) for t in sells)
    net = buy_v - sell_v
    bs_ratio = (buy_v / sell_v) if sell_v else 0

    buy_counts = Counter((t.ticker or "").upper() for t in buys if t.ticker)
    sell_counts = Counter((t.ticker or "").upper() for t in sells if t.ticker)
    top_buyer  = buy_counts.most_common(1)[0][0] if buy_counts else "—"
    top_seller = sell_counts.most_common(1)[0][0] if sell_counts else "—"

    # Active clusters — tickers w/ 3+ same-direction insiders.
    from collections import defaultdict
    cluster_count = 0
    by_ticker_dirs: dict[str, dict[str, set]] = defaultdict(lambda: {"buys": set(), "sells": set()})
    for t in trades:
        if not t.ticker:
            continue
        key = t.ticker.upper()
        if _insider_is_buy(t):
            by_ticker_dirs[key]["buys"].add(t.insider_name)
        else:
            by_ticker_dirs[key]["sells"].add(t.insider_name)
    for sets in by_ticker_dirs.values():
        if len(sets["buys"]) >= 3 or len(sets["sells"]) >= 3:
            cluster_count += 1

    return [
        {"label": "Filings",                     "value": f"{len(trades):,}",       "delta": None,  "up": None},
        {"label": "Net flow",                    "value": _net_format(net),         "delta": None,  "up": (net >= 0)},
        {"label": "Buy / Sell ratio",            "value": f"{bs_ratio:.2f}",        "delta": None,  "up": (bs_ratio >= 1)},
        {"label": "Active clusters",             "value": str(cluster_count),       "delta": None,  "up": None},
        {"label": "Top buyer",                   "value": top_buyer,                "delta": None,  "up": None},
        {"label": "Top seller",                  "value": top_seller,               "delta": None,  "up": None},
    ]


# ── Filings tab — direction / window / role / plan filtering ────────────

_INSIDERS_DIRECTIONS = {
    "latest":    {"label": "Latest",    "trade_type": ""},
    "purchases": {"label": "Purchases", "trade_type": "p"},
    "sales":     {"label": "Sales",     "trade_type": "s"},
}
_INSIDERS_WINDOWS = {
    "today":   {"label": "Today",         "days": 1,   "kpi": "today"},
    "7d":      {"label": "Last 7 days",   "days": 7,   "kpi": "7d"},
    "30d":     {"label": "Last 30 days",  "days": 30,  "kpi": "30d"},
    "quarter": {"label": "This quarter",  "days": None,"kpi": "QTD"},  # special: from quarter start
}
_INSIDERS_ROLE_KEYS = ("All", "CEO", "CFO", "Director", "10pct", "Other")
_INSIDERS_PLAN_KEYS = ("All", "open", "10b5-1")


def _insiders_window_since(window_key: str) -> str:
    """Resolve a window key to a 'YYYY-MM-DD' since-date for the OpenInsider
    fetcher.  'quarter' anchors to the start of the current calendar quarter."""
    today = datetime.now()
    if window_key == "quarter":
        q_start_month = ((today.month - 1) // 3) * 3 + 1
        return today.replace(month=q_start_month, day=1).strftime("%Y-%m-%d")
    days = _INSIDERS_WINDOWS.get(window_key, _INSIDERS_WINDOWS["30d"])["days"]
    if days is None or days <= 0:
        return ""
    return (today - timedelta(days=days)).strftime("%Y-%m-%d")


async def _fetch_insiders_filtered(*, direction: str, window: str, count: int = 200) -> list:
    """Pull insider trades filtered server-side by trade_type + since_date.

    Role + Plan are parsed from the OpenInsider title / trade_type strings
    after the fetch — the upstream API doesn't expose those as filters.
    """
    trade_type = _INSIDERS_DIRECTIONS.get(direction, _INSIDERS_DIRECTIONS["latest"])["trade_type"]
    since      = _insiders_window_since(window)
    try:
        from filings import insider_trading
        return await to_heavy(
            insider_trading.get_latest_insider_trades, trade_type, count, since,
        ) or []
    except Exception as exc:
        logger.warning("Insiders filtered fetch failed: %s", exc)
        return []


def _insiders_apply_role_plan(trades: list, role: str, plan: str) -> list:
    """Post-filter the returned list by role + plan (no upstream support)."""
    if role == "All" and plan == "All":
        return trades
    out = []
    for t in trades:
        if role != "All" and _insiders_role_key(t.title) != role:
            continue
        if plan != "All" and _insiders_plan(t.trade_type) != plan:
            continue
        out.append(t)
    return out


def _insiders_format_filtered_rows(trades: list, *, max_filings: int = 200) -> list[dict]:
    """Same shape as the existing rows — re-formatted from the filtered set.
    Used when we apply server-side filters; mirrors the row dict
    `_fetch_insiders_data` produces for cold paths."""
    rows = []
    for tr in trades[:max_filings]:
        action = _insiders_action(tr.trade_type)
        plan   = _insiders_plan(tr.trade_type)
        rows.append({
            "person":       tr.insider_name,
            "role":         _insiders_format_title(tr.title),
            "ticker":       (tr.ticker or "").upper(),
            "company_name": getattr(tr, "company_name", "") or "",
            "action":       action,
            "shares":       tr.qty or "—",
            "price":        tr.price or "—",
            "value":        tr.value or "—",
            "plan":         plan,
            "date":         (tr.trade_date or tr.filing_date or "")[:10],
            "sec_url":      getattr(tr, "sec_url", "") or "",
            "title":        tr.title or "",
            "flag":         action == "BUY" and plan == "open",
        })
    return rows


def _insiders_group_by_ticker(rows: list[dict]) -> list[dict]:
    """Collapse individual filings into one row per ticker for the dense
    summary view — each group carries the count + total $ + the
    constituent filings (revealed when the row is expanded)."""
    from collections import OrderedDict
    groups: "OrderedDict[str, dict]" = OrderedDict()
    for r in rows:
        tk = r.get("ticker") or "—"
        bucket = groups.setdefault(tk, {
            "ticker":     tk,
            "name":       "",
            "filings":    [],
            "buy_count":  0,
            "sell_count": 0,
            "buy_value":  0.0,
            "sell_value": 0.0,
        })
        bucket["filings"].append(r)
        # Best-available company name from the first row that carries one.
        if not bucket["name"]:
            bucket["name"] = (r.get("company_name") or r.get("name") or "")
        try:
            from filings.insider_trading import parse_dollar_value
            v = abs(parse_dollar_value(r.get("value") or ""))
        except Exception:
            v = 0.0
        if r.get("action") == "BUY":
            bucket["buy_count"]  += 1
            bucket["buy_value"]  += v
        else:
            bucket["sell_count"] += 1
            bucket["sell_value"] += v

    out = []
    for tk, b in groups.items():
        net = b["buy_value"] - b["sell_value"]
        # Summary tag: dominant direction + count.
        if b["buy_count"] and not b["sell_count"]:
            tag_kind  = "BUY"
            tag_label = f"{b['buy_count']} Buy" + ("s" if b["buy_count"] > 1 else "")
        elif b["sell_count"] and not b["buy_count"]:
            tag_kind  = "SELL"
            tag_label = f"{b['sell_count']} Sale" + ("s" if b["sell_count"] > 1 else "")
        else:
            tag_kind  = "MIXED"
            tag_label = f"{b['buy_count']} Buy / {b['sell_count']} Sale"
        out.append({
            "ticker":     tk,
            "name":       b["name"],
            "tag_kind":   tag_kind,
            "tag_label":  tag_label,
            "net":        net,
            "net_str":    _net_format(net) if (b["buy_value"] or b["sell_value"]) else "—",
            "filings":    b["filings"],
            "filing_n":   len(b["filings"]),
        })
    return out


def _insiders_momentum_chart(trades: list, *, top_buys: int = 5, top_sells: int = 5) -> dict:
    """Bar-chart payload for the Insider Momentum panel.

    Buckets trades by ticker, picks the top-N buys (positive net dollar) +
    top-N sells (negative net dollar), then computes SVG-ready bar geometry.
    Mirrors the v1 'Insider Momentum' module.
    """
    if not trades:
        return {"have_data": False, "bars": [], "y_labels": [], "grid_ys": []}

    from collections import defaultdict
    by_t: dict[str, dict] = defaultdict(lambda: {"buy": 0.0, "sell": 0.0})
    for tr in trades:
        tk = (tr.ticker or "").upper()
        if not tk:
            continue
        v = _insider_value_dollars(tr)
        if _insider_is_buy(tr):
            by_t[tk]["buy"]  += v
        else:
            by_t[tk]["sell"] += v
    if not by_t:
        return {"have_data": False, "bars": [], "y_labels": [], "grid_ys": []}

    nets = [(tk, b["buy"] - b["sell"]) for tk, b in by_t.items()]
    nets.sort(key=lambda x: x[1], reverse=True)
    buys  = [(tk, n) for tk, n in nets if n > 0][:top_buys]
    sells = [(tk, n) for tk, n in nets if n < 0][-top_sells:]
    sells.reverse()  # most-negative first → matches v1 ordering
    # `bars` order: buys (descending), then sells (most-negative first → least).
    ordered = buys + sells
    if not ordered:
        return {"have_data": False, "bars": [], "y_labels": [], "grid_ys": []}

    # Y-axis: anchor zero at the chart midline so positive bars grow up and
    # negative bars grow down.  ViewBox 1500×280 matches a typical full-width
    # container (~5.4:1) so `preserveAspectRatio="none"` is near-identity.
    width, height = 1500.0, 280.0
    pad_top, pad_bot, pad_left = 18.0, 28.0, 78.0
    plot_w = width - pad_left
    plot_h = height - pad_top - pad_bot

    max_val = max(abs(n) for _, n in ordered) or 1.0
    # Round up to a "nice" step so y labels read cleanly.
    step = _nice_axis_step(max_val * 2, target_steps=4)
    import math
    rng_top = math.ceil(max_val / step) * step
    rng_bot = -rng_top

    def _y_for(v: float) -> float:
        return pad_top + (1.0 - (v - rng_bot) / (rng_top - rng_bot)) * plot_h

    zero_y = _y_for(0)

    n_bars = len(ordered)
    bar_gap = 14.0
    bar_w = max(8.0, (plot_w - bar_gap * (n_bars + 1)) / n_bars)

    bars: list[dict] = []
    for i, (tk, n) in enumerate(ordered):
        x = pad_left + bar_gap + i * (bar_w + bar_gap)
        y_top = _y_for(n) if n >= 0 else zero_y
        y_bot = zero_y if n >= 0 else _y_for(n)
        bars.append({
            "ticker":  tk,
            "value":   n,
            "is_buy":  n >= 0,
            "value_str": _net_format(n),
            "x":       round(x, 1),
            "y":       round(y_top, 1),
            "w":       round(bar_w, 1),
            "h":       round(max(y_bot - y_top, 1.0), 1),
            "label_x": round(x + bar_w / 2, 1),
        })

    # Y-axis: 5 lines from -rng to +rng inclusive.
    y_labels: list[dict] = []
    grid_ys:  list[float] = []
    v = rng_bot
    while v <= rng_top + step / 2:
        y_pos = round(_y_for(v), 1)
        y_labels.append({"label": _format_dollars_compact(v), "y": y_pos})
        grid_ys.append(y_pos)
        v += step

    return {
        "have_data": True,
        "bars":      bars,
        "y_labels":  y_labels,
        "grid_ys":   grid_ys,
        "vb_width":  width,
        "vb_height": height,
        "zero_y":    round(zero_y, 1),
        "left_pad":  pad_left,
    }


_VALID_INSIDER_DIRECTIONS = tuple(_INSIDERS_DIRECTIONS.keys())
_VALID_INSIDER_WINDOWS    = tuple(_INSIDERS_WINDOWS.keys())
# Clusters tab sub-pill — filter the per-ticker cluster list by direction.
_INSIDERS_CLUSTER_KEYS = ("All", "BUY", "SELL")


@router.get("/insiders", response_class=HTMLResponse)
async def preview_insiders(
    request: Request,
    direction: str = "latest",
    role:      str = "All",
    plan:      str = "All",
    cluster:   str = "All",
):
    """Insiders page — Filings tab honours `?direction=&role=&plan=` query
    params; Clusters tab honours `?cluster=All|BUY|SELL`.  Every filter is
    server-rendered (no client-side decoration).

    The Filings tab + momentum chart use a fixed 30-day lookback; the wider
    6-month set still drives Clusters / People / Companies tabs.
    """
    window = "30d"  # fixed window for Filings + momentum (no UI control)
    if direction not in _VALID_INSIDER_DIRECTIONS: direction = "latest"
    if role      not in _INSIDERS_ROLE_KEYS:       role      = "All"
    if plan      not in _INSIDERS_PLAN_KEYS:       plan      = "All"
    if cluster   not in _INSIDERS_CLUSTER_KEYS:    cluster   = "All"

    bounded = functools.partial(_bounded, page="Insiders page")

    # Filings tab uses the user-selected filters on the recent 200-trade
    # pull; Clusters tab uses a wider always-on pull so the aggregation is
    # stable regardless of what's in the filter bar.
    filings_trades, wide_trades = await asyncio.gather(
        bounded(_fetch_insiders_filtered(direction=direction, window=window, count=200),
                timeout=8.0, fallback=[], name="filings"),
        bounded(_fetch_insider_trades_wide(200),
                timeout=8.0, fallback=[], name="wide"),
    )

    # Apply role + plan filters server-side (OpenInsider doesn't expose them).
    filings_trades = _insiders_apply_role_plan(filings_trades, role, plan)

    rows = _insiders_format_filtered_rows(filings_trades)
    grouped = _insiders_group_by_ticker(rows)

    # Momentum chart + KPI strip reflect the active Filings filter set.
    momentum_chart = _insiders_momentum_chart(filings_trades)

    # Pull the full cluster list (not just the top 4) so we can server-side
    # filter by direction before slicing to the 2x2 grid.
    clusters_all = (
        await to_light(_insiders_clusters_panel, wide_trades, 99)
        if wide_trades else []
    )

    # Cluster sub-pill: filter by direction (BUY/SELL/All), then take top 4.
    if cluster == "All":
        clusters_panel = clusters_all[:4]
    else:
        clusters_panel = [c for c in clusters_all if c["direction"] == cluster][:4]
    clusters_buy_total  = sum(1 for c in clusters_all if c["direction"] == "BUY")
    clusters_sell_total = sum(1 for c in clusters_all if c["direction"] == "SELL")

    ctx = {
        "request":          request,
        **(await _shell_context(request, "Insiders")),
        # KPI strip + filtered Filings tab payload
        "insiders_kpi":         _insiders_kpi_strip_real(filings_trades),
        "insiders_rows":        rows,
        "insiders_grouped":     grouped,
        "insiders_total":       len(filings_trades),
        "insiders_total_str":   f"{len(filings_trades):,}",
        "insiders_notable":     _insiders_notable(rows),
        "insiders_momentum":    momentum_chart,
        # Active filter state
        "insiders_direction":   direction,
        "insiders_directions":  [(k, v["label"]) for k, v in _INSIDERS_DIRECTIONS.items()],
        "insiders_role":        role,
        "insiders_role_keys":   _INSIDERS_ROLE_KEYS,
        "insiders_plan":        plan,
        "insiders_plan_keys":   _INSIDERS_PLAN_KEYS,
        # Clusters tab (independent of Filings filter)
        "insiders_clusters":    clusters_panel,
        "insiders_cluster":     cluster,
        "insiders_cluster_keys": _INSIDERS_CLUSTER_KEYS,
        "insiders_clusters_buy_count":  clusters_buy_total,
        "insiders_clusters_sell_count": clusters_sell_total,
        "insiders_clusters_all_count":  len(clusters_all),
        "insiders_tab":         "Filings",
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
        return d.strftime("%b %d %Y")
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


# Mock illustrative data for the still-unwired tabs.  These tabs need a
# user-prefs / billing schema we don't have yet — the design-faithful
# tables render real-shape rows so the page feels complete to a viewer
# without lying about what's actually wired.
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


_PROFILE_TABS = ("Watchlist", "Alerts", "Account", "Subscription")


@router.get("/profile", response_class=HTMLResponse)
async def preview_profile(request: Request, tab: str = "Watchlist"):
    """Profile page — all 4 tabs render content on the initial load.

    Watchlist is live; Alerts / Account / Subscription render design-faithful
    illustrative data until the user-prefs / billing schemas are in place.

    ``?tab=`` deep-links the active tab so other pages (sidebar Watchlist,
    topbar +Alert) can land on the right pane on load.
    """
    rows = await _fetch_profile_watchlist()
    if tab not in _PROFILE_TABS:
        tab = "Watchlist"

    # No "Profile" sidebar item — Watchlist is the closest entry-point so
    # highlight it whenever any Profile tab is in view.
    ctx = {
        "request":    request,
        **(await _shell_context(request, "Watchlist")),
        "user":       _PROFILE_USER,
        "watch_lists": [
            (label, len(rows) if i == 0 else 0)
            for i, label in enumerate(_PROFILE_LISTS_MOCK)
        ],
        "watch_active": _PROFILE_LISTS_MOCK[0],
        "watch_rows": rows,
        "watch_empty": len(rows) == 0,
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


# ─────────────────────────────────────────────────────────────────────────────
# NOTIFICATIONS — chronological feed of recent platform notifications.
# Mirrors the v1 /notifications page but in the v2 design language: filter
# chips at the top + inbox-style row list.  Reads from supabase_cache.
# ─────────────────────────────────────────────────────────────────────────────

_NOTIF_PAGE_TYPE_META: dict[str, dict[str, str]] = {
    "13f_change":      {"label": "13F",        "color": "#2563eb"},
    "youtube":         {"label": "YouTube",    "color": "#dc2626"},
    "reddit_velocity": {"label": "Reddit",     "color": "#f97316"},
    "congress_trade":  {"label": "Congress",   "color": "#059669"},
    "insider_trade":   {"label": "Insider",    "color": "#7c3aed"},
    "feature_release": {"label": "New feature","color": "#0ea5e9"},
}


def _time_ago_v2(iso_str: str) -> str:
    """Same shape as web._time_ago — duplicated so the redesign router stays
    importable without pulling all of web.py."""
    if not iso_str:
        return ""
    try:
        from datetime import datetime as _dt, timezone as _tz
        dt = _dt.fromisoformat(iso_str.replace("Z", "+00:00"))
        diff = _dt.now(_tz.utc) - dt
        seconds = int(diff.total_seconds())
        if seconds < 60:
            return "just now"
        minutes = seconds // 60
        if minutes < 60:
            return f"{minutes}m ago"
        hours = minutes // 60
        if hours < 24:
            return f"{hours}h ago"
        days = hours // 24
        if days < 7:
            return f"{days}d ago"
        return f"{days // 7}w ago"
    except Exception:
        return ""


@router.get("/notifications", response_class=HTMLResponse)
async def preview_notifications(request: Request, types: str = "", page: int = 1):
    """Notifications activity feed — chronological list with type filters."""
    valid_types = list(_NOTIF_PAGE_TYPE_META.keys())
    active_types: list[str] = []
    if types.strip():
        active_types = [t.strip() for t in types.split(",") if t.strip() in valid_types]

    page = max(int(page or 1), 1)
    per_page = 30
    offset = (page - 1) * per_page

    try:
        from filings import supabase_cache
        notifs = await to_heavy(
            supabase_cache.get_recent_notifications,
            per_page + 1,
            active_types or None,
            offset,
        )
    except Exception as exc:
        logger.warning("Notifications fetch failed: %s", exc)
        notifs = []

    has_next = len(notifs) > per_page
    notifs = notifs[:per_page]

    rows: list[dict] = []
    for n in notifs:
        ntype = n.get("type", "")
        meta = _NOTIF_PAGE_TYPE_META.get(ntype, {"label": ntype.title(), "color": "var(--pp-dim)"})
        ticker = ((n.get("metadata") or {}).get("ticker") or "").upper()
        rows.append({
            "id":        n.get("id"),
            "type":      ntype,
            "type_label": meta["label"],
            "type_color": meta["color"],
            "title":     n.get("title", ""),
            "message":   n.get("message", ""),
            "icon":      n.get("icon", "•"),
            "link":      n.get("link", ""),
            "ticker":    ticker,
            "time_ago":  _time_ago_v2(n.get("created_at", "")),
            "created_at": n.get("created_at", ""),
        })

    # Group by day for the timeline rail.
    from collections import OrderedDict
    by_day: "OrderedDict[str, list[dict]]" = OrderedDict()
    for r in rows:
        d_label = "Earlier"
        try:
            d = datetime.fromisoformat((r["created_at"] or "").replace("Z", "+00:00"))
            now = datetime.now(d.tzinfo) if d.tzinfo else datetime.now()
            delta_days = (now.date() - d.date()).days
            if delta_days == 0:
                d_label = "Today"
            elif delta_days == 1:
                d_label = "Yesterday"
            else:
                d_label = d.strftime("%a, %b %-d")
        except Exception:
            pass
        by_day.setdefault(d_label, []).append(r)

    shell = await _shell_context(request, "Notifications")
    # Visiting this page IS the "I've seen these" event — clear the badge
    # for the current render so the user doesn't see a stale count next
    # to the page they're already on.
    shell["notif_unread"] = 0

    ctx = {
        "request":         request,
        **shell,
        "notif_types":     [
            {"key": k, "label": _NOTIF_PAGE_TYPE_META[k]["label"], "color": _NOTIF_PAGE_TYPE_META[k]["color"]}
            for k in valid_types
        ],
        "notif_active_types": active_types,
        "notif_groups":    list(by_day.items()),
        "notif_total":     len(rows),
        "notif_page":      page,
        "notif_has_next":  has_next,
        "notif_has_prev":  page > 1,
    }
    response = templates.TemplateResponse("_redesign/notifications.html", ctx)
    # Persist the visit so subsequent page renders count only notifications
    # that arrive AFTER this moment.  Cookie is per-browser, no auth needed.
    response.set_cookie(
        _SHELL_NOTIF_COOKIE,
        datetime.now(timezone.utc).isoformat(),
        max_age=_SHELL_NOTIF_COOKIE_MAX_AGE,
        path="/",
        samesite="lax",
    )
    return response


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


# ── Retail Trends tab — Google Trends multi-line ──────────────────────────

# Colour cycle for the Trends multi-line chart (matches the design's accent /
# up / ink / dim sequence so series stay distinguishable on dark + light).
_TRENDS_LINE_COLORS = ["var(--pp-accent)", "var(--pp-up)", "var(--pp-ink)", "var(--pp-dim)"]
_TRENDS_DEFAULT_TICKERS = ["NVDA", "GME", "TSLA", "AAPL"]


def _retail_trends_chart_compute() -> dict | None:
    """Synchronous: pull 90-day Google Trends interest for the default basket
    and produce SVG-ready multi-series chart data.

    Returns ``None`` on upstream failure so the L2 cache doesn't poison
    itself with an empty payload — the bounded route fallback handles the
    rendering path. ``{"have_data": False, ...}`` is reserved for a
    successful fetch that yielded no usable points.

    Wrapped by `_retail_trends_chart` (L2-cached) — pytrends has tight
    rate limits and the 4 keyword set takes ~6-15s on a cold path.
    """
    try:
        from filings import google_trends
    except Exception:
        return None

    try:
        bundle = google_trends.fetch_interest_over_time(
            _TRENDS_DEFAULT_TICKERS,
            timeframe="today 3-m",
            geo="US",
        )
    except Exception as exc:
        logger.warning("Retail trends compute failed: %s", exc)
        return None

    if not bundle or not bundle.get("data"):
        return None

    keywords = bundle.get("keywords") or []
    points = bundle.get("data") or []          # [{date, values: {kw: int}}, ...]
    if not points:
        return {"have_data": False, "series": []}

    # ViewBox 1500×260 (~5.8:1) — matches the typical full-width container
    # ratio so `preserveAspectRatio="none"` is near-identity.
    width, height = 1500.0, 260.0
    pad_top, pad_bot = 15.0, 25.0
    plot_h = height - pad_top - pad_bot
    n = len(points)

    # Build per-keyword score arrays in the order keywords were returned.
    by_kw: dict[str, list[int]] = {kw: [] for kw in keywords}
    for p in points:
        vals = p.get("values") or {}
        for kw in keywords:
            by_kw[kw].append(int(vals.get(kw) or 0))

    # Trends scores are 0-100 globally — fix the y-range so the lines stay
    # comparable across keywords.
    series: list[dict] = []
    # Per-keyword screen-y arrays (parallel to `points`) so the JS hover
    # layer can plot a dot on every line at the active x.
    series_screen_ys: list[list[float]] = []
    for i, kw in enumerate(keywords):
        ys = by_kw[kw]
        if not ys:
            continue
        avg = sum(ys) / max(len(ys), 1)
        last = ys[-1]
        first_nonzero = next((y for y in ys if y > 0), ys[0])
        delta = ((last - first_nonzero) / first_nonzero * 100) if first_nonzero else 0.0
        d_path = []
        screen_ys: list[float] = []
        for j, y in enumerate(ys):
            sx = (j / max(n - 1, 1)) * width
            sy = pad_top + (1 - y / 100.0) * plot_h
            d_path.append(("M" if j == 0 else "L") + f"{sx:.1f} {sy:.1f}")
            screen_ys.append(round(sy, 1))
        series.append({
            "name":  kw,
            "color": _TRENDS_LINE_COLORS[i % len(_TRENDS_LINE_COLORS)],
            "line":  " ".join(d_path),
            "avg":   round(avg, 1),
            "last":  last,
            "delta_pct_str": f"{delta:+.0f}%" if delta else "—",
            "delta_up": delta >= 0,
        })
        series_screen_ys.append(screen_ys)

    # Per-x hover history: every point gets the date + value for every
    # keyword.  JS uses nearest-x to find the active index and renders a
    # dot per series + a multi-line tooltip.
    chart_history: list[dict] = []
    for j, p in enumerate(points):
        sx = round((j / max(n - 1, 1)) * width, 1)
        date_str = p.get("date") or ""
        try:
            from datetime import datetime as _dt
            date_str = _dt.strptime(date_str, "%b %d, %Y").strftime("%b %d %Y")
        except Exception:
            pass
        kw_values = []
        for s_idx, s in enumerate(series):
            kw = s["name"]
            v = (p.get("values") or {}).get(kw) or 0
            kw_values.append({
                "name":  kw,
                "color": s["color"],
                "value": int(v),
                "y":     series_screen_ys[s_idx][j] if j < len(series_screen_ys[s_idx]) else 0,
            })
        chart_history.append({"x": sx, "date": date_str, "kws": kw_values})

    # First / mid / last x-axis date labels (MMM d).
    def _label(idx: int) -> str:
        d = points[idx].get("date") or ""
        try:
            from datetime import datetime as _dt
            return _dt.strptime(d, "%b %d, %Y").strftime("%b %d")
        except Exception:
            return (d.split(",")[0]).strip() or ""

    if n >= 3:
        ticks = [_label(0), _label(n // 2), _label(n - 1)]
    else:
        ticks = [_label(0)] if n else []

    return {
        "have_data": True,
        "series":    series,
        "ticks":     ticks,
        "n":         n,
        "as_of":     bundle.get("fetched_at", ""),
        "vb_width":  width,
        "vb_height": height,
        "chart_history": chart_history,
    }


async def _retail_trends_chart() -> dict:
    """L2-cached wrapper — Google Trends payload is stable for hours."""
    return await _l2_cached(
        key="redesign:retail:trends_chart:v1",
        ttl_seconds=2 * 3600,
        compute=_retail_trends_chart_compute,
        category="redesign_retail",
    ) or {"have_data": False, "series": []}


# ── Retail WSB tab — index, distribution, top-ticker table ────────────────

def _retail_wsb_panel(top_rows: list[dict] | None) -> dict:
    """Build the WSB hero index + top-tickers table from get_wsb_top() rows."""
    rows = top_rows or []
    if not rows:
        return {"have_data": False, "rows": [], "index": 0, "dist": {}, "total_posts": 0}

    # Sentiment categorical → numeric score for the index aggregate.
    score_for = {"Bullish": 1.0, "Neutral": 0.0, "Bearish": -1.0}
    weighted_sum = 0.0
    weighted_total = 0.0
    counts = {"Bullish": 0, "Neutral": 0, "Bearish": 0}
    for r in rows:
        m = int(r.get("mentions") or 0)
        s = score_for.get(r.get("sentiment", "Neutral"), 0.0)
        weighted_sum += s * m
        weighted_total += m
        counts[r.get("sentiment", "Neutral")] = counts.get(r.get("sentiment", "Neutral"), 0) + 1
    weighted_avg = (weighted_sum / weighted_total) if weighted_total else 0.0
    # 0-100 index — design shows "+71" style.
    index_val = int(round(weighted_avg * 100))

    # Distribution percentages (counts of tickers, not mentions — easier to read).
    n_total = sum(counts.values()) or 1
    dist = {
        "bullish_pct": round(counts["Bullish"] / n_total * 100),
        "neutral_pct": round(counts["Neutral"] / n_total * 100),
        "bearish_pct": round(counts["Bearish"] / n_total * 100),
    }

    table_rows: list[dict] = []
    # Sort by mentions desc.
    for i, r in enumerate(sorted(rows, key=lambda x: int(x.get("mentions") or 0), reverse=True)[:12], start=1):
        m = int(r.get("mentions") or 0)
        u = int(r.get("upvotes") or 0)
        # Upvotes-per-mention ratio — proxy for "calls/puts" sentiment depth.
        ratio = u / m if m else 0
        # Map ratio onto a -100..+100 score for the chip.
        score = max(-100, min(100, int(round((ratio - 10) * 8))))
        sentiment_label = r.get("sentiment", "Neutral")
        table_rows.append({
            "rank":       i,
            "ticker":     (r.get("ticker") or "").upper(),
            "name":       r.get("name") or "",
            "posts":      m,
            "posts_str":  f"{m:,}",
            "upvotes":    u,
            "upvotes_str": f"{u:,}",
            "ratio":      ratio,
            "ratio_str":  f"{ratio:.1f}x",
            "ratio_pct":  min(int(ratio / 25 * 100), 100),
            "sentiment_label": sentiment_label,
            "score":      score,
            "score_str":  (f"+{score}" if score >= 0 else f"{score}"),
            "score_up":   score >= 0,
        })

    return {
        "have_data":    True,
        "index":        index_val,
        "index_str":    (f"+{index_val}" if index_val >= 0 else f"{index_val}"),
        "index_up":     index_val >= 0,
        "label":        ("Strongly bullish" if index_val >= 50
                         else "Bullish" if index_val >= 20
                         else "Neutral" if index_val >= -20
                         else "Bearish" if index_val >= -50
                         else "Strongly bearish"),
        "dist":         dist,
        "total_posts":  sum(int(r.get("mentions") or 0) for r in rows),
        "rows":         table_rows,
    }


_RETAIL_FG_BAND_LABELS = {
    "extreme_fear": "Extreme Fear", "fear": "Fear",
    "neutral": "Neutral",
    "greed": "Greed", "extreme_greed": "Extreme Greed",
}


def _retail_kpi_strip_v2(apewisdom: list[dict], fear_greed: dict | None) -> list[dict]:
    """Top KPI strip — six headline retail metrics."""
    total_mentions = sum(int(r.get("mentions") or 0) for r in apewisdom)
    total_upvotes  = sum(int(r.get("upvotes") or 0) for r in apewisdom)
    fg_score = fear_greed.get("score") if fear_greed else None
    fg_label = (fear_greed.get("rating") or "").title() if fear_greed else "—"
    bullish_count = sum(
        1 for r in apewisdom
        if int(r.get("mentions") or 0) > 0
        and int(r.get("upvotes") or 0) / max(int(r.get("mentions") or 1), 1) > 5
    )
    return [
        {"label": "Tickers tracked",   "value": f"{len(apewisdom):,}",      "delta": None, "up": None},
        {"label": "Total mentions",    "value": f"{total_mentions:,}",       "delta": None, "up": None},
        {"label": "Total upvotes",     "value": f"{total_upvotes:,}",        "delta": None, "up": None},
        {"label": "Bullish (>5 upv/m)", "value": f"{bullish_count}",         "delta": None, "up": None},
        {"label": "Fear & Greed",      "value": (str(int(fg_score)) if isinstance(fg_score, (int, float)) else "—"),
         "delta": fg_label or None, "up": None},
        {"label": "Market mood",       "value": fg_label or "—",             "delta": None, "up": None},
    ]


def _retail_sentiment_payload(apewisdom: list[dict], fear_greed: dict | None) -> dict:
    """Sentiment tab payload — Market Mood gauge + 3 callout cards."""
    # CNN Fear & Greed gauge data + four reference points.
    fg = None
    if fear_greed:
        def _fg_int(v):
            """Coerce CNN's float scores → display-ready integers."""
            try:
                return int(round(float(v))) if v is not None else None
            except (TypeError, ValueError):
                return None

        score_int = _fg_int(fear_greed.get("score"))
        rating = (fear_greed.get("rating") or "").lower().replace(" ", "_")
        fg = {
            "score":       score_int,
            "score_str":   f"{score_int}" if score_int is not None else "—",
            "rating":      _RETAIL_FG_BAND_LABELS.get(rating, (fear_greed.get("rating") or "—").title()),
            "rating_key":  rating or "neutral",
            "marker_pct":  max(0.0, min(100.0, float(score_int))) if score_int is not None else 50.0,
            "previous_close":  _fg_int(fear_greed.get("previous_close")),
            "one_week_ago":    _fg_int(fear_greed.get("one_week_ago")),
            "one_month_ago":   _fg_int(fear_greed.get("one_month_ago")),
            "one_year_ago":    _fg_int(fear_greed.get("one_year_ago")),
        }

    # Most mentioned — top by raw mention count.
    most_mentioned = None
    if apewisdom:
        top = sorted(apewisdom, key=lambda r: int(r.get("mentions") or 0), reverse=True)[0]
        most_mentioned = {
            "ticker":       (top.get("ticker") or "").upper(),
            "name":         top.get("name") or "",
            "mentions":     int(top.get("mentions") or 0),
            "mentions_str": f"{int(top.get('mentions') or 0):,}",
            "upvotes":      int(top.get("upvotes") or 0),
            "upvotes_str":  f"{int(top.get('upvotes') or 0):,}",
        }

    # Biggest rank mover — largest absolute rank improvement (rank_24h_ago - rank).
    biggest_mover = None
    if apewisdom:
        candidates = []
        for r in apewisdom:
            rank = int(r.get("rank") or 0)
            r24  = r.get("rank_24h_ago")
            if rank and r24:
                try:
                    delta = int(r24) - rank   # positive = rose in rank
                    candidates.append((abs(delta), delta, r))
                except (TypeError, ValueError):
                    continue
        if candidates:
            candidates.sort(key=lambda c: c[0], reverse=True)
            _abs, delta, top = candidates[0]
            biggest_mover = {
                "ticker":   (top.get("ticker") or "").upper(),
                "name":     top.get("name") or "",
                "delta":    delta,
                "delta_str": f"+{delta}" if delta > 0 else str(delta),
                "delta_up": delta > 0,
                "rank":     int(top.get("rank") or 0),
                "rank_24h": int(top.get("rank_24h_ago") or 0),
            }

    # Top 5 trending — first 5 by rank.
    top_trending = []
    for r in sorted(apewisdom or [], key=lambda x: int(x.get("rank") or 999))[:5]:
        top_trending.append({
            "ticker": (r.get("ticker") or "").upper(),
            "name":   r.get("name") or "",
        })

    return {
        "fear_greed":     fg,
        "most_mentioned": most_mentioned,
        "biggest_mover":  biggest_mover,
        "top_trending":   top_trending,
    }


def _retail_velocity_color(velocity_pct: float) -> str:
    """Map % velocity → CSS variable name for v2 design tokens.  Mirrors
    the v1 ``_velocity_to_color`` semantics but yields token names rather
    than raw hex so dark/light themes both resolve correctly."""
    if   velocity_pct >= 100: return "var(--pp-up)"
    elif velocity_pct >= 30:  return "rgba(35, 162, 110, 0.65)"
    elif velocity_pct >= 0:   return "rgba(35, 162, 110, 0.3)"
    elif velocity_pct >= -30: return "rgba(220, 38, 38, 0.35)"
    else:                     return "var(--pp-down)"


def _squarify_treemap(items: list[dict], width: float = 100.0, height: float = 100.0) -> list[dict]:
    """Squarified treemap layout (Bruls/Huijsen/van Wijk 2000).

    Packs `items` into a rectangle of `width × height` while keeping each
    box's aspect ratio as close to 1:1 as possible.  Coordinates are
    emitted in the same unit as the input dimensions — pass 100 to get
    percentages ready for HTML `style="left: X%; top: Y%; …"`.

    Each item must expose a positive `value`.  Output preserves every
    field of the input dicts plus `x`, `y`, `w`, `h`.
    """
    if not items:
        return []
    sorted_items = sorted(items, key=lambda d: -max(d.get("value", 0), 1))
    total_v = sum(max(it.get("value", 0), 1) for it in sorted_items) or 1
    scale = (width * height) / total_v

    def _row_worst_aspect(row: list[dict], side: float) -> float:
        if not row or side <= 0:
            return float("inf")
        s = sum(max(it.get("value", 0), 1) for it in row) * scale
        if s <= 0:
            return float("inf")
        thick = s / side
        worst = 1.0
        for it in row:
            long_edge = max(max(it.get("value", 0), 1) * scale / max(thick, 1e-9), 1e-9)
            ratio = max(thick / long_edge, long_edge / thick)
            if ratio > worst:
                worst = ratio
        return worst

    boxes: list[dict] = []
    queue = list(sorted_items)
    x, y, w, h = 0.0, 0.0, width, height

    while queue:
        side = min(w, h)
        if side <= 0:
            break
        row: list[dict] = []
        # Greedy: keep adding items while the worst aspect ratio is improving.
        while queue:
            cand = row + [queue[0]]
            if not row or _row_worst_aspect(cand, side) <= _row_worst_aspect(row, side):
                row = cand
                queue.pop(0)
            else:
                break
        if not row:
            break

        row_v = sum(max(it.get("value", 0), 1) for it in row) or 1
        if w >= h:
            # Long axis is horizontal — lay row out as a vertical strip on the left.
            thick = (row_v * scale) / h
            cy = y
            for it in row:
                bh = max(it.get("value", 0), 1) * h / row_v
                boxes.append({**it, "x": x, "y": cy, "w": thick, "h": bh})
                cy += bh
            x += thick
            w -= thick
        else:
            # Long axis is vertical — lay row out as a horizontal strip on top.
            thick = (row_v * scale) / w
            cx = x
            for it in row:
                bw = max(it.get("value", 0), 1) * w / row_v
                boxes.append({**it, "x": cx, "y": y, "w": bw, "h": thick})
                cx += bw
            y += thick
            h -= thick

    return boxes


def _retail_leaderboard_payload(lb: dict) -> dict:
    """Leaderboard tab payload — pre-computes treemap geometry, scatter
    bubble coords, and an enriched leaderboard table.

    Treemap uses the squarified algorithm (boxes get near-1:1 aspect
    ratios).  Bubble chart geometry is computed in viewBox space so the
    SVG renders without a JS library — keeps the page lightweight and
    consistent with v2 charts.
    """
    rows  = lb.get("leaderboard_rows") or []
    treem = lb.get("treemap_data") or []
    bub   = lb.get("bubble_data") or []
    meta  = lb.get("metadata") or {}

    # ── Treemap geometry: squarified, % units (HTML-positioned boxes).
    boxes: list[dict] = []
    if treem:
        layout = _squarify_treemap(treem, width=100.0, height=100.0)
        for d in layout:
            boxes.append({
                "ticker":   d.get("name") or "",
                "value":    d.get("value", 0),
                # Round to 2 decimals — keeps the inline-style strings short.
                "x":        round(d["x"], 2),
                "y":        round(d["y"], 2),
                "w":        round(d["w"], 2),
                "h":        round(d["h"], 2),
                "color":    _retail_velocity_color(d.get("velocity_pct", 0)),
                "mentions": d.get("mentions", 0),
                "velocity_pct": d.get("velocity_pct", 0),
                "engagement_ratio": d.get("engagement_ratio", 0),
                "guru_count": d.get("guru_count", 0),
            })

    # ── Bubble chart geometry.
    # x = engagement (upv/m), y = velocity (%).  Auto-scale to data extents.
    bub_w, bub_h = 760.0, 460.0
    pad_l, pad_r, pad_t, pad_b = 64.0, 18.0, 18.0, 36.0
    plot_w = bub_w - pad_l - pad_r
    plot_h = bub_h - pad_t - pad_b
    bubbles: list[dict] = []
    if bub:
        xs = [b.get("x", 0) for b in bub]
        ys = [b.get("y", 0) for b in bub]
        rs = [max(b.get("r", 0), 1) for b in bub]
        x_min, x_max = (min(xs), max(xs))
        y_min, y_max = (min(ys), max(ys))
        r_max = max(rs) or 1
        # Pad ranges 10% so bubbles aren't cropped at the edges.
        x_pad = (x_max - x_min) * 0.1 or 1
        y_pad = (y_max - y_min) * 0.1 or 1
        x_lo, x_hi = x_min - x_pad, x_max + x_pad
        y_lo, y_hi = y_min - y_pad, y_max + y_pad
        x_rng = (x_hi - x_lo) or 1
        y_rng = (y_hi - y_lo) or 1
        for b in bub:
            sx = pad_l + ((b.get("x", 0) - x_lo) / x_rng) * plot_w
            sy = pad_t + (1 - (b.get("y", 0) - y_lo) / y_rng) * plot_h
            radius = 6 + (max(b.get("r", 0), 1) / r_max) * 22
            bubbles.append({
                "ticker": b.get("ticker") or "",
                "name":   b.get("name") or "",
                "cx":     round(sx, 1),
                "cy":     round(sy, 1),
                "r":      round(radius, 1),
                "x":      b.get("x", 0),
                "y":      b.get("y", 0),
                "size":   b.get("r", 0),
                "guru_count": b.get("guru_count", 0),
                "rank":   b.get("rank", 0),
                "is_guru": (b.get("guru_count") or 0) > 0,
            })

    # ── Velocity table — keep top 50 to render server-side; full 500 not needed.
    table = []
    for r in rows[:50]:
        rc = r.get("rank_change", 0)
        table.append({
            **r,
            "rank_change":     rc,
            "rank_change_str": (f"+{rc}" if rc > 0 else (f"{rc}" if rc < 0 else "—")),
            "rank_change_up":  rc > 0,
            "velocity_str":    f"{r.get('velocity_pct', 0):+.1f}%",
            "velocity_up":     r.get("velocity_pct", 0) >= 0,
            "mentions_str":    f"{r.get('mentions', 0):,}",
            "engagement_str":  f"{r.get('engagement_ratio', 0):.1f}",
        })

    return {
        "treemap_boxes": boxes,
        "bubbles":       bubbles,
        "bubble_vb_w":   bub_w,
        "bubble_vb_h":   bub_h,
        "bubble_pad":    {"left": pad_l, "right": pad_r, "top": pad_t, "bot": pad_b},
        "table":         table,
        "total_count":   meta.get("count", 0),
        "timestamp":     meta.get("timestamp", ""),
        "market_mood":   meta.get("market_mood"),
        "market_score":  meta.get("market_score"),
    }


def _retail_calendar_payload(uploads: list[dict], channels: list[dict]) -> dict:
    """Calendar tab payload — recent YouTube uploads grid + channel directory."""
    from datetime import datetime, timezone

    def _fmt_relative(ts: str | None) -> str:
        if not ts:
            return ""
        try:
            dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            delta = datetime.now(timezone.utc) - dt
            secs = max(int(delta.total_seconds()), 0)
            if secs < 3600:        return f"{secs // 60}m ago"
            if secs < 86400:       return f"{secs // 3600}h ago"
            return f"{secs // 86400}d ago"
        except Exception:
            return ""

    upload_rows = []
    for u in uploads or []:
        tickers = u.get("tickers") or []
        # Tickers can land as list, comma-joined string, or null — normalise.
        if isinstance(tickers, str):
            tickers = [t.strip() for t in tickers.split(",") if t.strip()]
        upload_rows.append({
            "video_id":      u.get("video_id"),
            "title":         u.get("title") or "",
            "channel_name":  u.get("channel_name") or "",
            "channel_id":    u.get("channel_id"),
            "thumbnail_url": u.get("thumbnail_url") or "",
            "video_url":     u.get("video_url") or (
                f"https://www.youtube.com/watch?v={u.get('video_id')}" if u.get("video_id") else ""
            ),
            "ago_str":       _fmt_relative(u.get("scheduled_at")),
            "tickers":       tickers[:1],  # one chip per card for the v1 look
        })

    channel_rows = []
    for c in channels or []:
        subs = c.get("subscriber_count") or 0
        posts = c.get("avg_posts_per_week")
        channel_rows.append({
            "channel_id":    c.get("channel_id"),
            "channel_name":  c.get("channel_name") or "",
            "thumbnail_url": c.get("thumbnail_url") or "",
            "handle":        c.get("handle") or "",
            "subscribers":   int(subs),
            "subs_str":      f"{int(subs):,}" if subs else "—",
            "posts_str":     (f"{float(posts):.1f}" if posts is not None else "—"),
        })

    return {
        "uploads":      upload_rows[:12],   # cap visible grid at 12
        "uploads_total": len(upload_rows),
        "channels":     channel_rows,
        "channels_total": len(channel_rows),
    }


_RETAIL_VIEWS = ("sentiment", "leaderboard", "calendar")


def _retail_ticker_map_compute() -> dict:
    """Build the {ticker → [guru names]} map for the retail leaderboard.
    Sync compute fn — `_l2_cached` runs it in a worker thread and shares
    the result across uvicorn workers via Supabase, so we never amplify
    the heavy `load_cache_from_supabase` call across multiple workers.
    """
    from filings import cache as _cache, client as _client
    from filings.superinvestors import SUPERINVESTORS_BY_CIK
    fund_cache = _cache.load_cache_from_supabase() or {}
    return _client.build_ticker_ownership_map(fund_cache, SUPERINVESTORS_BY_CIK) or {}


@router.get("/retail", response_class=HTMLResponse)
async def preview_retail(request: Request, view: str = "sentiment"):
    """Retail page — three tabs (Sentiment / Leaderboard / Calendar), wired
    through the existing v1 helpers in :mod:`filings.sentiment` and
    :mod:`filings.youtube_cache`.

    Sentiment   — CNN Fear & Greed gauge + 3 callout cards (most-mentioned,
                  biggest rank mover, top-5 trending).
    Leaderboard — Reddit velocity heatmap + hype-vs-quality scatter + table.
    Calendar    — Recent YouTube uploads (48h) + finance-channel directory.
    """
    if view not in _RETAIL_VIEWS:
        view = "sentiment"

    bounded = functools.partial(_bounded, page="Retail page")
    from filings import sentiment as _sent
    from filings import youtube_cache as _yt

    # 5-way fan-out — every fetch is L2-cached.  `ticker_map` is the
    # Supabase-backed {ticker → guru names} overlay; first cold hit pays
    # ~5s, subsequent hits across all uvicorn workers are instant.
    apewisdom_data, fear_greed, yt_uploads, yt_channels, ticker_map = await asyncio.gather(
        bounded(to_heavy(_sent._get_apewisdom_all),     timeout=6.0, fallback=[],       name="apewisdom"),
        bounded(to_heavy(_sent._get_cnn_fear_greed),    timeout=4.0, fallback=None,     name="fear_greed"),
        bounded(to_heavy(_yt.get_recent_youtube_uploads, 50),
                                                        timeout=4.0, fallback=[],       name="yt_uploads"),
        bounded(to_heavy(_yt.get_youtube_channels),     timeout=4.0, fallback=[],       name="yt_channels"),
        bounded(
            _l2_cached(
                "redesign:retail:ticker_map_v1",
                ttl_seconds=3600,
                compute=_retail_ticker_map_compute,
                category="redesign_retail",
            ),
            timeout=10.0, fallback={}, name="ticker_map",
        ),
    )

    # `_sent.build_retail_leaderboard_data` has its own 30-min L1 cache that
    # ignores its arguments.  If we just got a real ticker_map but the L1
    # cache was filled earlier with an empty one, drop it so the next call
    # rebuilds with gurus.  (Encapsulation-violating cache poke; the proper
    # fix is teaching `build_retail_leaderboard_data` to key on its inputs.)
    if ticker_map:
        old = getattr(_sent, "_leaderboard_cache", None)
        if old:
            try:
                _ts, prev = old
                if not any((r.get("guru_count") or 0) > 0 for r in (prev.get("leaderboard_rows") or [])):
                    _sent._leaderboard_cache = None
            except Exception as exc:
                logger.warning("Retail page: leaderboard cache bust failed: %s", exc)

    leaderboard = await to_light(
        _sent.build_retail_leaderboard_data,
        apewisdom_data or [], ticker_map, fear_greed,
    )

    sentiment_ctx  = _retail_sentiment_payload(apewisdom_data or [], fear_greed)
    leaderboard_ctx = _retail_leaderboard_payload(leaderboard)
    calendar_ctx    = _retail_calendar_payload(yt_uploads or [], yt_channels or [])

    ctx = {
        "request":         request,
        **(await _shell_context(request, "Retail")),
        "retail_view":     view,
        "retail_kpi":      _retail_kpi_strip_v2(apewisdom_data or [], fear_greed),
        # Per-tab payloads
        "sentiment":       sentiment_ctx,
        "leaderboard":     leaderboard_ctx,
        "calendar":        calendar_ctx,
    }
    return templates.TemplateResponse("_redesign/retail.html", ctx)


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


async def _v2_events_calendar_payload(
    period: str = "this_week",
    country: str = "us",
    impact_filter: str = "all",
) -> dict:
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
        return {"have_data": False}

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
    """Map _compute_metrics() → 4-cell KPI strip."""
    if not metrics:
        return []
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


async def _v2_macro_earnings_payload(
    index: str, quarter: str | None, sector: str | None,
) -> dict:
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

async def _v2_macro_calendar_payload(
    request: Request, index: str, period: str,
) -> dict:
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


def _perf_momentum_chart(ad_line: dict) -> dict:
    """Dual-axis line chart: cumulative A/D vs index price (last 60 trading days)."""
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


async def _v2_macro_performance_payload(index: str, period: str) -> dict:
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
                await to_light(
                    supabase_cache.set_cached, _SENTIMENT_L2_KEY,
                    "macro", payload, ttl_seconds=_SENTIMENT_L2_TTL,
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
        cached, _is_fresh = await to_light(supabase_cache.get_cached_with_stale, _SENTIMENT_L2_KEY)
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


async def _v2_volatility_payload(ratio_type: str = "total") -> dict:
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
    from filings import market_data   as _market_data

    # All upstream data fetches dispatch in parallel.  Heatmap rows used
    # to live in a second sequential gather; folded into this one so the
    # worst-case wait is max(timeouts) instead of sum-of-the-two-blocks.
    # /macro went from ~24s p99 to ~12s p99 with this change.  None of
    # the heatmap inputs read from the other fetches' results, so the
    # merge is data-flow safe.
    (
        payload, yield_curve, fx_payload, idx_payload, calendar_rows,
        earnings_payload, calendar_payload, performance_payload,
        events_payload, debt_payload, volatility_payload, sentiment_payload,
        heatmap_companies_rows, heatmap_sectors_rows,
    ) = await asyncio.gather(
        bounded(_fetch_macro_indicators(),                          timeout=8.0, fallback={},   name="indicators"),
        bounded(to_heavy(_treasury_data.get_yield_curve),           timeout=6.0, fallback=None, name="yield_curve"),
        bounded(to_heavy(_frankfurter.get_fx_dashboard),            timeout=6.0, fallback=None, name="fx"),
        bounded(to_heavy(_market_data.get_index_market_data),       timeout=6.0, fallback=None, name="index_md"),
        bounded(_macro_calendar_rows(top_n=12),                     timeout=5.0, fallback=[],   name="calendar"),
        bounded(_v2_macro_earnings_payload(earn_index, earn_quarter or None, earn_sector or None),
                timeout=10.0, fallback={"have_data": False}, name="earnings"),
        bounded(_v2_macro_calendar_payload(request, cal_index, cal_period),
                timeout=8.0,  fallback={"have_data": False}, name="ecal"),
        bounded(_v2_macro_performance_payload(perf_index, perf_period),
                timeout=10.0, fallback={"have_data": False}, name="performance"),
        bounded(_v2_events_calendar_payload(ev_period, ev_country, ev_impact),
                timeout=6.0,  fallback={"have_data": False}, name="events"),
        bounded(to_heavy(_treasury_data.get_debt_data),             timeout=6.0, fallback=None, name="debt"),
        bounded(_v2_volatility_payload(pc_type),
                timeout=12.0, fallback={"have_data": False}, name="volatility"),
        # Sentiment is L2-only on the request path — slow Google Trends
        # fetch runs in a background task, so this should always be ~10ms.
        # Bumped from 2s after the simplify-pass review: a regional
        # Supabase blip can spike past 2s and would spuriously trigger
        # the warmer + show "warming up" placeholders even when data
        # exists.  4s is still well under any user-perceived stall.
        bounded(_v2_sentiment_payload(),
                timeout=4.0,  fallback={"have_data": False, "cards": [],
                                        "categories": [], "is_warming": True}, name="sentiment"),
        # Heatmap — same data + visual treatment as the homepage so the
        # macro tab and home tab read as the same tool.  Both fetchers
        # L2-cache + share the underlying yfinance batch within the 5-min
        # TTL window.
        bounded(_fetch_home_heatmap_companies(), timeout=12.0, fallback=[],
                name="heatmap_companies"),
        bounded(_fetch_home_heatmap_sectors(),   timeout=8.0,  fallback=[],
                name="heatmap_sectors_etf"),
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
        "heatmap_companies": heatmap_companies_rows,
        "heatmap_sectors":   heatmap_sectors_rows,
        "heatmap_indices":   _heatmap_global_indices(idx_payload),
        "macro_tab":        active_tab,
        "earn":             earnings_payload,
        "ecal":             calendar_payload,
        "perf":             performance_payload,
        "events":           events_payload,
        "ev_view":          ev_view,
        "earn_view":        earn_view,
        "vol":              volatility_payload,
        "sentiment":        sentiment_payload,
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


# ─────────────────────────────────────────────────────────────────────────────
# SUPPORT — Panda Fund landing page.  Reuses the existing v1 helpers in
# `web.py` (Stripe price IDs, monthly fund stats, funding history) so we
# don't fork the Stripe wiring; only the markup + CSS gets a v2 redesign.
# ─────────────────────────────────────────────────────────────────────────────


def _support_history_chart(months: list[str], raised: list[int], goal: int) -> dict:
    """Server-render bar-chart geometry for the funding history panel.

    Avoids the ECharts dependency the v1 page leaned on — every other v2
    chart uses inline SVG with `vector-effect="non-scaling-stroke"` and
    HTML axis labels, so this matches the established pattern.
    """
    n = max(len(months), 1)
    vb_w, vb_h = 1500.0, 320.0
    pad_top, pad_bot, pad_left = 18.0, 36.0, 64.0
    plot_w = vb_w - pad_left - 24
    plot_h = vb_h - pad_top - pad_bot

    if not months:
        return {"have_data": False, "bars": [], "y_labels": [], "grid_ys": [],
                "vb_width": vb_w, "vb_height": vb_h}

    # Y axis: nice round step up to (and beyond) the goal so the dashed
    # goal line always lands on a labelled tick.
    y_top = max(goal, max(raised) if raised else 1)
    step = _nice_axis_step(y_top, target_steps=4)
    import math
    y_top = math.ceil(y_top / step) * step

    def _y_for(v: float) -> float:
        return pad_top + (1.0 - v / y_top) * plot_h

    bar_gap = 14.0
    bar_w = max(40.0, (plot_w - bar_gap * (n + 1)) / n)

    bars: list[dict] = []
    for i, (label, v) in enumerate(zip(months, raised)):
        x = pad_left + bar_gap + i * (bar_w + bar_gap)
        y = _y_for(v)
        bars.append({
            "label":  label,
            "value":  v,
            "x":      round(x, 1),
            "y":      round(y, 1),
            "w":      round(bar_w, 1),
            "h":      round(_y_for(0) - y, 1),
            "label_x": round(x + bar_w / 2, 1),
            "is_funded": v >= goal,
            "value_str": f"${v}",
        })

    y_labels = []
    grid_ys = []
    v = 0.0
    while v <= y_top + step / 2:
        y_pos = round(_y_for(v), 1)
        y_labels.append({"label": f"${int(v)}", "y": y_pos})
        grid_ys.append(y_pos)
        v += step

    return {
        "have_data": True,
        "bars":      bars,
        "y_labels":  y_labels,
        "grid_ys":   grid_ys,
        "goal_y":    round(_y_for(goal), 1),
        "goal":      goal,
        "vb_width":  vb_w,
        "vb_height": vb_h,
        "left_pad":  pad_left,
    }


@router.get("/support", response_class=HTMLResponse)
async def preview_support(request: Request):
    """Support page — Panda Fund + Stripe checkout, v2 design."""
    from filings.web import _support_page_context  # reuse v1 helper

    base = await _support_page_context(request)
    base["chart"] = _support_history_chart(
        base.get("funding_history_months") or [],
        base.get("funding_history_raised") or [],
        base.get("monthly_goal") or 200,
    )
    base.update(await _shell_context(request, "Support"))
    return templates.TemplateResponse("_redesign/support.html", base)


@router.get("/support/thank-you", response_class=HTMLResponse)
async def preview_support_thank_you(request: Request):
    """Post-Stripe-checkout return — same template with thank-you flag."""
    from filings.web import _support_page_context

    base = await _support_page_context(request, extra={"show_thank_you": True})
    base["chart"] = _support_history_chart(
        base.get("funding_history_months") or [],
        base.get("funding_history_raised") or [],
        base.get("monthly_goal") or 200,
    )
    base.update(await _shell_context(request, "Support"))
    return templates.TemplateResponse("_redesign/support.html", base)
