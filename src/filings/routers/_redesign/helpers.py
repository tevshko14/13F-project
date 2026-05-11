"""Shared helpers for the redesign feature routers.

Anything used by 2+ feature modules lives here.  Feature-specific
helpers stay alongside their routes.

Public surface (sub-routers import from here, parent ``redesign_preview``
re-exports for backward-compat with ``web.py``):

  * Feature flags          — ``is_enabled``, ``is_placeholders_enabled``,
                              ``is_profile_preview_enabled``
  * Decorators             — ``_maybe_rate_limit``
  * State accessors        — ``_request_fund_cache``
  * Bounding utilities     — ``_bounded_call``, ``_bounded``
  * Date / format helpers  — ``_today_label``, ``_market_status``,
                              ``_short_date``, ``_initials_from_name``
  * Shell context          — ``_shell_context``
  * Chart axis helper      — ``_nice_axis_step``
  * Module constants       — ``_ASSET_VERSION``, ``SPARK``, ``SPARK_DOWN``
  * Crash-safety router    — ``GracefulRoute`` (APIRoute subclass that
                              catches unhandled exceptions and returns
                              a templated 503 instead of a blank 500)
"""

from __future__ import annotations

import asyncio
import logging
import math
import os
import re
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from typing import Callable

from fastapi import HTTPException, Request, Response
from fastapi.responses import HTMLResponse
from fastapi.routing import APIRoute

from filings import supabase_cache
from filings.app_state import limiter, templates
from filings.caching import TTLCache
from filings.concurrency import gate_supabase_async

logger = logging.getLogger(__name__)


# Module-import timestamp -- stable across all renders within one
# deploy, but unique per deploy so CSS changes don't get served from
# stale browser caches after a release.
_ASSET_VERSION = int(time.time())


# ── Feature flags ────────────────────────────────────────────────────


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


def is_profile_preview_enabled() -> bool:
    """Whether the v2 profile page is exposed.

    The Account / Alerts / Subscription tabs still render mock data
    (no real prefs / billing schema yet), so we gate the whole route
    + sidenav item behind ``PP_PROFILE_PREVIEW=1`` -- set locally for
    iteration, unset in production so v1 ``/profile`` (or a 302 home)
    serves real users.  Watchlist is its own page (``/watchlist``)
    and is unaffected by this flag.
    """
    return os.environ.get("PP_PROFILE_PREVIEW", "").lower() in ("1", "true", "yes")


# ── Decorators ────────────────────────────────────────────────────────


def _maybe_rate_limit(spec: str):
    """slowapi ``limiter.limit(spec)`` with a no-op fallback.

    Returns the real decorator when slowapi is installed, else a
    pass-through.  Lets routes use ``@_maybe_rate_limit("30/minute")``
    unconditionally without breaking dev environments that haven't
    installed the (optional) rate-limit dep.
    """
    if limiter is None:
        return lambda f: f
    return limiter.limit(spec)


# ── State accessors ───────────────────────────────────────────────────


def _request_fund_cache(request: Request) -> dict:
    """Read ``app.state.fund_cache``, defaulting to an empty dict.

    Centralised so the same ``getattr(request.app.state, ...) or {}``
    dance isn't repeated across handlers.  Returns the live cache dict
    by reference -- callers must not mutate.
    """
    return getattr(request.app.state, "fund_cache", {}) or {}


# ── Bounding utilities ────────────────────────────────────────────────


async def _bounded_call(coro, *, timeout: float, fallback, name: str):
    """Race a coroutine against a timeout; return fallback on any failure.

    Used at request-handler scope for fan-out fetches that can each
    fail independently without breaking the page.  Treats three signals
    as "render the fallback":
      - asyncio.TimeoutError (call took too long)
      - any other exception (call raised)
      - result is None (upstream's circuit-breaker returned no data)

    ``fallback`` may be a value or a zero-arg callable; callables are
    invoked only on the failure path, so callers can pass heavy-to-build
    payloads (sin/cos series, json.dumps, etc.) without paying for them
    on the happy path.

    Same shape as the ``_bounded`` closure inside ``build_stock_data_bundle``,
    but free-standing so the homepage handler can use it too without
    pulling in the bundle's source-status tracking.
    """
    try:
        result = await asyncio.wait_for(coro, timeout=timeout)
        if result is not None:
            return result
    except asyncio.TimeoutError:
        logger.warning("%s timed out after %.1fs", name, timeout)
    except Exception as exc:
        logger.warning("%s failed: %s", name, exc)
    return fallback() if callable(fallback) else fallback


async def _bounded(coro, *, timeout: float, fallback, name: str, page: str = "page"):
    """Wrap an awaitable so a slow upstream can't stall the whole render.

    ``page`` shows up in the warning log so the timing-out source is easy
    to spot when several routes share the same upstream.

    ``fallback`` may be a value or a zero-arg callable; callables are
    invoked only on the failure path so callers can pass full-shape
    payloads (which can be expensive to build) without paying for them
    on the happy path.  Same contract as :func:`_bounded_call`.
    """
    try:
        return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError:
        logger.warning("%s: %s timed out (>%ss)", page, name, timeout)
    except Exception as exc:
        logger.warning("%s: %s failed: %s", page, name, exc)
    return fallback() if callable(fallback) else fallback


# ── Date / format helpers ─────────────────────────────────────────────


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


def _short_date(iso: str) -> str:
    """Repeated 'YYYY-MM-DD…' → 'Mon DD YYYY' formatter used by every
    calendar parser and the activity feed.

    Returns the canonical product date format (MMM DD YYYY).  Year-less
    "MMM DD" callers should switch to ``filings.dates_format.format_date_short``.
    """
    from filings.dates_format import format_date
    return format_date(iso, fallback=iso[:10] if iso else "")


def _time_ago_iso(iso_str: str, *, suffix: str = " ago", just_now: str = "just now") -> str:
    """ISO-8601 → "3m ago" / "2h ago" / "1d ago" / "3w ago" relative time.

    `suffix=""` for the bare-number variant used inside `home._activity_*`
    (template appends " ago" itself); `just_now="now"` matches the same.
    """
    if not iso_str:
        return ""
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        diff = datetime.now(timezone.utc) - dt
        seconds = int(diff.total_seconds())
        if seconds < 60:
            return just_now
        minutes = seconds // 60
        if minutes < 60:
            return f"{minutes}m{suffix}"
        hours = minutes // 60
        if hours < 24:
            return f"{hours}h{suffix}"
        days = hours // 24
        if days < 7:
            return f"{days}d{suffix}"
        return f"{days // 7}w{suffix}"
    except Exception:
        return ""


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


# ── Chart axis ────────────────────────────────────────────────────────


def _nice_axis_step(rng: float, target_steps: int = 4) -> float:
    """Pick a 'nice' step size (1/2/5 × 10^k) that splits *rng* into ~target_steps."""
    if rng <= 0:
        return 1.0
    raw = rng / max(target_steps, 1)
    mag = 10 ** math.floor(math.log10(raw))
    n = raw / mag
    if   n < 1.5: nice = 1.0
    elif n < 3.5: nice = 2.0
    elif n < 7.5: nice = 5.0
    else:         nice = 10.0
    return nice * mag


# ── Dollar formatters (used by multiple feature routers) ─────────────


def _format_dollars_compact(v: float) -> str:
    """Tight $XB / $XM string used by chart axis labels (no trailing zeros).

    Accepts negative values (yields "$-1.2B"); used by both funds and
    insiders momentum charts.
    """
    av = abs(v)
    if av >= 1e12: out, suf = v / 1e12, "T"
    elif av >= 1e9:  out, suf = v / 1e9,  "B"
    elif av >= 1e6:  out, suf = v / 1e6,  "M"
    elif av >= 1e3:  out, suf = v / 1e3,  "K"
    else:            return f"${v:,.0f}"
    txt = f"{out:.1f}".rstrip("0").rstrip(".")
    return f"${txt}{suf}"


def _format_compact_dollars(v: float | int | None) -> str:
    """Compact dollar formatter for KPI / leaderboard cells.

    Positive-only (em-dashes None/0/negative); used by ownership,
    insider clusters, fund KPIs.  Subtly different from
    ``_format_dollars_compact`` -- this one cleans up presentation for
    summary cells, that one handles arbitrary numbers for chart axes.
    """
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


# ── Insider trade-type classifiers (shared by home + insiders pages) ─


def _insiders_action(trade_type: str) -> str:
    """Map OpenInsider trade_type → BUY/SELL chip."""
    t = (trade_type or "").lower()
    if "purchase" in t or "p - purchase" in t or t.startswith("p"):
        return "BUY"
    if "sale" in t or t.startswith("s"):
        return "SELL"
    return "—"


def _insiders_format_title(title: str) -> str:
    """Compact role shown in the table — strip filler words."""
    t = (title or "").strip()
    if not t:
        return "—"
    # OpenInsider gives strings like "CEO, Director" — keep just first segment
    # for the role column to match the design's compact display.
    return t.split(",")[0].strip()


# ── Congress trade-type classifier (shared by home + congress pages) ─


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


# ── Dollar-string compactors (shared by home + stock + congress) ────


_AMOUNT_RE = re.compile(r"[-+]?[\d,.]+")


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


# ── CUSIP → ticker resolver (shared by home + funds pages) ──────────


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


# ── Sparkline mock series (used by mock-data fallbacks) ──────────────


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


# ── Shell context (rendered on every page) ───────────────────────────


_SHELL_NOTIF_WINDOW_HOURS   = 24                # fallback "fresh" window for first-time visitors
_SHELL_NOTIF_BADGE_CAP      = 99                # avoid 4-digit badge overflow
_SHELL_NOTIF_COOKIE         = "pp-notif-seen"   # per-browser "last viewed notifications" timestamp
_SHELL_NOTIF_COOKIE_MAX_AGE = 60 * 60 * 24 * 90 # 90 days
_SHELL_PANDA_GOAL_CENTS     = 20_000            # $200/month — same goal as v1 widget

# L1 cache for shell-context Supabase calls.  These run on every page
# load via `_shell_context()` (header bell + panda fund widget) -- in
# 2026-05-10 prod they were the dominant source of default-pool
# saturation when Supabase queries slowed to >8s.  A short TTL is
# fine because:
#   - the bell badge reflects "new since last visit" cookie state, so
#     a 30s lag in the count is invisible to the user
#   - the panda fund total is a slow-moving metric (Stripe webhook
#     fires hourly at most) -- 30s lag matches the data freshness
# Keys are unique per request (cookie ISO + month) so the cache size
# stays bounded by distinct visitor patterns over the TTL window.
_SHELL_CACHE_TTL_S = 30
_shell_cache = TTLCache(ttl=_SHELL_CACHE_TTL_S, max_size=2000)


async def _shell_context(request: Request, active: str) -> dict:
    """Common context every redesign page needs for the app shell.

    Live values (replacing prior hardcodes):
      - ``notif_unread`` — count of notifications in the last 24h.  ``0`` when
        empty so the badge collapses (template already conditional on truthy).
      - ``panda_raised/goal/pct/month`` — real Stripe donation totals via
        ``supabase_cache.get_monthly_raised_cents``.  Goal stays a config
        knob (``_SHELL_PANDA_GOAL_CENTS``).
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

    # Quantize the bell cache key to the minute -- the bell count
    # doesn't change second-to-second, and the per-cookie raw ISO
    # would give a near-0% hit rate against unique visitors.  Minute
    # quantization collapses cardinality to (unique-cookies × minutes)
    # and pushes hit rate to ~100% for repeat-page-loads-within-minute.
    bell_key = f"bell:{seen_iso[:16]}"
    panda_month = today.strftime("%Y-%m")
    panda_key = f"panda:{panda_month}"

    bell_count = _shell_cache.get(bell_key)
    cents = _shell_cache.get(panda_key)

    async def _fetch_bell() -> int:
        try:
            result = await gate_supabase_async(
                supabase_cache.get_bell_state_async(seen_iso),
            )
            count, _latest = result if result is not None else (0, None)
            v = int(count or 0)
            _shell_cache.set(bell_key, v)
            return v
        except Exception as exc:
            logger.debug("shell: bell state failed: %s", exc)
            return _shell_cache.get_stale(bell_key, 0)

    async def _fetch_panda() -> int:
        try:
            c = await gate_supabase_async(
                supabase_cache.get_monthly_raised_cents_async(panda_month),
            )
            v = int(c or 0)
            _shell_cache.set(panda_key, v)
            return v
        except Exception as exc:
            logger.debug("shell: panda fund failed: %s", exc)
            return _shell_cache.get_stale(panda_key, 0)

    # Hot path (both cache hits) issues zero awaits.  Cold path runs
    # both Supabase reads in parallel rather than sequentially.
    if bell_count is None and cents is None:
        bell_count, cents = await asyncio.gather(_fetch_bell(), _fetch_panda())
    elif bell_count is None:
        bell_count = await _fetch_bell()
    elif cents is None:
        cents = await _fetch_panda()

    notif_unread: int | str = 0
    if bell_count and bell_count > _SHELL_NOTIF_BADGE_CAP:
        notif_unread = f"{_SHELL_NOTIF_BADGE_CAP}+"
    elif bell_count and bell_count > 0:
        notif_unread = bell_count

    # Panda Fund — Stripe-backed monthly donation total.  When the
    # `supporters` table has rows for this month, that's the truth.
    # When it doesn't (e.g. before any donations have been recorded
    # for the month, or for manual override during testing), fall
    # back to the `PANDA_FUND_RAISED` env var in dollars -- same
    # contract as the v1 `_get_panda_fund_stats` helper in web.py.
    # Without this fallback the bar shows $0 / $200 even when the
    # operator has set a manual amount via env.
    panda_raised, panda_goal, panda_pct = 0, _SHELL_PANDA_GOAL_CENTS // 100, 0
    if cents and cents > 0:
        panda_raised = min(cents // 100, panda_goal)
    else:
        panda_raised = min(int(os.environ.get("PANDA_FUND_RAISED", "0")), panda_goal)
    if panda_raised > 0:
        panda_pct = min(100, round(panda_raised / panda_goal * 100))

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
        # Local-only flag: drives whether the Profile sidenav item +
        # /profile route are exposed.  Gated because the Account /
        # Alerts / Subscription tabs still render mock data.
        "show_profile_nav":     is_profile_preview_enabled(),
    }


# ── Crash-safety router class ────────────────────────────────────────
#
# Wraps every redesign route handler in a try/except that catches any
# unhandled exception and returns a templated 503 instead of a blank
# 500.  Sub-routers opt in by passing ``route_class=GracefulRoute`` to
# their ``APIRouter()`` constructor.
#
# The handler attempts a 3-step graceful render:
#   1. Templated ``_redesign/error.html`` with full shell_context
#      (best UX -- nav + footer intact, user can navigate away).
#   2. If shell_context itself raises (e.g. Supabase down too), fall
#      back to a minimal error.html render without shell.
#   3. If templates are entirely broken, fall back to a hardcoded
#      HTML string (last-resort, no template dependency).
#
# All three steps log to Sentry via the existing logger.exception
# chain -- so we never silently swallow the original crash.

# Last-resort static HTML when even the error template fails.  Self-
# contained: no external CSS/JS, no template-engine dependency.  Same
# dark-mode default + Geist-stack font hint as the rest of the app.
_FALLBACK_ERROR_HTML = """<!doctype html>
<html lang="en" data-theme="dark">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Temporarily unavailable · PaperPanda</title>
  <meta name="robots" content="noindex,nofollow">
  <style>
    body { background: #0a0a0a; color: #f5f5f5;
           font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", system-ui, sans-serif;
           margin: 0; padding: 8vh 24px; text-align: center; }
    h1 { font-size: 28px; margin: 0 0 14px; font-weight: 700; letter-spacing: -0.02em; }
    p  { color: #999; font-size: 15px; max-width: 42ch; margin: 0 auto 24px; line-height: 1.55; }
    .actions { display: flex; gap: 12px; justify-content: center; flex-wrap: wrap; }
    a.btn { display: inline-block; padding: 9px 18px; border-radius: 8px;
            text-decoration: none; font-size: 15px;
            background: #f5f5f5; color: #0a0a0a; border: 1px solid #f5f5f5; }
    a.btn.secondary { background: transparent; color: #f5f5f5; border-color: #333; }
  </style>
</head>
<body>
  <div style="font-size: 48px; line-height: 1; margin-bottom: 18px;">🔄</div>
  <h1>Temporarily unavailable</h1>
  <p>This page hit a snag. Try refreshing in a moment, or head back to the homepage.</p>
  <div class="actions">
    <a href="/" class="btn">Home</a>
    <a href="javascript:location.reload()" class="btn secondary">Refresh</a>
  </div>
</body>
</html>"""


async def _render_graceful_error(
    request: Request, exc: BaseException, status_code: int = 503,
) -> Response:
    """3-step graceful error render.

    1. Templated error.html with shell_context (best UX).
    2. Templated error.html WITHOUT shell_context (skip the helper
       that might also be failing).
    3. Hardcoded ``_FALLBACK_ERROR_HTML`` string (last resort).
    """
    logger.exception(
        "Redesign route crashed -- rendering graceful 503: path=%s exc=%s",
        request.url.path, exc,
    )

    # Attempt 1: full templated page with shell (sidenav, footer).
    try:
        ctx = await _shell_context(request, "")
        ctx.update({
            "request":     request,
            "status_code": status_code,
            "message":     "Data temporarily unavailable. Please try again shortly.",
        })
        return templates.TemplateResponse(
            "_redesign/error.html", ctx, status_code=status_code,
        )
    except Exception as inner1:
        logger.warning(
            "Graceful error: shell_context render failed (%s); "
            "falling back to shell-less error template",
            inner1,
        )

    # Attempt 2: templated page WITHOUT shell_context.
    # Pass an empty stub so base.html's `{% if nav_active %}` clauses
    # render cleanly without crashing on missing keys.
    try:
        stub: dict = {
            "request":              request,
            "status_code":          status_code,
            "message":              "Data temporarily unavailable. Please try again shortly.",
            "nav_active":           "",
            "today_label":          "",
            "market_status":        "",
            "panda_raised":         0,
            "panda_goal":           _SHELL_PANDA_GOAL_CENTS // 100,
            "panda_month":          "",
            "panda_pct":            0,
            "user_initials":        "",
            "notif_unread":         0,
            "asset_version":        _ASSET_VERSION,
            "is_authed":            False,
            "show_placeholder_nav": False,
            "show_profile_nav":     False,
        }
        return templates.TemplateResponse(
            "_redesign/error.html", stub, status_code=status_code,
        )
    except Exception as inner2:
        logger.warning(
            "Graceful error: shell-less template render also failed (%s); "
            "falling back to static HTML",
            inner2,
        )

    # Attempt 3: hardcoded HTML, no template dependency, can't fail.
    return HTMLResponse(content=_FALLBACK_ERROR_HTML, status_code=status_code)


class GracefulRoute(APIRoute):
    """APIRoute subclass that catches unhandled exceptions and returns
    a templated 503 page instead of a blank 500.

    Sub-routers in ``_redesign/`` opt in by passing this class:

        router = APIRouter(route_class=GracefulRoute)

    ``HTTPException`` instances raised by the handler are re-raised
    unchanged so FastAPI's normal handling (custom status codes,
    redirect responses, etc.) keeps working.  Everything else --
    timeouts, KeyErrors, Jinja undefineds, AttributeErrors -- gets
    wrapped into a graceful 503.
    """

    def get_route_handler(self) -> Callable:
        original = super().get_route_handler()

        async def custom_route_handler(request: Request) -> Response:
            try:
                return await original(request)
            except HTTPException:
                # FastAPI status responses (e.g. 401, 404, 429) flow through unchanged.
                raise
            except Exception as exc:
                # Anything unhandled -> graceful 503 with templated error page.
                return await _render_graceful_error(request, exc, status_code=503)

        return custom_route_handler
