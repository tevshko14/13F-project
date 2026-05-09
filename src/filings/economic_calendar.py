"""Macro Economic Calendar — upcoming & recent economic events.

Fetches economic event data from Finnhub (primary) and FMP (fallback).
Normalises both APIs into a shared format, caches aggressively, and
provides deterministic mock data when no API keys are configured.

Used by the "Macro Dashboard" tab on ``/macro``.
"""

from __future__ import annotations

import logging
import os
import random
import threading
import time
from datetime import datetime, timedelta

import httpx

logger = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────

PERIOD_CHOICES = {
    "this_week": "This Week",
    "next_week": "Next Week",
    "next_2w": "Next 2 Weeks",
    "all": "All Upcoming",
}

IMPACT_CHOICES = {
    "all": "All Events",
    "high": "High Impact Only",
}

COUNTRY_CHOICES = {
    "us": "US Only",
    "all": "All Countries",
}

_TRACKED_CATEGORIES: dict[str, list[str]] = {
    "inflation": ["CPI", "PPI", "PCE", "Consumer Price", "Producer Price"],
    "employment": [
        "Nonfarm Payrolls", "NFP", "Unemployment Rate",
        "JOLTS", "Initial Claims", "Continuing Claims",
    ],
    "growth": ["GDP", "Gross Domestic Product"],
    "manufacturing": [
        "ISM Manufacturing", "ISM Services", "Empire State",
        "PMI", "Industrial Production", "Durable Goods",
    ],
    "monetary_policy": [
        "FOMC", "Fed Minutes", "Federal Reserve",
        "Fed Funds Rate", "Fed Chair", "Interest Rate Decision",
    ],
    "housing": [
        "Housing Starts", "Building Permits", "Existing Home Sales",
        "New Home Sales", "Case-Shiller",
    ],
    "consumer": [
        "Retail Sales", "Consumer Confidence", "Michigan Consumer",
        "Personal Income", "Personal Spending",
    ],
}

# ── Cache ────────────────────────────────────────────────────────
from filings.caching import TTLCache as _TTLCache
_lock = threading.Lock()              # only guards _raw_cache (single-entry)
_raw_cache: dict | None = None       # {"events": [...], "fetched_at": float}
_RAW_TTL = 3600                       # 1 hour

_RESULT_TTL = 1800                    # 30 min
# 4 periods × 5 countries × 3 impact filters = ~60 keys; 200 cap is generous.
_result_cache = _TTLCache(ttl=_RESULT_TTL, max_size=200)

_TIMEOUT = 10  # under heavy-pool 15s ceiling so threads return in time


# ── Helpers ──────────────────────────────────────────────────────

def _finnhub_key() -> str:
    return os.environ.get("FINNHUB_API_KEY", "").strip()


def _fmp_key() -> str:
    return os.environ.get("FMP_API_KEY", "").strip()


def _categorise(event_name: str) -> str:
    """Match event name to a category via keyword search."""
    upper = event_name.upper()
    for cat, keywords in _TRACKED_CATEGORIES.items():
        for kw in keywords:
            if kw.upper() in upper:
                return cat
    return "other"


def _date_range_for_period(period: str) -> tuple[str, str]:
    """Return (start_date, end_date) as ``YYYY-MM-DD`` strings."""
    today = datetime.now()
    weekday = today.weekday()  # 0=Mon, 6=Sun

    # Find current week's Monday
    monday = today - timedelta(days=weekday)
    if weekday >= 5:  # Weekend → next Monday
        monday = today + timedelta(days=(7 - weekday))

    if period == "this_week":
        start = monday
        end = monday + timedelta(days=4)  # Friday
    elif period == "next_week":
        start = monday + timedelta(weeks=1)
        end = start + timedelta(days=4)
    elif period == "next_2w":
        start = monday
        end = monday + timedelta(weeks=2, days=4)
    else:  # "all"
        start = today - timedelta(days=7)
        end = today + timedelta(weeks=4)

    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def _date_label(date_str: str) -> str:
    """Convert ``YYYY-MM-DD`` to human-friendly label.

    Uses the canonical product format (``MMM DD YYYY``) with a
    ``Today``/``Tomorrow`` prefix when applicable.
    """
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        today = datetime.now().date()
        d = dt.date()
        formatted = dt.strftime("%b %d %Y")
        if d == today:
            return f"Today — {formatted}"
        if d == today + timedelta(days=1):
            return f"Tomorrow — {formatted}"
        return formatted
    except ValueError:
        return date_str


def _fmt_val(val, unit: str) -> str:
    """Format a numeric value with its unit for display."""
    if val is None:
        return "—"
    try:
        v = float(val)
    except (TypeError, ValueError):
        return str(val)
    if unit == "%":
        return f"{v:.1f}%"
    elif unit in ("K", "k"):
        return f"{v:.0f}K"
    elif unit in ("B", "b"):
        return f"{v:.1f}B"
    elif unit in ("M", "m"):
        return f"{v:.1f}M"
    elif abs(v) >= 1_000_000:
        return f"{v / 1_000_000:.1f}M"
    elif abs(v) >= 1_000:
        return f"{v / 1_000:.0f}K"
    else:
        return f"{v:.2f}"


# ── API Fetchers ─────────────────────────────────────────────────

def _load_finnhub() -> list[dict] | None:
    """Fetch from Finnhub ``/calendar/economic``, return normalised list."""
    key = _finnhub_key()
    if not key:
        return None

    end = datetime.now() + timedelta(weeks=4)
    start = datetime.now() - timedelta(weeks=1)

    try:
        r = httpx.get(
            "https://finnhub.io/api/v1/calendar/economic",
            params={
                "from": start.strftime("%Y-%m-%d"),
                "to": end.strftime("%Y-%m-%d"),
                "token": key,
            },
            timeout=_TIMEOUT,
        )
        r.raise_for_status()
        data = r.json()

        raw = data.get("economicCalendar", [])
        if not raw:
            logger.info("Finnhub economic calendar: 0 entries")
            return []

        events = []
        for item in raw:
            event_name = item.get("event", "")
            if not event_name:
                continue
            # Finnhub's response format ships the date inside the `time`
            # field as a full ISO datetime ("YYYY-MM-DD HH:MM:SS").  The
            # legacy `date` key is gone, so derive both pieces from `time`.
            time_raw = str(item.get("time", "") or "")
            date_str = str(item.get("date", "") or "")
            time_str = ""
            if time_raw:
                # "2026-04-28 00:00:00" → date "2026-04-28", time "00:00"
                if len(time_raw) >= 10 and time_raw[4] == "-":
                    if not date_str:
                        date_str = time_raw[:10]
                    if len(time_raw) >= 16:
                        time_str = time_raw[11:16]
                else:
                    # Pre-existing "HH:MM:SS" or "HH:MM" form.
                    time_str = time_raw[:5]
            # 00:00 from Finnhub means "no time given" — drop it so the UI
            # doesn't render a misleading midnight slot.
            if time_str == "00:00":
                time_str = ""
            events.append({
                "event": event_name,
                "date": date_str,
                "time": time_str,
                "country": str(item.get("country", "")).upper(),
                "impact": str(item.get("impact", "low")).lower(),
                "actual": item.get("actual"),
                "estimate": item.get("estimate"),
                "previous": item.get("prev"),
                "unit": str(item.get("unit", "")),
                "source": "finnhub",
            })

        logger.info("Finnhub economic calendar: %d events", len(events))
        return events

    except Exception:
        logger.warning("Finnhub economic calendar fetch failed", exc_info=True)
        return None


def _load_fmp() -> list[dict] | None:
    """FMP /economic-calendar — disabled (402 on free tier)."""
    return None


def _load_raw_events() -> tuple[list[dict], bool]:
    """Load raw events with fallback chain. Returns (events, is_mock)."""
    global _raw_cache

    # Check raw cache first
    now = time.time()
    with _lock:
        if _raw_cache is not None and (now - _raw_cache["fetched_at"]) < _RAW_TTL:
            return _raw_cache["events"], _raw_cache.get("is_mock", False)

    # Try Finnhub first
    events = _load_finnhub()
    if events is not None:
        with _lock:
            _raw_cache = {"events": events, "fetched_at": time.time(), "is_mock": False}
        return events, False

    # Fallback to FMP
    events = _load_fmp()
    if events is not None:
        with _lock:
            _raw_cache = {"events": events, "fetched_at": time.time(), "is_mock": False}
        return events, False

    # No API keys → mock data
    if not _finnhub_key() and not _fmp_key():
        mock = _build_mock_events()
        with _lock:
            _raw_cache = {"events": mock, "fetched_at": time.time(), "is_mock": True}
        return mock, True

    # Keys exist but both APIs failed → return stale cache or empty
    with _lock:
        if _raw_cache is not None:
            return _raw_cache["events"], _raw_cache.get("is_mock", False)
    return [], False


# ── Main Public Function ─────────────────────────────────────────

def fetch_economic_events(
    period: str = "this_week",
    country: str = "us",
    impact_filter: str = "all",
) -> dict:
    """Fetch, filter, enrich and group economic events.

    Args:
        period: One of ``PERIOD_CHOICES`` keys.
        country: ``"us"`` or ``"all"``
        impact_filter: ``"all"`` or ``"high"``

    Returns:
        Dict with keys: events_by_date, metrics, countdown_target,
        period, period_label, country, impact_filter, is_mock,
        is_unavailable.
    """
    # ── Validate inputs ──────────────────────────────────────────
    if period not in PERIOD_CHOICES:
        period = "this_week"
    if country not in COUNTRY_CHOICES:
        country = "us"
    if impact_filter not in IMPACT_CHOICES:
        impact_filter = "all"

    # ── L1 result cache ──────────────────────────────────────────
    cache_key = f"econ:{period}:{country}:{impact_filter}"
    cached = _result_cache.get(cache_key)
    if cached is not None:
        return cached

    # ── Load raw data ────────────────────────────────────────────
    raw_events, is_mock = _load_raw_events()

    # ── Filter: date range ───────────────────────────────────────
    start_str, end_str = _date_range_for_period(period)
    filtered = []
    for ev in raw_events:
        d = ev.get("date", "")
        if d < start_str or d > end_str:
            continue
        # Country filter
        if country == "us" and ev.get("country", "") not in ("US", ""):
            continue
        # Impact filter
        if impact_filter == "high" and ev.get("impact", "") != "high":
            continue
        filtered.append(ev)

    # ── Enrich events ────────────────────────────────────────────
    today_str = datetime.now().strftime("%Y-%m-%d")
    now_time_str = datetime.now().strftime("%H:%M")

    for ev in filtered:
        # Category
        ev["category"] = _categorise(ev.get("event", ""))

        # Impact class for CSS
        imp = ev.get("impact", "low")
        ev["impact_class"] = {
            "high": "ed-impact-high",
            "medium": "ed-impact-medium",
            "low": "ed-impact-low",
        }.get(imp, "ed-impact-low")

        # Delta: actual vs estimate
        actual = ev.get("actual")
        estimate = ev.get("estimate")
        if actual is not None and estimate is not None:
            try:
                delta = float(actual) - float(estimate)
                ev["delta"] = delta
                ev["delta_direction"] = "hot" if delta > 0 else ("cold" if delta < 0 else "neutral")
            except (TypeError, ValueError):
                ev["delta"] = None
                ev["delta_direction"] = None
        else:
            ev["delta"] = None
            ev["delta_direction"] = None

        # Is released?
        ev["is_released"] = actual is not None

        # Display formatting
        ev["actual_fmt"] = _fmt_val(actual, ev.get("unit", ""))
        ev["estimate_fmt"] = _fmt_val(estimate, ev.get("unit", ""))
        ev["previous_fmt"] = _fmt_val(ev.get("previous"), ev.get("unit", ""))

        # Time label
        t = ev.get("time", "")
        if t:
            ev["time_label"] = t
        else:
            ev["time_label"] = "TBD"

    # ── Group by date ────────────────────────────────────────────
    date_groups: dict[str, list[dict]] = {}
    for ev in filtered:
        d = ev["date"]
        date_groups.setdefault(d, []).append(ev)

    # Sort entries within each day by time
    for entries in date_groups.values():
        entries.sort(key=lambda e: e.get("time", "") or "99:99")

    events_by_date = []
    for d in sorted(date_groups.keys()):
        events_by_date.append({
            "date": d,
            "date_label": _date_label(d),
            "entries": date_groups[d],
        })

    # ── Countdown target: next unreleased high-impact event ──────
    countdown_target = None
    for day in events_by_date:
        for ev in day["entries"]:
            if ev["is_released"]:
                continue
            if ev.get("impact") != "high":
                continue
            # Build ISO datetime for JS
            d = ev["date"]
            t = ev.get("time", "") or "08:30"  # Default to market open
            iso = f"{d}T{t}:00"
            countdown_target = {
                "event": ev["event"],
                "date": d,
                "time": t,
                "iso_datetime": iso,
            }
            break
        if countdown_target:
            break

    # ── Metrics ──────────────────────────────────────────────────
    total = len(filtered)
    high_count = sum(1 for e in filtered if e.get("impact") == "high")
    released = sum(1 for e in filtered if e["is_released"])
    upcoming = total - released

    metrics = {
        "total_events": total,
        "high_impact_count": high_count,
        "released_count": released,
        "upcoming_count": upcoming,
    }

    # ── Assemble result ──────────────────────────────────────────
    result = {
        "events_by_date": events_by_date,
        "metrics": metrics,
        "countdown_target": countdown_target,
        "period": period,
        "period_label": PERIOD_CHOICES.get(period, period),
        "country": country,
        "impact_filter": impact_filter,
        "is_mock": is_mock,
        "is_unavailable": not raw_events and not is_mock,
    }

    # Cache result
    _result_cache.set(cache_key, result)

    return result


# ── Mock Data ────────────────────────────────────────────────────

def _build_mock_events() -> list[dict]:
    """Deterministic mock events for development (no API keys)."""
    rng = random.Random(42)
    today = datetime.now()

    _MOCK_EVENTS = [
        ("CPI (MoM)", "08:30", "high", "%", 0.2, 0.3),
        ("CPI (YoY)", "08:30", "high", "%", 3.1, 3.2),
        ("Core CPI (MoM)", "08:30", "high", "%", 0.3, 0.3),
        ("PPI (MoM)", "08:30", "medium", "%", 0.1, 0.2),
        ("Nonfarm Payrolls", "08:30", "high", "K", 180, 200),
        ("Unemployment Rate", "08:30", "high", "%", 3.8, 3.7),
        ("FOMC Minutes", "14:00", "high", "", None, None),
        ("Fed Funds Rate Decision", "14:00", "high", "%", 5.25, 5.50),
        ("GDP (QoQ)", "08:30", "high", "%", 3.2, 2.8),
        ("Initial Claims", "08:30", "medium", "K", 210, 215),
        ("Retail Sales (MoM)", "08:30", "high", "%", 0.4, 0.3),
        ("ISM Manufacturing PMI", "10:00", "high", "", 49.1, 48.5),
        ("ISM Services PMI", "10:00", "high", "", 52.7, 51.8),
        ("Consumer Confidence", "10:00", "medium", "", 102.0, 104.0),
        ("Existing Home Sales", "10:00", "medium", "M", 3.95, 4.00),
        ("Durable Goods Orders", "08:30", "medium", "%", -0.8, 1.2),
        ("Personal Income (MoM)", "08:30", "medium", "%", 0.4, 0.3),
        ("PCE Price Index (MoM)", "08:30", "high", "%", 0.3, 0.2),
        ("Michigan Consumer Sentiment", "10:00", "medium", "", 69.7, 67.4),
        ("Building Permits", "08:30", "medium", "K", 1480, 1500),
    ]

    events = []
    used_slots: set[tuple[str, str]] = set()

    for i, (name, t, impact, unit, est, prev) in enumerate(_MOCK_EVENTS):
        # Spread across ~2 weeks
        day_offset = (i * 17 + rng.randint(0, 3)) % 14 - 3
        d = today + timedelta(days=day_offset)
        # Skip weekends
        while d.weekday() >= 5:
            d += timedelta(days=1)
        date_str = d.strftime("%Y-%m-%d")

        # Avoid duplicate time slots on same day
        slot = (date_str, t)
        while slot in used_slots:
            # Bump by 30 min
            hh, mm = t.split(":")
            mm = str((int(mm) + 30) % 60).zfill(2)
            if mm == "00":
                hh = str(int(hh) + 1).zfill(2)
            t = f"{hh}:{mm}"
            slot = (date_str, t)
        used_slots.add(slot)

        # Past events get actuals (~60%)
        actual = None
        if d.date() < today.date() or (d.date() == today.date() and rng.random() < 0.3):
            if est is not None:
                actual = round(est + rng.uniform(-0.3, 0.3) * abs(est or 1), 2)

        events.append({
            "event": name,
            "date": date_str,
            "time": t,
            "country": "US",
            "impact": impact,
            "actual": actual,
            "estimate": est,
            "previous": prev,
            "unit": unit,
            "source": "mock",
        })

    return events
