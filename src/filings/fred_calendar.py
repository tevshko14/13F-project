"""FRED Economic Calendar — upcoming & recent US economic events.

Uses the Federal Reserve Bank of St. Louis (FRED) API as the primary
data source.  FRED is free, official, and covers all major US macro
releases (CPI, NFP, FOMC, GDP, PCE, PPI, Retail Sales, etc.).

Caching strategy — 3-tier with dedicated Supabase table:
  L1 — in-memory dict (TTL 1 hr): avoids repeated DB reads within
        the same process lifetime.
  L2 — ``economic_events`` Supabase table (TTL 6 hr): survives
        deploys, shared across workers, queryable for historical data.
        Upcoming events are refreshed every 6 hours; released events
        (actual IS NOT NULL) are immutable and never re-fetched.
  L3 — FRED REST API: only called when L2 is cold/expired.
        Results are upserted into ``economic_events`` so every request
        benefits all future users.

Fallback: deterministic mock data when no FRED_API_KEY is configured.

Public surface (same interface as economic_calendar.py):
  PERIOD_CHOICES, IMPACT_CHOICES, COUNTRY_CHOICES
  fetch_economic_events(period, country, impact_filter) -> dict
"""

from __future__ import annotations

import logging
import os
import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import httpx

from filings import supabase_cache

logger = logging.getLogger(__name__)

# ── API config ───────────────────────────────────────────────────
_FRED_BASE = "https://api.stlouisfed.org/fred"
_TIMEOUT = 20


def _fred_key() -> str:
    return os.environ.get("FRED_API_KEY", "").strip()


def _finnhub_key() -> str:
    return os.environ.get("FINNHUB_API_KEY", "").strip()


def _fmp_key() -> str:
    return os.environ.get("FMP_API_KEY", "").strip()


# ── Public filter choices (same keys as economic_calendar.py) ───
PERIOD_CHOICES = {
    "this_week": "This Week",
    "next_week": "Next Week",
    "next_2w":   "Next 2 Weeks",
    "all":       "All Upcoming",
}

IMPACT_CHOICES = {
    "all":  "All Events",
    "high": "High Impact Only",
}

COUNTRY_CHOICES = {
    "us":  "US Only",
    "all": "All Countries",  # kept for UI parity; FRED is US-only
}


# ── Tracked FRED series ──────────────────────────────────────────
# Each entry maps a FRED series ID to display metadata.
# `units`:        FRED API transform — "lin" (raw), "pch" (MoM %),
#                 "chg" (absolute change from prior period)
# `display_unit`: what to show in the UI ("%", "K", "")
# `release_id`:   FRED release ID (used to fetch scheduled dates)
# `time`:         typical ET time the data is released (hardcoded)
_SERIES: dict[str, dict] = {
    # ── Inflation ────────────────────────────────────────────────
    "CPIAUCSL": {
        "name": "CPI (MoM)", "impact": "high", "category": "inflation",
        "units": "pch", "display_unit": "%", "release_id": 10, "time": "08:30",
    },
    "CPILFESL": {
        "name": "Core CPI (MoM)", "impact": "high", "category": "inflation",
        "units": "pch", "display_unit": "%", "release_id": 10, "time": "08:30",
    },
    "PCEPILFE": {
        "name": "Core PCE (MoM)", "impact": "high", "category": "inflation",
        "units": "pch", "display_unit": "%", "release_id": 54, "time": "08:30",
    },
    "PPIFID": {
        "name": "PPI Final Demand (MoM)", "impact": "medium", "category": "inflation",
        "units": "pch", "display_unit": "%", "release_id": 101, "time": "08:30",
    },
    # ── Employment ───────────────────────────────────────────────
    "PAYEMS": {
        "name": "Nonfarm Payrolls", "impact": "high", "category": "employment",
        "units": "chg", "display_unit": "K", "release_id": 50, "time": "08:30",
    },
    "UNRATE": {
        "name": "Unemployment Rate", "impact": "high", "category": "employment",
        "units": "lin", "display_unit": "%", "release_id": 50, "time": "08:30",
    },
    "ICSA": {
        "name": "Initial Jobless Claims", "impact": "medium", "category": "employment",
        "units": "lin", "display_unit": "K", "release_id": 176, "time": "08:30",
    },
    # ── Growth ───────────────────────────────────────────────────
    "A191RL1Q225SBEA": {
        "name": "GDP (QoQ, Annualized)", "impact": "high", "category": "growth",
        "units": "lin", "display_unit": "%", "release_id": 53, "time": "08:30",
    },
    # ── Consumer ─────────────────────────────────────────────────
    "RSXFS": {
        "name": "Retail Sales (MoM)", "impact": "high", "category": "consumer",
        "units": "pch", "display_unit": "%", "release_id": 337, "time": "08:30",
    },
    "UMCSENT": {
        "name": "Michigan Consumer Sentiment", "impact": "medium", "category": "consumer",
        "units": "lin", "display_unit": "", "release_id": 175, "time": "10:00",
    },
    # ── Manufacturing ────────────────────────────────────────────
    "DGORDER": {
        "name": "Durable Goods Orders (MoM)", "impact": "medium", "category": "manufacturing",
        "units": "pch", "display_unit": "%", "release_id": 97, "time": "08:30",
    },
    # ── Housing ──────────────────────────────────────────────────
    "HOUST": {
        "name": "Housing Starts", "impact": "medium", "category": "housing",
        "units": "lin", "display_unit": "K", "release_id": 60, "time": "08:30",
    },
}

# ── Category keyword map (used by _categorise) ──────────────────
_CATEGORY_KEYWORDS: dict[str, list[str]] = {
    "inflation": ["CPI", "PPI", "PCE", "CONSUMER PRICE", "PRODUCER PRICE", "INFLATION"],
    "employment": [
        "NONFARM", "NFP", "UNEMPLOYMENT", "JOLTS", "CLAIMS",
        "PAYROLL", "ADP", "LABOR",
    ],
    "growth": ["GDP", "GROSS DOMESTIC"],
    "manufacturing": [
        "ISM", "PMI", "EMPIRE STATE", "INDUSTRIAL PRODUCTION",
        "DURABLE GOODS", "MANUFACTURING", "FACTORY",
    ],
    "monetary_policy": [
        "FOMC", "FED MINUTES", "FEDERAL RESERVE", "FED FUNDS",
        "FED CHAIR", "INTEREST RATE", "RATE DECISION", "POWELL",
        "TREASURY", "BEIGE BOOK",
    ],
    "housing": [
        "HOUSING STARTS", "BUILDING PERMITS", "HOME SALES",
        "CASE-SHILLER", "NAHB", "MORTGAGE",
    ],
    "consumer": [
        "RETAIL SALES", "CONSUMER CONFIDENCE", "MICHIGAN",
        "PERSONAL INCOME", "PERSONAL SPENDING", "CONSUMER SENTIMENT",
    ],
}

# ── Precompiled regex for _dedup_key ─────────────────────────────
_RE_PARENS = re.compile(r"\s*\(.*?\)")
_RE_FREQ_SUFFIX = re.compile(r"\s*[MQY]/[MQY]$", re.IGNORECASE)
_RE_COUNTRY_PREFIX = re.compile(r"^(US|U\.S\.|UNITED STATES)\s+", re.IGNORECASE)
_RE_WHITESPACE = re.compile(r"\s+")

# FOMC rate-decision dates (ET 14:00).  The Fed publishes these a year in
# advance; update _FOMC_DATES when the next year's schedule is released.
_FOMC_DATES: list[tuple[str, str]] = [
    # 2026 schedule
    ("2026-01-29", "14:00"),
    ("2026-03-18", "14:00"),
    ("2026-04-29", "14:00"),
    ("2026-06-17", "14:00"),
    ("2026-07-29", "14:00"),
    ("2026-09-16", "14:00"),
    ("2026-10-28", "14:00"),
    ("2026-12-16", "14:00"),
]

# ── Caching ──────────────────────────────────────────────────────
_lock = threading.Lock()

# L1 in-memory cache: {"events": [...], "fetched_at": float, "is_mock": bool}
_l1_cache: dict | None = None
_L1_TTL = 3600          # 1 hour

# L2 DB freshness window — upcoming events are re-fetched after this
_DB_TTL = 6 * 3600      # 6 hours

# L1 result cache (filtered views, keyed by period/country/impact)
_result_cache: dict[str, tuple[float, dict]] = {}
_RESULT_TTL = 1800      # 30 min


# ── FRED API helpers ─────────────────────────────────────────────

def _fred_api(endpoint: str, params: dict) -> dict | None:
    """Call FRED REST API. Returns parsed JSON or None on error."""
    key = _fred_key()
    if not key:
        return None
    try:
        r = httpx.get(
            f"{_FRED_BASE}/{endpoint}",
            params={**params, "api_key": key, "file_type": "json"},
            timeout=_TIMEOUT,
        )
        r.raise_for_status()
        return r.json()
    except Exception:
        logger.warning("FRED API call failed: %s %s", endpoint, params, exc_info=True)
        return None


def _fetch_series_obs(series_id: str, units: str) -> list[dict]:
    """Return up to 3 recent observations [{date, value}] newest-first."""
    data = _fred_api("series/observations", {
        "series_id": series_id,
        "units": units,
        "sort_order": "desc",
        "limit": 3,
        "output_type": 1,
    })
    if not data:
        return []
    result = []
    for obs in data.get("observations", []):
        v = obs.get("value", ".")
        if v == ".":          # FRED uses "." for missing/not-yet-released
            continue
        try:
            result.append({"date": obs["date"], "value": float(v)})
        except (ValueError, KeyError):
            continue
    return result


def _fetch_release_dates(release_id: int, from_date: str, to_date: str) -> list[str]:
    """Return release dates (YYYY-MM-DD) for a FRED release within window."""
    data = _fred_api("release/dates", {
        "release_id": release_id,
        "include_release_dates_with_no_data": True,
        "sort_order": "asc",
        "limit": 30,
    })
    if not data:
        return []
    return [
        rd["date"]
        for rd in data.get("release_dates", [])
        if from_date <= rd.get("date", "") <= to_date
    ]


# ── Main FRED fetcher ────────────────────────────────────────────

def _fetch_series_events(
    series_id: str, meta: dict, window_start: str, window_end: str, today_str: str
) -> list[dict]:
    """Fetch events for one FRED series (obs + release dates). Called concurrently."""
    obs = _fetch_series_obs(series_id, meta["units"])
    latest = obs[0] if obs else None
    previous = obs[1] if len(obs) > 1 else None

    dates = _fetch_release_dates(meta["release_id"], window_start, window_end)
    if not dates:
        return []

    result = []
    for date_str in dates:
        is_past = date_str <= today_str
        result.append({
            "series_id": series_id,
            "event":    meta["name"],
            "date":     date_str,
            "time":     meta["time"],
            "country":  "US",
            "impact":   meta["impact"],
            "category": meta["category"],
            "actual":   round(latest["value"], 2) if (is_past and latest) else None,
            "previous": round(previous["value"], 2) if previous else None,
            "estimate": None,   # FRED does not publish consensus forecasts
            "unit":     meta["display_unit"],
            "source":   "fred",
        })
    return result


def _fetch_all_events() -> list[dict] | None:
    """
    Hit FRED API for every tracked series + FOMC dates in parallel.
    Returns a flat list of normalised event dicts, or None on total failure.
    """
    today = datetime.now()
    window_start = (today - timedelta(days=7)).strftime("%Y-%m-%d")
    window_end = (today + timedelta(days=28)).strftime("%Y-%m-%d")
    today_str = today.strftime("%Y-%m-%d")

    events: list[dict] = []
    failed = 0

    # Determine if we need the FEDFUNDS rate (any past FOMC in window)
    past_fomc_in_window = any(
        window_start <= d <= window_end and d <= today_str
        for d, _ in _FOMC_DATES
    )

    # ── Parallelise all series fetches + optional FEDFUNDS ───────
    with ThreadPoolExecutor(max_workers=6) as pool:
        series_futures = {
            pool.submit(_fetch_series_events, sid, meta, window_start, window_end, today_str): sid
            for sid, meta in _SERIES.items()
        }
        fedfunds_future = (
            pool.submit(_fetch_series_obs, "FEDFUNDS", "lin")
            if past_fomc_in_window else None
        )

        for fut, sid in series_futures.items():
            try:
                results = fut.result()
                if results:
                    events.extend(results)
                else:
                    failed += 1
            except Exception:
                logger.warning("FRED: series %s fetch failed", sid, exc_info=True)
                failed += 1

        fedfunds_rate = None
        if fedfunds_future:
            try:
                obs = fedfunds_future.result()
                if obs:
                    fedfunds_rate = round(obs[0]["value"], 2)
            except Exception:
                pass

    # ── FOMC rate-decision events (hardcoded schedule) ───────────
    for fomc_date, fomc_time in _FOMC_DATES:
        if not (window_start <= fomc_date <= window_end):
            continue
        is_past = fomc_date <= today_str
        events.append({
            "series_id": "FOMC",
            "event":    "FOMC Rate Decision",
            "date":     fomc_date,
            "time":     fomc_time,
            "country":  "US",
            "impact":   "high",
            "category": "monetary_policy",
            "actual":   fedfunds_rate if is_past else None,
            "previous": None,
            "estimate": None,
            "unit":     "%",
            "source":   "fred",
        })

    if not events and failed == len(_SERIES):
        logger.warning("FRED: all series failed to return data")
        return None

    logger.info("FRED calendar: %d events fetched", len(events))
    return events


# ── Finnhub + FMP supplemental fetchers ─────────────────────────

def _load_finnhub_economic() -> list[dict]:
    """Fetch Finnhub /calendar/economic → normalised event list."""
    key = _finnhub_key()
    if not key:
        return []
    try:
        end = datetime.now() + timedelta(weeks=4)
        start = datetime.now() - timedelta(weeks=1)
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
        raw = r.json().get("economicCalendar", [])
        events = []
        for item in raw:
            name = item.get("event", "")
            if not name:
                continue
            t = str(item.get("time", "") or "")
            if len(t) > 5:
                t = t[:5]
            country = str(item.get("country", "")).upper()
            events.append({
                "series_id": f"FH:{name[:40]}",
                "event": name,
                "date": str(item.get("date", "")),
                "time": t,
                "country": country,
                "impact": str(item.get("impact", "low")).lower(),
                "category": _categorise(name),
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
        return []


def _load_fmp_economic() -> list[dict]:
    """Fetch FMP /economic_calendar → normalised event list."""
    key = _fmp_key()
    if not key:
        return []
    try:
        end = datetime.now() + timedelta(weeks=4)
        start = datetime.now() - timedelta(weeks=1)
        r = httpx.get(
            "https://financialmodelingprep.com/api/v3/economic_calendar",
            params={
                "from": start.strftime("%Y-%m-%d"),
                "to": end.strftime("%Y-%m-%d"),
                "apikey": key,
            },
            timeout=_TIMEOUT,
        )
        r.raise_for_status()
        raw = r.json()
        if not isinstance(raw, list):
            return []
        events = []
        for item in raw:
            name = item.get("event", "")
            if not name:
                continue
            date_str = str(item.get("date", ""))
            t = ""
            if " " in date_str:
                date_str, time_part = date_str.split(" ", 1)
                t = time_part[:5] if len(time_part) >= 5 else time_part
            country = str(item.get("country", "")).upper()
            events.append({
                "series_id": f"FMP:{name[:40]}",
                "event": name,
                "date": date_str,
                "time": t,
                "country": country,
                "impact": str(item.get("impact", "Low")).lower(),
                "category": _categorise(name),
                "actual": item.get("actual"),
                "estimate": item.get("estimate"),
                "previous": item.get("previous"),
                "unit": str(item.get("unit", "")),
                "source": "fmp",
            })
        logger.info("FMP economic calendar: %d events", len(events))
        return events
    except Exception:
        logger.warning("FMP economic calendar fetch failed", exc_info=True)
        return []


def _categorise(event_name: str) -> str:
    """Match event name to a category via keyword search."""
    upper = event_name.upper()
    for cat, keywords in _CATEGORY_KEYWORDS.items():
        for kw in keywords:
            if kw in upper:
                return cat
    return "other"


def _dedup_key(ev: dict) -> str:
    """Generate a dedup key from event name + date.

    Normalises event names to catch near-duplicates across sources.
    E.g. "CPI (MoM)" from FRED vs "CPI m/m" from Finnhub,
    or "US CPI" from FMP vs "CPI" from FRED.
    """
    name = ev.get("event", "").upper()
    # Strip country prefixes: "US ", "U.S. ", "United States "
    name = _RE_COUNTRY_PREFIX.sub("", name)
    # Strip parenthetical suffixes: "(MoM)", "(QoQ)", "m/m", "q/q"
    name = _RE_PARENS.sub("", name)
    name = _RE_FREQ_SUFFIX.sub("", name)
    # Collapse whitespace
    name = _RE_WHITESPACE.sub(" ", name).strip()
    date = ev.get("date", "")
    return f"{date}:{name}"


def _merge_events(
    fred_events: list[dict],
    finnhub_events: list[dict],
    fmp_events: list[dict],
) -> list[dict]:
    """Merge events from all sources, preferring FRED > FMP > Finnhub.

    FRED events are authoritative (official release dates + actuals).
    FMP/Finnhub add estimate values and events FRED doesn't cover.
    """
    # Index FRED events by dedup key
    seen: dict[str, dict] = {}
    for ev in fred_events:
        seen[_dedup_key(ev)] = ev

    # Merge FMP (higher quality estimates than Finnhub)
    for ev in fmp_events:
        key = _dedup_key(ev)
        if key in seen:
            # FRED has this event — enrich with estimate if missing
            existing = seen[key]
            if existing.get("estimate") is None and ev.get("estimate") is not None:
                existing["estimate"] = ev["estimate"]
        else:
            seen[key] = ev

    # Merge Finnhub (fills remaining gaps)
    for ev in finnhub_events:
        key = _dedup_key(ev)
        if key in seen:
            existing = seen[key]
            if existing.get("estimate") is None and ev.get("estimate") is not None:
                existing["estimate"] = ev["estimate"]
        else:
            seen[key] = ev

    merged = list(seen.values())
    merged.sort(key=lambda e: (e.get("date", ""), e.get("time", "") or "99:99"))
    return merged


# ── Cache orchestration ──────────────────────────────────────────

def _event_to_row(ev: dict) -> dict:
    """Convert an internal event dict to an ``economic_events`` table row."""
    return {
        "series_id":    ev["series_id"],
        "event_name":   ev["event"],
        "release_date": ev["date"],
        "release_time": ev.get("time") or "08:30",
        "country":      ev.get("country", "US"),
        "category":     ev.get("category", "other"),
        "impact":       ev.get("impact", "medium"),
        "actual":       ev.get("actual"),
        "previous":     ev.get("previous"),
        "unit":         ev.get("unit", ""),
        "source":       ev.get("source", "fred"),
    }


def _row_to_event(row: dict) -> dict:
    """Convert an ``economic_events`` DB row back to an internal event dict."""
    return {
        "series_id": row["series_id"],
        "event":     row["event_name"],
        "date":      row["release_date"],
        "time":      row.get("release_time") or "08:30",
        "country":   row.get("country", "US"),
        "category":  row.get("category", "other"),
        "impact":    row.get("impact", "medium"),
        "actual":    float(row["actual"]) if row.get("actual") is not None else None,
        "previous":  float(row["previous"]) if row.get("previous") is not None else None,
        "estimate":  None,  # FRED has no consensus forecasts
        "unit":      row.get("unit", ""),
        "source":    row.get("source", "fred"),
    }


def _load_raw_events() -> tuple[list[dict], bool]:
    """
    Returns (events, is_mock).

    Cache hierarchy:
      L1 in-memory (1 hr)
      → L2 economic_events table, fresh rows only (TTL 6 hr)
      → L3 FRED API → upsert into economic_events
      → L2 stale fallback (any rows in window, regardless of age)
      → mock data
    """
    global _l1_cache

    now = time.time()

    # ── L1: in-memory ────────────────────────────────────────────
    with _lock:
        if _l1_cache and (now - _l1_cache["fetched_at"]) < _L1_TTL:
            return _l1_cache["events"], _l1_cache.get("is_mock", False)

    # ── No FRED key → mock immediately ───────────────────────────
    if not _fred_key():
        mock = _build_mock_events()
        with _lock:
            _l1_cache = {"events": mock, "fetched_at": now, "is_mock": True}
        return mock, True

    # Wide window covering all period options (-7d to +28d)
    today = datetime.now()
    window_start = (today - timedelta(days=7)).strftime("%Y-%m-%d")
    window_end   = (today + timedelta(days=28)).strftime("%Y-%m-%d")

    # ── L2: economic_events table (fresh rows only) ───────────────
    min_fetched = datetime.fromtimestamp(
        now - _DB_TTL, tz=timezone.utc
    ).isoformat()
    fresh_rows = supabase_cache.get_economic_events(
        window_start, window_end, min_fetched_at=min_fetched
    )
    if fresh_rows:
        events = [_row_to_event(r) for r in fresh_rows]
        with _lock:
            _l1_cache = {"events": events, "fetched_at": now, "is_mock": False}
        return events, False

    # ── L3: FRED API + Finnhub + FMP (merged, parallel) ─────────
    with ThreadPoolExecutor(max_workers=3) as pool:
        fred_fut = pool.submit(_fetch_all_events)
        finnhub_fut = pool.submit(_load_finnhub_economic)
        fmp_fut = pool.submit(_load_fmp_economic)
        fred_events = fred_fut.result()
        finnhub_events = finnhub_fut.result()
        fmp_events = fmp_fut.result()

    if fred_events or finnhub_events or fmp_events:
        merged = _merge_events(
            fred_events or [], finnhub_events, fmp_events
        )
        if merged:
            # Persist FRED-sourced rows to DB (skip Finnhub/FMP to avoid schema mismatch)
            fred_rows = [_event_to_row(e) for e in merged if e.get("source") == "fred"]
            if fred_rows:
                upserted = supabase_cache.upsert_economic_events(fred_rows)
                logger.info("FRED: upserted %d rows into economic_events", upserted)
            logger.info(
                "Economic calendar merged: %d FRED + %d Finnhub + %d FMP → %d total",
                len(fred_events or []), len(finnhub_events), len(fmp_events), len(merged),
            )
            with _lock:
                _l1_cache = {"events": merged, "fetched_at": now, "is_mock": False}
            return merged, False

    # ── Stale DB fallback ─────────────────────────────────────────
    stale_rows = supabase_cache.get_economic_events(window_start, window_end)
    if stale_rows:
        events = [_row_to_event(r) for r in stale_rows]
        with _lock:
            _l1_cache = {"events": events, "fetched_at": now, "is_mock": False}
        logger.warning("FRED API failed; serving stale economic_events data")
        return events, False

    # ── Last resort: mock data ────────────────────────────────────
    mock = _build_mock_events()
    with _lock:
        _l1_cache = {"events": mock, "fetched_at": now, "is_mock": True}
    logger.warning("FRED calendar unavailable; using mock data")
    return mock, True


# ── Value formatting ─────────────────────────────────────────────

def _fmt_val(val: float | None, unit: str) -> str:
    if val is None:
        return "—"
    if unit == "%":
        return f"{val:+.2f}%"
    if unit == "K":
        return f"{val:,.0f}K"
    if unit in ("B", "b"):
        return f"{val:.1f}B"
    return f"{val:.2f}"


def _date_label(date_str: str) -> str:
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        today = datetime.now().date()
        d = dt.date()
        if d == today:
            return f"Today — {dt.strftime('%A, %B %-d')}"
        if d == today + timedelta(days=1):
            return f"Tomorrow — {dt.strftime('%A, %B %-d')}"
        return dt.strftime("%A, %B %-d, %Y")
    except ValueError:
        return date_str


def _date_range_for_period(period: str) -> tuple[str, str]:
    today = datetime.now()
    weekday = today.weekday()
    monday = today - timedelta(days=weekday)
    if weekday >= 5:
        monday = today + timedelta(days=(7 - weekday))

    if period == "this_week":
        start, end = monday, monday + timedelta(days=4)
    elif period == "next_week":
        start = monday + timedelta(weeks=1)
        end = start + timedelta(days=4)
    elif period == "next_2w":
        start = monday
        end = monday + timedelta(weeks=2, days=4)
    else:  # "all"
        start = today - timedelta(days=7)
        end = today + timedelta(days=28)

    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


# ── Public API ───────────────────────────────────────────────────

def fetch_economic_events(
    period: str = "this_week",
    country: str = "us",
    impact_filter: str = "all",
) -> dict:
    """
    Fetch, filter, enrich and group economic events.

    Identical signature to economic_calendar.fetch_economic_events so
    web.py needs no changes beyond the import swap.

    Returns:
        events_by_date, metrics, countdown_target, period,
        period_label, country, impact_filter, is_mock, is_unavailable
    """
    if period not in PERIOD_CHOICES:
        period = "this_week"
    if country not in COUNTRY_CHOICES:
        country = "us"
    if impact_filter not in IMPACT_CHOICES:
        impact_filter = "all"

    # ── L1 result cache ──────────────────────────────────────────
    cache_key = f"fred_result:{period}:{country}:{impact_filter}"
    now = time.time()
    with _lock:
        cached = _result_cache.get(cache_key)
        if cached and (now - cached[0]) < _RESULT_TTL:
            return cached[1]

    # ── Load raw events ──────────────────────────────────────────
    raw_events, is_mock = _load_raw_events()

    # ── Filter ───────────────────────────────────────────────────
    start_str, end_str = _date_range_for_period(period)

    filtered = []
    for ev in raw_events:
        d = ev.get("date", "")
        if not (start_str <= d <= end_str):
            continue
        if country == "us" and ev.get("country", "") not in ("US", ""):
            continue
        if impact_filter == "high" and ev.get("impact") != "high":
            continue
        filtered.append(ev)

    # ── Enrich ───────────────────────────────────────────────────
    for ev in filtered:
        actual = ev.get("actual")
        previous = ev.get("previous")
        unit = ev.get("unit", "")

        ev["is_released"] = actual is not None

        # Delta: prefer actual vs estimate (beat/miss), fall back to actual vs previous
        estimate = ev.get("estimate")
        compare = estimate if estimate is not None else previous
        if actual is not None and compare is not None:
            try:
                delta = float(actual) - float(compare)
                ev["delta"] = delta
                ev["delta_direction"] = "hot" if delta > 0 else ("cold" if delta < 0 else "neutral")
            except (TypeError, ValueError):
                ev["delta"] = None
                ev["delta_direction"] = None
        else:
            ev["delta"] = None
            ev["delta_direction"] = None

        ev["impact_class"] = {
            "high":   "ed-impact-high",
            "medium": "ed-impact-medium",
            "low":    "ed-impact-low",
        }.get(ev.get("impact", "low"), "ed-impact-low")

        ev["actual_fmt"]   = _fmt_val(actual, unit)
        ev["estimate_fmt"] = _fmt_val(ev.get("estimate"), unit)
        ev["previous_fmt"] = _fmt_val(previous, unit)
        ev["time_label"]   = ev.get("time", "") or "TBD"

    # ── Group by date ────────────────────────────────────────────
    date_groups: dict[str, list[dict]] = {}
    for ev in filtered:
        date_groups.setdefault(ev["date"], []).append(ev)

    for entries in date_groups.values():
        entries.sort(key=lambda e: e.get("time", "") or "99:99")

    events_by_date = [
        {
            "date":       d,
            "date_label": _date_label(d),
            "entries":    date_groups[d],
        }
        for d in sorted(date_groups)
    ]

    # ── Countdown target ─────────────────────────────────────────
    countdown_target = None
    for day in events_by_date:
        for ev in day["entries"]:
            if ev["is_released"]:
                continue
            if ev.get("impact") != "high":
                continue
            d = ev["date"]
            t = ev.get("time", "") or "08:30"
            countdown_target = {
                "event":        ev["event"],
                "date":         d,
                "time":         t,
                "iso_datetime": f"{d}T{t}:00",
            }
            break
        if countdown_target:
            break

    # ── Metrics ──────────────────────────────────────────────────
    total     = len(filtered)
    high_cnt  = sum(1 for e in filtered if e.get("impact") == "high")
    released  = sum(1 for e in filtered if e["is_released"])
    upcoming  = total - released

    result = {
        "events_by_date":  events_by_date,
        "metrics": {
            "total_events":      total,
            "high_impact_count": high_cnt,
            "released_count":    released,
            "upcoming_count":    upcoming,
        },
        "countdown_target": countdown_target,
        "period":           period,
        "period_label":     PERIOD_CHOICES.get(period, period),
        "country":          country,
        "impact_filter":    impact_filter,
        "is_mock":          is_mock,
        "is_unavailable":   not raw_events and not is_mock,
        "source":           "merged",  # FRED + Finnhub + FMP
    }

    with _lock:
        _result_cache[cache_key] = (now, result)

    return result


# ── Mock data ────────────────────────────────────────────────────

def _build_mock_events() -> list[dict]:
    """Deterministic mock US economic events (seed 42) for dev/demo."""
    rng = random.Random(42)
    today = datetime.now()

    _MOCK = [
        # (series_id,            name,                          time,    impact,   unit, est,   prev)
        ("CPIAUCSL",        "CPI (MoM)",                "08:30", "high",   "%",  0.2,  0.3),
        ("CPILFESL",        "Core CPI (MoM)",           "08:30", "high",   "%",  0.3,  0.3),
        ("PCEPILFE",        "Core PCE (MoM)",           "08:30", "high",   "%",  0.3,  0.2),
        ("PPIFID",          "PPI Final Demand (MoM)",   "08:30", "medium", "%",  0.1,  0.2),
        ("PAYEMS",          "Nonfarm Payrolls",         "08:30", "high",   "K",  180,  200),
        ("UNRATE",          "Unemployment Rate",        "08:30", "high",   "%",  3.8,  3.9),
        ("ICSA",            "Initial Jobless Claims",   "08:30", "medium", "K",  210,  215),
        ("A191RL1Q225SBEA", "GDP (QoQ, Annualized)",    "08:30", "high",   "%",  3.2,  2.8),
        ("RSXFS",           "Retail Sales (MoM)",       "08:30", "high",   "%",  0.4,  0.3),
        ("DGORDER",         "Durable Goods Orders (MoM)","08:30","medium", "%", -0.8,  1.2),
        ("HOUST",           "Housing Starts",           "08:30", "medium", "K", 1480, 1500),
        ("UMCSENT",         "Michigan Consumer Sentiment","10:00","medium","",   69.7, 67.4),
        ("FOMC",            "FOMC Rate Decision",       "14:00", "high",   "%",  5.25, 5.50),
    ]

    events = []
    used_slots: set[tuple[str, str]] = set()
    for i, (series_id, name, t, impact, unit, est, prev) in enumerate(_MOCK):
        day_offset = (i * 17 + rng.randint(0, 3)) % 14 - 3
        d = today + timedelta(days=day_offset)
        while d.weekday() >= 5:
            d += timedelta(days=1)
        date_str = d.strftime("%Y-%m-%d")

        slot = (date_str, t)
        orig_t = t
        while slot in used_slots:
            hh, mm = orig_t.split(":")
            mm = str((int(mm) + 30) % 60).zfill(2)
            if mm == "00":
                hh = str(int(hh) + 1).zfill(2)
            orig_t = f"{hh}:{mm}"
            slot = (date_str, orig_t)
        t = orig_t
        used_slots.add(slot)

        is_past = d.date() < today.date()
        actual = None
        if is_past:
            actual = round(est + rng.uniform(-0.15, 0.15) * abs(est or 1), 2)

        cat = {
            "CPI": "inflation", "PCE": "inflation", "PPI": "inflation",
            "Nonfarm": "employment", "Unemployment": "employment",
            "Initial": "employment", "GDP": "growth",
            "Retail": "consumer", "Michigan": "consumer",
            "Durable": "manufacturing", "Housing": "housing",
            "FOMC": "monetary_policy",
        }
        category = next((v for k, v in cat.items() if k in name), "other")

        events.append({
            "series_id": series_id,
            "event":    name,
            "date":     date_str,
            "time":     t,
            "country":  "US",
            "impact":   impact,
            "category": category,
            "actual":   actual,
            "estimate": None,
            "previous": prev,
            "unit":     unit,
            "source":   "mock",
        })

    return events
