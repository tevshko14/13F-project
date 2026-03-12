"""FRED (Federal Reserve Economic Data) integration.

Requires FRED_API_KEY environment variable.
Free API key from https://fred.stlouisfed.org/docs/api/api_key.html

Key series tracked:
  GDP, CPIAUCSL (CPI), UNRATE (unemployment), FEDFUNDS,
  GS10 (10Y yield), T10Y2Y (yield spread), DGS2 (2Y yield)
"""

import json
import logging
import os
import threading
import time
import urllib.request

logger = logging.getLogger(__name__)

_FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
_lock = threading.Lock()
_cache: dict[str, tuple[float, list]] = {}
_TTL = 3600 * 4  # 4 hours

# Key economic series for the macro dashboard
SERIES_META = {
    "GDP":       {"name": "Real GDP",            "unit": "Billions $",  "freq": "quarterly", "chart": "bar",  "color": "#0d9488"},
    "CPIAUCSL":  {"name": "Consumer Price Index", "unit": "Index",      "freq": "monthly",   "chart": "line", "color": "#f59e0b"},
    "UNRATE":    {"name": "Unemployment Rate",    "unit": "%",          "freq": "monthly",   "chart": "area", "color": "#ef4444"},
    "FEDFUNDS":  {"name": "Fed Funds Rate",       "unit": "%",          "freq": "monthly",   "chart": "line", "color": "#8b5cf6"},
    "GS10":      {"name": "10-Year Treasury",     "unit": "%",          "freq": "daily",     "chart": "line", "color": "#3b82f6"},
    "T10Y2Y":    {"name": "10Y-2Y Spread",        "unit": "%",          "freq": "daily",     "chart": "area", "color": "#ec4899"},
}

# Shorter subset for quick summary cards
SUMMARY_SERIES = ["FEDFUNDS", "UNRATE", "CPIAUCSL", "T10Y2Y", "GS10", "GDP"]


def _get_api_key() -> str:
    return os.environ.get("FRED_API_KEY", "")


def _cached_or_fetch(cache_key: str, fetcher, ttl: int = _TTL):
    """Generic L1 → L2 → fetcher pattern."""
    with _lock:
        cached = _cache.get(cache_key)
        if cached and time.time() - cached[0] < ttl:
            return cached[1]

    try:
        from filings import supabase_cache
        l2 = supabase_cache.get_cached(cache_key)
        if l2:
            with _lock:
                _cache[cache_key] = (time.time(), l2)
            return l2
    except Exception:
        pass

    try:
        result = fetcher()
        if result:
            try:
                from filings import supabase_cache
                supabase_cache.set_cached(cache_key, "fred", result, 3600 * 12)
            except Exception:
                pass
            with _lock:
                _cache[cache_key] = (time.time(), result)
        return result
    except Exception as exc:
        logger.warning("FRED fetch failed for %s: %s", cache_key, exc)
        return None


def _fetch_series(series_id: str, limit: int) -> list[dict]:
    """Fetch a single FRED series from the API."""
    key = _get_api_key()
    if not key:
        logger.debug("FRED_API_KEY not set — skipping %s", series_id)
        return []

    url = (
        f"{_FRED_BASE}?series_id={series_id}"
        f"&api_key={key}&file_type=json"
        f"&sort_order=desc&limit={limit}"
    )
    req = urllib.request.Request(url, headers={"User-Agent": "PaperPanda/1.0"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read())

    observations = data.get("observations", [])
    result = []
    for obs in observations:
        val = obs.get("value", ".")
        if val == ".":
            continue
        result.append({"date": obs["date"], "value": float(val)})

    result.reverse()  # chronological order
    return result


def get_series(series_id: str, limit: int = 120) -> list[dict]:
    """Fetch a FRED time series. Returns list of {date, value}."""
    limit = min(limit, 500)  # clamp to prevent unbounded fetches
    return _cached_or_fetch(
        f"fred:{series_id}:{limit}",
        lambda: _fetch_series(series_id, limit),
    ) or []


def get_dashboard_data() -> dict:
    """Fetch all key economic indicators for the macro dashboard.

    Returns dict keyed by series_id with meta + data + latest/prev values.
    Uses thread pool to parallelize fetches on cold cache.
    """
    from concurrent.futures import ThreadPoolExecutor

    def _fetch_one(series_id: str):
        return series_id, get_series(series_id)

    result = {}
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = pool.map(_fetch_one, SUMMARY_SERIES)

    for series_id, data in futures:
        if data:
            meta = SERIES_META.get(series_id, {})
            latest = data[-1]
            prev = data[-2] if len(data) > 1 else None
            change = None
            if prev:
                change = latest["value"] - prev["value"]
            result[series_id] = {
                **meta,
                "series_id": series_id,
                "data": data[-60:],  # last 60 data points for charts
                "latest": latest,
                "prev": prev,
                "change": change,
            }
    return result
