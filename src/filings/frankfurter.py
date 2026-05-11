"""Frankfurter FX rates API.

Free, no API key, no rate limits.
Source: https://www.frankfurter.app/

Provides:
  - Latest exchange rates (base USD)
  - Historical time series for major pairs
  - Currency list
"""

import json
import logging
import urllib.request
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

from filings.caching import TTLCache as _TTLCache

_BASE_URL = "https://api.frankfurter.app"
_TTL = 3600 * 2  # 2 hours
# ~12 currency pairs × a handful of historical windows = small fixed set.
_cache = _TTLCache(ttl=_TTL, max_size=200)

# Major pairs we track (vs USD)
MAJOR_CURRENCIES = {
    "EUR": "Euro",
    "GBP": "British Pound",
    "JPY": "Japanese Yen",
    "CHF": "Swiss Franc",
    "CAD": "Canadian Dollar",
    "AUD": "Australian Dollar",
    "CNY": "Chinese Yuan",
    "INR": "Indian Rupee",
    "KRW": "South Korean Won",
    "BRL": "Brazilian Real",
    "MXN": "Mexican Peso",
    "SEK": "Swedish Krona",
}


def _cached_or_fetch(cache_key: str, fetcher, ttl: int = _TTL):
    """Generic L1 → L2 → fetcher pattern."""
    cached = _cache.get(cache_key)
    if cached is not None:
        return cached

    try:
        from filings import supabase_cache
        l2 = supabase_cache.get_cached(cache_key)
        if l2:
            _cache.set(cache_key, l2)
            return l2
    except Exception:
        pass

    try:
        result = fetcher()
        if result:
            try:
                from filings import supabase_cache
                supabase_cache.set_cached(cache_key, "frankfurter", result, 3600 * 6)
            except Exception:
                pass
            _cache.set(cache_key, result)
        return result
    except Exception as exc:
        logger.warning("Frankfurter fetch failed for %s: %s", cache_key, exc)
        return None


def _fetch_latest() -> dict | None:
    """Fetch latest FX rates from Frankfurter (base USD)."""
    try:
        symbols = ",".join(MAJOR_CURRENCIES.keys())
        url = f"{_BASE_URL}/latest?from=USD&to={symbols}"
        req = urllib.request.Request(url, headers={"User-Agent": "PaperPanda/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())

        rates = data.get("rates", {})
        date = data.get("date", "")

        result = []
        for code, name in MAJOR_CURRENCIES.items():
            if code in rates:
                result.append({
                    "code": code,
                    "name": name,
                    "rate": rates[code],
                    "inverse": round(1 / rates[code], 6) if rates[code] else None,
                })

        return {"date": date, "base": "USD", "rates": result}
    except Exception as exc:
        logger.warning("FX latest fetch failed: %s", exc)
        return None


def _fetch_timeseries(currency: str, days: int = 90) -> list[dict] | None:
    """Fetch historical rates for a single currency pair vs USD."""
    try:
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

        url = f"{_BASE_URL}/{start_date}..{end_date}?from=USD&to={currency}"
        req = urllib.request.Request(url, headers={"User-Agent": "PaperPanda/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())

        rates = data.get("rates", {})
        result = []
        for date_str in sorted(rates.keys()):
            val = rates[date_str].get(currency)
            if val is not None:
                result.append({"date": date_str, "rate": val})

        return result if result else None
    except Exception as exc:
        logger.warning("FX timeseries fetch failed for %s: %s", currency, exc)
        return None


def get_latest_rates() -> dict | None:
    """Get latest FX rates (cached)."""
    return _cached_or_fetch("fx:latest", _fetch_latest)


def get_timeseries(currency: str, days: int = 90) -> list[dict] | None:
    """Get historical rate for a currency pair (cached)."""
    cache_key = f"fx:ts:{currency}:{days}"
    return _cached_or_fetch(cache_key, lambda: _fetch_timeseries(currency, days))


def get_fx_dashboard() -> dict:
    """Get all FX data for the macro dashboard.

    Parallelizes sparkline fetches for performance.
    """
    from concurrent.futures import ThreadPoolExecutor

    latest = get_latest_rates()

    # Get mini sparkline data for top 4 currencies (parallel)
    sparkline_codes = ["EUR", "GBP", "JPY", "CNY"]

    def _fetch_ts(code):
        return code, get_timeseries(code, 30)

    sparklines = {}
    with ThreadPoolExecutor(max_workers=4) as pool:
        for code, ts in pool.map(_fetch_ts, sparkline_codes):
            if ts:
                sparklines[code] = ts

    return {
        "latest": latest,
        "sparklines": sparklines,
    }
