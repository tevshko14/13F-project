"""Tiingo REST API client — real-time IEX prices and historical EOD data.

Provides a reliable, low-cost ($10/mo) alternative to yfinance for:
  1. Real-time IEX stock quotes (sub-second latency)
  2. Batch quotes (up to 100 tickers per call)
  3. EOD historical close prices (30+ years of data)

All results are cached in memory with aggressive TTLs.
Falls back gracefully when TIINGO_API_KEY is not set.

Env vars:
    TIINGO_API_KEY  — required (get from api.tiingo.com)
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
import urllib.request
from datetime import date, timedelta

from filings.caching import TTLCache

logger = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────────────────────

_TIINGO_BASE = "https://api.tiingo.com"
_IEX_BASE = "https://api.tiingo.com/iex"
_TIMEOUT = 12  # seconds per HTTP request

# Cache TTLs
_QUOTE_TTL = 300          # 5 min for individual quotes (also used for batch)
_EOD_TTL = 3600           # 1 hour for EOD history
_CLOSE_DF_TTL = 3600      # 1 hour for S&P 500 close matrix

# ── Thread-safe in-memory cache ──────────────────────────────────────────────

# Lock covers the single-entry _close_df_cache below; per-ticker caches
# manage their own locking via TTLCache.
_lock = threading.Lock()

_MAX_CACHE_ENTRIES = 500

# Per-ticker quote cache
_quote_cache = TTLCache(ttl=_QUOTE_TTL, max_size=_MAX_CACHE_ENTRIES)

# Per-ticker EOD cache
_eod_cache = TTLCache(ttl=_EOD_TTL, max_size=_MAX_CACHE_ENTRIES)

# S&P 500 close DataFrame cache: (timestamp, DataFrame) | None
_close_df_cache: tuple[float, object] | None = None


# ── Public helpers ───────────────────────────────────────────────────────────


def has_tiingo_key() -> bool:
    """Check if Tiingo API key is configured."""
    return bool(os.environ.get("TIINGO_API_KEY"))


# ── HTTP helper ──────────────────────────────────────────────────────────────


def _tiingo_get(url: str, timeout: int = _TIMEOUT) -> dict | list | None:
    """GET request to Tiingo API with auth header.

    Returns parsed JSON or None on failure.
    """
    api_key = os.environ.get("TIINGO_API_KEY", "")
    if not api_key:
        return None

    req = urllib.request.Request(url)
    req.add_header("Authorization", f"Token {api_key}")
    req.add_header("Content-Type", "application/json")

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except Exception as exc:
        logger.debug("Tiingo GET %s failed: %s", url, exc)
        return None


# ── Real-time IEX Quotes ────────────────────────────────────────────────────


def get_quote(ticker: str) -> dict | None:
    """Fetch a single real-time IEX quote for a ticker.

    Returns dict with keys: last, prevClose, open, high, low, volume,
    mid, askPrice, bidPrice, timestamp, etc.

    Cached for 5 minutes.
    """
    key = ticker.upper()

    cached = _quote_cache.get(key)
    if cached is not None:
        return cached

    url = f"{_IEX_BASE}/{key}"
    data = _tiingo_get(url)

    if not data:
        return None

    # IEX returns a list with one element
    quote = data[0] if isinstance(data, list) and data else data
    if not isinstance(quote, dict):
        return None

    _quote_cache.set(key, quote)

    return quote


def get_quotes_batch(tickers: list[str]) -> dict[str, dict]:
    """Fetch real-time IEX quotes for multiple tickers in a single call.

    Tiingo supports comma-separated tickers (up to ~100 per call).
    Returns ``{TICKER: quote_dict}`` mapping.
    """
    if not tickers or not has_tiingo_key():
        return {}

    result: dict[str, dict] = {}
    needed: list[str] = []

    # Check cache first (BATCH_QUOTE_TTL matches QUOTE_TTL, so reuse _quote_cache)
    for tk in tickers:
        key = tk.upper()
        cached = _quote_cache.get(key)
        if cached is not None:
            result[key] = cached
        else:
            needed.append(key)

    if not needed:
        return result

    # Fetch in chunks of 100
    CHUNK = 100
    for i in range(0, len(needed), CHUNK):
        chunk = needed[i : i + CHUNK]
        tickers_csv = ",".join(chunk)
        url = f"{_IEX_BASE}/?tickers={tickers_csv}"
        data = _tiingo_get(url)

        if not data or not isinstance(data, list):
            continue

        for item in data:
            tk = (item.get("ticker") or "").upper()
            if tk:
                _quote_cache.set(tk, item)
                result[tk] = item

    return result


def get_price(ticker: str) -> float | None:
    """Get the current price for a ticker (convenience wrapper).

    Returns the 'last' traded price from IEX, or None.
    """
    quote = get_quote(ticker)
    if quote is None:
        return None
    return quote.get("last") or quote.get("tngoLast")


# ── EOD Historical Data ─────────────────────────────────────────────────────


def get_eod_history(
    ticker: str,
    start: str | None = None,
    end: str | None = None,
    period_days: int = 30,
) -> list[dict] | None:
    """Fetch EOD historical OHLCV data for a ticker.

    Args:
        ticker: Uppercase ticker symbol.
        start: ISO date start (default: period_days ago).
        end: ISO date end (default: today).
        period_days: Number of days to look back if start not given.

    Returns list of dicts with: date, close, high, low, open, volume,
    adjClose, adjHigh, adjLow, adjOpen, adjVolume, divCash, splitFactor.

    Cached for 1 hour.
    """
    key = ticker.upper()

    cached = _eod_cache.get(key)
    if cached is not None:
        return cached

    if not start:
        start = (date.today() - timedelta(days=period_days)).isoformat()
    if not end:
        end = date.today().isoformat()

    url = (
        f"{_TIINGO_BASE}/tiingo/daily/{key}/prices"
        f"?startDate={start}&endDate={end}"
    )
    data = _tiingo_get(url)

    if not data or not isinstance(data, list):
        return None

    _eod_cache.set(key, data)

    return data


def get_close_df_for_sp500(
    tickers: list[str],
    period_days: int = 30,
) -> object | None:
    """Fetch EOD close prices for a list of tickers, return as DataFrame.

    Used by ``market_data._ensure_close_df()`` as a faster alternative
    to yfinance bulk download. Chunks tickers into batches of 50.

    Returns a pandas DataFrame (columns=tickers, index=dates) or None.
    """
    global _close_df_cache

    with _lock:
        if _close_df_cache is not None:
            ts, df = _close_df_cache
            if time.time() - ts < _CLOSE_DF_TTL:
                return df

    if not tickers or not has_tiingo_key():
        return None

    try:
        import pandas as pd
    except ImportError:
        return None

    start = (date.today() - timedelta(days=period_days + 5)).isoformat()
    end = date.today().isoformat()

    all_series: dict[str, dict[str, float]] = {}
    CHUNK = 50
    for i in range(0, len(tickers), CHUNK):
        chunk = tickers[i : i + CHUNK]
        for tk in chunk:
            url = (
                f"{_TIINGO_BASE}/tiingo/daily/{tk}/prices"
                f"?startDate={start}&endDate={end}&format=json"
            )
            data = _tiingo_get(url, timeout=15)
            if not data or not isinstance(data, list):
                continue

            series: dict[str, float] = {}
            for row in data:
                dt = row.get("date", "")[:10]  # "2024-01-15T00:00:00+00:00" → "2024-01-15"
                close = row.get("adjClose") or row.get("close")
                if dt and close is not None:
                    series[dt] = float(close)

            if series:
                all_series[tk.upper()] = series

    if not all_series:
        return None

    # Build DataFrame
    df = pd.DataFrame(all_series)
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()

    # Only keep the latest period_days of trading rows
    if len(df) > period_days:
        df = df.tail(period_days)

    logger.info(
        "Tiingo close_df: %d rows x %d tickers",
        len(df), len(df.columns),
    )

    with _lock:
        _close_df_cache = (time.time(), df)

    return df


# ── Cache management ─────────────────────────────────────────────────────────


def invalidate_cache() -> None:
    """Clear all L1 caches."""
    global _close_df_cache
    with _lock:
        _quote_cache.clear()
        _eod_cache.clear()
        _close_df_cache = None


def warm_from_supabase() -> bool:
    """Placeholder for future Supabase warm-load of Tiingo data.

    Currently a no-op since Tiingo quotes are fast enough to fetch live.
    Returns True if Tiingo is configured.
    """
    if has_tiingo_key():
        logger.info("Tiingo API configured — real-time prices enabled")
        return True
    logger.info("Tiingo API not configured — using yfinance fallback")
    return False
