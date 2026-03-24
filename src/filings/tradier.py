"""Tradier REST API client — options chains with ORATS greeks.

Provides reliable options chain data with real greeks (delta, gamma,
theta, vega) as an alternative to yfinance for the options sync worker.

Supports both sandbox (free, delayed) and production (brokerage account,
real-time) modes, controlled by env vars.

Env vars:
    TRADIER_API_KEY   — required (get from developer.tradier.com)
    TRADIER_SANDBOX   — "true" (default) for sandbox, "false" for production
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
import urllib.request

logger = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────────────────────

_SANDBOX_BASE = "https://sandbox.tradier.com/v1"
_PROD_BASE = "https://api.tradier.com/v1"
_TIMEOUT = 12

# Cache TTLs
_EXPIRY_TTL = 3600      # 1 hour for expiration list
_CHAIN_TTL = 300         # 5 min for option chains
_QUOTE_TTL = 300         # 5 min for stock quotes

# ── Thread-safe in-memory cache ──────────────────────────────────────────────

_lock = threading.Lock()

# {TICKER: (timestamp, [expiry_dates])}
_expiry_cache: dict[str, tuple[float, list[str]]] = {}

# {TICKER:EXPIRY: (timestamp, chain_dict)}
_chain_cache: dict[str, tuple[float, dict]] = {}

# {TICKER: (timestamp, quote_dict)}
_quote_cache: dict[str, tuple[float, dict]] = {}

_MAX_CACHE_ENTRIES = 500


# ── Public helpers ───────────────────────────────────────────────────────────


def has_tradier_key() -> bool:
    """Check if Tradier API key is configured."""
    return bool(os.environ.get("TRADIER_API_KEY"))


def _base_url() -> str:
    """Return sandbox or production base URL."""
    sandbox = os.environ.get("TRADIER_SANDBOX", "true").lower()
    return _SANDBOX_BASE if sandbox in ("true", "1", "yes") else _PROD_BASE


# ── HTTP helper ──────────────────────────────────────────────────────────────


def _tradier_get(path: str, params: dict | None = None, timeout: int = _TIMEOUT) -> dict | list | None:
    """GET request to Tradier API with Bearer auth.

    Returns parsed JSON or None on failure.
    """
    api_key = os.environ.get("TRADIER_API_KEY", "")
    if not api_key:
        return None

    base = _base_url()
    url = f"{base}{path}"

    if params:
        qs = "&".join(f"{k}={urllib.request.quote(str(v))}" for k, v in params.items() if v is not None)
        url = f"{url}?{qs}"

    req = urllib.request.Request(url)
    req.add_header("Authorization", f"Bearer {api_key}")
    req.add_header("Accept", "application/json")

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except Exception as exc:
        logger.debug("Tradier GET %s failed: %s", url, exc)
        return None


def _evict_oldest(cache: dict, max_size: int = _MAX_CACHE_ENTRIES) -> None:
    """Evict oldest entries if cache exceeds max_size. Must hold _lock."""
    if len(cache) <= max_size:
        return
    sorted_keys = sorted(cache, key=lambda k: cache[k][0])
    for k in sorted_keys[: len(cache) - max_size]:
        del cache[k]


# ── Expirations ──────────────────────────────────────────────────────────────


def get_expirations(ticker: str) -> list[str]:
    """Get available option expiration dates for a ticker.

    Returns list of ISO date strings, e.g. ["2024-03-15", "2024-03-22"].
    Cached for 1 hour.
    """
    key = ticker.upper()

    with _lock:
        cached = _expiry_cache.get(key)
        if cached and time.time() - cached[0] < _EXPIRY_TTL:
            return cached[1]

    data = _tradier_get(f"/markets/options/expirations", params={
        "symbol": key,
        "includeAllRoots": "true",
    })

    if not data or not isinstance(data, dict):
        return []

    expirations = data.get("expirations", {})
    dates = expirations.get("date", []) if isinstance(expirations, dict) else []

    # Handle single date returned as string instead of list
    if isinstance(dates, str):
        dates = [dates]

    with _lock:
        _expiry_cache[key] = (time.time(), dates)
        _evict_oldest(_expiry_cache)

    return dates


# ── Option Chains ────────────────────────────────────────────────────────────


def get_option_chain(
    ticker: str,
    expiration: str,
    greeks: bool = True,
) -> dict | None:
    """Fetch full option chain for a ticker + expiration date.

    Args:
        ticker: Uppercase ticker symbol.
        expiration: ISO date string (e.g. "2024-03-15").
        greeks: Include ORATS greeks (delta, gamma, theta, vega).

    Returns raw Tradier chain dict, or None.
    Cached for 5 minutes.
    """
    cache_key = f"{ticker.upper()}:{expiration}"

    with _lock:
        cached = _chain_cache.get(cache_key)
        if cached and time.time() - cached[0] < _CHAIN_TTL:
            return cached[1]

    data = _tradier_get("/markets/options/chains", params={
        "symbol": ticker.upper(),
        "expiration": expiration,
        "greeks": "true" if greeks else "false",
    })

    if not data or not isinstance(data, dict):
        return None

    with _lock:
        _chain_cache[cache_key] = (time.time(), data)
        _evict_oldest(_chain_cache)

    return data


# ── Stock Quotes ─────────────────────────────────────────────────────────────


def get_quote(ticker: str) -> dict | None:
    """Fetch a stock quote from Tradier.

    Returns dict with keys: last, open, high, low, close, volume,
    prevclose, etc. Cached for 5 minutes.
    """
    key = ticker.upper()

    with _lock:
        cached = _quote_cache.get(key)
        if cached and time.time() - cached[0] < _QUOTE_TTL:
            return cached[1]

    data = _tradier_get("/markets/quotes", params={"symbols": key})

    if not data or not isinstance(data, dict):
        return None

    quotes = data.get("quotes", {})
    quote = quotes.get("quote")

    if not quote or not isinstance(quote, dict):
        return None

    with _lock:
        _quote_cache[key] = (time.time(), quote)
        _evict_oldest(_quote_cache)

    return quote


# ── DataFrame Adapter ────────────────────────────────────────────────────────


def chain_to_dataframes(
    chain_data: dict,
) -> tuple[object, object] | None:
    """Convert a Tradier chain response to (calls_df, puts_df) DataFrames.

    The DataFrames match yfinance column names so ``detect_unusual()`` can
    consume them unchanged:
        contractSymbol, strike, volume, openInterest, bid, ask, lastPrice,
        impliedVolatility, delta, gamma, theta, vega

    Returns ``(calls_df, puts_df)`` or None if no data.
    """
    try:
        import pandas as pd
    except ImportError:
        logger.warning("pandas not available for Tradier DataFrame adapter")
        return None

    options = chain_data.get("options", {})
    option_list = options.get("option", [])

    if not option_list:
        return None

    # Handle single option returned as dict instead of list
    if isinstance(option_list, dict):
        option_list = [option_list]

    calls_rows = []
    puts_rows = []

    for opt in option_list:
        greeks = opt.get("greeks") or {}

        row = {
            "contractSymbol": opt.get("symbol", ""),
            "strike": float(opt.get("strike", 0)),
            "volume": int(opt.get("volume") or 0),
            "openInterest": int(opt.get("open_interest") or 0),
            "bid": float(opt.get("bid") or 0),
            "ask": float(opt.get("ask") or 0),
            "lastPrice": float(opt.get("last") or 0),
            "impliedVolatility": float(greeks.get("mid_iv") or 0),
            "delta": _safe_greek(greeks.get("delta")),
            "gamma": _safe_greek(greeks.get("gamma")),
            "theta": _safe_greek(greeks.get("theta")),
            "vega": _safe_greek(greeks.get("vega")),
        }

        if opt.get("option_type") == "call":
            calls_rows.append(row)
        else:
            puts_rows.append(row)

    calls_df = pd.DataFrame(calls_rows) if calls_rows else pd.DataFrame()
    puts_df = pd.DataFrame(puts_rows) if puts_rows else pd.DataFrame()

    return calls_df, puts_df


def _safe_greek(val) -> float | None:
    """Convert a greek value to float, returning None for invalid."""
    if val is None:
        return None
    try:
        f = float(val)
        return f
    except (ValueError, TypeError):
        return None


# ── Cache management ─────────────────────────────────────────────────────────


def invalidate_cache() -> None:
    """Clear all L1 caches."""
    with _lock:
        _expiry_cache.clear()
        _chain_cache.clear()
        _quote_cache.clear()
