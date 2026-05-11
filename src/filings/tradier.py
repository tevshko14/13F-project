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
import urllib.request

from filings.caching import TTLCache

logger = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────────────────────

_SANDBOX_BASE = "https://sandbox.tradier.com/v1"
_PROD_BASE = "https://api.tradier.com/v1"
_TIMEOUT = 12

# Cache TTLs
_EXPIRY_TTL = 3600      # 1 hour for expiration list
_CHAIN_TTL = 300         # 5 min for option chains
_QUOTE_TTL = 300         # 5 min for stock quotes

# ── Thread-safe in-memory cache (each TTLCache manages its own lock) ─────────

_MAX_CACHE_ENTRIES = 500

# {TICKER: [expiry_dates]}
_expiry_cache = TTLCache(ttl=_EXPIRY_TTL, max_size=_MAX_CACHE_ENTRIES)

# {TICKER:EXPIRY: chain_dict}
_chain_cache = TTLCache(ttl=_CHAIN_TTL, max_size=_MAX_CACHE_ENTRIES)

# {TICKER: quote_dict}
_quote_cache = TTLCache(ttl=_QUOTE_TTL, max_size=_MAX_CACHE_ENTRIES)


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


# ── Expirations ──────────────────────────────────────────────────────────────


def get_expirations(ticker: str) -> list[str]:
    """Get available option expiration dates for a ticker.

    Returns list of ISO date strings, e.g. ["2024-03-15", "2024-03-22"].
    Cached for 1 hour.
    """
    key = ticker.upper()

    cached = _expiry_cache.get(key)
    if cached is not None:
        return cached

    data = _tradier_get("/markets/options/expirations", params={
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

    _expiry_cache.set(key, dates)

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

    cached = _chain_cache.get(cache_key)
    if cached is not None:
        return cached

    data = _tradier_get("/markets/options/chains", params={
        "symbol": ticker.upper(),
        "expiration": expiration,
        "greeks": "true" if greeks else "false",
    })

    if not data or not isinstance(data, dict):
        return None

    _chain_cache.set(cache_key, data)

    return data


# ── Stock Quotes ─────────────────────────────────────────────────────────────


def get_quote(ticker: str) -> dict | None:
    """Fetch a stock quote from Tradier.

    Returns dict with keys: last, open, high, low, close, volume,
    prevclose, etc. Cached for 5 minutes.
    """
    key = ticker.upper()

    cached = _quote_cache.get(key)
    if cached is not None:
        return cached

    data = _tradier_get("/markets/quotes", params={"symbols": key})

    if not data or not isinstance(data, dict):
        return None

    quotes = data.get("quotes", {})
    quote = quotes.get("quote")

    if not quote or not isinstance(quote, dict):
        return None

    _quote_cache.set(key, quote)

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
    _expiry_cache.clear()
    _chain_cache.clear()
    _quote_cache.clear()
