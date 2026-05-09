"""S&P 500 market data: heatmap, most-added table, ticker search index.

Data comes from yfinance (free bulk download) and Wikipedia (S&P 500
constituent list with sectors). All results are cached in memory with
configurable TTLs to avoid repeated downloads.
"""

from __future__ import annotations

import hashlib
import logging
import os
import threading
import time
from collections import OrderedDict
from datetime import datetime

from filings.caching import TTLCache

logger = logging.getLogger(__name__)

# ── Global yfinance timeout ──────────────────────────────────────────
# yfinance uses curl_cffi internally and defaults to NO timeout,
# meaning a hung Yahoo Finance server can permanently block a thread.
# We enforce a hard timeout on every yfinance request:
#   - yf.download() → use the built-in `timeout=` parameter
#   - yf.Ticker()   → pass a curl_cffi Session with default timeout
_YF_TIMEOUT = 15  # seconds


def _make_yf_session():
    """Create a curl_cffi Session that enforces a timeout on every request."""
    try:
        from curl_cffi.requests import Session

        class _TimeoutSession(Session):
            def request(self, *args, **kwargs):
                kwargs.setdefault("timeout", _YF_TIMEOUT)
                return super().request(*args, **kwargs)

        return _TimeoutSession()
    except Exception:
        return None


_yf_session = _make_yf_session()

# ── Thread lock for all cache reads/writes ────────────────────────────
_lock = threading.Lock()

# ── In-memory TTL caches ──────────────────────────────────────────────
_constituents_cache: tuple[float, list[dict]] | None = None
_CONSTITUENTS_TTL = 86_400  # 24 hours

_MARKET_DATA_TTL = 1_800  # 30 minutes
_market_data_cache = TTLCache(ttl=_MARKET_DATA_TTL, max_size=500)

# Stores the raw close DataFrame for multi-timeframe % change
_close_df_cache: tuple[float, object] | None = None  # (ts, DataFrame)
_CLOSE_DF_TTL = 1_800  # same as market data

_52w_cache: tuple[float, dict] | None = None
_52W_TTL = 1_800  # 30 minutes

_most_added_cache: tuple[float, list[dict]] | None = None
_MOST_ADDED_TTL = 1_800  # 30 minutes

_all_listings_cache: tuple[float, list[dict]] | None = None
_ALL_LISTINGS_TTL = 86_400  # 24 hours — listings change infrequently

_index_cache: tuple[float, dict] | None = None
_INDEX_TTL = 1_800  # 30 minutes

_news_cache: tuple[float, list[dict]] | None = None
_NEWS_TTL = 1_800  # 30 minutes

_HEATMAP_BUILT_TTL = 1_800  # 30 minutes — same as market data
_heatmap_built_cache = TTLCache(ttl=_HEATMAP_BUILT_TTL, max_size=500)

_SPARKLINE_TTL = 1_800  # 30 minutes
_SPARKLINE_MAX = 20     # bounded — keyed by ticker-set:num_points
_sparkline_cache = TTLCache(ttl=_SPARKLINE_TTL, max_size=_SPARKLINE_MAX)

# Intraday chart cache — keyed by symbol, 5-min TTL.
# Smaller than the daily-index cache (30 min) because bars roll over every 15
# min and a stale cell on the masthead would visibly misrepresent the tape.
_INTRADAY_TTL = 300
_INTRADAY_MAX = 30   # only used for a handful of indices on the Home page
_intraday_cache = TTLCache(ttl=_INTRADAY_TTL, max_size=_INTRADAY_MAX)


# ── Supabase warm-load (survive redeploys) ───────────────────────────


def warm_from_supabase() -> bool:
    """Hydrate memory caches from Supabase for fast cold starts.

    Called once during lifespan startup (before yfinance prefetch).
    Returns True if at least one cache was successfully loaded.
    """
    global _close_df_cache, _index_cache
    warmed = False

    try:
        from filings import supabase_cache

        # 1. Close DataFrame (heatmap + ticker tape)
        cached, _ = supabase_cache.get_cached_with_stale("market:close_df")
        if cached and "__dates__" in cached:
            close_data = _dict_to_df(cached)
            with _lock:
                _close_df_cache = (time.time(), close_data)
            logger.info(
                "Supabase warm: close_df loaded (%d rows x %d tickers)",
                len(close_data), len(close_data.columns),
            )
            warmed = True

        # 2. Index/commodity data (market overview)
        cached, _ = supabase_cache.get_cached_with_stale("market:indices")
        if cached and isinstance(cached, dict) and len(cached) > 0:
            with _lock:
                _index_cache = (time.time(), cached)
            logger.info(
                "Supabase warm: index data loaded (%d symbols)", len(cached),
            )
            warmed = True

    except Exception as e:
        logger.warning("Supabase warm-load failed: %s", e)

    return warmed


# ── S&P 500 Constituents ──────────────────────────────────────────────

# Hardcoded fallback for top ~50 S&P 500 names (used if Wikipedia fails)
_FALLBACK_SP500 = [
    {"ticker": "AAPL", "name": "Apple Inc.", "sector": "Information Technology"},
    {"ticker": "MSFT", "name": "Microsoft Corp.", "sector": "Information Technology"},
    {"ticker": "AMZN", "name": "Amazon.com Inc.", "sector": "Consumer Discretionary"},
    {"ticker": "NVDA", "name": "NVIDIA Corp.", "sector": "Information Technology"},
    {"ticker": "GOOGL", "name": "Alphabet Inc.", "sector": "Communication Services"},
    {
        "ticker": "META",
        "name": "Meta Platforms Inc.",
        "sector": "Communication Services",
    },
    {"ticker": "BRK-B", "name": "Berkshire Hathaway", "sector": "Financials"},
    {"ticker": "TSLA", "name": "Tesla Inc.", "sector": "Consumer Discretionary"},
    {"ticker": "UNH", "name": "UnitedHealth Group", "sector": "Health Care"},
    {"ticker": "LLY", "name": "Eli Lilly", "sector": "Health Care"},
    {"ticker": "JPM", "name": "JPMorgan Chase", "sector": "Financials"},
    {"ticker": "V", "name": "Visa Inc.", "sector": "Financials"},
    {"ticker": "XOM", "name": "Exxon Mobil", "sector": "Energy"},
    {"ticker": "MA", "name": "Mastercard Inc.", "sector": "Financials"},
    {"ticker": "JNJ", "name": "Johnson & Johnson", "sector": "Health Care"},
    {"ticker": "PG", "name": "Procter & Gamble", "sector": "Consumer Staples"},
    {"ticker": "HD", "name": "Home Depot", "sector": "Consumer Discretionary"},
    {"ticker": "COST", "name": "Costco Wholesale", "sector": "Consumer Staples"},
    {"ticker": "ABBV", "name": "AbbVie Inc.", "sector": "Health Care"},
    {"ticker": "CRM", "name": "Salesforce Inc.", "sector": "Information Technology"},
    {"ticker": "BAC", "name": "Bank of America", "sector": "Financials"},
    {"ticker": "AVGO", "name": "Broadcom Inc.", "sector": "Information Technology"},
    {"ticker": "KO", "name": "Coca-Cola Co.", "sector": "Consumer Staples"},
    {"ticker": "WMT", "name": "Walmart Inc.", "sector": "Consumer Staples"},
    {"ticker": "MRK", "name": "Merck & Co.", "sector": "Health Care"},
    {"ticker": "PEP", "name": "PepsiCo Inc.", "sector": "Consumer Staples"},
    {"ticker": "CVX", "name": "Chevron Corp.", "sector": "Energy"},
    {"ticker": "TMO", "name": "Thermo Fisher", "sector": "Health Care"},
    {"ticker": "ADBE", "name": "Adobe Inc.", "sector": "Information Technology"},
    {"ticker": "NFLX", "name": "Netflix Inc.", "sector": "Communication Services"},
    {"ticker": "AMD", "name": "AMD Inc.", "sector": "Information Technology"},
    {"ticker": "CSCO", "name": "Cisco Systems", "sector": "Information Technology"},
    {"ticker": "DIS", "name": "Walt Disney Co.", "sector": "Communication Services"},
    {"ticker": "INTC", "name": "Intel Corp.", "sector": "Information Technology"},
    {"ticker": "WFC", "name": "Wells Fargo", "sector": "Financials"},
    {"ticker": "ABT", "name": "Abbott Labs", "sector": "Health Care"},
    {"ticker": "ORCL", "name": "Oracle Corp.", "sector": "Information Technology"},
    {"ticker": "PM", "name": "Philip Morris", "sector": "Consumer Staples"},
    {"ticker": "GE", "name": "GE Aerospace", "sector": "Industrials"},
    {"ticker": "CAT", "name": "Caterpillar Inc.", "sector": "Industrials"},
    {"ticker": "NOW", "name": "ServiceNow Inc.", "sector": "Information Technology"},
    {"ticker": "QCOM", "name": "Qualcomm Inc.", "sector": "Information Technology"},
    {"ticker": "INTU", "name": "Intuit Inc.", "sector": "Information Technology"},
    {"ticker": "GS", "name": "Goldman Sachs", "sector": "Financials"},
    {"ticker": "ISRG", "name": "Intuitive Surgical", "sector": "Health Care"},
    {"ticker": "T", "name": "AT&T Inc.", "sector": "Communication Services"},
    {"ticker": "AXP", "name": "American Express", "sector": "Financials"},
    {"ticker": "BLK", "name": "BlackRock Inc.", "sector": "Financials"},
    {"ticker": "NEE", "name": "NextEra Energy", "sector": "Utilities"},
    {"ticker": "LMT", "name": "Lockheed Martin", "sector": "Industrials"},
]


def get_sp500_constituents() -> list[dict]:
    """Fetch S&P 500 constituents with sectors from Wikipedia.

    Returns list of dicts: [{"ticker", "name", "sector"}, ...]
    Uses 24-hour in-memory cache. Falls back to hardcoded list on failure.
    """
    global _constituents_cache

    with _lock:
        if _constituents_cache is not None:
            ts, data = _constituents_cache
            if time.time() - ts < _CONSTITUENTS_TTL:
                return data

    try:
        import pandas as pd

        tables = pd.read_html(
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
            storage_options={
                "timeout": 10,
                "User-Agent": "PaperPanda/1.0 (market data; contact@paperpanda.io)",
            },
        )
        df = tables[0]

        constituents = []
        for _, row in df.iterrows():
            ticker = str(row.get("Symbol", "")).strip()
            # Wikipedia uses dots (BRK.B), yfinance uses dashes (BRK-B)
            ticker = ticker.replace(".", "-")
            name = str(row.get("Security", "")).strip()
            sector = str(row.get("GICS Sector", "")).strip()
            if ticker and name:
                constituents.append(
                    {
                        "ticker": ticker,
                        "name": name,
                        "sector": sector,
                    }
                )

        if len(constituents) > 400:
            logger.info(
                "Fetched %d S&P 500 constituents from Wikipedia", len(constituents)
            )
            with _lock:
                _constituents_cache = (time.time(), constituents)
            return constituents

    except Exception as e:
        logger.warning("Wikipedia S&P 500 fetch failed: %s — using fallback", e)

    # Fallback
    with _lock:
        _constituents_cache = (time.time(), _FALLBACK_SP500)
    return _FALLBACK_SP500


# ── NASDAQ 100 constituents ──────────────────────────────────────────

_nasdaq100_cache: tuple[float, list[dict]] | None = None


def get_nasdaq100_constituents() -> list[dict]:
    """Fetch NASDAQ 100 constituents with sectors from Wikipedia.

    Returns list of dicts: [{"ticker", "name", "sector"}, ...]
    Uses 24-hour in-memory cache. Falls back to filtering SP500 on failure.
    """
    global _nasdaq100_cache

    with _lock:
        if _nasdaq100_cache is not None:
            ts, data = _nasdaq100_cache
            if time.time() - ts < _CONSTITUENTS_TTL:
                return data

    try:
        import pandas as pd

        tables = pd.read_html(
            "https://en.wikipedia.org/wiki/Nasdaq-100",
            storage_options={
                "timeout": 10,
                "User-Agent": "PaperPanda/1.0 (market data; contact@paperpanda.io)",
            },
        )
        # The constituents table has columns: Company, Ticker, GICS Sector, GICS Sub-Industry
        df = None
        for t in tables:
            cols = [c.lower() for c in t.columns]
            if "ticker" in cols or "symbol" in cols:
                df = t
                break
        if df is None:
            raise ValueError("No table with Ticker/Symbol column found")

        constituents = []
        for _, row in df.iterrows():
            ticker = str(
                row.get("Ticker", row.get("Symbol", ""))
            ).strip()
            ticker = ticker.replace(".", "-")
            name = str(row.get("Company", row.get("Security", ""))).strip()
            sector = str(
                row.get("GICS Sector", row.get("Sector", ""))
            ).strip()
            if ticker and name:
                constituents.append(
                    {"ticker": ticker, "name": name, "sector": sector}
                )

        if len(constituents) > 80:
            logger.info(
                "Fetched %d NASDAQ 100 constituents from Wikipedia",
                len(constituents),
            )
            with _lock:
                _nasdaq100_cache = (time.time(), constituents)
            return constituents

    except Exception as e:
        logger.warning("Wikipedia NASDAQ 100 fetch failed: %s — using SP500 overlap", e)

    # Fallback: return the top-100 tech-heavy names from SP500
    sp = get_sp500_constituents()
    tech_sectors = {"Information Technology", "Communication Services", "Consumer Discretionary"}
    fallback = [c for c in sp if c.get("sector") in tech_sectors][:100]
    with _lock:
        _nasdaq100_cache = (time.time(), fallback)
    return fallback


# ── Market Data (daily % change) ──────────────────────────────────────


def _df_to_dict(df) -> dict:
    """Serialize a pandas DataFrame to a JSON-safe dict for Supabase storage."""
    import pandas as pd
    result = {}
    dates = [d.isoformat() for d in df.index]
    result["__dates__"] = dates
    for col in df.columns:
        result[col] = [
            None if pd.isna(v) else round(float(v), 4) for v in df[col]
        ]
    return result


def _dict_to_df(data: dict):
    """Deserialize a dict back to a pandas DataFrame."""
    import pandas as pd
    dates = pd.DatetimeIndex(data.pop("__dates__"))
    df = pd.DataFrame(data, index=dates)
    return df


def _ensure_close_df():
    """Download 1-month of S&P 500 close data and cache it.

    Returns the close DataFrame (columns = tickers, rows = dates).
    Covers all heatmap timeframes: 1D (last 2 rows), 1W (~5 rows), 1M (all rows).

    On cold start, tries Supabase first (~2-3s) before falling back to
    yfinance (~15-30s). Writes back to Supabase after a successful download.
    """
    global _close_df_cache

    with _lock:
        if _close_df_cache is not None:
            ts, df = _close_df_cache
            if time.time() - ts < _CLOSE_DF_TTL:
                return df

    # ── Phase 1: Try Supabase (fast, survives redeploys) ──
    try:
        from filings import supabase_cache

        cached, is_fresh = supabase_cache.get_cached_with_stale("market:close_df")
        if cached and "__dates__" in cached:
            close_data = _dict_to_df(cached)
            logger.info(
                "Warm-loaded close_df from Supabase (%d rows x %d tickers, %s)",
                len(close_data), len(close_data.columns),
                "fresh" if is_fresh else "stale",
            )
            with _lock:
                _close_df_cache = (time.time(), close_data)
            return close_data
    except Exception as e:
        logger.debug("Supabase close_df warm-load failed: %s", e)

    # ── Phase 2a: Try Tiingo (fast, reliable, $10/mo) ──
    constituents = get_sp500_constituents()
    tickers = [c["ticker"] for c in constituents]

    try:
        from filings import tiingo

        if tiingo.has_tiingo_key():
            tiingo_df = tiingo.get_close_df_for_sp500(tickers, period_days=30)
            if tiingo_df is not None and not tiingo_df.empty:
                logger.info(
                    "Loaded close_df from Tiingo: %d rows x %d tickers",
                    len(tiingo_df), len(tiingo_df.columns),
                )
                with _lock:
                    _close_df_cache = (time.time(), tiingo_df)

                # Write back to Supabase for next cold start
                try:
                    serialized = _df_to_dict(tiingo_df)
                    supabase_cache.set_cached(
                        "market:close_df", "market_data", serialized,
                        ttl_seconds=_CLOSE_DF_TTL,
                    )
                except Exception:
                    pass

                return tiingo_df
    except Exception as e:
        logger.debug("Tiingo close_df failed, falling back to yfinance: %s", e)

    # ── Phase 2b: Download from yfinance (slow, but authoritative) ──
    try:
        import yfinance as yf

        df = yf.download(tickers, period="1mo", threads=True, progress=False, timeout=_YF_TIMEOUT)

        if df.empty:
            logger.warning("yfinance returned empty DataFrame for S&P 500")
            return None

        close_data = (
            df["Close"]
            if "Close" in df.columns.get_level_values(0)
            else df.get("Close")
        )

        if close_data is None or close_data.empty:
            return None

        logger.info(
            "Downloaded 1-month close data: %d rows x %d tickers",
            len(close_data),
            len(close_data.columns),
        )
        with _lock:
            _close_df_cache = (time.time(), close_data)

        # ── Write back to Supabase for next cold start ──
        try:
            from filings import supabase_cache

            serialized = _df_to_dict(close_data)
            supabase_cache.set_cached(
                "market:close_df", "market_data", serialized,
                ttl_seconds=_CLOSE_DF_TTL,
            )
            logger.info("Persisted close_df to Supabase (%d tickers)", len(close_data.columns))
        except Exception as e:
            logger.debug("Supabase close_df write failed: %s", e)

        return close_data

    except Exception as e:
        logger.warning("yfinance S&P 500 download failed: %s", e)
        return None


def get_sp500_market_data(period: str = "1D") -> dict:
    """Compute % change for S&P 500 tickers over a given period.

    period: "1D" (daily), "1W" (last ~5 trading days), "1M" (full month)

    Returns dict keyed by ticker:
    {"AAPL": {"pct_change": 1.23, "price": 185.50}, ...
     "_metadata": {"fetched_at": "...", "count": 503, "period": "1D"}}

    Uses 30-min TTL cache per period. Returns empty dict on failure.
    """
    # Cache lookup by period key — each period cached independently
    cached = _market_data_cache.get(period)
    if cached is not None:
        return cached

    close_data = _ensure_close_df()
    if close_data is None:
        return {}

    constituents = get_sp500_constituents()
    tickers = [c["ticker"] for c in constituents]

    # Determine how far back to look for the "start" price
    if period == "1W":
        lookback = 5  # ~5 trading days
    elif period == "1M":
        lookback = None  # use first available row
    else:
        lookback = 1  # 1D default

    result: dict = {}
    for ticker in tickers:
        try:
            if ticker not in close_data.columns:
                continue
            series = close_data[ticker].dropna()
            if len(series) < 2:
                continue

            last_close = series.iloc[-1]
            if lookback is None:
                start_close = series.iloc[0]
            else:
                idx = min(lookback, len(series) - 1)
                start_close = series.iloc[-1 - idx]

            if start_close > 0:
                pct = round((last_close - start_close) / start_close * 100, 2)
                result[ticker] = {
                    "pct_change": pct,
                    "price": round(float(last_close), 2),
                }
        except Exception:
            continue

    period_labels = {"1D": "today", "1W": "past week", "1M": "past month"}
    result["_metadata"] = {
        "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "count": len(result) - 1,
        "period": period,
        "period_label": period_labels.get(period, "today"),
    }

    logger.info("Computed %s market data for %d tickers", period, len(result) - 1)
    _market_data_cache.set(period, result)
    return result


# ── Sparkline Data ────────────────────────────────────────────────────


def get_sparkline_points(tickers: list[str], num_points: int = 20) -> dict[str, list[float]]:
    """Return normalized sparkline points (0-1 range) for a list of tickers.

    Uses the cached 1-month close DataFrame (same data as heatmap).
    Returns {ticker: [0.0, 0.15, 0.42, ..., 1.0]} with `num_points` values.
    Missing tickers are silently omitted.  Results are cached for 30 min.
    """
    key_body = ",".join(sorted(tickers))
    cache_key = f"{hashlib.md5(key_body.encode()).hexdigest()}:{num_points}"
    cached = _sparkline_cache.get(cache_key)
    if cached is not None:
        return cached

    close_data = _ensure_close_df()
    if close_data is None:
        return {}

    result: dict[str, list[float]] = {}
    for ticker in tickers:
        try:
            if ticker not in close_data.columns:
                continue
            series = close_data[ticker].dropna()
            if len(series) < 3:
                continue

            # Downsample to num_points evenly spaced values
            values = series.values.tolist()
            if len(values) > num_points:
                step = (len(values) - 1) / (num_points - 1)
                values = [values[round(i * step)] for i in range(num_points)]

            # Normalize to 0-1 range
            lo = min(values)
            hi = max(values)
            span = hi - lo
            if span > 0:
                result[ticker] = [round((v - lo) / span, 3) for v in values]
            else:
                result[ticker] = [0.5] * len(values)
        except Exception:
            continue

    _sparkline_cache.set(cache_key, result)

    return result


# ── Index & Commodity Market Data ─────────────────────────────────────

_INDEX_SYMBOLS: dict[str, dict] = {
    # Indices
    "^GSPC": {"name": "S&P 500", "tab": "indices"},
    "^IXIC": {"name": "Nasdaq", "tab": "indices"},
    "^DJI": {"name": "Dow Jones", "tab": "indices"},
    "^RUT": {"name": "Russell 2000", "tab": "indices"},
    "^VIX": {"name": "VIX", "tab": "indices"},
    # Rates — TNX is the CBOE 10-Year Treasury Note Yield index (in %).
    # Added for the redesign Home KPI strip; comes through get_index_market_data
    # for free (same yfinance batch) and is cached/L2-mirrored like the rest.
    "^TNX": {"name": "10Y", "tab": "rates"},
    # Commodities
    "GC=F": {"name": "Gold", "tab": "commodities"},
    "CL=F": {"name": "Crude Oil", "tab": "commodities"},
    "SI=F": {"name": "Silver", "tab": "commodities"},
    "NG=F": {"name": "Natural Gas", "tab": "commodities"},
}


def get_index_market_data() -> dict:
    """Fetch 1-year daily close data for major indices and commodities.

    Returns dict keyed by symbol (e.g. "^GSPC"):
        {symbol: {name, tab, price, pct_change, point_change, spark, history}}

    - spark: normalized 0-1 list (20 points) for inline sparklines
    - history: [[epoch_ms, close], ...] for ECharts line chart (full 1Y)

    Uses 30-min TTL cache.  Returns empty dict on failure.
    """
    global _index_cache

    with _lock:
        if _index_cache is not None:
            ts, data = _index_cache
            if time.time() - ts < _INDEX_TTL:
                return data

    # ── Phase 1: Try Supabase (fast, survives redeploys) ──
    try:
        from filings import supabase_cache

        cached, is_fresh = supabase_cache.get_cached_with_stale("market:indices")
        if cached and isinstance(cached, dict) and len(cached) > 0:
            logger.info(
                "Warm-loaded index data from Supabase (%d symbols, %s)",
                len(cached), "fresh" if is_fresh else "stale",
            )
            with _lock:
                _index_cache = (time.time(), cached)
            return cached
    except Exception as e:
        logger.debug("Supabase index warm-load failed: %s", e)

    # ── Phase 2: Download from yfinance (slow, but authoritative) ──
    symbols = list(_INDEX_SYMBOLS.keys())

    try:
        import yfinance as yf

        df = yf.download(symbols, period="1y", threads=True, progress=False, timeout=_YF_TIMEOUT)

        if df.empty:
            logger.warning("yfinance returned empty DataFrame for indices")
            return {}

        # Extract close prices — handle both MultiIndex and flat columns
        if isinstance(df.columns, type(df.columns)) and hasattr(df.columns, "get_level_values"):
            try:
                levels = df.columns.get_level_values(0).unique().tolist()
                if "Close" in levels:
                    close_data = df["Close"]
                else:
                    close_data = df
            except Exception:
                close_data = df
        else:
            close_data = df

        result: dict = {}
        for sym, meta in _INDEX_SYMBOLS.items():
            try:
                col = sym
                if col not in close_data.columns:
                    # Try without special chars
                    continue
                series = close_data[col].dropna()
                if len(series) < 2:
                    continue

                last_close = float(series.iloc[-1])
                prev_close = float(series.iloc[-2])
                pct = round((last_close - prev_close) / prev_close * 100, 2) if prev_close else 0.0
                point_chg = round(last_close - prev_close, 2)

                # Sparkline (normalized 0-1, 20 points)
                values = series.values.tolist()
                num_points = 20
                if len(values) > num_points:
                    step = (len(values) - 1) / (num_points - 1)
                    spark_vals = [values[round(i * step)] for i in range(num_points)]
                else:
                    spark_vals = values
                lo, hi = min(spark_vals), max(spark_vals)
                span = hi - lo
                if span > 0:
                    spark = [round((v - lo) / span, 3) for v in spark_vals]
                else:
                    spark = [0.5] * len(spark_vals)

                # History for ECharts: [[epoch_ms, close], ...]
                history = []
                for dt, val in zip(series.index, series.values):
                    epoch_ms = int(dt.timestamp() * 1000)
                    history.append([epoch_ms, round(float(val), 2)])

                result[sym] = {
                    "name": meta["name"],
                    "tab": meta["tab"],
                    "price": round(last_close, 2),
                    "pct_change": pct,
                    "point_change": point_chg,
                    "spark": spark,
                    "history": history,
                }
            except Exception:
                continue

        logger.info("Fetched index/commodity data for %d symbols", len(result))
        with _lock:
            _index_cache = (time.time(), result)

        # ── Write back to Supabase for next cold start ──
        try:
            from filings import supabase_cache

            supabase_cache.set_cached(
                "market:indices", "market_data", result,
                ttl_seconds=_INDEX_TTL,
            )
            logger.info("Persisted index data to Supabase (%d symbols)", len(result))
        except Exception as e:
            logger.debug("Supabase index write failed: %s", e)

        return result

    except Exception as e:
        logger.warning("yfinance index download failed: %s", e)
        return {}


# Approximate trading days per period for slicing chart history
_PERIOD_TRADING_DAYS = {"1D": 2, "1W": 5, "1M": 22, "3M": 66, "1Y": 253}

# Per-symbol on-demand chart cache for extended stock history (LRU-bounded)
_overview_chart_cache: OrderedDict[str, tuple[float, list]] = OrderedDict()
_OVERVIEW_CHART_TTL = 1_800  # 30 min
_OVERVIEW_CHART_MAX = 100    # max entries — prevent unbounded growth


def _slice_history(history: list, period: str) -> list:
    """Slice a full history list to the requested period."""
    num_days = _PERIOD_TRADING_DAYS.get(period)
    if num_days is None or len(history) <= num_days:
        return history
    return history[-num_days:]


def _pct_change_for_history(history: list) -> float:
    """Compute % change from first to last point in a history list."""
    if len(history) < 2:
        return 0.0
    start = history[0][1]
    end = history[-1][1]
    if start and start > 0:
        return round((end - start) / start * 100, 2)
    return 0.0


def get_intraday_chart(symbol: str, interval: str = "15m") -> dict | None:
    """Fetch today's intraday bars for *symbol*.  Falls back to daily history
    when intraday is unavailable (off-hours, weekends, vendor outage) so the
    Home masthead chart never renders empty.

    Returns:
        {
          "history":  [[epoch_ms, close], ...]   # one entry per bar
          "ohlcv":    {open, high, low, close, volume, prev_close}
          "source":   "intraday" | "stale_intraday" | "daily_fallback"
          "label":    "INTRADAY · 15M" | "PRIOR SESSION · 15M" | "1M · DAILY"
        }
        or None if every path fails (very rare; would mean Supabase + yfinance
        both unreachable).

    The function is resilient by design — try fresh intraday, then last-session
    intraday, then the cached daily 1Y history.  The caller renders whichever
    one comes back and uses ``label`` to tell the user what they're looking at.

    Cache:  L1 in-memory 5-min TTL (per symbol).  Daily fallback comes from the
    already-cached _index_cache so that path is essentially free.
    """
    cache_key = f"{symbol}:{interval}"
    cached = _intraday_cache.get(cache_key)
    if cached is not None:
        return cached

    result: dict | None = None

    # ── Path 1: try fresh intraday from yfinance ────────────────────────────
    try:
        import yfinance as yf
        import pandas as pd  # noqa: F401  (yf returns a DataFrame; we test it)

        df = yf.Ticker(symbol).history(period="1d", interval=interval, timeout=_YF_TIMEOUT)
        if df is not None and len(df) > 1:
            history = []
            for ts, row in df.iterrows():
                if pd_isna(row.get("Close")):
                    continue
                epoch_ms = int(ts.timestamp() * 1000)
                history.append([epoch_ms, round(float(row["Close"]), 2)])

            if len(history) >= 2:
                last_close = history[-1][1]
                prev_close = _fetch_prev_close(symbol)
                ohlcv = _intraday_ohlcv(df, prev_close)
                result = {
                    "history": history,
                    "ohlcv":   ohlcv,
                    "source":  "intraday",
                    "label":   f"INTRADAY · {interval.upper()}",
                }
    except Exception as exc:
        logger.debug("Intraday fetch for %s failed: %s", symbol, exc)

    # ── Path 2: try the last 5 trading days at intraday resolution.
    # When the market is closed (overnight / weekend), period="1d" can return
    # only a couple of bars.  period="5d" lets us still render a meaningful
    # chart from the most recent session. ────────────────────────────────────
    if result is None or len(result.get("history", [])) < 6:
        try:
            import yfinance as yf

            df = yf.Ticker(symbol).history(period="5d", interval=interval, timeout=_YF_TIMEOUT)
            if df is not None and len(df) > 6:
                # Slice to the last session by walking backwards until we find a
                # gap > 90 minutes (overnight) — the chunk after that is "today".
                last_session = _slice_last_session(df)
                history = []
                for ts, row in last_session.iterrows():
                    if pd_isna(row.get("Close")):
                        continue
                    epoch_ms = int(ts.timestamp() * 1000)
                    history.append([epoch_ms, round(float(row["Close"]), 2)])
                if len(history) >= 2:
                    prev_close = _fetch_prev_close(symbol)
                    ohlcv = _intraday_ohlcv(last_session, prev_close)
                    result = {
                        "history": history,
                        "ohlcv":   ohlcv,
                        "source":  "stale_intraday",
                        "label":   f"PRIOR SESSION · {interval.upper()}",
                    }
        except Exception as exc:
            logger.debug("Stale intraday fetch for %s failed: %s", symbol, exc)

    # ── Path 3: daily 1M fallback from the existing _index_cache.  Always
    # available on warm cache; never blocks on yfinance. ─────────────────────
    if result is None:
        try:
            idx = get_index_market_data()
            row = idx.get(symbol) if idx else None
            if row and row.get("history"):
                full = row["history"]
                # Last ~22 trading days for a 1M view
                history = full[-22:] if len(full) > 22 else full
                last_close = history[-1][1] if history else None
                prev_close = history[-2][1] if len(history) >= 2 else last_close
                first = history[0][1] if history else None
                highs = [p[1] for p in history]
                ohlcv = {
                    "open":       first,
                    "high":       max(highs) if highs else None,
                    "low":        min(highs) if highs else None,
                    "close":      last_close,
                    "volume":     None,
                    "prev_close": prev_close,
                }
                result = {
                    "history": history,
                    "ohlcv":   ohlcv,
                    "source":  "daily_fallback",
                    "label":   "1M · DAILY",
                }
        except Exception as exc:
            logger.debug("Daily fallback for %s failed: %s", symbol, exc)

    if result is not None:
        _intraday_cache.set(cache_key, result)
    return result


def pd_isna(v) -> bool:
    """Cheap NaN guard that doesn't require importing pandas at module load."""
    try:
        import math as _m
        return v is None or (isinstance(v, float) and _m.isnan(v))
    except Exception:
        return v is None


def _fetch_prev_close(symbol: str) -> float | None:
    """Get the previous trading day's close from the cached index data."""
    try:
        idx = get_index_market_data()
        row = idx.get(symbol) if idx else None
        history = row.get("history") if row else None
        if history and len(history) >= 2:
            return history[-2][1]
    except Exception:
        pass
    return None


def _intraday_ohlcv(df, prev_close: float | None) -> dict:
    """Reduce an intraday bars DataFrame to today's session OHLCV.

    OPEN  = first bar's open
    HIGH  = max of all bars' highs
    LOW   = min of all bars' lows
    CLOSE = last bar's close
    VOLUME = sum of all bars' volumes
    """
    try:
        opens   = df["Open"].dropna()
        highs   = df["High"].dropna()
        lows    = df["Low"].dropna()
        closes  = df["Close"].dropna()
        volumes = df["Volume"].dropna()
        return {
            "open":       float(opens.iloc[0])  if len(opens) else None,
            "high":       float(highs.max())    if len(highs) else None,
            "low":        float(lows.min())     if len(lows) else None,
            "close":      float(closes.iloc[-1]) if len(closes) else None,
            "volume":     int(volumes.sum())    if len(volumes) else None,
            "prev_close": prev_close,
        }
    except Exception:
        return {"open": None, "high": None, "low": None, "close": None,
                "volume": None, "prev_close": prev_close}


def _slice_last_session(df):
    """Return the rows of *df* belonging to the most recent contiguous session.

    Walks the index backwards looking for a gap > 90 minutes — the chunk after
    that gap is "today" (or the latest open session).  Used by the 5d-interval
    fallback path so we never render a multi-day broken-line chart.
    """
    if df is None or len(df) == 0:
        return df
    idx = df.index
    last = idx[-1]
    cutoff = None
    for i in range(len(idx) - 2, -1, -1):
        gap = (idx[i + 1] - idx[i]).total_seconds()
        if gap > 90 * 60:
            cutoff = idx[i + 1]
            break
    if cutoff is None:
        return df
    return df.loc[cutoff:last]


def get_overview_chart_data(symbol: str, period: str = "1M") -> dict | None:
    """Return chart data for any symbol (stock, index, or commodity).

    period: "1D", "1W", "1M", "3M", "1Y"

    Returns {"name": str, "price": float, "pct_change": float,
             "history": [[epoch_ms, close], ...]}
    or None if not found.
    """
    if period not in _PERIOD_TRADING_DAYS:
        period = "1M"

    # 1. Check index/commodity cache first (has full 1Y of data)
    with _lock:
        if _index_cache is not None:
            _, idx_data = _index_cache
            if symbol in idx_data:
                d = idx_data[symbol]
                sliced = _slice_history(d["history"], period)
                return {
                    "name": d["name"],
                    "price": d["price"],
                    "pct_change": _pct_change_for_history(sliced),
                    "history": sliced,
                }

    # 2. For stocks — try cached close DataFrame first (has ~1M of data)
    num_days = _PERIOD_TRADING_DAYS[period]

    if num_days <= 22:
        # 1M or less — use the existing S&P 500 close DataFrame
        close_data = _ensure_close_df()
        if close_data is not None and symbol in close_data.columns:
            series = close_data[symbol].dropna()
            if len(series) >= 2:
                sliced_series = series[-num_days:] if num_days < len(series) else series
                history = []
                for dt, val in zip(sliced_series.index, sliced_series.values):
                    epoch_ms = int(dt.timestamp() * 1000)
                    history.append([epoch_ms, round(float(val), 2)])

                constituents = get_sp500_constituents()
                name = symbol
                for c in constituents:
                    if c["ticker"] == symbol:
                        name = c["name"]
                        break

                return {
                    "name": name,
                    "price": round(float(sliced_series.iloc[-1]), 2),
                    "pct_change": _pct_change_for_history(history),
                    "history": history,
                }

    # 3. Longer period for stocks — on-demand download with cache
    cache_key = f"{symbol}:{period}"
    with _lock:
        cached = _overview_chart_cache.get(cache_key)
        if cached is not None:
            ts, data = cached
            if time.time() - ts < _OVERVIEW_CHART_TTL:
                _overview_chart_cache.move_to_end(cache_key)
                return data

    try:
        import yfinance as yf

        yf_periods = {"3M": "3mo", "1Y": "1y"}
        yf_period = yf_periods.get(period, "3mo")

        dl = yf.download([symbol], period=yf_period, threads=True, progress=False, timeout=_YF_TIMEOUT)
        if dl.empty:
            return None

        # Extract close prices
        if hasattr(dl.columns, "get_level_values"):
            levels = dl.columns.get_level_values(0).unique().tolist()
            close_col = dl["Close"] if "Close" in levels else dl
        else:
            close_col = dl

        # Handle single-ticker DataFrame (may have symbol as sub-column)
        if hasattr(close_col, "columns") and symbol in close_col.columns:
            series = close_col[symbol].dropna()
        elif hasattr(close_col, "columns") and len(close_col.columns) == 1:
            series = close_col.iloc[:, 0].dropna()
        else:
            series = close_col.squeeze().dropna()

        if len(series) < 2:
            return None

        history = []
        for dt, val in zip(series.index, series.values):
            epoch_ms = int(dt.timestamp() * 1000)
            history.append([epoch_ms, round(float(val), 2)])

        constituents = get_sp500_constituents()
        name = symbol
        for c in constituents:
            if c["ticker"] == symbol:
                name = c["name"]
                break

        result = {
            "name": name,
            "price": round(float(series.iloc[-1]), 2),
            "pct_change": _pct_change_for_history(history),
            "history": history,
        }

        with _lock:
            _overview_chart_cache[cache_key] = (time.time(), result)
            _overview_chart_cache.move_to_end(cache_key)
            while len(_overview_chart_cache) > _OVERVIEW_CHART_MAX:
                _overview_chart_cache.popitem(last=False)
        return result

    except Exception as e:
        logger.warning("On-demand chart download failed for %s: %s", symbol, e)
        return None


# ── OHLCV candlestick data (self-hosted stock chart) ─────────────────

_ohlcv_cache: OrderedDict[str, tuple[float, dict]] = OrderedDict()
_OHLCV_TTL = 1_800   # 30 min
_OHLCV_MAX = 100      # max entries

# Tuples of (yfinance period string, yfinance interval string).  Intraday
# intervals (5m, 30m) are used for short ranges so the chart actually has
# enough bars to read; daily/weekly otherwise to keep the bar count sane.
# Without 1D/1W keys the previous code fell through to "1Y" -- which is why
# selecting 1W still drew a year of candles.
_OHLCV_YF_PERIODS = {
    "1D": ("1d",  "5m"),
    "1W": ("5d",  "30m"),
    "1M": ("1mo", "1d"),
    "3M": ("3mo", "1d"),
    "6M": ("6mo", "1d"),
    "1Y": ("1y",  "1d"),
    "5Y": ("5y",  "1wk"),
}


def get_stock_ohlcv(ticker: str, period: str = "1Y") -> dict | None:
    """Return OHLCV candlestick data for a stock ticker.

    Returns ``{"ticker", "name", "price", "pct_change",
               "ohlcv": [[epoch_ms, open, high, low, close, volume], ...]}``
    or *None* if data is unavailable.
    """
    if period not in _OHLCV_YF_PERIODS:
        period = "1Y"

    cache_key = f"{ticker}:{period}"
    with _lock:
        cached = _ohlcv_cache.get(cache_key)
        if cached is not None:
            ts, data = cached
            if time.time() - ts < _OHLCV_TTL:
                _ohlcv_cache.move_to_end(cache_key)
                return data

    try:
        import yfinance as yf

        yf_period, yf_interval = _OHLCV_YF_PERIODS[period]
        dl = yf.download(
            [ticker], period=yf_period, interval=yf_interval,
            threads=False, progress=False, timeout=_YF_TIMEOUT,
        )
        if dl.empty:
            return None

        # Normalise MultiIndex columns from yfinance
        if hasattr(dl.columns, "get_level_values"):
            levels = dl.columns.get_level_values(0).unique().tolist()
            needed = {"Open", "High", "Low", "Close", "Volume"}
            if not needed.issubset(set(levels)):
                return None
        else:
            return None

        # Extract per-ticker series (handle single-ticker sub-column)
        def _col(name: str):
            col = dl[name]
            if hasattr(col, "columns") and ticker in col.columns:
                return col[ticker].dropna()
            if hasattr(col, "columns") and len(col.columns) == 1:
                return col.iloc[:, 0].dropna()
            return col.squeeze().dropna()

        o, h, l, c, v = _col("Open"), _col("High"), _col("Low"), _col("Close"), _col("Volume")
        if len(c) < 2:
            return None

        ohlcv: list[list] = []
        for idx in c.index:
            epoch_ms = int(idx.timestamp() * 1000)
            ohlcv.append([
                epoch_ms,
                round(float(o.get(idx, 0)), 2),
                round(float(h.get(idx, 0)), 2),
                round(float(l.get(idx, 0)), 2),
                round(float(c[idx]), 2),
                int(float(v.get(idx, 0))),
            ])

        # Resolve name
        constituents = get_sp500_constituents()
        name = ticker
        for cst in constituents:
            if cst["ticker"] == ticker:
                name = cst["name"]
                break

        price = ohlcv[-1][4]
        start_price = ohlcv[0][4]
        pct = round((price - start_price) / start_price * 100, 2) if start_price else 0.0

        result = {
            "ticker": ticker,
            "name": name,
            "price": price,
            "pct_change": pct,
            "ohlcv": ohlcv,
        }

        with _lock:
            _ohlcv_cache[cache_key] = (time.time(), result)
            _ohlcv_cache.move_to_end(cache_key)
            while len(_ohlcv_cache) > _OHLCV_MAX:
                _ohlcv_cache.popitem(last=False)
        return result

    except Exception as e:
        logger.warning("OHLCV download failed for %s (%s): %s", ticker, period, e)
        return None


# ── Market News ──────────────────────────────────────────────────────


def _time_ago(unix_ts: int) -> str:
    """Convert a UNIX timestamp to a human-readable 'time ago' string."""
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    dt = datetime.fromtimestamp(unix_ts, tz=timezone.utc)
    diff = now - dt
    secs = int(diff.total_seconds())

    if secs < 60:
        return "just now"
    if secs < 3600:
        m = secs // 60
        return f"{m}m ago"
    if secs < 86400:
        h = secs // 3600
        return f"{h}h ago"
    d = secs // 86400
    if d == 1:
        return "1d ago"
    return f"{d}d ago"


def get_market_news(category: str = "general", max_articles: int = 20) -> list[dict]:
    """Fetch general market news from Finnhub.

    Returns list of dicts:
        [{headline, summary, source, datetime_iso, time_ago, image,
          related_tickers, url}, ...]

    Uses 30-min TTL cache. Returns empty list on failure or missing API key.
    """
    global _news_cache

    with _lock:
        if _news_cache is not None:
            ts, data = _news_cache
            if time.time() - ts < _NEWS_TTL:
                return data

    try:
        from filings.sentiment import get_finnhub_client
        from datetime import datetime, timezone

        client = get_finnhub_client()
        if not client:
            logger.warning("FINNHUB_API_KEY not set — market news unavailable")
            return []
        raw = client.general_news(category, min_id=0)

        if not raw or not isinstance(raw, list):
            logger.warning("Finnhub general_news returned empty/invalid response")
            return []

        articles: list[dict] = []
        for item in raw[:max_articles]:
            unix_ts = item.get("datetime", 0)
            dt = datetime.fromtimestamp(unix_ts, tz=timezone.utc)

            # Parse related tickers
            related_raw = item.get("related", "") or ""
            related_tickers = [
                t.strip()
                for t in related_raw.split(",")
                if t.strip() and t.strip().isalpha() and len(t.strip()) <= 5
            ]

            articles.append({
                "headline": item.get("headline", ""),
                "summary": item.get("summary", ""),
                "source": item.get("source", ""),
                "datetime_iso": dt.isoformat(),
                "time_ago": _time_ago(unix_ts),
                "image": item.get("image", ""),
                "related_tickers": related_tickers,
                "url": item.get("url", ""),
            })

        logger.info("Fetched %d market news articles from Finnhub", len(articles))
        with _lock:
            _news_cache = (time.time(), articles)
        return articles

    except Exception as e:
        logger.warning("Finnhub market news fetch failed: %s", e)
        return []


# ── 52-Week Range (bulk) ──────────────────────────────────────────────


def get_52_week_range_bulk(tickers: list[str]) -> dict:
    """Get 52-week high/low/current for tickers via yfinance bulk download.

    Returns {ticker: {"low": 120.5, "high": 198.3, "current": 185.5, "pct_of_range": 83.4}}
    Uses 30-min TTL cache. Returns empty dict on failure.
    """
    global _52w_cache

    with _lock:
        if _52w_cache is not None:
            ts, data = _52w_cache
            if time.time() - ts < _52W_TTL:
                return {t: data[t] for t in tickers if t in data}

    # ── L2: Try Supabase (fast, survives redeploys) ──
    try:
        from filings import supabase_cache

        cached, is_fresh = supabase_cache.get_cached_with_stale("market:52w_range")
        if cached and isinstance(cached, dict) and len(cached) > 50:
            logger.info(
                "Warm-loaded 52w_range from Supabase (%d tickers, %s)",
                len(cached), "fresh" if is_fresh else "stale",
            )
            with _lock:
                _52w_cache = (time.time(), cached)
            return {t: cached[t] for t in tickers if t in cached}
    except Exception as e:
        logger.debug("Supabase 52w_range warm-load failed: %s", e)

    try:
        import yfinance as yf

        # Download 1 year of daily data
        constituents = get_sp500_constituents()
        all_tickers = [c["ticker"] for c in constituents]

        df = yf.download(all_tickers, period="1y", threads=True, progress=False, timeout=_YF_TIMEOUT)

        if df.empty:
            return {}

        result: dict = {}
        high_data = df.get("High")
        low_data = df.get("Low")
        close_data = df.get("Close")

        if high_data is None or low_data is None or close_data is None:
            return {}

        for t in all_tickers:
            try:
                if t not in close_data.columns:
                    continue
                highs = high_data[t].dropna()
                lows = low_data[t].dropna()
                closes = close_data[t].dropna()

                if len(highs) < 10 or len(lows) < 10 or len(closes) < 1:
                    continue

                w52_high = float(highs.max())
                w52_low = float(lows.min())
                current = float(closes.iloc[-1])
                range_span = w52_high - w52_low

                pct = (
                    round((current - w52_low) / range_span * 100, 1)
                    if range_span > 0
                    else 50.0
                )
                result[t] = {
                    "low": round(w52_low, 2),
                    "high": round(w52_high, 2),
                    "current": round(current, 2),
                    "pct_of_range": pct,
                }
            except Exception:
                continue

        with _lock:
            _52w_cache = (time.time(), result)

        # ── Write back to Supabase L2 ──
        try:
            from filings import supabase_cache

            supabase_cache.set_cached(
                "market:52w_range", "market_data", result,
                ttl_seconds=_52W_TTL,
            )
        except Exception:
            logger.debug("Supabase 52w_range write-back failed")

        return {t: result[t] for t in tickers if t in result}

    except Exception as e:
        logger.warning("52-week range download failed: %s", e)
        return {}


# ── Current Prices Batch ─────────────────────────────────────────────

_prices_batch_cache: tuple[float, dict[str, float]] | None = None
_PRICES_BATCH_TTL = 1_800  # 30 minutes


def get_current_prices_batch(tickers: list[str]) -> dict[str, float]:
    """Get current prices for a list of tickers via yfinance.

    Returns ``{ticker: price}``.  Uses a 30-min in-memory TTL cache.
    Tries existing S&P 500 market data first (already cached), then does
    a small yfinance batch download for any remaining tickers.
    """
    global _prices_batch_cache

    # Check full-set cache first
    with _lock:
        if _prices_batch_cache is not None:
            ts, data = _prices_batch_cache
            if time.time() - ts < _PRICES_BATCH_TTL:
                return {t: data[t] for t in tickers if t in data}

    result: dict[str, float] = {}

    # 1. Pull from S&P 500 market data (already cached, cheap)
    try:
        sp500 = get_sp500_market_data("1D")
        for t in tickers:
            entry = sp500.get(t)
            if entry and isinstance(entry, dict) and "price" in entry:
                result[t] = entry["price"]
    except Exception:
        pass

    # 2. Batch-download missing tickers via yfinance
    missing = [t for t in tickers if t not in result and t]
    if missing:
        try:
            import yfinance as yf

            # Limit batch to avoid huge downloads
            batch = missing[:80]
            df = yf.download(batch, period="1d", threads=True, progress=False, timeout=_YF_TIMEOUT)
            if not df.empty:
                close = df.get("Close")
                if close is not None:
                    for t in batch:
                        try:
                            if t in close.columns:
                                val = close[t].dropna()
                                if len(val) > 0:
                                    result[t] = round(float(val.iloc[-1]), 2)
                        except Exception:
                            continue
        except Exception as e:
            logger.debug("yfinance batch price fetch failed: %s", e)

    with _lock:
        _prices_batch_cache = (time.time(), result)

    return {t: result[t] for t in tickers if t in result}


# ── Heatmap Builder ───────────────────────────────────────────────────


def pct_to_color(pct: float) -> str:
    """Map % change to hex color. -5%=deep red, 0=gray, +5%=deep green."""
    pct = max(-5.0, min(5.0, pct))
    if pct >= 0:
        t = pct / 5.0
        r = int(153 + (27 - 153) * t)
        g = int(153 + (94 - 153) * t)
        b = int(153 + (32 - 153) * t)
    else:
        t = abs(pct) / 5.0
        r = int(153 + (183 - 153) * t)
        g = int(153 + (28 - 153) * t)
        b = int(153 + (28 - 153) * t)
    return f"#{r:02x}{g:02x}{b:02x}"


# Approximate S&P 500 market-cap weights (top ~50).
# Source: S&P Dow Jones Indices, last verified 2026-03-15.
# Stocks not listed get a default small weight. Update quarterly.
_SP500_WEIGHTS: dict[str, float] = {
    "AAPL": 7.0, "MSFT": 6.5, "NVDA": 6.0, "AMZN": 3.8, "META": 2.6,
    "GOOGL": 2.1, "GOOG": 1.8, "BRK-B": 1.7, "AVGO": 1.7, "TSLA": 1.6,
    "JPM": 1.4, "LLY": 1.3, "V": 1.1, "UNH": 1.1, "MA": 1.0,
    "XOM": 1.0, "COST": 0.9, "HD": 0.9, "PG": 0.8, "JNJ": 0.8,
    "NFLX": 0.8, "ABBV": 0.7, "CRM": 0.7, "BAC": 0.7, "ORCL": 0.7,
    "CVX": 0.6, "WMT": 0.6, "MRK": 0.6, "KO": 0.6, "CSCO": 0.5,
    "AMD": 0.5, "PEP": 0.5, "ACN": 0.5, "LIN": 0.5, "ADBE": 0.5,
    "TMO": 0.5, "MCD": 0.5, "ABT": 0.5, "PM": 0.4, "IBM": 0.4,
    "GE": 0.4, "ISRG": 0.4, "NOW": 0.4, "CAT": 0.4, "TXN": 0.4,
    "INTU": 0.4, "QCOM": 0.4, "GS": 0.4, "AMGN": 0.4, "BKNG": 0.4,
}
_SP500_DEFAULT_WEIGHT = 0.15  # ~0.15% for unlisted stocks


def build_heatmap_data(
    market_data: dict,
    constituents: list[dict],
    superinvestor_ticker_counts: dict[str, int],
    period: str = "1D",
) -> list[dict]:
    """Build ECharts treemap data grouped by sector.

    Returns list of sector groups:
    [{"name": "Tech", "children": [{"name": "AAPL", "value": 1, ...}]}]

    Results are cached per-period for _HEATMAP_BUILT_TTL seconds.
    """
    cached = _heatmap_built_cache.get(period)
    if cached is not None:
        return cached

    # ── L2: Try Supabase (survives redeploys) ──
    try:
        from filings import supabase_cache

        sb_cached, is_fresh = supabase_cache.get_cached_with_stale(f"heatmap:built:{period}")
        if sb_cached and isinstance(sb_cached, list) and len(sb_cached) > 3:
            logger.info("Warm-loaded heatmap:%s from Supabase (%d sectors, %s)", period, len(sb_cached), "fresh" if is_fresh else "stale")
            _heatmap_built_cache.set(period, sb_cached)
            return sb_cached
    except Exception:
        pass

    sectors: dict[str, list[dict]] = {}

    for c in constituents:
        ticker = c["ticker"]
        mkt = market_data.get(ticker)
        if not mkt:
            continue

        pct = mkt["pct_change"]
        count = superinvestor_ticker_counts.get(ticker.upper(), 0)
        sector = c.get("sector", "Other")

        weight = _SP500_WEIGHTS.get(ticker, _SP500_DEFAULT_WEIGHT)

        node = {
            "name": ticker,
            "value": weight,
            "pct_change": pct,
            "full_name": c["name"],
            "superinvestor_count": count,
            "link": f"/stock/{ticker}",
            "itemStyle": {
                "color": pct_to_color(pct),
                "borderColor": "rgba(0,0,0,0.15)",
                "borderWidth": 1,
            },
        }

        if sector not in sectors:
            sectors[sector] = []
        sectors[sector].append(node)

    # Sort sectors by total weight (largest first), children by weight
    sector_weights = {s: sum(n["value"] for n in nodes) for s, nodes in sectors.items()}
    result = []
    for name in sorted(sectors, key=lambda s: -sector_weights[s]):
        children = sorted(sectors[name], key=lambda n: -n["value"])
        result.append(
            {
                "name": name,
                "children": children,
            }
        )

    _heatmap_built_cache.set(period, result)

    # ── Write back to Supabase L2 ──
    try:
        from filings import supabase_cache

        supabase_cache.set_cached(
            f"heatmap:built:{period}", "market_data", result,
            ttl_seconds=_HEATMAP_BUILT_TTL,
        )
    except Exception:
        pass

    return result


# ── Most Added by Superinvestors ──────────────────────────────────────


def build_most_added_table(
    cache_data: dict,
    superinvestors_by_cik: dict,
) -> list[dict]:
    """Build table of stocks most added by superinvestors this quarter.

    Reads from fund_cache changes (status=NEW/INCREASED), groups by CUSIP,
    counts number of superinvestors that added. Returns top 25.
    """
    global _most_added_cache

    with _lock:
        if _most_added_cache is not None:
            ts, data = _most_added_cache
            if time.time() - ts < _MOST_ADDED_TTL:
                return data

    by_cusip: dict[str, dict] = {}

    for cik, fund_data in cache_data.items():
        si = superinvestors_by_cik.get(cik)
        if not si:
            continue

        # Build ticker lookup from holdings
        ticker_by_cusip: dict[str, str | None] = {}
        for h in fund_data.get("all_holdings", []):
            ticker_by_cusip[h["cusip"]] = h.get("ticker")

        for change in fund_data.get("changes", []):
            status = change.get("status", "")
            if status not in ("NEW", "INCREASED"):
                continue

            cusip = change["cusip"]
            if cusip not in by_cusip:
                by_cusip[cusip] = {
                    "ticker": ticker_by_cusip.get(cusip),
                    "issuer_name": change["issuer"],
                    "cusip": cusip,
                    "add_count": 0,
                    "adders": [],
                    "total_value": 0,
                }

            by_cusip[cusip]["add_count"] += 1
            by_cusip[cusip]["adders"].append(si.display_name)
            by_cusip[cusip]["total_value"] += change.get("current_value", 0)

            # Prefer non-None ticker
            if ticker_by_cusip.get(cusip) and not by_cusip[cusip]["ticker"]:
                by_cusip[cusip]["ticker"] = ticker_by_cusip[cusip]

    # Sort by add_count desc, then total_value desc
    entries = sorted(
        by_cusip.values(),
        key=lambda e: (-e["add_count"], -e["total_value"]),
    )[:25]

    # Add rank
    for i, entry in enumerate(entries):
        entry["rank"] = i + 1

    with _lock:
        _most_added_cache = (time.time(), entries)
    return entries


# ── All US Listed Tickers (NYSE + NASDAQ) ────────────────────────────

_EXCHANGE_MAP = {"Q": "NASDAQ", "N": "NYSE", "A": "AMEX", "P": "ARCA", "Z": "BATS"}

_NASDAQTRADED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt"


def get_all_listed_tickers() -> list[dict]:
    """Fetch all NYSE + NASDAQ listed tickers from NASDAQ Trader.

    Returns list of dicts: [{"ticker", "name", "exchange"}, ...]
    Uses 24-hour in-memory cache. Falls back to empty list on failure.
    File is pipe-delimited with a timestamp footer row to strip.
    """
    global _all_listings_cache

    with _lock:
        if _all_listings_cache is not None:
            ts, data = _all_listings_cache
            if time.time() - ts < _ALL_LISTINGS_TTL:
                return data

    try:
        import urllib.request

        req = urllib.request.Request(
            _NASDAQTRADED_URL,
            headers={"User-Agent": "PaperPanda/1.0"},
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8")

        lines = raw.strip().split("\n")
        if len(lines) < 2:
            logger.warning("nasdaqtraded.txt returned fewer than 2 lines")
            return []

        # First line is header, last line is timestamp footer
        header = lines[0].split("|")
        col = {name.strip(): i for i, name in enumerate(header)}

        # Required columns
        sym_col = col.get("Symbol")
        name_col = col.get("Security Name")
        exch_col = col.get("Listing Exchange")
        test_col = col.get("Test Issue")
        etf_col = col.get("ETF")

        if sym_col is None or name_col is None:
            logger.warning("nasdaqtraded.txt missing expected columns: %s", header)
            return []

        results: list[dict] = []
        for line in lines[1:-1]:  # skip header and footer
            parts = line.split("|")
            if len(parts) <= max(sym_col, name_col):
                continue

            symbol = parts[sym_col].strip()
            security_name = parts[name_col].strip()

            # Skip test issues
            if test_col is not None and len(parts) > test_col:
                if parts[test_col].strip().upper() == "Y":
                    continue

            # Skip if no valid symbol
            if not symbol or not security_name:
                continue

            # Skip symbols with special characters (warrants, units, etc.)
            # Keep only standard tickers: letters, dots, dashes
            if any(c in symbol for c in ["$", " ", "+"]):
                continue

            # Determine exchange
            exchange = ""
            if exch_col is not None and len(parts) > exch_col:
                exchange = _EXCHANGE_MAP.get(parts[exch_col].strip(), "")

            # Only include NYSE and NASDAQ (skip AMEX, ARCA, BATS)
            if exchange not in ("NYSE", "NASDAQ"):
                continue

            # Check if ETF — include but tag
            is_etf = False
            if etf_col is not None and len(parts) > etf_col:
                is_etf = parts[etf_col].strip().upper() == "Y"

            # Clean up security name: remove common suffixes for brevity
            clean_name = security_name
            for suffix in [
                " - Common Stock",
                " - Common Shares",
                " Common Stock",
                " Common Shares",
                " - Ordinary Shares",
                " Ordinary Shares",
                " - Class A",
                " - Class B",
                " - Class C",
            ]:
                if clean_name.endswith(suffix):
                    clean_name = clean_name[: -len(suffix)].strip()
                    break

            results.append(
                {
                    "ticker": symbol,
                    "name": clean_name,
                    "exchange": exchange,
                    "is_etf": is_etf,
                }
            )

        logger.info("Fetched %d NYSE/NASDAQ listings from NASDAQ Trader", len(results))
        with _lock:
            _all_listings_cache = (time.time(), results)
        return results

    except Exception as e:
        logger.warning(
            "NASDAQ Trader listings fetch failed: %s — search will use S&P 500 + holdings only",
            e,
        )
        return []


# ── Ticker Search Index ───────────────────────────────────────────────


def get_ticker_search_list(cache_data: dict) -> list[dict]:
    """Build the comprehensive autocomplete search index.

    Merges data from four sources (in priority order):
    1. Superinvestor holdings (from 13F cache)
    2. S&P 500 constituents (from Wikipedia — includes sector info)
    3. All NYSE/NASDAQ listings (from NASDAQ Trader — ~6000 tickers)
    4. Superinvestor profiles (investors, not tickers)

    Returns deduplicated list with metadata for Fuse.js client-side search:
    [{"ticker": "AAPL", "name": "Apple Inc.", "held_by_super": true,
      "in_sp500": true, "type": "ticker", "exchange": "NASDAQ", "sector": "..."},
     {"ticker": "Warren Buffett", "name": "Berkshire Hathaway",
      "type": "investor", "cik": "1067983"}]
    """
    from filings.superinvestors import SUPERINVESTORS

    # ── 1. Collect tickers held by superinvestors ──
    super_tickers: dict[str, str] = {}  # ticker -> issuer name
    for cik, fund_data in cache_data.items():
        for h in fund_data.get("all_holdings", []):
            t = h.get("ticker")
            if t:
                t_upper = t.strip().upper()
                if t_upper not in super_tickers:
                    super_tickers[t_upper] = h.get("issuer", t_upper)

    # ── 2. Get S&P 500 constituents (includes sector) ──
    try:
        constituents = get_sp500_constituents()
    except Exception:
        constituents = []

    sp500_set = {c["ticker"].upper() for c in constituents}
    sector_map = {c["ticker"].upper(): c.get("sector", "") for c in constituents}

    # ── 3. Get all NYSE/NASDAQ listings ──
    all_listings = get_all_listed_tickers()
    listings_map = {item["ticker"].upper(): item for item in all_listings}

    # ── Build merged index ──
    all_items: dict[str, dict] = {}

    # Start with all listings (lowest priority — will be overwritten)
    for listing in all_listings:
        t = listing["ticker"].upper()
        all_items[t] = {
            "ticker": t,
            "name": listing["name"],
            "held_by_super": False,
            "in_sp500": t in sp500_set,
            "type": "ticker",
            "exchange": listing["exchange"],
            "sector": sector_map.get(t, ""),
        }

    # Overlay S&P 500 data (better names, sector info)
    for c in constituents:
        t = c["ticker"].upper()
        listing = listings_map.get(t)
        all_items[t] = {
            "ticker": t,
            "name": c["name"],
            "held_by_super": t in super_tickers,
            "in_sp500": True,
            "type": "ticker",
            "exchange": listing["exchange"] if listing else "",
            "sector": c.get("sector", ""),
        }

    # Overlay superinvestor holdings (highest priority for names)
    for t, issuer in super_tickers.items():
        if t in all_items:
            all_items[t]["held_by_super"] = True
            # Keep existing better name if it's longer/better
            if len(issuer) > len(all_items[t]["name"]):
                all_items[t]["name"] = issuer
        else:
            listing = listings_map.get(t)
            all_items[t] = {
                "ticker": t,
                "name": issuer,
                "held_by_super": True,
                "in_sp500": t in sp500_set,
                "type": "ticker",
                "exchange": listing["exchange"] if listing else "",
                "sector": sector_map.get(t, ""),
            }

    # ── 4. Add superinvestor profiles ──
    for si in SUPERINVESTORS:
        key = f"_investor_{si.cik}"
        all_items[key] = {
            "ticker": si.display_name,
            "name": si.fund_name,
            "held_by_super": False,
            "in_sp500": False,
            "type": "investor",
            "cik": si.cik,
        }

    # ── 5. Add congress member profiles ──
    try:
        from filings import supabase_cache as _supa

        _congress_members = _supa.get_all_congress_members() or []
        for m in _congress_members:
            mid = m.get("member_id", "")
            if not mid:
                continue
            key = f"_politician_{mid}"
            party_short = m.get("party", "")[:1]  # "D" or "R"
            chamber = m.get("chamber", "")
            state_abbr = m.get("state_abbr", "")
            all_items[key] = {
                "ticker": m.get("full_name", ""),  # display name in search
                "name": f"{party_short} - {chamber} - {state_abbr}",
                "held_by_super": False,
                "in_sp500": False,
                "type": "politician",
                "member_id": mid,
                "party": m.get("party", ""),
                "chamber": chamber,
            }
    except Exception:
        pass  # Graceful degradation if congress data unavailable

    # Sort: tickers first (superinvestor-held → S&P 500 → others), then investors/politicians
    result = sorted(
        all_items.values(),
        key=lambda x: (
            x.get("type", "ticker") != "ticker",  # tickers first
            not x.get("held_by_super", False),
            not x.get("in_sp500", False),
            x.get("ticker", ""),
        ),
    )

    return result
