"""S&P 500 market data: heatmap, most-added table, ticker search index.

Data comes from yfinance (free bulk download) and Wikipedia (S&P 500
constituent list with sectors). All results are cached in memory with
configurable TTLs to avoid repeated downloads.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from collections import OrderedDict
from datetime import datetime

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

_market_data_cache: dict[str, tuple[float, dict]] = {}
_MARKET_DATA_TTL = 1_800  # 30 minutes

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

_heatmap_built_cache: dict[str, tuple[float, list[dict]]] = {}
_HEATMAP_BUILT_TTL = 1_800  # 30 minutes — same as market data

_sparkline_cache: dict[str, tuple[float, dict[str, list[float]]]] = {}
_SPARKLINE_TTL = 1_800  # 30 minutes


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
    with _lock:
        cached = _market_data_cache.get(period)
        if cached is not None:
            ts, data = cached
            if time.time() - ts < _MARKET_DATA_TTL:
                return data

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
    with _lock:
        _market_data_cache[period] = (time.time(), result)
    return result


# ── Sparkline Data ────────────────────────────────────────────────────


def get_sparkline_points(tickers: list[str], num_points: int = 20) -> dict[str, list[float]]:
    """Return normalized sparkline points (0-1 range) for a list of tickers.

    Uses the cached 1-month close DataFrame (same data as heatmap).
    Returns {ticker: [0.0, 0.15, 0.42, ..., 1.0]} with `num_points` values.
    Missing tickers are silently omitted.  Results are cached for 30 min.
    """
    cache_key = f"{','.join(sorted(tickers))}:{num_points}"
    with _lock:
        cached = _sparkline_cache.get(cache_key)
        if cached is not None:
            ts, data = cached
            if time.time() - ts < _SPARKLINE_TTL:
                return data

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

    with _lock:
        _sparkline_cache[cache_key] = (time.time(), result)

    return result


# ── Index & Commodity Market Data ─────────────────────────────────────

_INDEX_SYMBOLS: dict[str, dict] = {
    # Indices
    "^GSPC": {"name": "S&P 500", "tab": "indices"},
    "^IXIC": {"name": "Nasdaq", "tab": "indices"},
    "^DJI": {"name": "Dow Jones", "tab": "indices"},
    "^RUT": {"name": "Russell 2000", "tab": "indices"},
    "^VIX": {"name": "VIX", "tab": "indices"},
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
_OVERVIEW_CHART_MAX = 200    # max entries — prevent unbounded growth


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
_OHLCV_MAX = 200      # max entries

_OHLCV_YF_PERIODS = {
    "1M": "1mo", "3M": "3mo", "6M": "6mo", "1Y": "1y", "5Y": "5y",
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

        yf_period = _OHLCV_YF_PERIODS[period]
        dl = yf.download(
            [ticker], period=yf_period,
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

    api_key = os.environ.get("FINNHUB_API_KEY", "")
    if not api_key:
        logger.warning("FINNHUB_API_KEY not set — market news unavailable")
        return []

    try:
        import finnhub
        from datetime import datetime, timezone

        client = finnhub.Client(api_key=api_key)
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
    with _lock:
        cached = _heatmap_built_cache.get(period)
        if cached is not None:
            ts, data = cached
            if time.time() - ts < _HEATMAP_BUILT_TTL:
                return data

    sectors: dict[str, list[dict]] = {}

    for c in constituents:
        ticker = c["ticker"]
        mkt = market_data.get(ticker)
        if not mkt:
            continue

        pct = mkt["pct_change"]
        count = superinvestor_ticker_counts.get(ticker.upper(), 0)
        sector = c.get("sector", "Other")

        node = {
            "name": ticker,
            "value": 1,  # equal weight
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

    # Sort sectors by number of children (largest first)
    result = []
    for name in sorted(sectors, key=lambda s: -len(sectors[s])):
        result.append(
            {
                "name": name,
                "children": sectors[name],
            }
        )

    with _lock:
        _heatmap_built_cache[period] = (time.time(), result)

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
