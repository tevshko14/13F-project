"""Market Breadth — advancers vs decliners, A/D line, sector heatmap.

Fetches daily close data from yfinance for index constituents, computes
breadth metrics, cumulative advance-decline line, and sector treemap data.
Falls back to deterministic mock data when yfinance is unavailable.
"""

from __future__ import annotations

import logging
import random
import threading

import pandas as pd

from filings.caching import TTLCache

logger = logging.getLogger(__name__)

# ── Cache ────────────────────────────────────────────────────────
_cache = TTLCache(ttl=1800, max_size=100)             # 30 min L1 in-memory
_lock = threading.Lock()                              # guards _key_locks only
_key_locks: dict[str, threading.Lock] = {}
_L2_TTL = 21600  # 6 hours (Supabase — survives redeploys)
from filings.market_data import _YF_TIMEOUT

# ── Constants ────────────────────────────────────────────────────
INDEX_CHOICES = {
    "sp500": "S&P 500",
    "nasdaq": "NASDAQ 100",
    "dow": "Dow 30",
}

PERIOD_CHOICES = {
    "1d": "1 Day",
    "1w": "1 Week",
    "1m": "1 Month",
    "ytd": "Year to Date",
}

INDEX_SYMBOLS = {
    "sp500": "^GSPC",
    "nasdaq": "^IXIC",
    "dow": "^DJI",
}

DOW_30 = [
    {"ticker": "AAPL", "name": "Apple Inc.", "sector": "Information Technology"},
    {"ticker": "AMGN", "name": "Amgen Inc.", "sector": "Health Care"},
    {"ticker": "AXP", "name": "American Express Co.", "sector": "Financials"},
    {"ticker": "BA", "name": "Boeing Co.", "sector": "Industrials"},
    {"ticker": "CAT", "name": "Caterpillar Inc.", "sector": "Industrials"},
    {"ticker": "CRM", "name": "Salesforce Inc.", "sector": "Information Technology"},
    {"ticker": "CSCO", "name": "Cisco Systems Inc.", "sector": "Information Technology"},
    {"ticker": "CVX", "name": "Chevron Corp.", "sector": "Energy"},
    {"ticker": "DIS", "name": "Walt Disney Co.", "sector": "Communication Services"},
    {"ticker": "GS", "name": "Goldman Sachs Group Inc.", "sector": "Financials"},
    {"ticker": "HD", "name": "Home Depot Inc.", "sector": "Consumer Discretionary"},
    {"ticker": "HON", "name": "Honeywell International Inc.", "sector": "Industrials"},
    {"ticker": "IBM", "name": "International Business Machines", "sector": "Information Technology"},
    {"ticker": "JNJ", "name": "Johnson & Johnson", "sector": "Health Care"},
    {"ticker": "JPM", "name": "JPMorgan Chase & Co.", "sector": "Financials"},
    {"ticker": "KO", "name": "Coca-Cola Co.", "sector": "Consumer Staples"},
    {"ticker": "MCD", "name": "McDonald's Corp.", "sector": "Consumer Discretionary"},
    {"ticker": "MMM", "name": "3M Co.", "sector": "Industrials"},
    {"ticker": "MRK", "name": "Merck & Co.", "sector": "Health Care"},
    {"ticker": "MSFT", "name": "Microsoft Corp.", "sector": "Information Technology"},
    {"ticker": "NKE", "name": "Nike Inc.", "sector": "Consumer Discretionary"},
    {"ticker": "NVDA", "name": "NVIDIA Corp.", "sector": "Information Technology"},
    {"ticker": "PG", "name": "Procter & Gamble Co.", "sector": "Consumer Staples"},
    {"ticker": "SHW", "name": "Sherwin-Williams Co.", "sector": "Materials"},
    {"ticker": "TRV", "name": "Travelers Companies Inc.", "sector": "Financials"},
    {"ticker": "UNH", "name": "UnitedHealth Group Inc.", "sector": "Health Care"},
    {"ticker": "V", "name": "Visa Inc.", "sector": "Financials"},
    {"ticker": "VZ", "name": "Verizon Communications Inc.", "sector": "Communication Services"},
    {"ticker": "WMT", "name": "Walmart Inc.", "sector": "Consumer Staples"},
    {"ticker": "AMZN", "name": "Amazon.com Inc.", "sector": "Consumer Discretionary"},
]

_NASDAQ_100_FALLBACK = [
    {"ticker": "AAPL", "name": "Apple Inc.", "sector": "Information Technology"},
    {"ticker": "MSFT", "name": "Microsoft Corp.", "sector": "Information Technology"},
    {"ticker": "AMZN", "name": "Amazon.com Inc.", "sector": "Consumer Discretionary"},
    {"ticker": "NVDA", "name": "NVIDIA Corp.", "sector": "Information Technology"},
    {"ticker": "GOOGL", "name": "Alphabet Inc.", "sector": "Communication Services"},
    {"ticker": "META", "name": "Meta Platforms Inc.", "sector": "Communication Services"},
    {"ticker": "TSLA", "name": "Tesla Inc.", "sector": "Consumer Discretionary"},
    {"ticker": "AVGO", "name": "Broadcom Inc.", "sector": "Information Technology"},
    {"ticker": "COST", "name": "Costco Wholesale", "sector": "Consumer Staples"},
    {"ticker": "NFLX", "name": "Netflix Inc.", "sector": "Communication Services"},
    {"ticker": "AMD", "name": "Advanced Micro Devices", "sector": "Information Technology"},
    {"ticker": "ADBE", "name": "Adobe Inc.", "sector": "Information Technology"},
    {"ticker": "PEP", "name": "PepsiCo Inc.", "sector": "Consumer Staples"},
    {"ticker": "LIN", "name": "Linde plc", "sector": "Materials"},
    {"ticker": "CSCO", "name": "Cisco Systems Inc.", "sector": "Information Technology"},
    {"ticker": "INTC", "name": "Intel Corp.", "sector": "Information Technology"},
    {"ticker": "QCOM", "name": "Qualcomm Inc.", "sector": "Information Technology"},
    {"ticker": "INTU", "name": "Intuit Inc.", "sector": "Information Technology"},
    {"ticker": "TXN", "name": "Texas Instruments", "sector": "Information Technology"},
    {"ticker": "AMGN", "name": "Amgen Inc.", "sector": "Health Care"},
    {"ticker": "ISRG", "name": "Intuitive Surgical", "sector": "Health Care"},
    {"ticker": "CMCSA", "name": "Comcast Corp.", "sector": "Communication Services"},
    {"ticker": "HON", "name": "Honeywell International", "sector": "Industrials"},
    {"ticker": "BKNG", "name": "Booking Holdings", "sector": "Consumer Discretionary"},
    {"ticker": "AMAT", "name": "Applied Materials", "sector": "Information Technology"},
    {"ticker": "ADP", "name": "Automatic Data Processing", "sector": "Industrials"},
    {"ticker": "VRTX", "name": "Vertex Pharmaceuticals", "sector": "Health Care"},
    {"ticker": "SBUX", "name": "Starbucks Corp.", "sector": "Consumer Discretionary"},
    {"ticker": "GILD", "name": "Gilead Sciences", "sector": "Health Care"},
    {"ticker": "MDLZ", "name": "Mondelez International", "sector": "Consumer Staples"},
    {"ticker": "ADI", "name": "Analog Devices", "sector": "Information Technology"},
    {"ticker": "PANW", "name": "Palo Alto Networks", "sector": "Information Technology"},
    {"ticker": "LRCX", "name": "Lam Research", "sector": "Information Technology"},
    {"ticker": "REGN", "name": "Regeneron Pharmaceuticals", "sector": "Health Care"},
    {"ticker": "KLAC", "name": "KLA Corp.", "sector": "Information Technology"},
    {"ticker": "SNPS", "name": "Synopsys Inc.", "sector": "Information Technology"},
    {"ticker": "CDNS", "name": "Cadence Design Systems", "sector": "Information Technology"},
    {"ticker": "MU", "name": "Micron Technology", "sector": "Information Technology"},
    {"ticker": "MELI", "name": "MercadoLibre Inc.", "sector": "Consumer Discretionary"},
    {"ticker": "PYPL", "name": "PayPal Holdings", "sector": "Financials"},
    {"ticker": "MNST", "name": "Monster Beverage", "sector": "Consumer Staples"},
    {"ticker": "CSX", "name": "CSX Corp.", "sector": "Industrials"},
    {"ticker": "MAR", "name": "Marriott International", "sector": "Consumer Discretionary"},
    {"ticker": "ORLY", "name": "O'Reilly Automotive", "sector": "Consumer Discretionary"},
    {"ticker": "NXPI", "name": "NXP Semiconductors", "sector": "Information Technology"},
    {"ticker": "FTNT", "name": "Fortinet Inc.", "sector": "Information Technology"},
    {"ticker": "CTAS", "name": "Cintas Corp.", "sector": "Industrials"},
    {"ticker": "MRVL", "name": "Marvell Technology", "sector": "Information Technology"},
    {"ticker": "ABNB", "name": "Airbnb Inc.", "sector": "Consumer Discretionary"},
    {"ticker": "DASH", "name": "DoorDash Inc.", "sector": "Consumer Discretionary"},
]


# ── Constituent resolution ───────────────────────────────────────

def _get_constituents(index: str) -> list[dict]:
    """Return list of {ticker, name, sector} for the given index."""
    if index == "sp500":
        try:
            from filings import market_data
            return market_data.get_sp500_constituents()
        except Exception:
            logger.exception("Failed to get S&P 500 constituents")
            return []
    elif index == "nasdaq":
        return list(_NASDAQ_100_FALLBACK)
    elif index == "dow":
        return list(DOW_30)
    return []


# ── Price download (batched) ────────────────────────────────────

def _download_batch(
    tickers: list[str], period: str = "3mo",
) -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
    """Download close prices and volume for a single batch of tickers."""
    try:
        import yfinance as yf
        df = yf.download(
            tickers, period=period, threads=True,
            progress=False, timeout=_YF_TIMEOUT,
        )
        if df.empty:
            return None, None

        if isinstance(df.columns, pd.MultiIndex):
            lvl0 = df.columns.get_level_values(0)
            close = df["Close"] if "Close" in lvl0 else None
            vol = df["Volume"] if "Volume" in lvl0 else None
        elif len(tickers) == 1:
            t = tickers[0]
            close = df[["Close"]].rename(columns={"Close": t}) if "Close" in df.columns else None
            vol = df[["Volume"]].rename(columns={"Volume": t}) if "Volume" in df.columns else None
        else:
            close = df.get("Close")
            vol = df.get("Volume")

        if close is not None:
            close = close.dropna(how="all")
        if vol is not None:
            vol = vol.dropna(how="all")
        return close, vol
    except Exception:
        logger.exception("yfinance batch download failed")
        return None, None


def _download_market_data(
    tickers: list[str], period: str = "3mo",
) -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
    """Download close + volume, batching large ticker lists to avoid rate limits."""
    BATCH = 100
    if len(tickers) <= BATCH:
        return _download_batch(tickers, period)

    all_close: list[pd.DataFrame] = []
    all_vol: list[pd.DataFrame] = []
    for i in range(0, len(tickers), BATCH):
        batch = tickers[i : i + BATCH]
        logger.info("Downloading batch %d–%d of %d tickers", i, i + len(batch), len(tickers))
        close, vol = _download_batch(batch, period)
        if close is not None:
            all_close.append(close)
        if vol is not None:
            all_vol.append(vol)

    close_df = pd.concat(all_close, axis=1) if all_close else None
    vol_df = pd.concat(all_vol, axis=1) if all_vol else None
    # Deduplicate columns that may appear in multiple batches
    if close_df is not None:
        close_df = close_df.loc[:, ~close_df.columns.duplicated()]
    if vol_df is not None:
        vol_df = vol_df.loc[:, ~vol_df.columns.duplicated()]
    return close_df, vol_df



def _download_index_prices(symbol: str, period: str = "3mo") -> pd.Series | None:
    """Download index price series (e.g. ^GSPC) separately from constituents."""
    try:
        import yfinance as yf
        df = yf.download(symbol, period=period, progress=False, timeout=_YF_TIMEOUT)
        if df.empty:
            return None
        close = df["Close"]
        # Single-ticker yfinance may return DataFrame with ticker as column
        if isinstance(close, pd.DataFrame):
            close = close.iloc[:, 0]
        return close.dropna()
    except Exception:
        logger.exception("Failed to download index prices for %s", symbol)
        return None


def _get_raw_data(
    index: str, period: str = "3mo",
) -> tuple[pd.DataFrame | None, pd.DataFrame | None, list[dict], pd.Series | None]:
    """Get cached raw market data (close + volume + constituents + index price).

    Uses a per-key lock so concurrent callers (e.g. asyncio.gather) for the
    same index wait instead of triggering duplicate yfinance downloads.
    """
    cache_key = f"raw:{index}:{period}"

    # Fast path: check cache
    cached = _cache.get(cache_key)
    if cached is not None:
        return cached

    # Get or create per-key lock to serialize downloads for the same key
    with _lock:
        if cache_key not in _key_locks:
            _key_locks[cache_key] = threading.Lock()
        key_lock = _key_locks[cache_key]

    with key_lock:
        # Re-check cache — another thread may have populated it while we waited
        cached = _cache.get(cache_key)
        if cached is not None:
            return cached

        # ── L2: Try Supabase (fast, survives redeploys) ──
        try:
            from filings import supabase_cache

            sb_key = f"breadth:{cache_key}"
            sb_cached, is_fresh = supabase_cache.get_cached_with_stale(sb_key)
            if sb_cached and isinstance(sb_cached, dict) and sb_cached.get("close"):
                close_df = pd.DataFrame(sb_cached["close"])
                close_df.index = pd.to_datetime(close_df.index)
                vol_df = pd.DataFrame(sb_cached["volume"]) if sb_cached.get("volume") else None
                if vol_df is not None:
                    vol_df.index = pd.to_datetime(vol_df.index)
                constituents = sb_cached.get("constituents", [])
                idx_prices = None
                if sb_cached.get("idx_prices"):
                    idx_prices = pd.Series(sb_cached["idx_prices"])
                    idx_prices.index = pd.to_datetime(idx_prices.index)
                result = (close_df, vol_df, constituents, idx_prices)
                logger.info(
                    "Warm-loaded breadth %s from Supabase (%d rows, %s)",
                    cache_key, len(close_df), "fresh" if is_fresh else "stale",
                )
                _cache.set(cache_key, result)
                return result
        except Exception as e:
            logger.debug("Supabase breadth warm-load failed: %s", e)

        constituents = _get_constituents(index)
        if not constituents:
            result = (None, None, [], None)
            _cache.set(cache_key, result)
            return result

        tickers = [c["ticker"] for c in constituents]
        close_df, vol_df = _download_market_data(tickers, period)

        # Download index price separately — yfinance drops index symbols
        # (^GSPC etc.) from multi-ticker batch downloads
        idx_prices = None
        idx_symbol = INDEX_SYMBOLS.get(index)
        if idx_symbol:
            idx_prices = _download_index_prices(idx_symbol, period)

        result = (close_df, vol_df, constituents, idx_prices)

        _cache.set(cache_key, result)

        # ── Write back to Supabase L2 ──
        try:
            from filings import supabase_cache

            serialized = {
                "close": close_df.to_dict() if close_df is not None else None,
                "volume": vol_df.to_dict() if vol_df is not None else None,
                "constituents": constituents,
                "idx_prices": idx_prices.to_dict() if idx_prices is not None else None,
            }
            sb_key = f"breadth:{cache_key}"
            supabase_cache.set_cached(sb_key, "market_breadth", serialized, ttl_seconds=_L2_TTL)
        except Exception:
            logger.debug("Supabase breadth write-back failed for %s", cache_key)

        return result


# ── Color mapping (shared with market_data) ─────────────────────

from filings.market_data import pct_to_color as _pct_to_color


# ── Core computation ─────────────────────────────────────────────

def _compute_pct_changes(df: pd.DataFrame, period: str) -> pd.Series:
    """Return per-ticker % change for the given period as a Series.

    Handles duplicate-column guards and NaN/zero filtering in one place
    so callers don't repeat this logic.
    """
    lookback_idx = _get_lookback_index(df, period)
    last = df.iloc[-1]
    prev = df.iloc[lookback_idx]
    # Guard: extract scalar if duplicate columns slipped through
    if last.index.duplicated().any():
        last = last.groupby(level=0).first()
    if prev.index.duplicated().any():
        prev = prev.groupby(level=0).first()
    valid = last.notna() & prev.notna() & (prev != 0)
    return ((last - prev) / prev * 100).where(valid)


def _get_lookback_index(df: pd.DataFrame, period: str) -> int:
    """Return the DataFrame integer index of the comparison row for a period.

    For '1d', compares against previous trading day.
    For '1w'/'1m', finds the row closest to 5/21 trading days ago.
    For 'ytd', finds the first trading day of the current year.
    """
    if period == "1d":
        return len(df) - 2

    last_date = df.index[-1]

    if period == "ytd":
        year_start = pd.Timestamp(last_date.year, 1, 1)
        # Find first trading day on or after Jan 1
        mask = df.index >= year_start
        if mask.any():
            return int(mask.argmax())  # first True position
        return 0  # fallback: beginning of data

    # 1w / 1m — use approximate trading day offsets
    offsets = {"1w": 5, "1m": 21}
    target_offset = offsets.get(period, 1)
    target_idx = max(0, len(df) - 1 - target_offset)
    return target_idx


def _compute_breadth(
    df: pd.DataFrame, pct: pd.Series, period: str = "1d",
) -> dict:
    """Compute breadth metrics from pre-computed % changes."""
    if df is None or len(df) < 2:
        return _empty_metrics()

    # For 1d use tight threshold; for longer periods any move counts
    threshold = 0.005 if period == "1d" else 0.0
    valid_pct = pct.dropna()
    advances = int((valid_pct > threshold).sum())
    declines = int((valid_pct < -threshold).sum())
    total = len(valid_pct)
    unchanged = total - advances - declines

    return {
        "total": total,
        "advances": advances,
        "declines": declines,
        "unchanged": max(0, unchanged),
        "breadth_ratio": round(advances / max(declines, 1), 2),
        "advance_pct": round(advances / max(total, 1) * 100, 1),
        "decline_pct": round(declines / max(total, 1) * 100, 1),
    }


def _build_treemap(
    df: pd.DataFrame,
    constituents: list[dict],
    pct_changes: pd.Series,
    vol_df: pd.DataFrame | None = None,
) -> list[dict]:
    """Build ECharts treemap data grouped by sector, weighted by dollar volume."""
    if df is None or len(df) < 2:
        return []

    last = df.iloc[-1]
    ticker_map = {c["ticker"]: c for c in constituents}
    sectors: dict[str, list[dict]] = {}

    # Pre-compute average volume for all tickers at once (vectorized)
    avg_vol = vol_df.iloc[-5:].mean() if vol_df is not None else None

    for ticker, pct in pct_changes.dropna().items():
        c = ticker_map.get(ticker)
        if not c:
            continue
        pct_rounded = round(float(pct), 2)

        # Weight by dollar volume (close * 5-day avg volume) in millions
        weight = 1
        p_last = last.get(ticker)
        if isinstance(p_last, pd.Series):
            p_last = p_last.iloc[0]
        if avg_vol is not None and ticker in avg_vol.index:
            v = avg_vol[ticker]
            if pd.notna(v) and v > 0 and pd.notna(p_last):
                weight = max(1, int(float(p_last) * float(v) / 1e6))

        sector = c.get("sector", "Other")
        node = {
            "name": ticker,
            "value": weight,
            "pct_change": pct_rounded,
            "full_name": c.get("name", ticker),
            "link": f"/stock/{ticker}",
            "itemStyle": {
                "color": _pct_to_color(pct_rounded),
                "borderColor": "rgba(0,0,0,0.15)",
                "borderWidth": 1,
            },
        }
        sectors.setdefault(sector, []).append(node)

    return [
        {"name": name, "children": children}
        for name, children in sorted(
            sectors.items(), key=lambda x: -len(x[1]),
        )
    ]


def _compute_ad_line(
    df: pd.DataFrame, index: str, idx_prices: pd.Series | None = None,
) -> dict:
    """Compute daily net breadth and cumulative A/D line."""
    if df is None or len(df) < 3:
        return _empty_ad_line(index)

    dates: list[str] = []
    cumulative: list[int] = []
    running = 0
    total_stocks = int(df.columns.size)

    for i in range(1, len(df)):
        day_close = df.iloc[i]
        prev_close = df.iloc[i - 1]
        valid = day_close.notna() & prev_close.notna() & (prev_close != 0)
        changes = ((day_close - prev_close) / prev_close * 100).where(valid)

        adv = int((changes > 0.005).sum())
        dec = int((changes < -0.005).sum())
        running += adv - dec

        dates.append(df.index[i].strftime("%Y-%m-%d"))
        cumulative.append(running)

    # Overlay index price (pre-downloaded with constituent data)
    index_price_list: list[float | None] = []
    if idx_prices is not None:
        idx_date_map = {
            d.strftime("%Y-%m-%d"): round(float(v), 2)
            for d, v in idx_prices.items()
        }
        for d in dates:
            index_price_list.append(idx_date_map.get(d))
    else:
        index_price_list = [None] * len(dates)

    return {
        "dates": dates,
        "cumulative_ad": cumulative,
        "index_prices": index_price_list,
        "index_name": INDEX_CHOICES.get(index, index),
        "total_stocks": total_stocks,
    }


def _empty_metrics() -> dict:
    return {
        "total": 0, "advances": 0, "declines": 0, "unchanged": 0,
        "breadth_ratio": 0, "advance_pct": 0, "decline_pct": 0,
    }


def _empty_ad_line(index: str) -> dict:
    return {
        "dates": [], "cumulative_ad": [],
        "index_prices": [],
        "index_name": INDEX_CHOICES.get(index, index),
        "total_stocks": 0,
    }


# ── Additional insight computations ──────────────────────────────

def _compute_sector_breadth(
    pct_changes: pd.Series, constituents: list[dict], period: str = "1d",
) -> list[dict]:
    """Compute up/down breakdown per sector from pre-computed % changes."""
    threshold = 0.005 if period == "1d" else 0.0
    ticker_map = {c["ticker"]: c.get("sector", "Other") for c in constituents}
    sectors: dict[str, dict] = {}

    for ticker, pct in pct_changes.dropna().items():
        sector = ticker_map.get(ticker)
        if not sector:
            continue
        if sector not in sectors:
            sectors[sector] = {"up": 0, "down": 0, "total": 0}
        sectors[sector]["total"] += 1
        if pct > threshold:
            sectors[sector]["up"] += 1
        elif pct < -threshold:
            sectors[sector]["down"] += 1

    result = []
    for name, counts in sectors.items():
        t = counts["total"]
        result.append({
            "sector": name,
            "up": counts["up"],
            "down": counts["down"],
            "total": t,
            "up_pct": round(counts["up"] / max(t, 1) * 100, 1),
        })
    return sorted(result, key=lambda x: -x["up_pct"])


def _compute_pct_above_ma(df: pd.DataFrame, window: int = 50) -> dict | None:
    """Compute % of stocks trading above their N-day moving average."""
    if df is None or len(df) < window:
        return None

    # Only compute the mean of the last `window` rows — equivalent to
    # df.rolling(window).mean().iloc[-1] but avoids allocating the full
    # (rows × cols) rolling matrix.
    sma = df.iloc[-window:].mean()
    last = df.iloc[-1]
    valid = last.notna() & sma.notna()
    above = int(((last > sma) & valid).sum())
    total = int(valid.sum())
    return {
        "above": above,
        "total": total,
        "pct": round(above / max(total, 1) * 100, 1),
    }


def _compute_top_movers(
    pct_changes: pd.Series, constituents: list[dict], n: int = 5,
) -> dict:
    """Return top N gainers and losers from pre-computed % changes."""
    ticker_map = {c["ticker"]: c for c in constituents}

    movers: list[dict] = []
    for ticker, pct in pct_changes.dropna().items():
        c = ticker_map.get(ticker)
        if not c:
            continue
        movers.append({
            "ticker": ticker,
            "name": c.get("name", ticker),
            "sector": c.get("sector", ""),
            "pct": round(float(pct), 2),
        })

    movers.sort(key=lambda x: x["pct"])
    return {
        "gainers": list(reversed(movers[-n:])),
        "losers": movers[:n],
    }


def detect_divergence(
    ad_line: dict,
    window: int = 10,
) -> dict | None:
    """Detect divergence between A/D line and index price over recent days.

    Accepts the dict returned by fetch_ad_line_history().
    Returns a dict with divergence info, or None if no divergence detected.
    """
    cumulative_ad = ad_line.get("cumulative_ad", [])
    index_prices = ad_line.get("index_prices", [])
    if len(cumulative_ad) < window or len(index_prices) < window:
        return None

    # Get last `window` data points
    ad_recent = cumulative_ad[-window:]
    price_recent = [p for p in index_prices[-window:] if p is not None]
    if len(price_recent) < window // 2:
        return None

    # Simple slope: (last - first) / window
    ad_slope = (ad_recent[-1] - ad_recent[0])
    # Normalize price to similar scale as A/D for comparison
    price_slope = (price_recent[-1] - price_recent[0]) / price_recent[0] * 1000

    # Check for meaningful divergence (both must have noticeable trends)
    min_ad_move = max(abs(ad_recent[-1] - ad_recent[0]), 1)
    min_price_move = abs(price_recent[-1] - price_recent[0]) / price_recent[0] * 100

    if min_price_move < 0.5 or min_ad_move < 5:
        return None  # Not enough movement to call divergence

    if ad_slope < 0 and price_slope > 0:
        return {
            "type": "bearish",
            "message": "Momentum declining while index price rises — narrowing rally",
        }
    elif ad_slope > 0 and price_slope < 0:
        return {
            "type": "bullish",
            "message": "Momentum rising while index price drops — potential opportunity",
        }

    return None


# ── Public API ───────────────────────────────────────────────────

def fetch_breadth_data(index: str = "sp500", period: str = "1d") -> dict:
    """Fetch breadth metrics + treemap data (L1 30 min, L2 6 hours)."""
    cache_key = f"breadth:{index}:{period}"

    cached = _cache.get(cache_key)
    if cached is not None:
        return cached

    # L2: check Supabase for processed results
    sb_key = f"result:{cache_key}"
    try:
        from filings import supabase_cache
        l2 = supabase_cache.get_cached(sb_key)
        if l2 and isinstance(l2, dict) and l2.get("metrics"):
            _cache.set(cache_key, l2)
            logger.info("Breadth result warm-loaded from L2: %s", cache_key)
            return l2
    except Exception:
        pass

    close_df, vol_df, constituents, _idx = _get_raw_data(index)

    if close_df is None or len(close_df) < 2 or not constituents:
        data = _build_mock_data(index, period)
        _cache.set(cache_key, data)
        return data

    # Compute % changes once — shared by all downstream functions
    pct_changes = _compute_pct_changes(close_df, period)

    metrics = _compute_breadth(close_df, pct_changes, period)
    treemap = _build_treemap(close_df, constituents, pct_changes, vol_df)

    data = {
        "metrics": metrics,
        "treemap": treemap,
        "index_name": INDEX_CHOICES.get(index, index),
        "index_key": index,
        "period": period,
        "period_label": PERIOD_CHOICES.get(period, period),
        # Insight fields — reuse pre-computed pct_changes
        "sector_breadth": _compute_sector_breadth(pct_changes, constituents, period),
        "above_50d": _compute_pct_above_ma(close_df),
        "top_movers": _compute_top_movers(pct_changes, constituents),
    }

    _cache.set(cache_key, data)

    # Write processed result to L2
    try:
        from filings import supabase_cache
        supabase_cache.set_cached(sb_key, "market_breadth", data, ttl_seconds=_L2_TTL)
    except Exception:
        pass

    return data


def fetch_ad_line_history(index: str = "sp500") -> dict:
    """Fetch cumulative A/D line data (L1 30 min, L2 6 hours)."""
    cache_key = f"ad_line:{index}"

    cached = _cache.get(cache_key)
    if cached is not None:
        return cached

    # L2: check Supabase for processed results
    sb_key = f"result:{cache_key}"
    try:
        from filings import supabase_cache
        l2 = supabase_cache.get_cached(sb_key)
        if l2 and isinstance(l2, dict) and l2.get("dates"):
            _cache.set(cache_key, l2)
            logger.info("A/D line warm-loaded from L2: %s", cache_key)
            return l2
    except Exception:
        pass

    close_df, _vol_df, constituents, idx_prices = _get_raw_data(index)

    if close_df is None or len(close_df) < 3 or not constituents:
        data = _build_mock_ad_line(index)
        _cache.set(cache_key, data)
        return data

    data = _compute_ad_line(close_df, index, idx_prices)

    _cache.set(cache_key, data)

    # Write processed result to L2
    try:
        from filings import supabase_cache
        supabase_cache.set_cached(sb_key, "market_breadth", data, ttl_seconds=_L2_TTL)
    except Exception:
        pass

    return data


# ── Mock data fallback ───────────────────────────────────────────

_MOCK_COMPANIES = [
    ("AAPL", "Apple Inc.", "Information Technology"),
    ("MSFT", "Microsoft Corp.", "Information Technology"),
    ("GOOGL", "Alphabet Inc.", "Communication Services"),
    ("AMZN", "Amazon.com Inc.", "Consumer Discretionary"),
    ("NVDA", "NVIDIA Corp.", "Information Technology"),
    ("META", "Meta Platforms Inc.", "Communication Services"),
    ("TSLA", "Tesla Inc.", "Consumer Discretionary"),
    ("JPM", "JPMorgan Chase & Co.", "Financials"),
    ("V", "Visa Inc.", "Financials"),
    ("JNJ", "Johnson & Johnson", "Health Care"),
    ("UNH", "UnitedHealth Group", "Health Care"),
    ("PG", "Procter & Gamble", "Consumer Staples"),
    ("HD", "Home Depot Inc.", "Consumer Discretionary"),
    ("MA", "Mastercard Inc.", "Financials"),
    ("DIS", "Walt Disney Co.", "Communication Services"),
    ("NFLX", "Netflix Inc.", "Communication Services"),
    ("CRM", "Salesforce Inc.", "Information Technology"),
    ("COST", "Costco Wholesale", "Consumer Staples"),
    ("PFE", "Pfizer Inc.", "Health Care"),
    ("XOM", "Exxon Mobil Corp.", "Energy"),
    ("CVX", "Chevron Corp.", "Energy"),
    ("AVGO", "Broadcom Inc.", "Information Technology"),
    ("LLY", "Eli Lilly & Co.", "Health Care"),
    ("ABBV", "AbbVie Inc.", "Health Care"),
    ("MRK", "Merck & Co.", "Health Care"),
    ("WMT", "Walmart Inc.", "Consumer Staples"),
    ("BAC", "Bank of America", "Financials"),
    ("KO", "Coca-Cola Co.", "Consumer Staples"),
    ("PEP", "PepsiCo Inc.", "Consumer Staples"),
    ("TMO", "Thermo Fisher Scientific", "Health Care"),
]


def _build_mock_data(index: str, period: str = "1d") -> dict:
    """Deterministic mock breadth data."""
    rng = random.Random(42)
    total = {"sp500": 503, "nasdaq": 100, "dow": 30}.get(index, 100)

    advances = int(total * rng.uniform(0.42, 0.58))
    declines = int(total * rng.uniform(0.30, 0.48))
    if advances + declines > total:
        declines = total - advances - 5
    unchanged = total - advances - declines

    metrics = {
        "total": total,
        "advances": advances,
        "declines": max(0, declines),
        "unchanged": max(0, unchanged),
        "breadth_ratio": round(advances / max(declines, 1), 2),
        "advance_pct": round(advances / total * 100, 1),
        "decline_pct": round(max(0, declines) / total * 100, 1),
    }

    # Mock treemap with weighted values
    sectors: dict[str, list[dict]] = {}
    companies = _MOCK_COMPANIES[:min(total, len(_MOCK_COMPANIES))]
    for i, (sym, name, sector) in enumerate(companies):
        pct = round(rng.uniform(-4, 5), 2)
        weight = max(1, int(rng.uniform(50, 5000)))  # simulate dollar volume
        node = {
            "name": sym,
            "value": weight,
            "pct_change": pct,
            "full_name": name,
            "link": f"/stock/{sym}",
            "itemStyle": {
                "color": _pct_to_color(pct),
                "borderColor": "rgba(0,0,0,0.15)",
                "borderWidth": 1,
            },
        }
        sectors.setdefault(sector, []).append(node)

    treemap = [
        {"name": s, "children": c}
        for s, c in sorted(sectors.items(), key=lambda x: -len(x[1]))
    ]

    return {
        "metrics": metrics,
        "treemap": treemap,
        "index_name": INDEX_CHOICES.get(index, index),
        "index_key": index,
        "period": period,
        "period_label": PERIOD_CHOICES.get(period, period),
        "is_mock": True,
    }


def _build_mock_ad_line(index: str) -> dict:
    """Deterministic mock A/D line data."""
    rng = random.Random(43)
    days = 60
    dates: list[str] = []
    cumulative: list[int] = []
    index_prices: list[float] = []
    running = 0
    price = {"sp500": 5800, "nasdaq": 18000, "dow": 39000}.get(index, 5000)
    total = {"sp500": 503, "nasdaq": 100, "dow": 30}.get(index, 100)

    from datetime import datetime, timedelta
    start = datetime.now() - timedelta(days=days + 20)
    d = start
    for _ in range(days):
        d += timedelta(days=1)
        if d.weekday() >= 5:
            continue
        net = rng.randint(-80, 120)
        running += net
        price *= 1 + rng.uniform(-0.015, 0.018)
        dates.append(d.strftime("%Y-%m-%d"))
        cumulative.append(running)
        index_prices.append(round(price, 2))

    return {
        "dates": dates,
        "cumulative_ad": cumulative,
        "index_prices": index_prices,
        "index_name": INDEX_CHOICES.get(index, index),
        "total_stocks": total,
        "is_mock": True,
    }
