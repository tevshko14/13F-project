"""CBOE volatility & options sentiment data — Put/Call Ratio, VIX Term Structure, SKEW.

Free data sources:
    - CBOE CDN CSVs for Put/Call ratios (index + equity)
    - yfinance for VIX tenor indices (^VIX, ^VIX9D, ^VIX3M, ^VIX6M)
    - yfinance for SKEW index (^SKEW)
    - yfinance options chains for per-ticker IV Rank

Caching:
    L1 — in-memory dict with TTL (1h for daily data, 30min for intraday)
    L2 — Supabase ``api_cache`` table via supabase_cache module
"""

from __future__ import annotations

import csv
import io
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

logger = logging.getLogger(__name__)

# ── In-memory L1 cache (single keyed dict) ──────────────────────────────────
_lock = threading.Lock()
_cache: dict[str, tuple[float, any]] = {}

# L1 TTLs (seconds)
_TTLS = {
    "putcall": 3600,    # 1 hour
    "vix_term": 1800,   # 30 min
    "skew": 3600,       # 1 hour
    "ivrank": 1800,     # 30 min
}

# L2 Supabase cache TTLs (seconds)
_L2_TTLS = {
    "putcall": 86400,   # 24 hours
    "vix_term": 21600,  # 6 hours
    "skew": 86400,      # 24 hours
    "ivrank": 21600,    # 6 hours
}

# ── CBOE CDN URLs ────────────────────────────────────────────────────────────
# Note: CBOE stopped updating these CSVs in Oct 2019. For current data,
# we fall back to yfinance ^PCALL / ^CPCE tickers.
_TOTAL_PC_URL = "https://cdn.cboe.com/resources/options/volume_and_call_put_ratios/totalpc.csv"
_INDEX_PC_URL = "https://cdn.cboe.com/resources/options/volume_and_call_put_ratios/indexpc.csv"
_EQUITY_PC_URL = "https://cdn.cboe.com/resources/options/volume_and_call_put_ratios/equitypc.csv"

# ── yfinance timeout session ────────────────────────────────────────────────
_YF_TIMEOUT = 15


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


def _cached_or_fetch(cache_key: str, fetcher, kind: str = "putcall"):
    """Generic L1 → L2 → fetcher pattern for all CBOE data."""
    l1_ttl = _TTLS.get(kind, 3600)
    l2_ttl = _L2_TTLS.get(kind, 86400)

    # L1
    with _lock:
        cached = _cache.get(cache_key)
        if cached and time.time() - cached[0] < l1_ttl:
            return cached[1]

    # L2
    try:
        from filings import supabase_cache
        l2 = supabase_cache.get_cached(cache_key)
        if l2:
            with _lock:
                _cache[cache_key] = (time.time(), l2)
            return l2
    except Exception:
        pass

    # L3: live fetch
    try:
        result = fetcher()
        if result:
            try:
                from filings import supabase_cache
                supabase_cache.set_cached(cache_key, "cboe", result, l2_ttl)
            except Exception:
                pass
            with _lock:
                _cache[cache_key] = (time.time(), result)
        return result
    except Exception as exc:
        logger.warning("CBOE fetch failed for %s: %s", cache_key, exc)
        return None


# ═════════════════════════════════════════════════════════════════════════════
# PUT / CALL RATIO
# ═════════════════════════════════════════════════════════════════════════════

def get_put_call_ratio(ratio_type: str = "total") -> list[dict]:
    """Return ~252 trading days of put/call ratio data.

    Args:
        ratio_type: "index", "equity", or "total" (average of both)

    Returns:
        List of dicts: [{date, ratio, call_vol, put_vol}, ...]
        Sorted chronologically (oldest first).
    """
    if ratio_type not in ("index", "equity", "total"):
        ratio_type = "total"

    return _cached_or_fetch(
        f"cboe:putcall:{ratio_type}",
        lambda: _fetch_putcall_from_cboe(ratio_type),
        "putcall",
    ) or []


def _fetch_putcall_from_cboe(ratio_type: str) -> list[dict]:
    """Fetch Put/Call ratio data — try CBOE CSVs first, fall back to yfinance."""
    # Try CBOE CSV first
    try:
        url_map = {"total": _TOTAL_PC_URL, "index": _INDEX_PC_URL, "equity": _EQUITY_PC_URL}
        raw = _download_csv(url_map.get(ratio_type, _TOTAL_PC_URL))
        result = _parse_cboe_csv(raw, ratio_type)
        if result:
            return result
    except Exception as exc:
        logger.debug("CBOE CSV fetch failed for %s: %s", ratio_type, exc)

    # Fallback: yfinance P/C ratio tickers
    return _fetch_putcall_from_yfinance(ratio_type)


def _fetch_putcall_from_yfinance(ratio_type: str) -> list[dict]:
    """Fetch P/C ratio from yfinance as fallback (uses CBOE equity P/C index ^CPCE)."""
    try:
        import yfinance as yf

        # ^CPCE = CBOE equity put/call ratio (most commonly referenced)
        ticker = "^CPCE"
        df = yf.download(ticker, period="1y", progress=False, timeout=_YF_TIMEOUT)
        if df.empty:
            logger.warning("yfinance returned empty P/C ratio data for %s", ticker)
            return []

        # Handle multi-level columns
        if hasattr(df.columns, "levels") and len(df.columns.levels) > 1:
            df.columns = df.columns.droplevel(1)

        result = []
        for idx, row in df.iterrows():
            try:
                val = float(row["Close"])
                if val > 0:
                    result.append({
                        "date": idx.strftime("%Y-%m-%d"),
                        "ratio": round(val, 4),
                        "call_vol": None,
                        "put_vol": None,
                    })
            except (ValueError, KeyError):
                continue

        result.sort(key=lambda x: x["date"])
        return result[-252:]
    except Exception as exc:
        logger.warning("yfinance P/C ratio fallback failed: %s", exc)
        return []


def _download_csv(url: str) -> str:
    """Download CSV content from URL."""
    import urllib.request

    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (PaperPanda/1.0)",
    })
    with urllib.request.urlopen(req, timeout=20) as resp:
        return resp.read().decode("utf-8", errors="replace")


def _parse_cboe_csv(raw: str, source: str) -> list[dict]:
    """Parse CBOE put/call ratio CSV into list of dicts.

    CBOE CSV columns vary but generally:
      Index: DATE, CALLS, PUTS, TOTAL, P/C RATIO
      Equity: DATE, CALLS, PUTS, TOTAL, P/C RATIO
    """
    result = []
    reader = csv.reader(io.StringIO(raw.strip()))

    # Read header to find column positions
    # CBOE CSVs have a title row (e.g. "Index P/C Ratios") before the actual header
    # Header row contains "Trade_date" or similar — detect by substring match
    header = None
    for row in reader:
        if not row:
            continue
        cleaned = [c.strip().upper() for c in row]
        # Check if any column contains "DATE" as substring
        if any("DATE" in c for c in cleaned):
            header = cleaned
            break

    if not header:
        logger.warning("Could not find header in CBOE %s CSV", source)
        return []

    # Find column indices
    date_idx = next((i for i, h in enumerate(header) if "DATE" in h), 0)
    ratio_idx = next((i for i, h in enumerate(header) if "RATIO" in h), -1)
    call_idx = next((i for i, h in enumerate(header) if "CALL" in h), -1)
    put_idx = next((i for i, h in enumerate(header) if "PUT" in h and "RATIO" not in h), -1)

    for row in reader:
        if not row or len(row) <= max(date_idx, ratio_idx):
            continue
        try:
            date_str = row[date_idx].strip()
            # Parse various date formats
            dt = _parse_date(date_str)
            if dt is None:
                continue

            ratio_val = float(row[ratio_idx].strip()) if ratio_idx >= 0 and row[ratio_idx].strip() else None
            call_vol = _safe_int(row[call_idx]) if call_idx >= 0 else None
            put_vol = _safe_int(row[put_idx]) if put_idx >= 0 else None

            if ratio_val is not None:
                result.append({
                    "date": dt.strftime("%Y-%m-%d"),
                    "ratio": round(ratio_val, 4),
                    "call_vol": call_vol,
                    "put_vol": put_vol,
                })
        except (ValueError, IndexError):
            continue

    # Sort chronologically, take last 252 trading days
    result.sort(key=lambda x: x["date"])
    return result[-252:]



def _parse_date(s: str) -> datetime | None:
    """Try multiple date formats."""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y", "%m-%d-%Y"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def _safe_int(val: str) -> int | None:
    """Parse int from string, returning None on failure."""
    try:
        return int(val.strip().replace(",", ""))
    except (ValueError, AttributeError):
        return None


# ═════════════════════════════════════════════════════════════════════════════
# VIX TERM STRUCTURE
# ═════════════════════════════════════════════════════════════════════════════

def get_vix_term_structure() -> dict:
    """Return current VIX term structure using yfinance tenor indices.

    Returns:
        {
            "tenors": [{"label": "9D", "value": 18.5}, {"label": "1M", "value": 20.1}, ...],
            "state": "contango" | "backwardation",
            "spot": 20.1,
            "updated": "2026-03-11 14:30"
        }
    """
    return _cached_or_fetch("cboe:vix_term", _fetch_vix_term_structure, "vix_term") or {}


def _fetch_vix_term_structure() -> dict:
    """Fetch VIX tenor indices from yfinance."""
    try:
        import yfinance as yf

        tenor_symbols = {
            "^VIX9D": "9D",
            "^VIX": "1M (VIX)",
            "^VIX3M": "3M",
            "^VIX6M": "6M",
        }

        tenors = []
        spot = None

        for sym, label in tenor_symbols.items():
            try:
                ticker = yf.Ticker(sym, session=_yf_session)
                info = ticker.fast_info
                price = getattr(info, "last_price", None) or getattr(info, "previous_close", None)
                if price and price > 0:
                    tenors.append({"label": label, "value": round(float(price), 2)})
                    if sym == "^VIX":
                        spot = round(float(price), 2)
            except Exception as exc:
                logger.debug("VIX tenor %s fetch failed: %s", sym, exc)

        if not tenors:
            return {}

        # Determine contango/backwardation
        values = [t["value"] for t in tenors]
        state = "contango" if len(values) >= 2 and values[-1] > values[0] else "backwardation"

        return {
            "tenors": tenors,
            "state": state,
            "spot": spot,
            "updated": datetime.now().strftime("%Y-%m-%d %H:%M"),
        }
    except Exception as exc:
        logger.warning("Failed to fetch VIX term structure: %s", exc)
        return {}


# ═════════════════════════════════════════════════════════════════════════════
# SKEW INDEX
# ═════════════════════════════════════════════════════════════════════════════

def get_skew_index() -> list[dict]:
    """Return ~252 trading days of CBOE SKEW index history.

    Returns:
        List of dicts: [{date, value}, ...] sorted chronologically.
    """
    return _cached_or_fetch("cboe:skew", _fetch_skew_from_yfinance, "skew") or []


def _fetch_skew_from_yfinance() -> list[dict]:
    """Fetch SKEW index history from yfinance."""
    try:
        import yfinance as yf

        df = yf.download("^SKEW", period="1y", progress=False, timeout=_YF_TIMEOUT)
        if df.empty:
            logger.warning("yfinance returned empty SKEW data")
            return []

        result = []
        close_col = "Close"
        # Handle multi-level columns from yfinance
        if hasattr(df.columns, "levels"):
            df.columns = df.columns.droplevel(1) if len(df.columns.levels) > 1 else df.columns

        for idx, row in df.iterrows():
            try:
                val = float(row[close_col])
                if val > 0:
                    result.append({
                        "date": idx.strftime("%Y-%m-%d"),
                        "value": round(val, 2),
                    })
            except (ValueError, KeyError):
                continue

        result.sort(key=lambda x: x["date"])
        return result[-252:]
    except Exception as exc:
        logger.warning("Failed to fetch SKEW index: %s", exc)
        return []


# ═════════════════════════════════════════════════════════════════════════════
# IV RANK (per-ticker, batch)
# ═════════════════════════════════════════════════════════════════════════════

def get_iv_rank_batch() -> list[dict]:
    """Compute IV Rank for the most-active options tickers.

    Gets the top 50 tickers from unusual_options_activity, then computes
    IV Rank for each using yfinance ATM options IV.

    Returns:
        List of dicts sorted by IV Rank descending:
        [{ticker, company, current_iv, iv_rank, iv_52w_high, iv_52w_low}, ...]
    """
    return _cached_or_fetch("cboe:ivrank_batch", _compute_iv_rank_batch, "ivrank") or []


def _get_active_tickers() -> list[dict]:
    """Get top 50 tickers from unusual_options_activity table."""
    from filings import supabase_cache

    client = supabase_cache._get_client()
    if client is None:
        return []

    try:
        resp = (
            client.table("unusual_options_activity")
            .select("ticker, company_name, implied_vol")
            .order("fetched_at", desc=True)
            .limit(200)
            .execute()
        )
        if not resp.data:
            return []

        # Deduplicate by ticker, keep first (most recent)
        seen = {}
        for row in resp.data:
            t = row.get("ticker")
            if t and t not in seen:
                seen[t] = {
                    "ticker": t,
                    "company": row.get("company_name", t),
                }
            if len(seen) >= 50:
                break
        return list(seen.values())
    except Exception as exc:
        logger.warning("Failed to get active tickers for IV rank: %s", exc)
        return []


def _compute_iv_rank_for_ticker(ticker: str) -> dict | None:
    """Compute IV Rank for a single ticker using yfinance options chain.

    IV Rank = (current_IV - 52w_low_IV) / (52w_high_IV - 52w_low_IV) * 100
    """
    try:
        import yfinance as yf

        t = yf.Ticker(ticker, session=_yf_session)

        # Get available expiry dates
        try:
            expirations = t.options
        except Exception:
            return None

        if not expirations:
            return None

        # Get the nearest expiry chain for current IV
        chain = t.option_chain(expirations[0])
        if chain is None:
            return None

        calls = chain.calls
        if calls.empty:
            return None

        # Find ATM option (closest to current price)
        try:
            info = t.fast_info
            spot = getattr(info, "last_price", None) or getattr(info, "previous_close", None)
        except Exception:
            spot = None

        if spot is None or spot <= 0:
            return None

        calls = calls.dropna(subset=["impliedVolatility"])
        if calls.empty:
            return None

        calls["dist"] = abs(calls["strike"] - spot)
        atm = calls.loc[calls["dist"].idxmin()]
        current_iv = float(atm["impliedVolatility"]) * 100  # Convert to percentage

        # Get historical IV from stock's historical volatility as proxy
        # (True IV rank needs daily ATM IV snapshots; we approximate with
        # realized volatility from price history)
        hist = t.history(period="1y")
        if hist.empty or len(hist) < 30:
            return None

        # Use rolling 30-day realized vol as IV proxy for 52-week range
        returns = hist["Close"].pct_change().dropna()
        rolling_vol = (returns.rolling(30).std() * (252 ** 0.5) * 100).dropna()

        if rolling_vol.empty:
            return None

        iv_high = round(float(rolling_vol.max()), 2)
        iv_low = round(float(rolling_vol.min()), 2)

        if iv_high == iv_low:
            iv_rank = 50.0  # Neutral if no range
        else:
            iv_rank = round((current_iv - iv_low) / (iv_high - iv_low) * 100, 1)
            iv_rank = max(0.0, min(100.0, iv_rank))  # Clamp 0-100

        return {
            "current_iv": round(current_iv, 2),
            "iv_rank": iv_rank,
            "iv_52w_high": iv_high,
            "iv_52w_low": iv_low,
        }
    except Exception as exc:
        logger.debug("IV rank computation failed for %s: %s", ticker, exc)
        return None


def _compute_iv_rank_batch() -> list[dict]:
    """Compute IV Rank for top 50 active tickers in parallel."""
    tickers = _get_active_tickers()
    if not tickers:
        logger.warning("No active tickers found for IV rank batch")
        return []

    results = []

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {
            pool.submit(_compute_iv_rank_for_ticker, t["ticker"]): t
            for t in tickers
        }
        for future in as_completed(futures):
            ticker_info = futures[future]
            try:
                iv_data = future.result(timeout=30)
                if iv_data:
                    results.append({
                        "ticker": ticker_info["ticker"],
                        "company": ticker_info["company"],
                        **iv_data,
                    })
            except Exception:
                continue

    # Sort by IV Rank descending (highest IV rank = most expensive premium)
    results.sort(key=lambda x: x.get("iv_rank", 0), reverse=True)
    return results
