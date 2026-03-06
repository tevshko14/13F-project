"""Earnings Scorecard — aggregated earnings season metrics.

Fetches earnings surprises, beat/miss rates, and market reaction data
from Financial Modeling Prep (FMP) API. Falls back to mock data when
FMP_API_KEY is not configured.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime, timedelta

import httpx

logger = logging.getLogger(__name__)

# ── Cache ────────────────────────────────────────────────────────────
_lock = threading.Lock()
_earnings_cache: dict[str, tuple[float, dict]] = {}
_EARNINGS_TTL = 3600  # 1 hour

_FMP_BASE = "https://financialmodelingprep.com/api/v3"
_TIMEOUT = 15

# ── Index constituents (simplified) ─────────────────────────────────
INDEX_CHOICES = {
    "sp500": "S&P 500",
    "nasdaq": "NASDAQ 100",
}

# Sectors for filtering
SECTORS = [
    "Technology",
    "Healthcare",
    "Financials",
    "Consumer Cyclical",
    "Industrials",
    "Communication Services",
    "Consumer Defensive",
    "Energy",
    "Utilities",
    "Real Estate",
    "Basic Materials",
]

# Available fiscal quarters
def get_available_quarters() -> list[str]:
    """Return last 8 quarters as 'Q1 2024' style strings."""
    now = datetime.now()
    quarters = []
    y, q = now.year, (now.month - 1) // 3 + 1
    for _ in range(8):
        quarters.append(f"Q{q} {y}")
        q -= 1
        if q == 0:
            q = 4
            y -= 1
    return quarters


def _api_key() -> str:
    return os.environ.get("FMP_API_KEY", "")


def _fmp_get(path: str, params: dict | None = None) -> list | dict | None:
    """Make a GET request to FMP API."""
    key = _api_key()
    if not key:
        return None
    params = params or {}
    params["apikey"] = key
    try:
        r = httpx.get(f"{_FMP_BASE}{path}", params=params, timeout=_TIMEOUT)
        r.raise_for_status()
        return r.json()
    except Exception:
        logger.exception("FMP API error for %s", path)
        return None


# ── Core data fetchers ───────────────────────────────────────────────

def _parse_quarter_filter(quarter: str) -> tuple[int, int] | None:
    """Parse 'Q1 2024' into (year, quarter_num)."""
    try:
        parts = quarter.strip().split()
        q = int(parts[0].replace("Q", ""))
        y = int(parts[1])
        return (y, q)
    except Exception:
        return None


def _quarter_date_range(year: int, q: int) -> tuple[str, str]:
    """Return (start_date, end_date) for a fiscal quarter."""
    starts = {1: "01-01", 2: "04-01", 3: "07-01", 4: "10-01"}
    ends = {1: "03-31", 2: "06-30", 3: "09-30", 4: "12-31"}
    return f"{year}-{starts[q]}", f"{year}-{ends[q]}"


def fetch_earnings_data(
    index: str = "sp500",
    quarter: str | None = None,
    sector: str | None = None,
) -> dict:
    """Fetch and aggregate earnings season data.

    Returns dict with keys: metrics, results, quarter, index, sector.
    """
    cache_key = f"{index}:{quarter or 'latest'}:{sector or 'all'}"

    with _lock:
        if cache_key in _earnings_cache:
            ts, data = _earnings_cache[cache_key]
            if time.time() - ts < _EARNINGS_TTL:
                return data

    data = _fetch_earnings_from_api(index, quarter, sector)

    with _lock:
        _earnings_cache[cache_key] = (time.time(), data)

    return data


def _fetch_earnings_from_api(
    index: str, quarter: str | None, sector: str | None
) -> dict:
    """Fetch earnings surprises from FMP and compute aggregated metrics."""
    # Determine date range
    if quarter:
        parsed = _parse_quarter_filter(quarter)
        if parsed:
            y, q = parsed
            start, end = _quarter_date_range(y, q)
        else:
            quarter = None

    if not quarter:
        # Default to current quarter
        now = datetime.now()
        q = (now.month - 1) // 3 + 1
        start, end = _quarter_date_range(now.year, q)
        quarter = f"Q{q} {now.year}"

    # Fetch earnings surprises
    surprises = _fmp_get("/earnings-surprises", {"from": start, "to": end})

    if not surprises or not isinstance(surprises, list):
        return _build_mock_data(quarter, index, sector)

    # If we have index constituents, filter to them
    constituents = _get_index_constituents(index)

    results = []
    for item in surprises:
        symbol = item.get("symbol", "")
        if constituents and symbol not in constituents:
            continue

        actual_eps = item.get("actualEarningResult")
        est_eps = item.get("estimatedEarning")
        actual_rev = item.get("actualRevenue")
        est_rev = item.get("estimatedRevenue")
        report_date = item.get("date", "")
        company_name = item.get("companyName", symbol)
        item_sector = item.get("sector", "")

        if sector and item_sector and item_sector != sector:
            continue

        eps_beat = None
        if actual_eps is not None and est_eps is not None:
            eps_beat = actual_eps > est_eps

        rev_beat = None
        if actual_rev is not None and est_rev is not None:
            rev_beat = actual_rev > est_rev

        # Surprise percentages
        eps_surprise_pct = None
        if actual_eps is not None and est_eps is not None and est_eps != 0:
            eps_surprise_pct = round(
                ((actual_eps - est_eps) / abs(est_eps)) * 100, 2
            )

        rev_surprise_pct = None
        if actual_rev is not None and est_rev is not None and est_rev != 0:
            rev_surprise_pct = round(
                ((actual_rev - est_rev) / abs(est_rev)) * 100, 2
            )

        # Price reaction (from FMP data if available)
        price_change = item.get("priceReaction")

        # Guidance tag
        guide = item.get("guidance", "—")
        if not guide:
            guide = "—"

        results.append(
            {
                "symbol": symbol,
                "name": company_name,
                "date": report_date,
                "sector": item_sector,
                "actual_eps": actual_eps,
                "est_eps": est_eps,
                "eps_beat": eps_beat,
                "eps_surprise_pct": eps_surprise_pct,
                "actual_rev": actual_rev,
                "est_rev": est_rev,
                "rev_beat": rev_beat,
                "rev_surprise_pct": rev_surprise_pct,
                "price_change": price_change,
                "guide": guide,
            }
        )

    # Sort by date descending
    results.sort(key=lambda r: r["date"] or "", reverse=True)

    metrics = _compute_metrics(results)
    metrics["quarter"] = quarter

    return {
        "metrics": metrics,
        "results": results,
        "quarter": quarter,
        "index": INDEX_CHOICES.get(index, index),
        "index_key": index,
        "sector": sector,
    }


def _get_index_constituents(index: str) -> set[str] | None:
    """Return a set of ticker symbols for a given index."""
    if index == "sp500":
        data = _fmp_get("/sp500_constituent")
        if data and isinstance(data, list):
            return {item.get("symbol", "") for item in data}
    elif index == "nasdaq":
        data = _fmp_get("/nasdaq_constituent")
        if data and isinstance(data, list):
            return {item.get("symbol", "") for item in data}
    return None


def _compute_metrics(results: list[dict]) -> dict:
    """Compute aggregated scorecard metrics from earnings results."""
    total = len(results)
    if total == 0:
        return {
            "total": 0,
            "eps_beats": 0,
            "eps_misses": 0,
            "eps_inline": 0,
            "eps_beat_rate": 0,
            "rev_beats": 0,
            "rev_misses": 0,
            "rev_inline": 0,
            "rev_beat_rate": 0,
            "dual_beats": 0,
            "avg_price_change": 0,
            "avg_eps_surprise": 0,
        }

    eps_beats = sum(1 for r in results if r["eps_beat"] is True)
    eps_misses = sum(1 for r in results if r["eps_beat"] is False)
    eps_inline = total - eps_beats - eps_misses

    rev_beats = sum(1 for r in results if r["rev_beat"] is True)
    rev_misses = sum(1 for r in results if r["rev_beat"] is False)
    rev_inline = total - rev_beats - rev_misses

    dual_beats = sum(
        1
        for r in results
        if r["eps_beat"] is True and r["rev_beat"] is True
    )

    price_changes = [
        r["price_change"] for r in results if r["price_change"] is not None
    ]
    avg_price_change = (
        round(sum(price_changes) / len(price_changes), 2) if price_changes else 0
    )

    eps_surprises = [
        r["eps_surprise_pct"]
        for r in results
        if r["eps_surprise_pct"] is not None
    ]
    avg_eps_surprise = (
        round(sum(eps_surprises) / len(eps_surprises), 2) if eps_surprises else 0
    )

    return {
        "total": total,
        "eps_beats": eps_beats,
        "eps_misses": eps_misses,
        "eps_inline": eps_inline,
        "eps_beat_rate": round(eps_beats / total * 100, 1) if total else 0,
        "rev_beats": rev_beats,
        "rev_misses": rev_misses,
        "rev_inline": rev_inline,
        "rev_beat_rate": round(rev_beats / total * 100, 1) if total else 0,
        "dual_beats": dual_beats,
        "avg_price_change": avg_price_change,
        "avg_eps_surprise": avg_eps_surprise,
    }


# ── Mock data fallback ───────────────────────────────────────────────

def _build_mock_data(quarter: str, index: str, sector: str | None) -> dict:
    """Return realistic mock data when FMP_API_KEY is not set."""
    import random

    random.seed(42)  # Deterministic for consistency

    symbols = [
        ("AAPL", "Apple Inc.", "Technology"),
        ("MSFT", "Microsoft Corp.", "Technology"),
        ("GOOGL", "Alphabet Inc.", "Communication Services"),
        ("AMZN", "Amazon.com Inc.", "Consumer Cyclical"),
        ("NVDA", "NVIDIA Corp.", "Technology"),
        ("META", "Meta Platforms Inc.", "Communication Services"),
        ("TSLA", "Tesla Inc.", "Consumer Cyclical"),
        ("JPM", "JPMorgan Chase & Co.", "Financials"),
        ("V", "Visa Inc.", "Financials"),
        ("JNJ", "Johnson & Johnson", "Healthcare"),
        ("UNH", "UnitedHealth Group", "Healthcare"),
        ("PG", "Procter & Gamble", "Consumer Defensive"),
        ("HD", "Home Depot Inc.", "Consumer Cyclical"),
        ("MA", "Mastercard Inc.", "Financials"),
        ("DIS", "Walt Disney Co.", "Communication Services"),
        ("NFLX", "Netflix Inc.", "Communication Services"),
        ("CRM", "Salesforce Inc.", "Technology"),
        ("COST", "Costco Wholesale", "Consumer Defensive"),
        ("PFE", "Pfizer Inc.", "Healthcare"),
        ("XOM", "Exxon Mobil Corp.", "Energy"),
        ("CVX", "Chevron Corp.", "Energy"),
        ("AVGO", "Broadcom Inc.", "Technology"),
        ("LLY", "Eli Lilly & Co.", "Healthcare"),
        ("ABBV", "AbbVie Inc.", "Healthcare"),
        ("MRK", "Merck & Co.", "Healthcare"),
        ("WMT", "Walmart Inc.", "Consumer Defensive"),
        ("BAC", "Bank of America", "Financials"),
        ("KO", "Coca-Cola Co.", "Consumer Defensive"),
        ("PEP", "PepsiCo Inc.", "Consumer Defensive"),
        ("TMO", "Thermo Fisher Scientific", "Healthcare"),
    ]

    if sector:
        symbols = [(s, n, sec) for s, n, sec in symbols if sec == sector]

    results = []
    for sym, name, sec in symbols:
        est_eps = round(random.uniform(1.0, 5.0), 2)
        surprise = random.choice(
            [0.05, 0.10, 0.15, -0.03, -0.08, 0.02, 0.20, -0.12, 0.08, 0.01]
        )
        actual_eps = round(est_eps * (1 + surprise), 2)

        est_rev = round(random.uniform(5, 80), 2) * 1e9
        rev_surprise = random.choice(
            [0.02, 0.05, -0.01, 0.03, -0.02, 0.08, -0.05, 0.01, 0.04, -0.03]
        )
        actual_rev = round(est_rev * (1 + rev_surprise))

        price_change = round(random.uniform(-8, 12), 2)
        guide = random.choice(["Raised", "Maintained", "Lowered", "—"])

        eps_beat = actual_eps > est_eps
        rev_beat = actual_rev > est_rev

        results.append(
            {
                "symbol": sym,
                "name": name,
                "date": "2026-02-15",
                "sector": sec,
                "actual_eps": actual_eps,
                "est_eps": est_eps,
                "eps_beat": eps_beat,
                "eps_surprise_pct": round(surprise * 100, 2),
                "actual_rev": actual_rev,
                "est_rev": est_rev,
                "rev_beat": rev_beat,
                "rev_surprise_pct": round(rev_surprise * 100, 2),
                "price_change": price_change,
                "guide": guide,
            }
        )

    metrics = _compute_metrics(results)
    metrics["quarter"] = quarter

    return {
        "metrics": metrics,
        "results": results,
        "quarter": quarter,
        "index": INDEX_CHOICES.get(index, index),
        "index_key": index,
        "sector": sector,
        "is_mock": True,
    }


def fetch_historical_beat_rates(index: str = "sp500") -> list[dict]:
    """Fetch beat rates for the last 8 quarters for trend chart.

    Returns list of dicts with keys: quarter, eps_beat_rate, rev_beat_rate,
    avg_price_change.
    """
    cache_key = f"history:{index}"

    with _lock:
        if cache_key in _earnings_cache:
            ts, data = _earnings_cache[cache_key]
            if time.time() - ts < _EARNINGS_TTL:
                return data

    quarters = get_available_quarters()
    trend = []

    for q_label in reversed(quarters):  # oldest first
        parsed = _parse_quarter_filter(q_label)
        if not parsed:
            continue
        y, q = parsed
        start, end = _quarter_date_range(y, q)

        surprises = _fmp_get("/earnings-surprises", {"from": start, "to": end})
        if surprises and isinstance(surprises, list):
            m = _compute_metrics(
                [
                    {
                        "eps_beat": (
                            s.get("actualEarningResult", 0)
                            > s.get("estimatedEarning", 0)
                            if s.get("actualEarningResult") is not None
                            and s.get("estimatedEarning") is not None
                            else None
                        ),
                        "rev_beat": (
                            s.get("actualRevenue", 0) > s.get("estimatedRevenue", 0)
                            if s.get("actualRevenue") is not None
                            and s.get("estimatedRevenue") is not None
                            else None
                        ),
                        "eps_surprise_pct": None,
                        "price_change": s.get("priceReaction"),
                    }
                    for s in surprises
                ]
            )
            trend.append(
                {
                    "quarter": q_label,
                    "eps_beat_rate": m["eps_beat_rate"],
                    "rev_beat_rate": m["rev_beat_rate"],
                    "avg_price_change": m["avg_price_change"],
                }
            )
        else:
            trend.append(_mock_quarter_trend(q_label))

    if not trend:
        trend = [_mock_quarter_trend(q) for q in reversed(quarters)]

    with _lock:
        _earnings_cache[cache_key] = (time.time(), trend)

    return trend


def _mock_quarter_trend(quarter: str) -> dict:
    """Generate a single mock quarter trend point."""
    import hashlib

    h = int(hashlib.md5(quarter.encode()).hexdigest(), 16)
    return {
        "quarter": quarter,
        "eps_beat_rate": 68 + (h % 20),
        "rev_beat_rate": 58 + (h % 22),
        "avg_price_change": round(-2 + (h % 800) / 100, 2),
    }
