"""Macro Earnings Scorecard — aggregated earnings season metrics.

Fetches earnings surprises, beat/miss rates, and market reaction data
from Financial Modeling Prep (FMP) API.  Falls back to deterministic
mock data when ``FMP_API_KEY`` is not configured.

This module is independent of the per-ticker ``earnings.py`` module.
"""

from __future__ import annotations

import hashlib
import logging
import os
import random
import threading
import time
from datetime import datetime

import httpx

logger = logging.getLogger(__name__)

# ── Cache ────────────────────────────────────────────────────────
_lock = threading.Lock()
_cache: dict[str, tuple[float, object]] = {}
_TTL = 3600  # 1 hour (L1 in-memory)
_DB_TTL = 604_800  # 7 days (L2 Supabase)

_FMP_BASE = "https://financialmodelingprep.com/api/v3"
_TIMEOUT = 15

# ── Constants ────────────────────────────────────────────────────
INDEX_CHOICES = {
    "sp500": "S&P 500",
    "nasdaq": "NASDAQ 100",
}

SECTORS = [
    "Basic Materials",
    "Communication Services",
    "Consumer Cyclical",
    "Consumer Defensive",
    "Energy",
    "Financials",
    "Healthcare",
    "Industrials",
    "Real Estate",
    "Technology",
    "Utilities",
]


# ── Helpers ──────────────────────────────────────────────────────

def get_available_quarters() -> list[str]:
    """Return the last 8 fiscal quarters as ``'Q1 2024'`` strings."""
    now = datetime.now()
    y, q = now.year, (now.month - 1) // 3 + 1
    quarters: list[str] = []
    for _ in range(8):
        quarters.append(f"Q{q} {y}")
        q -= 1
        if q == 0:
            q, y = 4, y - 1
    return quarters


def _api_key() -> str:
    return os.environ.get("FMP_API_KEY", "")


def _fmp_get(path: str, params: dict | None = None) -> list | dict | None:
    key = _api_key()
    if not key:
        return None
    p = dict(params or {})
    p["apikey"] = key
    try:
        r = httpx.get(f"{_FMP_BASE}{path}", params=p, timeout=_TIMEOUT)
        r.raise_for_status()
        return r.json()
    except Exception:
        logger.exception("FMP API error: %s", path)
        return None


def _parse_quarter(label: str) -> tuple[int, int] | None:
    """Parse ``'Q1 2024'`` → ``(2024, 1)``."""
    try:
        parts = label.strip().split()
        return int(parts[1]), int(parts[0].replace("Q", ""))
    except Exception:
        return None


def _quarter_dates(year: int, q: int) -> tuple[str, str]:
    starts = {1: "01-01", 2: "04-01", 3: "07-01", 4: "10-01"}
    ends = {1: "03-31", 2: "06-30", 3: "09-30", 4: "12-31"}
    return f"{year}-{starts[q]}", f"{year}-{ends[q]}"


# ── Core data fetchers ───────────────────────────────────────────

def fetch_earnings_data(
    index: str = "sp500",
    quarter: str | None = None,
    sector: str | None = None,
) -> dict:
    """Fetch and aggregate earnings-season data.

    3-tier cache: L1 in-memory (1 h) → L2 Supabase (7 d) → L3 FMP API.
    """
    cache_key = f"{index}:{quarter or 'latest'}:{sector or 'all'}"

    # L1: in-memory
    with _lock:
        cached = _cache.get(cache_key)
        if cached and time.time() - cached[0] < _TTL:
            return cached[1]

    # L2: Supabase DB
    from filings import supabase_cache

    db_hit = supabase_cache.get_scorecard_cache(cache_key, max_age_seconds=_DB_TTL)
    if db_hit is not None:
        with _lock:
            _cache[cache_key] = (time.time(), db_hit)
        return db_hit

    # L3: FMP API
    data = _fetch_from_fmp(index, quarter, sector)

    # Write back to L2 (skip mock data)
    if not data.get("is_mock"):
        supabase_cache.upsert_scorecard_cache(
            cache_key, index, data.get("quarter", ""), sector, data,
        )

    with _lock:
        _cache[cache_key] = (time.time(), data)
    return data


def _fetch_from_fmp(
    index: str, quarter: str | None, sector: str | None,
) -> dict:
    # Resolve quarter → date range
    if quarter:
        parsed = _parse_quarter(quarter)
        if parsed:
            y, q = parsed
            start, end = _quarter_dates(y, q)
        else:
            quarter = None

    if not quarter:
        now = datetime.now()
        q = (now.month - 1) // 3 + 1
        start, end = _quarter_dates(now.year, q)
        quarter = f"Q{q} {now.year}"

    surprises = _fmp_get("/earnings-surprises", {"from": start, "to": end})
    if not surprises or not isinstance(surprises, list):
        # Only show mock data in dev (no API key). In production (key set
        # but API down), return an empty result so users never see fake data.
        if not _api_key():
            return _build_mock_data(quarter, index, sector)
        return _build_empty_data(quarter, index, sector)

    constituents = _get_index_constituents(index)

    results: list[dict] = []
    for item in surprises:
        symbol = item.get("symbol", "")
        if constituents and symbol not in constituents:
            continue
        item_sector = item.get("sector", "")
        if sector and item_sector and item_sector != sector:
            continue

        actual_eps = item.get("actualEarningResult")
        est_eps = item.get("estimatedEarning")
        actual_rev = item.get("actualRevenue")
        est_rev = item.get("estimatedRevenue")

        eps_beat = (
            actual_eps > est_eps
            if actual_eps is not None and est_eps is not None
            else None
        )
        rev_beat = (
            actual_rev > est_rev
            if actual_rev is not None and est_rev is not None
            else None
        )
        eps_surprise_pct = (
            round(((actual_eps - est_eps) / abs(est_eps)) * 100, 2)
            if actual_eps is not None and est_eps and est_eps != 0
            else None
        )
        rev_surprise_pct = (
            round(((actual_rev - est_rev) / abs(est_rev)) * 100, 2)
            if actual_rev is not None and est_rev and est_rev != 0
            else None
        )

        results.append({
            "symbol": symbol,
            "name": item.get("companyName", symbol),
            "date": item.get("date", ""),
            "sector": item_sector,
            "actual_eps": actual_eps,
            "est_eps": est_eps,
            "eps_beat": eps_beat,
            "eps_surprise_pct": eps_surprise_pct,
            "rev_beat": rev_beat,
            "rev_surprise_pct": rev_surprise_pct,
            "price_change": item.get("priceReaction"),
            "guide": item.get("guidance") or "—",
        })

    results.sort(key=lambda r: r["date"] or "", reverse=True)
    metrics = _compute_metrics(results)

    return {
        "metrics": metrics,
        "results": results,
        "quarter": quarter,
        "index": INDEX_CHOICES.get(index, index),
        "index_key": index,
        "sector": sector,
    }


def _get_index_constituents(index: str) -> set[str] | None:
    if index == "sp500":
        data = _fmp_get("/sp500_constituent")
    elif index == "nasdaq":
        data = _fmp_get("/nasdaq_constituent")
    else:
        return None
    if data and isinstance(data, list):
        return {item.get("symbol", "") for item in data}
    return None


def _compute_metrics(results: list[dict]) -> dict:
    total = len(results)
    if total == 0:
        return {
            "total": 0, "eps_beats": 0, "eps_misses": 0, "eps_inline": 0,
            "eps_beat_rate": 0, "rev_beats": 0, "rev_misses": 0,
            "rev_inline": 0, "rev_beat_rate": 0, "dual_beats": 0,
            "avg_price_change": 0, "avg_eps_surprise": 0,
        }

    eps_beats = sum(1 for r in results if r["eps_beat"] is True)
    eps_misses = sum(1 for r in results if r["eps_beat"] is False)
    rev_beats = sum(1 for r in results if r["rev_beat"] is True)
    rev_misses = sum(1 for r in results if r["rev_beat"] is False)
    dual_beats = sum(
        1 for r in results
        if r["eps_beat"] is True and r["rev_beat"] is True
    )
    prices = [r["price_change"] for r in results if r["price_change"] is not None]
    eps_surp = [r["eps_surprise_pct"] for r in results if r["eps_surprise_pct"] is not None]

    return {
        "total": total,
        "eps_beats": eps_beats,
        "eps_misses": eps_misses,
        "eps_inline": total - eps_beats - eps_misses,
        "eps_beat_rate": round(eps_beats / total * 100, 1),
        "rev_beats": rev_beats,
        "rev_misses": rev_misses,
        "rev_inline": total - rev_beats - rev_misses,
        "rev_beat_rate": round(rev_beats / total * 100, 1),
        "dual_beats": dual_beats,
        "avg_price_change": round(sum(prices) / len(prices), 2) if prices else 0,
        "avg_eps_surprise": round(sum(eps_surp) / len(eps_surp), 2) if eps_surp else 0,
    }


# ── Historical trend data ────────────────────────────────────────

def fetch_historical_beat_rates(index: str = "sp500") -> list[dict]:
    """Beat rates for the last 8 quarters (for the trend chart).

    3-tier cache: L1 in-memory (1 h) → L2 Supabase (7 d) → L3 FMP API.
    """
    cache_key = f"history:{index}"

    # L1: in-memory
    with _lock:
        cached = _cache.get(cache_key)
        if cached and time.time() - cached[0] < _TTL:
            return cached[1]

    # L2: Supabase DB
    from filings import supabase_cache

    db_hit = supabase_cache.get_scorecard_cache(cache_key, max_age_seconds=_DB_TTL)
    if db_hit is not None:
        with _lock:
            _cache[cache_key] = (time.time(), db_hit)
        return db_hit

    # L3: FMP API
    quarters = get_available_quarters()
    trend: list[dict] = []
    has_real_data = False

    for q_label in reversed(quarters):  # oldest first
        parsed = _parse_quarter(q_label)
        if not parsed:
            continue
        y, q = parsed
        start, end = _quarter_dates(y, q)

        surprises = _fmp_get("/earnings-surprises", {"from": start, "to": end})
        if surprises and isinstance(surprises, list):
            has_real_data = True
            rows = [
                {
                    "eps_beat": (
                        s.get("actualEarningResult", 0) > s.get("estimatedEarning", 0)
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
            m = _compute_metrics(rows)
            trend.append({
                "quarter": q_label,
                "eps_beat_rate": m["eps_beat_rate"],
                "rev_beat_rate": m["rev_beat_rate"],
                "avg_price_change": m["avg_price_change"],
            })
        else:
            # Only use mock quarter data in dev (no API key)
            if not _api_key():
                trend.append(_mock_quarter_trend(q_label))

    if not trend and not _api_key():
        trend = [_mock_quarter_trend(q) for q in reversed(quarters)]

    # Write back to L2 (skip if all mock data)
    if has_real_data:
        supabase_cache.upsert_scorecard_cache(
            cache_key, index, "multi", None, trend,
        )

    with _lock:
        _cache[cache_key] = (time.time(), trend)
    return trend


# ── Mock data fallback ───────────────────────────────────────────

_MOCK_COMPANIES_SP500 = [
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

_MOCK_COMPANIES_NASDAQ = [
    ("AAPL", "Apple Inc.", "Technology"),
    ("MSFT", "Microsoft Corp.", "Technology"),
    ("GOOGL", "Alphabet Inc.", "Communication Services"),
    ("AMZN", "Amazon.com Inc.", "Consumer Cyclical"),
    ("NVDA", "NVIDIA Corp.", "Technology"),
    ("META", "Meta Platforms Inc.", "Communication Services"),
    ("TSLA", "Tesla Inc.", "Consumer Cyclical"),
    ("AVGO", "Broadcom Inc.", "Technology"),
    ("NFLX", "Netflix Inc.", "Communication Services"),
    ("CRM", "Salesforce Inc.", "Technology"),
    ("COST", "Costco Wholesale", "Consumer Defensive"),
    ("AMD", "Advanced Micro Devices", "Technology"),
    ("INTC", "Intel Corp.", "Technology"),
    ("ADBE", "Adobe Inc.", "Technology"),
    ("QCOM", "Qualcomm Inc.", "Technology"),
    ("INTU", "Intuit Inc.", "Technology"),
    ("ISRG", "Intuitive Surgical", "Healthcare"),
    ("REGN", "Regeneron Pharmaceuticals", "Healthcare"),
    ("BKNG", "Booking Holdings", "Consumer Cyclical"),
    ("PANW", "Palo Alto Networks", "Technology"),
    ("SNPS", "Synopsys Inc.", "Technology"),
    ("CDNS", "Cadence Design Systems", "Technology"),
    ("MELI", "MercadoLibre Inc.", "Consumer Cyclical"),
    ("LRCX", "Lam Research", "Technology"),
    ("KLAC", "KLA Corp.", "Technology"),
]

_MOCK_COMPANIES: dict[str, list[tuple[str, str, str]]] = {
    "sp500": _MOCK_COMPANIES_SP500,
    "nasdaq": _MOCK_COMPANIES_NASDAQ,
}


def _build_empty_data(quarter: str, index: str, sector: str | None) -> dict:
    """Return a valid but empty result for when the API is unavailable.

    Used in production (API key set) when FMP is temporarily down, so
    users never see fabricated numbers.
    """
    return {
        "metrics": _compute_metrics([]),
        "results": [],
        "quarter": quarter,
        "index": INDEX_CHOICES.get(index, index),
        "index_key": index,
        "sector": sector,
        "is_unavailable": True,
    }


def _build_mock_data(quarter: str, index: str, sector: str | None) -> dict:
    rng = random.Random(hash((42, index)))  # deterministic, varies by index

    all_companies = _MOCK_COMPANIES.get(index, _MOCK_COMPANIES_SP500)
    symbols = (
        [(s, n, sec) for s, n, sec in all_companies if sec == sector]
        if sector
        else list(all_companies)
    )

    results: list[dict] = []
    for sym, name, sec in symbols:
        est_eps = round(rng.uniform(1.0, 5.0), 2)
        surprise = rng.choice([0.05, 0.10, 0.15, -0.03, -0.08, 0.02, 0.20, -0.12, 0.08, 0.01])
        actual_eps = round(est_eps * (1 + surprise), 2)
        est_rev = round(rng.uniform(5, 80), 2) * 1e9
        rev_surp = rng.choice([0.02, 0.05, -0.01, 0.03, -0.02, 0.08, -0.05, 0.01, 0.04, -0.03])
        actual_rev = round(est_rev * (1 + rev_surp))

        results.append({
            "symbol": sym, "name": name, "date": "2026-02-15", "sector": sec,
            "actual_eps": actual_eps, "est_eps": est_eps,
            "eps_beat": actual_eps > est_eps,
            "eps_surprise_pct": round(surprise * 100, 2),
            "rev_beat": actual_rev > est_rev,
            "rev_surprise_pct": round(rev_surp * 100, 2),
            "price_change": round(rng.uniform(-8, 12), 2),
            "guide": rng.choice(["Raised", "Maintained", "Lowered", "—"]),
        })

    metrics = _compute_metrics(results)
    return {
        "metrics": metrics, "results": results, "quarter": quarter,
        "index": INDEX_CHOICES.get(index, index), "index_key": index,
        "sector": sector, "is_mock": True,
    }


def _mock_quarter_trend(quarter: str) -> dict:
    h = int(hashlib.md5(quarter.encode()).hexdigest(), 16)
    return {
        "quarter": quarter,
        "eps_beat_rate": 68 + (h % 20),
        "rev_beat_rate": 58 + (h % 22),
        "avg_price_change": round(-2 + (h % 800) / 100, 2),
    }
