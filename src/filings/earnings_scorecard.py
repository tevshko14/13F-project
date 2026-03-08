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
        url = f"{_FMP_BASE}{path}"
        logger.info("FMP request: %s params=%s", url, {k: v for k, v in p.items() if k != "apikey"})
        r = httpx.get(url, params=p, timeout=_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list):
            logger.info("FMP %s returned %d items", path, len(data))
        elif isinstance(data, dict):
            # FMP returns {"Error Message": "..."} when plan lacks access
            err_msg = data.get("Error Message") or data.get("message") or data.get("error")
            if err_msg:
                logger.error("FMP %s plan/access error: %s", path, err_msg)
            else:
                logger.warning("FMP %s returned dict (expected list): %s", path, str(data)[:200])
        else:
            logger.warning("FMP %s returned unexpected type: %s", path, str(data)[:200])
        return data
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

    Cache tiers:
      L1  in-memory (1 h)
      L2  scorecard_cache table (7 d)
      L3  earnings_history table (primary source — populated by per-ticker sync)
      L4  FMP earning_calendar API (fallback)
      L5  deterministic mock data (dev-only, when no API key)
    """
    cache_key = f"{index}:{quarter or 'latest'}:{sector or 'all'}"

    # L1: in-memory
    with _lock:
        cached = _cache.get(cache_key)
        if cached and time.time() - cached[0] < _TTL:
            return cached[1]

    # L2: Supabase scorecard_cache
    from filings import supabase_cache

    db_hit = supabase_cache.get_scorecard_cache(cache_key, max_age_seconds=_DB_TTL)
    if db_hit is not None:
        with _lock:
            _cache[cache_key] = (time.time(), db_hit)
        return db_hit

    # L3 / L4: earnings_history table → FMP fallback
    data = _fetch_scorecard(index, quarter, sector)

    # Write back to L2 (skip mock / unavailable data)
    if not data.get("is_mock") and not data.get("is_unavailable"):
        supabase_cache.upsert_scorecard_cache(
            cache_key, index, data.get("quarter", ""), sector, data,
        )

    with _lock:
        _cache[cache_key] = (time.time(), data)
    return data


def _build_company_lookup(index: str) -> dict[str, dict]:
    """Build {symbol: {name, sector}} from market_data constituents."""
    try:
        from filings import market_data

        if index == "sp500":
            rows = market_data.get_sp500_constituents()
        else:
            rows = market_data.get_sp500_constituents()  # fallback
        return {
            r["ticker"]: {"name": r.get("name", ""), "sector": r.get("sector", "")}
            for r in rows
        }
    except Exception:
        return {}


def _resolve_quarter(quarter: str | None) -> tuple[str, str, str]:
    """Return ``(quarter_label, start_date, end_date)``."""
    if quarter:
        parsed = _parse_quarter(quarter)
        if parsed:
            y, q = parsed
            start, end = _quarter_dates(y, q)
            return quarter, start, end

    now = datetime.now()
    q = (now.month - 1) // 3 + 1
    start, end = _quarter_dates(now.year, q)
    return f"Q{q} {now.year}", start, end


def _fetch_scorecard(
    index: str, quarter: str | None, sector: str | None,
) -> dict:
    """Primary scorecard fetch — tries earnings_history DB, then FMP."""
    quarter, start, end = _resolve_quarter(quarter)
    company_info = _build_company_lookup(index)

    # ── L3: earnings_history table (primary source) ──────────
    results = _fetch_from_earnings_history(start, end, company_info, sector)

    # ── L4: FMP earning_calendar (fallback) ──────────────────
    if results is None:
        results = _fetch_from_fmp(start, end, company_info, sector)

    # ── L5: mock data (dev only) ─────────────────────────────
    if results is None:
        if not _api_key():
            return _build_mock_data(quarter, index, sector)
        return _build_empty_data(quarter, index, sector)

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


def _fetch_from_earnings_history(
    start: str, end: str,
    company_info: dict[str, dict],
    sector: str | None,
) -> list[dict] | None:
    """Query the ``earnings_history`` Supabase table (populated by per-ticker sync)."""
    try:
        from filings import supabase_cache

        rows = supabase_cache.query_earnings_history(start, end)
        if rows is None:
            return None  # Supabase not configured or query failed

        tickers = set(company_info.keys()) if company_info else None
        results: list[dict] = []

        for row in rows:
            ticker = row.get("ticker", "")
            if tickers and ticker not in tickers:
                continue

            info = company_info.get(ticker, {})
            item_sector = info.get("sector", "")
            if sector and item_sector and item_sector != sector:
                continue

            actual_eps = _safe_float(row.get("eps_actual"))
            est_eps = _safe_float(row.get("eps_estimate"))
            actual_rev = _safe_float(row.get("revenue_actual"))
            est_rev = _safe_float(row.get("revenue_estimate"))

            # Skip rows without any actuals
            if actual_eps is None:
                continue

            eps_beat = row.get("beat_eps")
            rev_beat = row.get("beat_revenue")
            eps_surprise_pct = _safe_float(row.get("eps_surprise_pct"))
            rev_surprise_pct = _safe_float(row.get("revenue_surprise_pct"))

            results.append({
                "symbol": ticker,
                "name": info.get("name") or ticker,
                "date": str(row.get("report_date", "")),
                "sector": item_sector,
                "actual_eps": actual_eps,
                "est_eps": est_eps,
                "eps_beat": eps_beat,
                "eps_surprise_pct": round(eps_surprise_pct, 2) if eps_surprise_pct is not None else None,
                "rev_beat": rev_beat,
                "rev_surprise_pct": round(rev_surprise_pct, 2) if rev_surprise_pct is not None else None,
                "price_change": None,
                "guide": "—",
            })

        logger.info("earnings_history returned %d rows for %s–%s", len(results), start, end)
        return results
    except Exception:
        logger.exception("_fetch_from_earnings_history failed")
        return None


def _fetch_from_fmp(
    start: str, end: str,
    company_info: dict[str, dict],
    sector: str | None,
) -> list[dict] | None:
    """Fallback: FMP ``/earning_calendar`` bulk endpoint (premium)."""
    calendar = _fmp_get("/earning_calendar", {"from": start, "to": end})
    if calendar is None or not isinstance(calendar, list):
        return None

    tickers = set(company_info.keys()) if company_info else None
    results: list[dict] = []

    for item in calendar:
        symbol = item.get("symbol", "")
        if tickers and symbol not in tickers:
            continue

        info = company_info.get(symbol, {})
        item_sector = info.get("sector", "")
        if sector and item_sector and item_sector != sector:
            continue

        actual_eps = item.get("eps")
        est_eps = item.get("epsEstimated")
        actual_rev = item.get("revenue")
        est_rev = item.get("revenueEstimated")

        if actual_eps is None and actual_rev is None:
            continue

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
            "name": info.get("name") or symbol,
            "date": item.get("date", ""),
            "sector": item_sector,
            "actual_eps": actual_eps,
            "est_eps": est_eps,
            "eps_beat": eps_beat,
            "eps_surprise_pct": eps_surprise_pct,
            "rev_beat": rev_beat,
            "rev_surprise_pct": rev_surprise_pct,
            "price_change": None,
            "guide": "—",
        })

    logger.info("FMP earning_calendar returned %d rows for %s–%s", len(results), start, end)
    return results


def _safe_float(val) -> float | None:
    """Convert a value to float, returning None for anything invalid."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
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


# ── Historical trend helpers ─────────────────────────────────────

def _trend_from_db(
    start: str, end: str, tickers: set[str] | None, supabase_cache,
) -> list[dict] | None:
    """Build per-quarter metric rows from earnings_history."""
    try:
        rows = supabase_cache.query_earnings_history(start, end)
        if rows is None:
            return None

        result = []
        for r in rows:
            ticker = r.get("ticker", "")
            if tickers and ticker not in tickers:
                continue
            if r.get("eps_actual") is None:
                continue
            result.append({
                "eps_beat": r.get("beat_eps"),
                "rev_beat": r.get("beat_revenue"),
                "eps_surprise_pct": _safe_float(r.get("eps_surprise_pct")),
                "price_change": None,
            })
        return result if result else None
    except Exception:
        logger.exception("_trend_from_db failed")
        return None


def _trend_from_fmp(start: str, end: str) -> list[dict] | None:
    """Build per-quarter metric rows from FMP earning_calendar (fallback)."""
    calendar = _fmp_get("/earning_calendar", {"from": start, "to": end})
    if calendar is None or not isinstance(calendar, list):
        return None

    rows = [
        {
            "eps_beat": (
                s.get("eps", 0) > s.get("epsEstimated", 0)
                if s.get("eps") is not None
                and s.get("epsEstimated") is not None
                else None
            ),
            "rev_beat": (
                s.get("revenue", 0) > s.get("revenueEstimated", 0)
                if s.get("revenue") is not None
                and s.get("revenueEstimated") is not None
                else None
            ),
            "eps_surprise_pct": None,
            "price_change": None,
        }
        for s in calendar
        if s.get("eps") is not None
    ]
    return rows if rows else None


# ── Historical trend data ────────────────────────────────────────

def fetch_historical_beat_rates(index: str = "sp500") -> list[dict]:
    """Beat rates for the last 8 quarters (for the trend chart).

    Cache tiers mirror ``fetch_earnings_data``:
      L1 in-memory → L2 scorecard_cache → L3 earnings_history → L4 FMP.
    """
    cache_key = f"history:{index}"

    # L1: in-memory
    with _lock:
        cached = _cache.get(cache_key)
        if cached and time.time() - cached[0] < _TTL:
            return cached[1]

    # L2: Supabase scorecard_cache
    from filings import supabase_cache

    db_hit = supabase_cache.get_scorecard_cache(cache_key, max_age_seconds=_DB_TTL)
    if db_hit is not None:
        with _lock:
            _cache[cache_key] = (time.time(), db_hit)
        return db_hit

    # L3/L4: build trend from earnings_history (or FMP fallback)
    quarters = get_available_quarters()
    company_info = _build_company_lookup(index)
    tickers = set(company_info.keys()) if company_info else None

    trend: list[dict] = []
    has_real_data = False

    for q_label in reversed(quarters):  # oldest first
        parsed = _parse_quarter(q_label)
        if not parsed:
            continue
        y, q = parsed
        start, end = _quarter_dates(y, q)

        # Try earnings_history first
        q_rows = _trend_from_db(start, end, tickers, supabase_cache)

        # Fallback to FMP
        if q_rows is None:
            q_rows = _trend_from_fmp(start, end)

        if q_rows is not None:
            has_real_data = True
            m = _compute_metrics(q_rows)
            trend.append({
                "quarter": q_label,
                "eps_beat_rate": m["eps_beat_rate"],
                "rev_beat_rate": m["rev_beat_rate"],
                "avg_price_change": m["avg_price_change"],
            })
        elif not _api_key():
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


# ── Revenue backfill ─────────────────────────────────────────────

def backfill_revenue(index: str = "sp500") -> dict:
    """Backfill revenue data for index constituents via FMP per-symbol API.

    Iterates over S&P 500 / NASDAQ 100 tickers, fetches FMP historical
    revenue data, and updates ``earnings_history`` rows that have NULL
    revenue columns.

    Returns ``{updated: int, skipped: int, failed: int, total: int}``.
    """
    from filings.earnings import _fetch_fmp_revenue, _enrich_rows_with_revenue
    from filings import supabase_cache

    company_info = _build_company_lookup(index)
    tickers = sorted(company_info.keys())
    updated = 0
    skipped = 0
    failed = 0

    for i, ticker in enumerate(tickers):
        if i > 0 and i % 50 == 0:
            logger.info("backfill_revenue progress: %d/%d tickers (updated=%d)", i, len(tickers), updated)

        try:
            # Get existing rows for this ticker that lack revenue
            client = supabase_cache._get_client()
            if client is None:
                failed += len(tickers) - i
                break

            resp = (
                client.table("earnings_history")
                .select("ticker,report_date,fiscal_quarter,eps_estimate,eps_actual,"
                        "eps_surprise_pct,beat_eps,updated_at")
                .eq("ticker", ticker)
                .is_("revenue_actual", "null")
                .order("report_date", desc=True)
                .limit(20)
                .execute()
            )
            db_rows = resp.data
            if not db_rows:
                skipped += 1
                continue

            # Fetch FMP revenue data for this ticker
            fmp_data = _fetch_fmp_revenue(ticker)
            if fmp_data is None:
                # FMP unavailable (plan issue) — stop trying
                failed += len(tickers) - i
                logger.warning("backfill_revenue: FMP unavailable, stopping at ticker %d/%d", i, len(tickers))
                break

            # Build complete rows with revenue merged in
            now_iso = datetime.utcnow().isoformat()
            rows_to_update: list[dict] = []
            for row in db_rows:
                rd = row.get("report_date", "")
                # Construct a full row for upsert
                full_row = {
                    "ticker": ticker,
                    "report_date": rd,
                    "fiscal_quarter": row.get("fiscal_quarter", ""),
                    "eps_estimate": row.get("eps_estimate"),
                    "eps_actual": row.get("eps_actual"),
                    "eps_surprise_pct": row.get("eps_surprise_pct"),
                    "revenue_estimate": None,
                    "revenue_actual": None,
                    "revenue_surprise_pct": None,
                    "beat_eps": row.get("beat_eps"),
                    "beat_revenue": None,
                    "updated_at": now_iso,
                }
                rows_to_update.append(full_row)

            # Enrich with FMP revenue
            rows_to_update = _enrich_rows_with_revenue(rows_to_update, fmp_data)

            # Only upsert rows that actually got revenue data
            enriched = [r for r in rows_to_update if r.get("revenue_actual") is not None]
            if enriched:
                supabase_cache.upsert_earnings_history(enriched)
                updated += len(enriched)
            else:
                skipped += 1

            # Throttle: 200ms between tickers to respect FMP rate limits
            time.sleep(0.2)

        except Exception:
            logger.exception("backfill_revenue failed for %s", ticker)
            failed += 1

    logger.info(
        "backfill_revenue complete: updated=%d skipped=%d failed=%d total=%d",
        updated, skipped, failed, len(tickers),
    )
    return {"updated": updated, "skipped": skipped, "failed": failed, "total": len(tickers)}


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
