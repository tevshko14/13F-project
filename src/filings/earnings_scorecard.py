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
from datetime import datetime, timedelta

import httpx

from filings.caching import TTLCache

logger = logging.getLogger(__name__)

# ── Cache ────────────────────────────────────────────────────────
_lock = threading.Lock()                          # guards _ecal_result_cache below
_cache = TTLCache(ttl=3600, max_size=100)         # L1 in-memory for scorecard payloads
_DB_TTL = 604_800  # 7 days (L2 Supabase)

# ── Constants ────────────────────────────────────────────────────
INDEX_CHOICES = {
    "all": "All Stocks",
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

CALENDAR_PERIODS = {
    "this_week": "This Week",
    "next_week": "Next Week",
    "next_2w": "Next 2 Weeks",
    "all": "All Upcoming",
}

_HOUR_LABELS = {"bmo": "Before Open", "amc": "After Close", "dmh": "During Market"}


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
    cached = _cache.get(cache_key)
    if cached is not None:
        return cached

    # L2: Supabase scorecard_cache
    from filings import supabase_cache

    db_hit = supabase_cache.get_scorecard_cache(cache_key, max_age_seconds=_DB_TTL)
    if db_hit is not None:
        _cache.set(cache_key, db_hit)
        return db_hit

    # L3 / L4: earnings_history table → FMP fallback
    data = _fetch_scorecard(index, quarter, sector)

    # Write back to L2 (skip mock / unavailable data)
    if not data.get("is_mock") and not data.get("is_unavailable"):
        supabase_cache.upsert_scorecard_cache(
            cache_key, index, data.get("quarter", ""), sector, data,
        )

    _cache.set(cache_key, data)
    return data


def build_company_lookup(index: str) -> dict[str, dict]:
    """Build {symbol: {name, sector}} from market_data constituents.

    For ``"all"`` index, merges both S&P 500 and NASDAQ 100 for display
    enrichment (company names / sectors).  Callers use the ``index`` value
    separately to decide whether to filter by this set.
    """
    try:
        from filings import market_data

        if index == "all":
            # Merge both indices so we have names/sectors for display
            # but return empty filtering set (tickers=None in caller)
            sp = market_data.get_sp500_constituents()
            nq = market_data.get_nasdaq100_constituents()
            merged = {
                r["ticker"]: {"name": r.get("name", ""), "sector": r.get("sector", "")}
                for r in sp
            }
            for r in nq:
                if r["ticker"] not in merged:
                    merged[r["ticker"]] = {"name": r.get("name", ""), "sector": r.get("sector", "")}
            return merged
        elif index == "nasdaq":
            rows = market_data.get_nasdaq100_constituents()
        else:
            rows = market_data.get_sp500_constituents()
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


def _fetch_eod_around_date(
    ticker: str, report_date: str,
) -> float | None:
    """Fetch Tiingo EOD data around an earnings date, return % price change.

    Computes close-to-close change: report-date close vs prior-day close.
    This captures both BMO (gap at open → close vs prior) and AMC reactions
    (since the report-date close already reflects same-day moves).
    """
    from filings.tiingo import _tiingo_get, has_tiingo_key

    if not has_tiingo_key():
        return None
    try:
        dt = datetime.strptime(report_date, "%Y-%m-%d")
        start = (dt - timedelta(days=5)).strftime("%Y-%m-%d")
        end = (dt + timedelta(days=3)).strftime("%Y-%m-%d")

        url = (
            f"https://api.tiingo.com/tiingo/daily/{ticker}/prices"
            f"?startDate={start}&endDate={end}"
        )
        eod = _tiingo_get(url)
        if not eod or not isinstance(eod, list) or len(eod) < 2:
            return None

        # Build date→close map
        by_date: dict[str, float] = {}
        for bar in eod:
            d = (bar.get("date") or "")[:10]
            close = bar.get("adjClose") or bar.get("close")
            if d and close:
                by_date[d] = float(close)

        sorted_dates = sorted(by_date.keys())

        # Find report date (or nearest prior trading day)
        report_idx = None
        for i, d in enumerate(sorted_dates):
            if d == report_date:
                report_idx = i
                break
            if d > report_date:
                report_idx = i - 1 if i > 0 else None
                break
        if report_idx is None:
            report_idx = len(sorted_dates) - 1

        if report_idx < 1:
            return None

        close_before = by_date[sorted_dates[report_idx - 1]]
        close_after = by_date[sorted_dates[report_idx]]
        if close_before > 0:
            return round(((close_after - close_before) / close_before) * 100, 2)
    except Exception:
        logger.debug("Price reaction fetch failed for %s on %s", ticker, report_date)
    return None


def _enrich_price_changes(results: list[dict]) -> list[dict]:
    """Populate ``price_change`` for each earnings result using Tiingo EOD.

    Runs Tiingo calls in parallel (8 workers). Gracefully skips tickers
    where price data is unavailable.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    # Collect tickers that need enrichment
    to_fetch: dict[str, str] = {}  # {symbol: date}
    for r in results:
        sym = r.get("symbol", "")
        dt = r.get("date", "")
        if sym and dt and r.get("price_change") is None:
            to_fetch[sym] = dt

    if not to_fetch:
        return results

    reactions: dict[str, float | None] = {}
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {
            pool.submit(_fetch_eod_around_date, tk, dt): tk
            for tk, dt in to_fetch.items()
        }
        for future in as_completed(futures):
            tk = futures[future]
            try:
                reactions[tk] = future.result()
            except Exception:
                reactions[tk] = None

    enriched = 0
    for r in results:
        sym = r.get("symbol", "")
        if sym in reactions and reactions[sym] is not None:
            r["price_change"] = reactions[sym]
            enriched += 1

    logger.info("Enriched %d/%d results with price reactions", enriched, len(to_fetch))
    return results


def _fetch_scorecard(
    index: str, quarter: str | None, sector: str | None,
) -> dict:
    """Primary scorecard fetch — tries earnings_history DB, then FMP."""
    quarter, start, end = _resolve_quarter(quarter)
    company_info = build_company_lookup(index)
    # For "all" index, don't filter — show every ticker in DB
    tickers = None if index == "all" else (set(company_info.keys()) or None)

    # ── L3: earnings_history table (primary source) ──────────
    results = _fetch_from_earnings_history(start, end, company_info, tickers, sector)

    # ── L4: FMP earning_calendar (fallback) ──────────────────
    if results is None:
        results = _fetch_from_fmp(start, end, company_info, sector)

    # ── L5: mock data (dev only) ─────────────────────────────
    if results is None:
        if not _api_key():
            return _build_mock_data(quarter, index, sector)
        return _build_empty_data(quarter, index, sector)

    # ── Enrich with stock price reactions ─────────────────────
    results = _enrich_price_changes(results)

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
    tickers: set[str] | None,
    sector: str | None,
) -> list[dict] | None:
    """Query the ``earnings_history`` Supabase table (populated by per-ticker sync).

    *tickers* limits results to the given set; pass ``None`` for all.
    """
    try:
        from filings import supabase_cache

        rows = supabase_cache.query_earnings_history(start, end)
        if rows is None:
            return None  # Supabase not configured or query failed
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
                "price_change": _safe_float(row.get("price_change")),
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
    """Fallback: FMP earnings-calendar via shared bulk cache."""
    from filings.fmp_cache import get_earnings_in_range, actual_eps as _aeps, actual_rev as _arev

    calendar = get_earnings_in_range(start, end)
    if not calendar:
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

        actual_eps = _aeps(item)
        est_eps = item.get("epsEstimated")
        actual_rev = _arev(item)
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
    # Only count companies that have revenue data for the revenue denominator
    rev_total = sum(1 for r in results if r["rev_beat"] is not None)
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
        "rev_total": rev_total,
        "rev_inline": rev_total - rev_beats - rev_misses,
        "rev_beat_rate": round(rev_beats / rev_total * 100, 1) if rev_total > 0 else 0,
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
                "symbol": ticker,
                "date": str(r.get("report_date", "")),
                "eps_beat": r.get("beat_eps"),
                "rev_beat": r.get("beat_revenue"),
                "eps_surprise_pct": _safe_float(r.get("eps_surprise_pct")),
                "price_change": _safe_float(r.get("price_change")),
            })
        return result if result else None
    except Exception:
        logger.exception("_trend_from_db failed")
        return None


def _trend_from_fmp(start: str, end: str) -> list[dict] | None:
    """Build per-quarter metric rows from FMP earnings-calendar (fallback)."""
    from filings.fmp_cache import get_earnings_in_range, actual_eps as _aeps, actual_rev as _arev

    calendar = get_earnings_in_range(start, end)
    if not calendar:
        return None

    rows = []
    for s in calendar:
        eps = _aeps(s)
        if eps is None:
            continue
        rev = _arev(s)
        est_eps = s.get("epsEstimated")
        est_rev = s.get("revenueEstimated")
        rows.append({
            "symbol": s.get("symbol", ""),
            "date": s.get("date", ""),
            "eps_beat": (
                eps > est_eps
                if est_eps is not None else None
            ),
            "rev_beat": (
                rev > est_rev
                if rev is not None and est_rev is not None else None
            ),
            "eps_surprise_pct": None,
            "price_change": None,
        })
    return rows if rows else None


# ── Historical trend data ────────────────────────────────────────

def fetch_historical_beat_rates(index: str = "sp500") -> list[dict]:
    """Beat rates for the last 8 quarters (for the trend chart).

    Cache tiers mirror ``fetch_earnings_data``:
      L1 in-memory → L2 scorecard_cache → L3 earnings_history → L4 FMP.
    """
    cache_key = f"history:{index}"

    # L1: in-memory
    cached = _cache.get(cache_key)
    if cached is not None:
        return cached

    # L2: Supabase scorecard_cache
    from filings import supabase_cache

    db_hit = supabase_cache.get_scorecard_cache(cache_key, max_age_seconds=_DB_TTL)
    if db_hit is not None:
        _cache.set(cache_key, db_hit)
        return db_hit

    # L3/L4: build trend from earnings_history (or FMP fallback)
    quarters = get_available_quarters()
    company_info = build_company_lookup(index)
    # For "all" index, don't filter by ticker set
    tickers = None if index == "all" else (set(company_info.keys()) if company_info else None)

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
            # Only enrich via Tiingo when a small number of rows lack
            # DB-sourced price_change; calling Tiingo for 1000+ tickers is too slow.
            missing_count = sum(1 for r in q_rows if r.get("price_change") is None)
            if 0 < missing_count < len(q_rows) // 2:
                q_rows = _enrich_price_changes(q_rows)
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

    _cache.set(cache_key, trend)
    return trend


# ── Revenue backfill ─────────────────────────────────────────────

def backfill_revenue(index: str = "sp500") -> dict:
    """Backfill revenue data using Finnhub bulk earnings calendar.

    Fetches the Finnhub ``/calendar/earnings`` bulk endpoint (last 7 weeks),
    then matches entries to ``earnings_history`` rows that have NULL revenue.
    Only 1-2 API calls needed — far faster than per-ticker fetching.

    Returns ``{updated: int, skipped: int, failed: int, total: int}``.
    """
    from filings.earnings import _load_finnhub_bulk_calendar, _enrich_rows_with_revenue
    from filings import supabase_cache

    company_info = build_company_lookup(index)
    tickers = sorted(company_info.keys())

    # ── Step 1: fetch bulk calendar from Finnhub ──────────────
    cal = _load_finnhub_bulk_calendar()
    if cal is None:
        logger.warning("backfill_revenue: Finnhub bulk calendar unavailable")
        return {"updated": 0, "skipped": 0, "failed": len(tickers), "total": len(tickers)}

    # Filter to index constituents
    index_tickers = set(tickers)
    matched_tickers = {sym for sym in cal if sym in index_tickers}
    logger.info(
        "backfill_revenue: Finnhub calendar has %d symbols, %d match %s",
        len(cal), len(matched_tickers), index,
    )

    # ── Step 2: update DB rows per matched ticker ─────────────
    updated = 0
    skipped = 0
    failed = 0
    client = supabase_cache._get_client()
    if client is None:
        return {"updated": 0, "skipped": 0, "failed": len(tickers), "total": len(tickers)}

    for i, ticker in enumerate(tickers):
        if i > 0 and i % 100 == 0:
            logger.info("backfill_revenue progress: %d/%d (updated=%d)", i, len(tickers), updated)

        rev_data = cal.get(ticker)
        if not rev_data:
            skipped += 1
            continue

        try:
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

            now_iso = datetime.now().isoformat()
            rows_to_update: list[dict] = []
            for row in db_rows:
                full_row = {
                    "ticker": ticker,
                    "report_date": row.get("report_date", ""),
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

            rows_to_update = _enrich_rows_with_revenue(rows_to_update, rev_data)
            enriched = [r for r in rows_to_update if r.get("revenue_actual") is not None]

            if enriched:
                supabase_cache.upsert_earnings_history(enriched)
                updated += len(enriched)
            else:
                skipped += 1

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


# ═══════════════════════════════════════════════════════════════════
# Earnings Calendar — upcoming/recent earnings dates
# ═══════════════════════════════════════════════════════════════════

_ecal_cache: dict | None = None  # {"entries": [...], "fetched_at": float}
_ecal_lock = threading.Lock()
_ECAL_TTL = 3600  # 1 hour — Finnhub data changes infrequently

# Outer cache for processed results (keyed by index:period)
_ecal_result_cache: dict[str, tuple[float, dict]] = {}
_ECAL_RESULT_TTL = 1800  # 30 minutes


def _fmt_rev(val: float | None) -> str:
    """Format revenue for display: $94.2B, $12.3M, etc."""
    if val is None:
        return ""
    abs_val = abs(val)
    sign = "-" if val < 0 else ""
    if abs_val >= 1e12:
        return f"{sign}${abs_val / 1e12:.1f}T"
    if abs_val >= 1e9:
        return f"{sign}${abs_val / 1e9:.1f}B"
    if abs_val >= 1e6:
        return f"{sign}${abs_val / 1e6:.1f}M"
    if abs_val >= 1e3:
        return f"{sign}${abs_val / 1e3:.0f}K"
    return f"{sign}${abs_val:.0f}"


_finnhub_symbols_cache: dict | None = None
_finnhub_symbols_ts: float = 0


def _load_finnhub_symbol_names() -> dict[str, str]:
    """Fetch Finnhub /stock/symbol?exchange=US → {symbol: description}.

    Cached for 24 hours.  Returns empty dict on failure.
    """
    global _finnhub_symbols_cache, _finnhub_symbols_ts

    now = time.time()
    if _finnhub_symbols_cache is not None and (now - _finnhub_symbols_ts) < 86400:
        return _finnhub_symbols_cache

    key = os.environ.get("FINNHUB_API_KEY", "").strip()
    if not key:
        return {}

    try:
        r = httpx.get(
            "https://finnhub.io/api/v1/stock/symbol",
            params={"exchange": "US", "token": key},
            timeout=30,
            follow_redirects=True,
        )
        r.raise_for_status()
        data = r.json()
        result = {}
        for item in data:
            sym = item.get("symbol", "")
            desc = item.get("description", "")
            if sym and desc:
                result[sym] = desc
        _finnhub_symbols_cache = result
        _finnhub_symbols_ts = now
        logger.info("Finnhub symbol names: %d tickers loaded", len(result))
        return result
    except Exception:
        logger.warning("Finnhub /stock/symbol fetch failed", exc_info=True)
        if _finnhub_symbols_cache is not None:
            return _finnhub_symbols_cache
        return {}


def _load_finnhub_earnings_calendar() -> list[dict] | None:
    """Fetch Finnhub /calendar/earnings for past 1 week + next 4 weeks.

    Finnhub caps each response at ~1500 entries.  A single 5-week query
    routinely hits that cap and silently drops the earliest week (so the
    current week's reports vanish mid-week — see CRWV/IREN repro).  We
    paginate by fetching in 1-week chunks (~200-400 entries each) and
    deduplicating on (symbol, date) before caching the merged list.

    Returns the merged entry list, or None on hard failure.  Cached
    in-memory for ``_ECAL_TTL`` seconds.
    """
    global _ecal_cache

    now = time.time()
    with _ecal_lock:
        if _ecal_cache is not None and (now - _ecal_cache["fetched_at"]) < _ECAL_TTL:
            return _ecal_cache["entries"]

    key = os.environ.get("FINNHUB_API_KEY", "").strip()
    if not key:
        return None

    today = datetime.now()
    earliest = today - timedelta(weeks=1)
    latest   = today + timedelta(weeks=4)

    # Build week-sized windows: [earliest, +6d], (+7d, +13d], … until we
    # cover `latest`.  Each request is independent + idempotent — we
    # tolerate a partial failure on any single window.
    windows: list[tuple[str, str]] = []
    cursor = earliest
    while cursor <= latest:
        end_of_week = min(cursor + timedelta(days=6), latest)
        windows.append((cursor.strftime("%Y-%m-%d"), end_of_week.strftime("%Y-%m-%d")))
        cursor = end_of_week + timedelta(days=1)

    seen: set[tuple[str, str]] = set()
    result: list[dict] = []
    failed = 0

    for w_start, w_end in windows:
        try:
            r = httpx.get(
                "https://finnhub.io/api/v1/calendar/earnings",
                params={"from": w_start, "to": w_end, "token": key},
                timeout=15,
            )
            r.raise_for_status()
            entries = r.json().get("earningsCalendar") or []
        except Exception as exc:
            failed += 1
            logger.warning("Finnhub %s..%s fetch failed: %s", w_start, w_end, exc)
            continue

        for item in entries:
            sym = item.get("symbol", "")
            d   = item.get("date", "")
            if not sym or not d:
                continue
            sig = (sym, d)
            if sig in seen:
                continue
            seen.add(sig)
            result.append({
                "symbol": sym,
                "date": d,
                "epsActual": item.get("epsActual"),
                "epsEstimate": item.get("epsEstimate"),
                "revenueActual": item.get("revenueActual"),
                "revenueEstimate": item.get("revenueEstimate"),
                "hour": item.get("hour", ""),
                "quarter": item.get("quarter"),
                "year": item.get("year"),
            })

    if not result and failed == len(windows):
        # Total wipeout — preserve last good cache rather than poison.
        with _ecal_lock:
            if _ecal_cache is not None:
                return _ecal_cache["entries"]
        return None

    with _ecal_lock:
        _ecal_cache = {"entries": result, "fetched_at": time.time()}

    logger.info(
        "Finnhub earnings calendar: %d entries across %d windows (%s..%s, failed=%d)",
        len(result), len(windows),
        windows[0][0] if windows else "?",
        windows[-1][1] if windows else "?",
        failed,
    )
    return result


def _calendar_date_range(period: str) -> tuple[str, str]:
    """Compute (start_date, end_date) for the given calendar period."""
    today = datetime.now()
    weekday = today.weekday()  # 0=Mon, 6=Sun

    # Find current week's Monday
    monday = today - timedelta(days=weekday)
    if weekday >= 5:  # Weekend — shift to next Monday
        monday = today + timedelta(days=(7 - weekday))

    if period == "this_week":
        start = monday
        end = monday + timedelta(days=4)  # Friday
    elif period == "next_week":
        start = monday + timedelta(weeks=1)
        end = start + timedelta(days=4)
    elif period == "next_2w":
        start = monday
        end = monday + timedelta(weeks=2, days=4)
    else:  # "all"
        start = today
        end = today + timedelta(weeks=4)

    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def fetch_earnings_calendar(
    index: str = "sp500",
    period: str = "this_week",
    si_tickers: set[str] | None = None,
) -> dict:
    """Fetch upcoming/recent earnings calendar, grouped by date.

    Args:
        index: ``"sp500"`` or ``"nasdaq"``
        period: ``"this_week"``, ``"next_week"``, ``"next_2w"``, or ``"all"``
        si_tickers: Set of tickers held by superinvestors (for badges)

    Returns dict with keys: upcoming, just_reported, metrics, index,
    index_key, period, period_label, is_mock.
    """
    if si_tickers is None:
        si_tickers = set()

    # ── L1 result cache ──────────────────────────────────────────
    cache_key = f"ecal:{index}:{period}"
    now = time.time()
    with _lock:
        cached = _ecal_result_cache.get(cache_key)
        if cached and (now - cached[0]) < _ECAL_RESULT_TTL:
            return cached[1]

    # ── Raw data ─────────────────────────────────────────────────
    raw = _load_finnhub_earnings_calendar()
    if raw is None:
        key = os.environ.get("FINNHUB_API_KEY", "").strip()
        if not key:
            result = _build_mock_calendar(index, period, si_tickers)
            with _lock:
                _ecal_result_cache[cache_key] = (now, result)
            return result
        return _build_empty_calendar(index, period)

    # ── Filter by index constituents (unless "all") ────────────
    # Always load company info for name resolution
    _all_company_info = build_company_lookup("sp500")
    _nasdaq_info = build_company_lookup("nasdaq")
    _all_company_info.update(_nasdaq_info)  # merge both

    # Finnhub symbol names for tickers not in any index
    _finnhub_names = _load_finnhub_symbol_names()

    if index == "all":
        company_info = None
        norm_lookup = None
    else:
        company_info = build_company_lookup(index)
        if not company_info:
            return _build_empty_calendar(index, period)
        # Normalize: Finnhub uses "." (e.g. BRK.B), our lookup uses "-"
        norm_lookup = {}
        for sym in company_info:
            norm_lookup[sym] = sym
            dot_ver = sym.replace("-", ".")
            if dot_ver != sym:
                norm_lookup[dot_ver] = sym

    # ── Date boundaries ──────────────────────────────────────────
    start_str, end_str = _calendar_date_range(period)
    today_str = datetime.now().strftime("%Y-%m-%d")

    # ── Split into upcoming vs just_reported ─────────────────────
    upcoming_by_date: dict[str, list[dict]] = {}
    just_reported: list[dict] = []

    for item in raw:
        finnhub_sym = item["symbol"]

        if norm_lookup is not None:
            # Index-filtered mode
            our_sym = norm_lookup.get(finnhub_sym)
            if our_sym is None:
                continue
            info = company_info.get(our_sym, {})
        else:
            # All-stocks mode — use Finnhub symbol directly
            our_sym = finnhub_sym.replace(".", "-")  # normalize to our format
            known = _all_company_info.get(our_sym, {})
            fh_name = _finnhub_names.get(finnhub_sym, "")
            info = {
                "name": known.get("name") or fh_name or item.get("name") or our_sym,
                "sector": known.get("sector", ""),
            }

        d = item["date"]
        is_si = our_sym in si_tickers

        entry = {
            "symbol": our_sym,
            "name": info.get("name", our_sym),
            "sector": info.get("sector", ""),
            "date": d,
            "hour": item.get("hour", ""),
            "hour_label": _HOUR_LABELS.get(item.get("hour", ""), "TBD"),
            "epsActual": item.get("epsActual"),
            "epsEstimate": item.get("epsEstimate"),
            "revenueActual": item.get("revenueActual"),
            "revenueEstimate": item.get("revenueEstimate"),
            "revenueEstimate_fmt": _fmt_rev(item.get("revenueEstimate")),
            "revenueActual_fmt": _fmt_rev(item.get("revenueActual")),
            "quarter": item.get("quarter"),
            "year": item.get("year"),
            "is_superinvestor": is_si,
            "si_names": [],  # filled in by web.py from ownership_map
            "has_page": True,
        }

        # Upcoming: within requested period and not yet reported
        if start_str <= d <= end_str and item.get("epsActual") is None:
            upcoming_by_date.setdefault(d, []).append(entry)
        # Just reported: past week, has actual data
        elif d < today_str and item.get("epsActual") is not None:
            entry["eps_beat"] = (
                entry["epsActual"] >= entry["epsEstimate"]
                if entry["epsActual"] is not None and entry["epsEstimate"] is not None
                else None
            )
            entry["rev_beat"] = (
                entry["revenueActual"] >= entry["revenueEstimate"]
                if entry["revenueActual"] is not None
                and entry["revenueEstimate"] is not None
                else None
            )
            just_reported.append(entry)

    # ── Group upcoming by date (sorted chronologically) ──────────
    upcoming: list[dict] = []
    for d in sorted(upcoming_by_date.keys()):
        try:
            dt = datetime.strptime(d, "%Y-%m-%d")
            label = dt.strftime("%b %d %Y")  # canonical "Mar 10 2026"
            dow   = dt.strftime("%a").upper()  # "MON" — used in day-tab kicker
        except ValueError:
            label = d
            dow   = ""
        # Sort entries within a day: by revenue estimate descending (market cap proxy)
        # Large-cap companies (higher revenue) appear first
        entries = sorted(
            upcoming_by_date[d],
            key=lambda e: (e.get("revenueEstimate") or 0),
            reverse=True,
        )
        upcoming.append({"date": d, "date_label": label, "dow": dow, "entries": entries})

    # Sort just_reported by date descending, then by revenue estimate descending
    just_reported.sort(
        key=lambda e: (e["date"], e.get("revenueActual") or e.get("revenueEstimate") or 0),
        reverse=True,
    )

    # Cap just_reported for "all" mode (can be huge)
    max_reported = 50 if index == "all" else 25

    # ── Summary metrics ──────────────────────────────────────────
    all_upcoming = [e for day in upcoming for e in day["entries"]]
    metrics = {
        "reporting_count": len(all_upcoming),
        "bmo_count": sum(1 for e in all_upcoming if e["hour"] == "bmo"),
        "amc_count": sum(1 for e in all_upcoming if e["hour"] == "amc"),
        "dmh_count": sum(
            1 for e in all_upcoming if e["hour"] not in ("bmo", "amc")
        ),
        "si_reporting_count": sum(
            1 for e in all_upcoming if e["is_superinvestor"]
        ),
    }

    result = {
        "upcoming": upcoming,
        "just_reported": just_reported[:max_reported],
        "metrics": metrics,
        "index": INDEX_CHOICES.get(index, index),
        "index_key": index,
        "period": period,
        "period_label": CALENDAR_PERIODS.get(period, period),
        "is_mock": False,
    }

    with _lock:
        _ecal_result_cache[cache_key] = (now, result)

    return result


def _build_empty_calendar(index: str, period: str) -> dict:
    """Empty calendar result when API is unavailable."""
    return {
        "upcoming": [],
        "just_reported": [],
        "metrics": {
            "reporting_count": 0, "bmo_count": 0,
            "amc_count": 0, "dmh_count": 0, "si_reporting_count": 0,
        },
        "index": INDEX_CHOICES.get(index, index),
        "index_key": index,
        "period": period,
        "period_label": CALENDAR_PERIODS.get(period, period),
        "is_mock": False,
        "is_unavailable": True,
    }


def _build_mock_calendar(
    index: str, period: str, si_tickers: set[str],
) -> dict:
    """Deterministic mock calendar for dev (no FINNHUB_API_KEY)."""
    rng = random.Random(42)
    company_info = build_company_lookup(index)
    symbols = sorted(company_info.keys())[:30]

    today = datetime.now()
    monday = today - timedelta(days=today.weekday())

    upcoming: list[dict] = []
    for day_offset in range(5):  # Mon-Fri
        d = monday + timedelta(days=day_offset)
        d_str = d.strftime("%Y-%m-%d")
        d_label = d.strftime("%b %d %Y")    # canonical MMM DD YYYY
        d_dow   = d.strftime("%a").upper()  # match the live path's `dow` field
        count = rng.randint(2, 6)
        chosen = rng.sample(symbols, min(count, len(symbols)))
        entries = []
        for sym in chosen:
            info = company_info.get(sym, {})
            hour = rng.choice(["bmo", "amc", "dmh"])
            entries.append({
                "symbol": sym,
                "name": info.get("name", sym),
                "sector": info.get("sector", ""),
                "date": d_str,
                "hour": hour,
                "hour_label": _HOUR_LABELS.get(hour, "TBD"),
                "epsEstimate": round(rng.uniform(0.5, 5.0), 2),
                "epsActual": None,
                "revenueEstimate": round(rng.uniform(1, 50), 1) * 1e9,
                "revenueEstimate_fmt": _fmt_rev(
                    round(rng.uniform(1, 50), 1) * 1e9
                ),
                "revenueActual": None,
                "revenueActual_fmt": "",
                "quarter": rng.randint(1, 4),
                "year": today.year,
                "is_superinvestor": sym in si_tickers,
                "si_names": [],
                "has_page": True,
            })
        upcoming.append({"date": d_str, "date_label": d_label, "dow": d_dow, "entries": entries})

    all_entries = [e for day in upcoming for e in day["entries"]]
    return {
        "upcoming": upcoming,
        "just_reported": [],
        "metrics": {
            "reporting_count": len(all_entries),
            "bmo_count": sum(1 for e in all_entries if e["hour"] == "bmo"),
            "amc_count": sum(1 for e in all_entries if e["hour"] == "amc"),
            "dmh_count": sum(
                1 for e in all_entries if e["hour"] not in ("bmo", "amc")
            ),
            "si_reporting_count": sum(
                1 for e in all_entries if e["is_superinvestor"]
            ),
        },
        "index": INDEX_CHOICES.get(index, index),
        "index_key": index,
        "period": period,
        "period_label": CALENDAR_PERIODS.get(period, period),
        "is_mock": True,
    }
