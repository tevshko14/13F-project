"""Fetch analyst ratings from Finnhub and yfinance (free tier).

Both sources provide firm-level upgrade/downgrade data. Results are
merged, deduplicated, and cached in memory for 5 minutes.
"""

from __future__ import annotations

import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from filings.models import AnalystRating

# ── Thread lock + cache ──────────────────────────────────────────────
_lock = threading.Lock()
_cache: dict[str, tuple[float, list[AnalystRating]]] = {}
_CACHE_TTL = 300  # 5 minutes
_MAX_CACHE_ENTRIES = 2000


def _get_cached(ticker: str) -> list[AnalystRating] | None:
    key = ticker.upper()
    with _lock:
        if key in _cache:
            ts, data = _cache[key]
            if time.time() - ts < _CACHE_TTL:
                return data
    return None


def _set_cached(ticker: str, data: list[AnalystRating]) -> None:
    with _lock:
        _cache[ticker.upper()] = (time.time(), data)
        # Evict oldest if over limit
        if len(_cache) > _MAX_CACHE_ENTRIES:
            sorted_keys = sorted(_cache, key=lambda k: _cache[k][0])
            for k in sorted_keys[: len(_cache) - _MAX_CACHE_ENTRIES]:
                del _cache[k]


# ── Finnhub source ──────────────────────────────────────────────────


def _fetch_finnhub(ticker: str) -> list[AnalystRating]:
    """Fetch upgrade/downgrade data from Finnhub free API."""
    api_key = os.environ.get("FINNHUB_API_KEY", "")
    if not api_key:
        return []

    try:
        import finnhub

        client = finnhub.Client(api_key=api_key)
        data = client.upgrade_downgrade(symbol=ticker.upper())
    except Exception:
        return []

    ratings: list[AnalystRating] = []
    for item in data or []:
        action = (item.get("action") or "").lower()
        # Normalise action names
        action_map = {
            "up": "upgrade",
            "down": "downgrade",
            "main": "maintain",
            "init": "init",
            "reit": "reiterate",
        }
        action = action_map.get(action, action)

        grade_date = item.get("gradeTime", "")
        if grade_date:
            # Finnhub returns "YYYY-MM-DD HH:MM:SS" — keep date part
            grade_date = grade_date[:10]

        ratings.append(
            AnalystRating(
                firm=item.get("company", "Unknown"),
                action=action,
                from_grade=item.get("fromGrade", ""),
                to_grade=item.get("toGrade", ""),
                date=grade_date,
            )
        )

    return ratings


# ── yfinance source ─────────────────────────────────────────────────


def _fetch_yfinance(ticker: str) -> list[AnalystRating]:
    """Fetch upgrade/downgrade data from yfinance (free, no API key).

    Uses ``Ticker.upgrades_downgrades`` which returns per-firm ratings
    with columns: Firm, ToGrade, FromGrade, Action, and GradeDate index.
    """
    try:
        import yfinance as yf

        from filings.market_data import _yf_session
        tk = yf.Ticker(ticker.upper(), session=_yf_session)
        ud = tk.upgrades_downgrades
    except Exception:
        return []

    if ud is None or ud.empty:
        return []

    ratings: list[AnalystRating] = []
    for row in ud.itertuples():
        action_raw = str(getattr(row, "Action", "") or "").strip().lower()

        # Normalise action names
        action_map = {
            "up": "upgrade",
            "down": "downgrade",
            "main": "maintain",
            "init": "init",
            "reit": "reiterate",
        }
        action = action_map.get(action_raw, action_raw)

        # Extract date from GradeDate index (Timestamp)
        idx = row.Index
        if hasattr(idx, "strftime"):
            date_str = idx.strftime("%Y-%m-%d")
        else:
            date_str = str(idx)[:10]

        firm = str(getattr(row, "Firm", "Unknown") or "Unknown")
        to_grade = str(getattr(row, "ToGrade", "") or "")
        from_grade = str(getattr(row, "FromGrade", "") or "")

        ratings.append(
            AnalystRating(
                firm=firm,
                action=action,
                from_grade=from_grade if from_grade != "nan" else "",
                to_grade=to_grade if to_grade != "nan" else "",
                date=date_str,
            )
        )

    return ratings


# ── Merge & deduplicate ─────────────────────────────────────────────


def _normalize_firm(name: str) -> str:
    """Normalize firm name for dedup matching."""
    return name.lower().strip().replace(",", "").replace(".", "")


def _merge_ratings(
    finnhub_ratings: list[AnalystRating],
    yf_ratings: list[AnalystRating],
) -> list[AnalystRating]:
    """Merge ratings from both sources, deduplicating by firm + date."""
    seen: set[tuple[str, str]] = set()
    merged: list[AnalystRating] = []

    # Prefer Finnhub ratings (generally cleaner data)
    for r in finnhub_ratings:
        key = (_normalize_firm(r.firm), r.date)
        if key not in seen:
            seen.add(key)
            merged.append(r)

    # Add yfinance ratings that don't overlap
    for r in yf_ratings:
        key = (_normalize_firm(r.firm), r.date)
        if key not in seen:
            seen.add(key)
            merged.append(r)

    # Sort by date descending (most recent first)
    def sort_key(r: AnalystRating) -> str:
        return r.date or "0000-00-00"

    merged.sort(key=sort_key, reverse=True)

    return merged


# ── Public API ───────────────────────────────────────────────────────


def get_analyst_ratings(ticker: str) -> list[AnalystRating]:
    """Get merged analyst ratings for a ticker.

    Uses in-memory cache (5 min TTL). Fetches from Finnhub (if API key
    is set) and yfinance **in parallel**, then merges and deduplicates.
    """
    cached = _get_cached(ticker)
    if cached is not None:
        return cached

    with ThreadPoolExecutor(max_workers=2) as executor:
        f_finnhub = executor.submit(_fetch_finnhub, ticker)
        f_yf = executor.submit(_fetch_yfinance, ticker)
        try:
            finnhub_data = f_finnhub.result(timeout=15)
        except Exception:
            finnhub_data = []
        try:
            yf_data = f_yf.result(timeout=15)
        except Exception:
            yf_data = []

    merged = _merge_ratings(finnhub_data, yf_data)

    _set_cached(ticker, merged)
    return merged


def get_consensus_summary(ratings: list[AnalystRating]) -> dict[str, int]:
    """Compute a consensus summary from the most recent rating per firm.

    Returns dict with keys: buy, hold, sell, other, total
    """
    # Get the most recent rating per firm
    latest_by_firm: dict[str, AnalystRating] = {}
    for r in ratings:
        norm = _normalize_firm(r.firm)
        if norm not in latest_by_firm:
            latest_by_firm[norm] = r  # ratings are already sorted date-desc

    buy_grades = {
        "buy",
        "strong buy",
        "strongbuy",
        "outperform",
        "overweight",
        "positive",
        "accumulate",
        "sector outperform",
        "market outperform",
        "top pick",
        "conviction buy",
    }
    hold_grades = {
        "hold",
        "neutral",
        "equal-weight",
        "equal weight",
        "equalweight",
        "market perform",
        "sector perform",
        "in-line",
        "inline",
        "peer perform",
        "sector weight",
    }
    sell_grades = {
        "sell",
        "strong sell",
        "strongsell",
        "underperform",
        "underweight",
        "negative",
        "reduce",
        "sector underperform",
        "market underperform",
    }

    counts = {"buy": 0, "hold": 0, "sell": 0, "other": 0}
    for r in latest_by_firm.values():
        grade = (r.to_grade or "").lower().strip()
        if grade in buy_grades:
            counts["buy"] += 1
        elif grade in hold_grades:
            counts["hold"] += 1
        elif grade in sell_grades:
            counts["sell"] += 1
        else:
            counts["other"] += 1

    counts["total"] = sum(counts.values())
    return counts
