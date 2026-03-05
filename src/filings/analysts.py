"""Fetch analyst ratings from Finnhub and yfinance (free tier).

Both sources provide firm-level upgrade/downgrade data. Results are
merged, deduplicated, and cached in memory for 5 minutes.

Ratings are also persisted to Supabase (analyst_ratings table) with a
12-hour TTL — subsequent requests for the same ticker within that window
are served from DB without hitting external APIs. This eliminates rate
limiting issues and builds a history of analyst stance changes over time.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from filings.models import AnalystRating

logger = logging.getLogger(__name__)

# ── Thread lock + in-memory cache ────────────────────────────────────
_lock = threading.Lock()
_cache: dict[str, tuple[float, list[AnalystRating]]] = {}
_CACHE_TTL = 300  # 5 minutes
_MAX_CACHE_ENTRIES = 2000

# ── Supabase client (lazy-initialized) ───────────────────────────────
_sb_client = None
_sb_init_lock = threading.Lock()
_sb_initialized = False
_DB_TTL = 43200  # 12 hours — re-fetch from APIs if DB data is older


def _get_supabase_client():
    """Return a shared Supabase client (created once), or None if not configured."""
    global _sb_client, _sb_initialized
    if _sb_initialized:
        return _sb_client
    with _sb_init_lock:
        if _sb_initialized:
            return _sb_client
        _sb_initialized = True
        url = os.environ.get("SUPABASE_URL", "").strip()
        key = os.environ.get("SUPABASE_SERVICE_KEY", "").strip()
        if not url or not key:
            return None
        try:
            from supabase import create_client

            _sb_client = create_client(url, key)
        except Exception as exc:
            logger.warning("Supabase client init failed in analysts.py: %s", exc)
            _sb_client = None
    return _sb_client


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


# ── Supabase DB layer ────────────────────────────────────────────────


def _fetch_from_db(ticker: str) -> list[AnalystRating] | None:
    """Fetch analyst ratings from DB if they were fetched within _DB_TTL seconds.

    Returns:
        List of AnalystRating if fresh data exists in DB.
        None if no data, stale data, or DB is unavailable.
    """
    client = _get_supabase_client()
    if client is None:
        return None

    try:
        resp = (
            client.table("analyst_ratings")
            .select("*")
            .eq("ticker", ticker.upper())
            .order("grade_date", desc=True)
            .limit(500)
            .execute()
        )
        rows = resp.data or []
    except Exception as exc:
        logger.warning("DB fetch failed for %s: %s", ticker, exc)
        return None

    if not rows:
        return None

    # Check if any row was fetched recently enough
    now = datetime.now(timezone.utc)
    most_recent_fetch: datetime | None = None
    for row in rows:
        ft = row.get("fetched_at")
        if ft:
            try:
                ft_dt = datetime.fromisoformat(ft.replace("Z", "+00:00"))
                if most_recent_fetch is None or ft_dt > most_recent_fetch:
                    most_recent_fetch = ft_dt
            except Exception:
                pass

    if most_recent_fetch is None:
        return None

    age_seconds = (now - most_recent_fetch).total_seconds()
    if age_seconds > _DB_TTL:
        return None  # Stale — caller should re-fetch from APIs

    # Convert rows → AnalystRating objects
    ratings: list[AnalystRating] = []
    for row in rows:
        cpt = row.get("current_price_target")
        ppt = row.get("prior_price_target")
        ratings.append(
            AnalystRating(
                firm=row.get("firm") or "Unknown",
                action=row.get("action") or "",
                from_grade=row.get("from_grade") or "",
                to_grade=row.get("to_grade") or "",
                date=str(row.get("grade_date") or "")[:10],
                current_price_target=float(cpt) if cpt is not None else None,
                prior_price_target=float(ppt) if ppt is not None else None,
                price_target_action=row.get("price_target_action"),
                source=row.get("source") or "yfinance",
            )
        )

    logger.debug(
        "Served %d analyst ratings for %s from DB (age %.0fs)",
        len(ratings),
        ticker,
        age_seconds,
    )
    return ratings


def _upsert_to_db(ticker: str, ratings: list[AnalystRating]) -> None:
    """Upsert analyst ratings to Supabase. Runs in background thread."""
    client = _get_supabase_client()
    if client is None or not ratings:
        return

    now_iso = datetime.now(timezone.utc).isoformat()
    rows = []
    for r in ratings:
        if not r.date or not r.firm:
            continue
        rows.append(
            {
                "ticker": ticker.upper(),
                "firm": r.firm,
                "action": r.action or "",
                "from_grade": r.from_grade or "",
                "to_grade": r.to_grade or "",
                "grade_date": r.date,
                "current_price_target": r.current_price_target,
                "prior_price_target": r.prior_price_target,
                "price_target_action": r.price_target_action,
                "source": r.source,
                "fetched_at": now_iso,
            }
        )

    if not rows:
        return

    try:
        client.table("analyst_ratings").upsert(
            rows,
            on_conflict="ticker,firm,grade_date",
        ).execute()
        logger.debug("Upserted %d analyst ratings for %s to DB", len(rows), ticker)
    except Exception as exc:
        logger.warning("DB upsert failed for %s: %s", ticker, exc)


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
                source="finnhub",
            )
        )

    return ratings


# ── yfinance source ─────────────────────────────────────────────────


def _fetch_yfinance(ticker: str) -> list[AnalystRating]:
    """Fetch upgrade/downgrade data from yfinance (free, no API key).

    Uses ``Ticker.upgrades_downgrades`` which returns per-firm ratings
    with columns: Firm, ToGrade, FromGrade, Action, and GradeDate index.

    Note: deliberately does NOT share the market-data ``_yf_session`` — that
    session is used for price/info calls and can be rate-limited independently.
    yfinance 1.x manages its own curl_cffi session when none is provided.
    """
    try:
        import yfinance as yf

        tk = yf.Ticker(ticker.upper())
        ud = tk.upgrades_downgrades
    except Exception as e:
        logger.warning("yfinance upgrades_downgrades failed for %s: %s", ticker, e)
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

        # Price target fields (available in some yfinance versions)
        cpt_raw = getattr(row, "currentPriceTarget", None)
        ppt_raw = getattr(row, "priorPriceTarget", None)
        pta_raw = getattr(row, "priceTargetAction", None)

        def _safe_float(v) -> float | None:  # noqa: E306
            try:
                f = float(v)
                return f if f == f else None  # NaN check
            except (TypeError, ValueError):
                return None

        ratings.append(
            AnalystRating(
                firm=firm,
                action=action,
                from_grade=from_grade if from_grade != "nan" else "",
                to_grade=to_grade if to_grade != "nan" else "",
                date=date_str,
                current_price_target=_safe_float(cpt_raw),
                prior_price_target=_safe_float(ppt_raw),
                price_target_action=(
                    str(pta_raw).strip() if pta_raw and str(pta_raw) != "nan" else None
                ),
                source="yfinance",
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

    Priority:
    1. In-memory cache (5 min TTL) — fastest, avoids all I/O
    2. Supabase DB (12 hr TTL) — avoids hitting external APIs
    3. Live API fetch (Finnhub + yfinance in parallel) + async DB upsert
    """
    # 1. In-memory cache
    cached = _get_cached(ticker)
    if cached is not None:
        return cached

    # 2. DB cache
    db_ratings = _fetch_from_db(ticker)
    if db_ratings is not None:
        _set_cached(ticker, db_ratings)
        return db_ratings

    # 3. Live fetch from APIs
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

    # Upsert to DB in background so we don't delay the response
    threading.Thread(
        target=_upsert_to_db,
        args=(ticker, merged),
        daemon=True,
    ).start()

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
