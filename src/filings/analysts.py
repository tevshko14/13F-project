"""Fetch analyst ratings from TipRanks, Finnhub, and yfinance.

Data tier priority (per ticker):
  1. In-memory cache (5 min TTL)
  2. Supabase analyst_stock_ratings (TipRanks individual, 12 hr TTL) → "individual" view
  3. Live TipRanks fetch → upsert to DB → "individual" view
  4. Supabase analyst_ratings (firm-level, 12 hr TTL) → "firm" view
  5. Live yfinance + Finnhub fetch → upsert to analyst_ratings → "firm" view

The `get_analyst_ratings_with_profiles()` function drives the stock page
Analyst Ratings tab, returning the richest available data.
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

# ── Shared thread pool (avoids per-call pool creation) ────────────────
_analyst_pool = ThreadPoolExecutor(max_workers=3, thread_name_prefix="analyst")

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


# ── TipRanks DB cache (analyst_stock_ratings + analyst_profiles) ────


def _fetch_tipranks_from_db(ticker: str) -> tuple[list[dict], dict[str, dict]] | None:
    """Read TipRanks per-analyst ratings from DB if fresh (12h TTL).

    Returns (ratings_list, profiles_dict) or None if absent / stale.
    profiles_dict keys are analyst_id.
    """
    client = _get_supabase_client()
    if client is None:
        return None

    try:
        # Fetch ratings with profile info joined
        resp = (
            client.table("analyst_stock_ratings")
            .select("*, analyst_profiles(name, firm, star_rating, success_rate, tipranks_rank)")
            .eq("ticker", ticker.upper())
            .order("grade_date", desc=True)
            .limit(500)
            .execute()
        )
        rows = resp.data or []
    except Exception as exc:
        logger.warning("TipRanks DB fetch failed for %s: %s", ticker, exc)
        return None

    if not rows:
        return None

    # Freshness check
    now = datetime.now(timezone.utc)
    most_recent: datetime | None = None
    for row in rows:
        ft = row.get("fetched_at")
        if ft:
            try:
                ft_dt = datetime.fromisoformat(ft.replace("Z", "+00:00"))
                if most_recent is None or ft_dt > most_recent:
                    most_recent = ft_dt
            except Exception:
                pass

    if most_recent is None or (now - most_recent).total_seconds() > _DB_TTL:
        return None

    # Build ratings list and profiles dict
    ratings: list[dict] = []
    profiles: dict[str, dict] = {}

    for row in rows:
        analyst_id = row.get("analyst_id") or ""
        profile = row.get("analyst_profiles") or {}

        # Populate profiles dict
        if analyst_id and analyst_id not in profiles:
            profiles[analyst_id] = {
                "analyst_id": analyst_id,
                "name": profile.get("name") or row.get("analyst_name") or "",
                "firm": profile.get("firm") or row.get("firm") or "",
                "star_rating": profile.get("star_rating"),
                "success_rate": profile.get("success_rate"),
                "tipranks_rank": profile.get("tipranks_rank"),
            }

        pt = row.get("price_target")
        ppt = row.get("prior_price_target")
        ratings.append(
            {
                "analyst_id": analyst_id,
                "analyst_name": row.get("analyst_name") or "",
                "firm": row.get("firm") or "",
                "action": row.get("action") or "",
                "to_grade": row.get("to_grade") or "",
                "from_grade": row.get("from_grade") or "",
                "price_target": float(pt) if pt is not None else None,
                "prior_price_target": float(ppt) if ppt is not None else None,
                "grade_date": str(row.get("grade_date") or "")[:10],
            }
        )

    logger.debug("Served %d TipRanks ratings for %s from DB", len(ratings), ticker)
    return ratings, profiles


def _upsert_tipranks_to_db(ticker: str, analysts_data: list[dict]) -> None:
    """Upsert TipRanks analyst profiles + stock ratings to DB. Background thread."""
    client = _get_supabase_client()
    if client is None or not analysts_data:
        return

    from filings import analyst_scraper

    analyst_scraper.upsert_analyst_profiles(analysts_data, client)
    analyst_scraper.upsert_analyst_stock_ratings(ticker, analysts_data, client)


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

    # 3. Live fetch from APIs (uses persistent thread pool)
    f_finnhub = _analyst_pool.submit(_fetch_finnhub, ticker)
    f_yf = _analyst_pool.submit(_fetch_yfinance, ticker)
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


def get_analyst_ratings_with_profiles(
    ticker: str,
) -> tuple[list[dict], dict[str, dict], str]:
    """Get analyst ratings enriched with individual profiles (TipRanks if available).

    Returns:
        (ratings, profiles, data_view)
        - ratings:   list of rating dicts (keys vary by view)
        - profiles:  dict[analyst_id → profile dict]  (empty for "firm" view)
        - data_view: "individual" (TipRanks) | "firm" (yfinance fallback)

    Priority:
      1. analyst_stock_ratings DB (TipRanks, 12h TTL) → "individual"
      2. Live TipRanks fetch + async DB upsert → "individual"
      3. Existing firm-level data (yfinance/Finnhub, 12h TTL) → "firm"
    """
    ticker = ticker.upper()

    # 1. TipRanks DB cache
    db_result = _fetch_tipranks_from_db(ticker)
    if db_result is not None:
        ratings, profiles = db_result
        return ratings, profiles, "individual"

    # 2. Live TipRanks fetch
    from filings import analyst_scraper

    tr_analysts = analyst_scraper.fetch_tipranks_analysts(ticker)
    if tr_analysts:
        # Upsert in background
        threading.Thread(
            target=_upsert_tipranks_to_db,
            args=(ticker, tr_analysts),
            daemon=True,
        ).start()

        # Build return values from fresh data
        ratings: list[dict] = []
        profiles: dict[str, dict] = {}
        for a in tr_analysts:
            analyst_id = a["analyst_id"]
            profiles[analyst_id] = {
                "analyst_id": analyst_id,
                "name": a["name"],
                "firm": a["firm"],
                "star_rating": a.get("star_rating"),
                "success_rate": a.get("success_rate"),
                "tipranks_rank": a.get("tipranks_rank"),
            }
            for r in a.get("ratings") or []:
                ratings.append(
                    {
                        "analyst_id": analyst_id,
                        "analyst_name": a["name"],
                        "firm": a["firm"],
                        "action": r.get("action") or "",
                        "to_grade": r.get("to_grade") or "",
                        "from_grade": r.get("from_grade") or "",
                        "price_target": r.get("price_target"),
                        "prior_price_target": r.get("prior_price_target"),
                        "grade_date": r.get("grade_date") or "",
                    }
                )
        ratings.sort(
            key=lambda x: x.get("grade_date") or "0000-00-00", reverse=True
        )
        return ratings, profiles, "individual"

    # 3. Fall back to firm-level data
    firm_ratings = get_analyst_ratings(ticker)
    # Enrich with FinViz price targets if firm ratings have no price targets
    has_targets = any(
        r.current_price_target is not None for r in firm_ratings
    )
    if not has_targets and firm_ratings:
        _enrich_with_finviz(ticker, firm_ratings)

    firm_dicts = [
        {
            "analyst_id": None,
            "analyst_name": None,
            "firm": r.firm,
            "action": r.action,
            "to_grade": r.to_grade,
            "from_grade": r.from_grade,
            "price_target": r.current_price_target,
            "prior_price_target": r.prior_price_target,
            "grade_date": r.date,
        }
        for r in firm_ratings
    ]
    return firm_dicts, {}, "firm"


def _enrich_with_finviz(ticker: str, ratings: list[AnalystRating]) -> None:
    """Try to fill in price targets from FinViz for firm-level ratings (in-place).

    Matches FinViz events to existing ratings by normalized firm name + date.
    Silently skips on any error.
    """
    try:
        from filings import analyst_scraper

        fv_targets = analyst_scraper.fetch_finviz_targets(ticker)
        if not fv_targets:
            return

        # Build lookup: (firm_norm, date) → (price_target, prior_price_target)
        fv_map: dict[tuple[str, str], tuple[float | None, float | None]] = {}
        for item in fv_targets:
            key = (_normalize_firm(item["firm"]), item["grade_date"])
            fv_map[key] = (item.get("price_target"), item.get("prior_price_target"))

        # Enrich matching ratings
        enriched = 0
        for r in ratings:
            if r.current_price_target is not None:
                continue
            key = (_normalize_firm(r.firm), r.date)
            if key in fv_map:
                pt, ppt = fv_map[key]
                r.current_price_target = pt
                r.prior_price_target = ppt
                enriched += 1

        logger.debug("FinViz enriched %d ratings for %s", enriched, ticker)
    except Exception as exc:
        logger.debug("FinViz enrichment failed for %s: %s", ticker, exc)


def get_consensus_summary_from_raw(
    ratings: list[dict], data_view: str
) -> dict:
    """Compute consensus from raw rating dicts (individual or firm view).

    Returns dict with keys:
        buy, hold, sell, other, total,
        mean_price_target, high_price_target, low_price_target,
        consensus_label  ("Strong Buy", "Buy", "Hold", "Sell", "Strong Sell")
    """
    # Deduplicate: take most recent per (analyst_id or firm)
    seen: dict[str, dict] = {}
    for r in ratings:
        key = r.get("analyst_id") or _normalize_firm(r.get("firm") or "")
        if key and key not in seen:
            seen[key] = r

    buy_grades = {
        "buy", "strong buy", "strongbuy", "outperform", "overweight",
        "positive", "accumulate", "sector outperform", "market outperform",
        "top pick", "conviction buy",
    }
    hold_grades = {
        "hold", "neutral", "equal-weight", "equal weight", "equalweight",
        "market perform", "sector perform", "in-line", "inline",
        "peer perform", "sector weight",
    }
    sell_grades = {
        "sell", "strong sell", "strongsell", "underperform", "underweight",
        "negative", "reduce", "sector underperform", "market underperform",
    }

    counts = {"buy": 0, "hold": 0, "sell": 0, "other": 0}
    price_targets: list[float] = []

    for r in seen.values():
        grade = (r.get("to_grade") or "").lower().strip()
        if grade in buy_grades:
            counts["buy"] += 1
        elif grade in hold_grades:
            counts["hold"] += 1
        elif grade in sell_grades:
            counts["sell"] += 1
        else:
            counts["other"] += 1

        pt = r.get("price_target")
        if pt is not None:
            price_targets.append(float(pt))

    counts["total"] = sum(counts.values())
    counts["mean_price_target"] = (
        round(sum(price_targets) / len(price_targets), 2) if price_targets else None
    )
    counts["high_price_target"] = max(price_targets) if price_targets else None
    counts["low_price_target"] = min(price_targets) if price_targets else None

    # Derive consensus label from buy/hold/sell ratio
    total_rated = counts["buy"] + counts["hold"] + counts["sell"]
    if total_rated > 0:
        buy_pct = counts["buy"] / total_rated
        sell_pct = counts["sell"] / total_rated
        if buy_pct >= 0.7:
            counts["consensus_label"] = "Strong Buy"
        elif buy_pct >= 0.45:
            counts["consensus_label"] = "Buy"
        elif sell_pct >= 0.7:
            counts["consensus_label"] = "Strong Sell"
        elif sell_pct >= 0.45:
            counts["consensus_label"] = "Sell"
        else:
            counts["consensus_label"] = "Hold"
    else:
        counts["consensus_label"] = "N/A"

    return counts


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
