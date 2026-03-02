"""Fetch sentiment data from multiple free sources.

Sources:
  1. Finnhub  – per-ticker news sentiment (bull/bear %, buzz metrics)
  2. CNN      – market-wide Fear & Greed Index (0-100)
  3. ApeWisdom – Reddit mention counts (WSB, r/stocks, etc.)
  4. Alpha Vantage – per-ticker NLP-scored news articles

Glassdoor employee sentiment was migrated to vitals.py (Vitals tab).

All results are cached in memory with aggressive TTLs to avoid
hitting rate limits and to keep page loads fast.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import urllib.request

logger = logging.getLogger(__name__)

# ── Thread lock for all cache reads/writes ────────────────────────────
_lock = threading.Lock()

# ── Cache TTLs ──────────────────────────────────────────────────────
_FINNHUB_TTL = 7200  # 2 hours
_CNN_TTL = 3600  # 1 hour
_APEWISDOM_TTL = 1800  # 30 minutes
_ALPHAVANTAGE_TTL = 43200  # 12 hours

# ── LRU max entries for per-ticker caches ─────────────────────────────
_MAX_CACHE_ENTRIES = 2000

# ── Per-ticker caches: {TICKER: (timestamp, data)} ─────────────────
_finnhub_cache: dict[str, tuple[float, dict | None]] = {}
_alphavantage_cache: dict[str, tuple[float, dict | None]] = {}

# ── Global caches: (timestamp, data) ───────────────────────────────
_cnn_cache: tuple[float, dict | None] | None = None
_apewisdom_cache: tuple[float, list[dict]] | None = None
_leaderboard_cache: tuple[float, dict] | None = None
_LEADERBOARD_TTL = 1800  # 30 minutes

# ── Alpha Vantage daily budget tracker ──────────────────────────────
_av_daily_count = 0
_av_daily_reset: float = 0.0
_AV_DAILY_MAX = 20  # leave 5-call buffer from the 25/day limit


def _evict_oldest(cache: dict, max_size: int = _MAX_CACHE_ENTRIES) -> None:
    """Evict oldest entries if cache exceeds max_size. Must hold _lock."""
    if len(cache) <= max_size:
        return
    sorted_keys = sorted(cache, key=lambda k: cache[k][0])
    for k in sorted_keys[: len(cache) - max_size]:
        del cache[k]


# ── Helpers ─────────────────────────────────────────────────────────


def has_finnhub_key() -> bool:
    return bool(os.environ.get("FINNHUB_API_KEY"))


def has_alphavantage_key() -> bool:
    return bool(os.environ.get("ALPHAVANTAGE_API_KEY"))


_BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def _http_get_json(
    url: str, headers: dict | None = None, timeout: int = 10
) -> dict | list | None:
    """Simple GET→JSON helper using stdlib urllib."""
    req = urllib.request.Request(url)
    req.add_header("User-Agent", _BROWSER_UA)
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except Exception as exc:
        logger.debug("HTTP GET %s failed: %s", url, exc)
        return None


# ═══════════════════════════════════════════════════════════════════
# 1. Finnhub News Sentiment
# ═══════════════════════════════════════════════════════════════════


def _get_finnhub_sentiment(ticker: str) -> dict | None:
    """Per-ticker news sentiment from Finnhub.

    Returns dict with: bullish_pct, bearish_pct, buzz_articles_last_week,
    buzz_score, buzz_weekly_avg, company_news_score, sector_avg_bullish_pct,
    sector_avg_news_score.
    """
    key = ticker.upper()

    # Check cache
    with _lock:
        if key in _finnhub_cache:
            ts, data = _finnhub_cache[key]
            if time.time() - ts < _FINNHUB_TTL:
                return data

    api_key = os.environ.get("FINNHUB_API_KEY", "")
    if not api_key:
        return None

    try:
        import finnhub

        client = finnhub.Client(api_key=api_key)
        raw = client.news_sentiment(key)
    except Exception as exc:
        logger.warning("Finnhub news_sentiment(%s) failed: %s", key, exc)
        return None

    if not raw or not isinstance(raw, dict):
        with _lock:
            _finnhub_cache[key] = (time.time(), None)
            _evict_oldest(_finnhub_cache)
        return None

    buzz = raw.get("buzz") or {}
    sentiment = raw.get("sentiment") or {}

    result = {
        "bullish_pct": sentiment.get("bullishPercent", 0),
        "bearish_pct": sentiment.get("bearishPercent", 0),
        "buzz_articles_last_week": buzz.get("articlesInLastWeek", 0),
        "buzz_score": buzz.get("buzz", 0),
        "buzz_weekly_avg": buzz.get("weeklyAverage", 0),
        "company_news_score": raw.get("companyNewsScore", 0),
        "sector_avg_bullish_pct": raw.get("sectorAverageBullishPercent", 0),
        "sector_avg_news_score": raw.get("sectorAverageNewsScore", 0),
    }

    with _lock:
        _finnhub_cache[key] = (time.time(), result)
        _evict_oldest(_finnhub_cache)
    return result


# ═══════════════════════════════════════════════════════════════════
# 2. CNN Fear & Greed Index
# ═══════════════════════════════════════════════════════════════════


def _get_cnn_fear_greed() -> dict | None:
    """Market-wide Fear & Greed Index (0-100).

    Uses stale-while-revalidate: returns cached data immediately even if
    expired, then refreshes in the background.

    Returns dict with: score, rating, previous_close, one_week_ago,
    one_month_ago, one_year_ago.
    """
    global _cnn_cache

    now = time.time()
    with _lock:
        if _cnn_cache is not None:
            ts, data = _cnn_cache
            if now - ts < _CNN_TTL:
                return data
            # Stale but usable — return immediately, refresh in background
            if data is not None:
                _schedule_cnn_refresh()
                return data

    # Cold start — must fetch synchronously
    return _fetch_cnn_fear_greed()


_cnn_refreshing = False


def _schedule_cnn_refresh() -> None:
    """Kick off a background thread to refresh CNN Fear & Greed data."""
    global _cnn_refreshing
    with _lock:
        if _cnn_refreshing:
            return
        _cnn_refreshing = True

    import threading as _thr

    def _do_refresh():
        global _cnn_refreshing
        try:
            _fetch_cnn_fear_greed()
        finally:
            with _lock:
                _cnn_refreshing = False

    _thr.Thread(target=_do_refresh, daemon=True).start()


def _fetch_cnn_fear_greed() -> dict | None:
    """Fetch CNN Fear & Greed data and update cache."""
    global _cnn_cache

    url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
    raw = _http_get_json(
        url,
        headers={
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://edition.cnn.com/",
            "Origin": "https://edition.cnn.com",
        },
    )

    if not raw or not isinstance(raw, dict):
        with _lock:
            _cnn_cache = (time.time(), None)
        return None

    fg = raw.get("fear_and_greed") or {}
    score = fg.get("score")
    if score is None:
        with _lock:
            _cnn_cache = (time.time(), None)
        return None

    result = {
        "score": round(float(score), 1),
        "rating": str(fg.get("rating", "unknown")).title(),
        "previous_close": fg.get("previous_close"),
        "one_week_ago": fg.get("previous_1_week"),
        "one_month_ago": fg.get("previous_1_month"),
        "one_year_ago": fg.get("previous_1_year"),
    }

    with _lock:
        _cnn_cache = (time.time(), result)
    return result


# ═══════════════════════════════════════════════════════════════════
# 3. ApeWisdom (Reddit Mentions)
# ═══════════════════════════════════════════════════════════════════


def _get_apewisdom_all() -> list[dict]:
    """Fetch all-stocks ranked list from ApeWisdom (pages 1-5).

    Uses stale-while-revalidate: returns cached data immediately even if
    expired, then refreshes in the background so the *next* caller gets
    fresh data without waiting.  Pages are fetched concurrently.
    """
    global _apewisdom_cache

    now = time.time()
    with _lock:
        if _apewisdom_cache is not None:
            ts, data = _apewisdom_cache
            if now - ts < _APEWISDOM_TTL:
                return data
            # Stale but usable — return it and refresh in background
            if data:
                _schedule_apewisdom_refresh()
                return data

    # Cold start — no cached data at all, must fetch synchronously
    return _fetch_apewisdom_pages()


def _fetch_apewisdom_pages() -> list[dict]:
    """Fetch 5 ApeWisdom pages concurrently and update cache."""
    global _apewisdom_cache

    def _fetch_page(page: int) -> list[dict]:
        url = f"https://apewisdom.io/api/v1.0/filter/all-stocks/page/{page}"
        raw = _http_get_json(url, timeout=8)
        if not raw or not isinstance(raw, dict):
            return []
        return raw.get("results") or []

    all_results: list[dict] = []
    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = {pool.submit(_fetch_page, p): p for p in range(1, 6)}
        # Collect in page order so ranking stays consistent
        page_results: dict[int, list[dict]] = {}
        for future in as_completed(futures):
            page_results[futures[future]] = future.result()
        for p in sorted(page_results):
            all_results.extend(page_results[p])

    with _lock:
        _apewisdom_cache = (time.time(), all_results)
    return all_results


_apewisdom_refreshing = False  # guard to prevent duplicate background refreshes


def _schedule_apewisdom_refresh() -> None:
    """Kick off a background thread to refresh ApeWisdom data."""
    global _apewisdom_refreshing
    with _lock:
        if _apewisdom_refreshing:
            return
        _apewisdom_refreshing = True

    import threading as _thr

    def _do_refresh():
        global _apewisdom_refreshing
        try:
            _fetch_apewisdom_pages()
        finally:
            with _lock:
                _apewisdom_refreshing = False

    _thr.Thread(target=_do_refresh, daemon=True).start()


def _get_apewisdom_for_ticker(ticker: str) -> dict | None:
    """Look up a single ticker in the ApeWisdom data."""
    all_data = _get_apewisdom_all()
    t = ticker.upper()
    for item in all_data:
        if (item.get("ticker") or "").upper() == t:
            return {
                "rank": item.get("rank", 0),
                "name": item.get("name", ""),
                "mentions": int(item.get("mentions", 0)),
                "upvotes": int(item.get("upvotes", 0)),
                "rank_24h_ago": item.get("rank_24h_ago", 0),
                "mentions_24h_ago": int(item.get("mentions_24h_ago", 0)),
            }
    return None


# ═══════════════════════════════════════════════════════════════════
# 4. Alpha Vantage News Sentiment
# ═══════════════════════════════════════════════════════════════════


def _check_and_increment_av_budget() -> bool:
    """Atomically check and increment the Alpha Vantage daily budget.

    Returns ``True`` if the call is allowed (and the counter has already
    been incremented).  Returns ``False`` if the daily limit is reached.
    Thread-safe: holds ``_lock`` for the entire check-then-increment.
    """
    global _av_daily_count, _av_daily_reset
    with _lock:
        now = time.time()
        if now - _av_daily_reset > 86400:
            _av_daily_count = 0
            _av_daily_reset = now
        if _av_daily_count >= _AV_DAILY_MAX:
            return False
        _av_daily_count += 1
        return True


def _get_alphavantage_sentiment(ticker: str) -> dict | None:
    """Per-ticker NLP news sentiment from Alpha Vantage.

    Returns dict with: articles (list of dicts), avg_sentiment_score,
    avg_sentiment_label.
    """
    global _av_daily_count

    key = ticker.upper()

    # Check cache
    with _lock:
        if key in _alphavantage_cache:
            ts, data = _alphavantage_cache[key]
            if time.time() - ts < _ALPHAVANTAGE_TTL:
                return data

    api_key = os.environ.get("ALPHAVANTAGE_API_KEY", "")
    if not api_key:
        return None

    if not _check_and_increment_av_budget():
        logger.info(
            "Alpha Vantage daily budget exhausted (%d/%d)",
            _av_daily_count,
            _AV_DAILY_MAX,
        )
        return None

    url = (
        f"https://www.alphavantage.co/query"
        f"?function=NEWS_SENTIMENT&tickers={key}&apikey={api_key}&limit=10"
    )
    raw = _http_get_json(url, timeout=15)

    if not raw or not isinstance(raw, dict):
        with _lock:
            _alphavantage_cache[key] = (time.time(), None)
            _evict_oldest(_alphavantage_cache)
        return None

    # Check for error/rate-limit responses
    if "Note" in raw or "Error Message" in raw or "Information" in raw:
        logger.warning(
            "Alpha Vantage returned error for %s: %s",
            key,
            raw.get("Note") or raw.get("Error Message") or raw.get("Information"),
        )
        with _lock:
            _alphavantage_cache[key] = (time.time(), None)
            _evict_oldest(_alphavantage_cache)
        return None

    feed = raw.get("feed") or []
    if not feed:
        with _lock:
            _alphavantage_cache[key] = (time.time(), None)
            _evict_oldest(_alphavantage_cache)
        return None

    articles: list[dict] = []
    total_score = 0.0
    score_count = 0

    for item in feed[:10]:
        # Find the ticker-specific sentiment from the per-ticker array
        ticker_sent = {}
        for ts_item in item.get("ticker_sentiment") or []:
            if (ts_item.get("ticker") or "").upper() == key:
                ticker_sent = ts_item
                break

        t_score = float(ticker_sent.get("ticker_sentiment_score", 0))
        t_label = ticker_sent.get("ticker_sentiment_label", "Neutral")

        articles.append(
            {
                "title": item.get("title", ""),
                "url": item.get("url", ""),
                "source": item.get("source", ""),
                "published": item.get("time_published", "")[:10],  # YYYYMMDD or similar
                "ticker_sentiment_score": t_score,
                "ticker_sentiment_label": t_label,
            }
        )

        total_score += t_score
        score_count += 1

    avg_score = total_score / score_count if score_count > 0 else 0.0
    if avg_score >= 0.35:
        avg_label = "Bullish"
    elif avg_score >= 0.15:
        avg_label = "Somewhat-Bullish"
    elif avg_score >= -0.15:
        avg_label = "Neutral"
    elif avg_score >= -0.35:
        avg_label = "Somewhat-Bearish"
    else:
        avg_label = "Bearish"

    result = {
        "articles": articles,
        "avg_sentiment_score": round(avg_score, 3),
        "avg_sentiment_label": avg_label,
    }

    with _lock:
        _alphavantage_cache[key] = (time.time(), result)
        _evict_oldest(_alphavantage_cache)
    return result


# ═══════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════


def get_sentiment_data(ticker: str) -> dict:
    """Aggregate sentiment from all sources for a ticker.

    Each source is fetched independently **in parallel**; failures in
    one do not affect the others.  Returns dict with keys:
        finnhub, cnn_fear_greed, apewisdom, alphavantage
    Each value is either a dict of data or None.

    Note: Glassdoor data was migrated to vitals.py (Vitals tab).
    """
    tasks = {
        "finnhub": lambda: _get_finnhub_sentiment(ticker),
        "cnn_fear_greed": lambda: _get_cnn_fear_greed(),
        "apewisdom": lambda: _get_apewisdom_for_ticker(ticker),
        "alphavantage": lambda: _get_alphavantage_sentiment(ticker),
    }
    result: dict[str, dict | None] = {}

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(fn): key for key, fn in tasks.items()}
        for future in as_completed(futures):
            key = futures[future]
            try:
                result[key] = future.result()
            except Exception as exc:
                logger.warning("%s sentiment failed: %s", key, exc)
                result[key] = None

    return result


# ═══════════════════════════════════════════════════════════════════
# 5. Retail Leaderboard Data Builder
# ═══════════════════════════════════════════════════════════════════


def _velocity_to_color(velocity_pct: float) -> str:
    """Map mention velocity % to hex color (red-gray-green gradient).

    Mirrors ``market_data._pct_to_color`` but normalises the wider
    velocity range (divides by 20 to clamp -100..+100 % into -5..+5).
    """
    clamped = max(-5.0, min(5.0, velocity_pct / 20.0))
    if clamped >= 0:
        t = clamped / 5.0
        r = int(153 + (27 - 153) * t)
        g = int(153 + (94 - 153) * t)
        b = int(153 + (32 - 153) * t)
    else:
        t = abs(clamped) / 5.0
        r = int(153 + (183 - 153) * t)
        g = int(153 + (28 - 153) * t)
        b = int(153 + (28 - 153) * t)
    return f"#{r:02x}{g:02x}{b:02x}"


def build_retail_leaderboard_data(
    apewisdom_data: list[dict],
    superinvestor_tickers: dict[str, list[str]] | None = None,
    fear_greed: dict | None = None,
    treemap_limit: int = 40,
    bubble_limit: int = 10,
) -> dict:
    """Build enriched leaderboard data with velocity, engagement and guru overlap.

    Args:
        apewisdom_data: Raw ApeWisdom ranked stock list.
        superinvestor_tickers: Mapping of ``TICKER -> [display_name, ...]``
            from ``client.build_ticker_ownership_map``.
        fear_greed: CNN Fear & Greed dict (included in metadata).
        treemap_limit: Max tickers in the treemap (default 40).
        bubble_limit: Max tickers in the bubble chart (default 10).

    Returns dict with keys:
        treemap_data  - list for ECharts treemap
        bubble_data   - list for Chart.js bubble chart
        leaderboard_rows - full enriched list for the HTML table
        metadata      - count, timestamp, market_mood
    """
    global _leaderboard_cache

    # ── Fast path: cached result ──
    with _lock:
        if _leaderboard_cache is not None:
            ts, data = _leaderboard_cache
            if time.time() - ts < _LEADERBOARD_TTL:
                return data

    if superinvestor_tickers is None:
        superinvestor_tickers = {}

    rows: list[dict] = []
    for item in apewisdom_data:
        ticker = (item.get("ticker") or "").upper()
        if not ticker:
            continue

        mentions = int(item.get("mentions") or 0)
        mentions_24h = int(item.get("mentions_24h_ago") or 0)
        upvotes = int(item.get("upvotes") or 0)
        rank = int(item.get("rank") or 0)
        rank_24h = item.get("rank_24h_ago")

        # Velocity: % change in mentions over 24h
        velocity_pct = (
            ((mentions - mentions_24h) / max(mentions_24h, 1)) * 100
            if mentions_24h > 0
            else 0.0
        )

        # Engagement ratio: upvotes per mention (quality proxy)
        engagement_ratio = round(upvotes / max(mentions, 1), 1)

        # Rank change
        rank_change = (int(rank_24h) if rank_24h else rank) - rank

        # Superinvestor overlap
        guru_names = superinvestor_tickers.get(ticker, [])
        guru_count = len(guru_names)

        # Heat level 0-5 (based on absolute velocity)
        heat = min(5, max(0, int(abs(velocity_pct) / 20)))

        rows.append(
            {
                "rank": rank,
                "ticker": ticker,
                "name": item.get("name") or "",
                "mentions": mentions,
                "mentions_24h_ago": mentions_24h,
                "velocity_pct": round(velocity_pct, 1),
                "upvotes": upvotes,
                "engagement_ratio": engagement_ratio,
                "rank_change": rank_change,
                "guru_count": guru_count,
                "guru_names": guru_names[:5],  # cap for JSON size
                "heat": heat,
            }
        )

    # ── Treemap data: top N by mentions ──
    treemap_sorted = sorted(rows, key=lambda r: r["mentions"], reverse=True)
    treemap_data = []
    for r in treemap_sorted[:treemap_limit]:
        treemap_data.append(
            {
                "name": r["ticker"],
                "value": max(r["mentions"], 1),  # treemap needs value > 0
                "velocity_pct": r["velocity_pct"],
                "mentions": r["mentions"],
                "engagement_ratio": r["engagement_ratio"],
                "upvotes": r["upvotes"],
                "guru_count": r["guru_count"],
                "guru_names": r["guru_names"],
                "link": f"/stock/{r['ticker']}",
                "itemStyle": {"color": _velocity_to_color(r["velocity_pct"])},
            }
        )

    # ── Bubble data: top N by absolute velocity ──
    bubble_sorted = sorted(rows, key=lambda r: abs(r["velocity_pct"]), reverse=True)
    bubble_data = []
    for r in bubble_sorted[:bubble_limit]:
        bubble_data.append(
            {
                "ticker": r["ticker"],
                "name": r["name"],
                "x": r["engagement_ratio"],
                "y": r["velocity_pct"],
                "r": r["mentions"],
                "guru_count": r["guru_count"],
                "rank": r["rank"],
            }
        )

    result = {
        "treemap_data": treemap_data,
        "bubble_data": bubble_data,
        "leaderboard_rows": rows,
        "metadata": {
            "count": len(rows),
            "timestamp": time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime()),
            "market_mood": fear_greed.get("rating") if fear_greed else None,
            "market_score": fear_greed.get("score") if fear_greed else None,
        },
    }

    with _lock:
        _leaderboard_cache = (time.time(), result)

    return result


# ═══════════════════════════════════════════════════════════════════
# 6. Retail Sentiment Overview (Homepage Card)
# ═══════════════════════════════════════════════════════════════════

_sentiment_overview_cache: tuple[float, dict] | None = None
_SENTIMENT_OVERVIEW_TTL = 900  # 15 minutes


def get_retail_sentiment_overview() -> dict:
    """Aggregate CNN Fear & Greed + ApeWisdom top movers for the homepage card.

    Returns dict with:
        fear_greed: {score, rating, previous_close, one_week_ago, one_month_ago, one_year_ago} | None
        top_movers: top 5 tickers by 24h velocity change
        top_mentioned: top 5 tickers by total mentions
        metadata: {timestamp}
    """
    global _sentiment_overview_cache

    now = time.time()
    with _lock:
        if _sentiment_overview_cache is not None:
            ts, data = _sentiment_overview_cache
            if now - ts < _SENTIMENT_OVERVIEW_TTL:
                return data

    try:
        fear_greed = _get_cnn_fear_greed()
    except Exception:
        logger.warning("Failed to fetch CNN Fear & Greed for overview card")
        fear_greed = None

    try:
        ape_all = _get_apewisdom_all()
    except Exception:
        logger.warning("Failed to fetch ApeWisdom for overview card")
        ape_all = []

    # ── Build enriched rows with velocity ──
    rows: list[dict] = []
    for item in ape_all:
        ticker = (item.get("ticker") or "").upper()
        if not ticker:
            continue
        mentions = int(item.get("mentions") or 0)
        mentions_24h = int(item.get("mentions_24h_ago") or 0)
        upvotes = int(item.get("upvotes") or 0)
        rank = int(item.get("rank") or 0)

        velocity_pct = (
            ((mentions - mentions_24h) / max(mentions_24h, 1)) * 100
            if mentions_24h > 0
            else 0.0
        )

        rows.append({
            "rank": rank,
            "ticker": ticker,
            "name": item.get("name") or "",
            "mentions": mentions,
            "upvotes": upvotes,
            "velocity_pct": round(velocity_pct, 1),
        })

    # Top 5 by absolute velocity
    top_movers = sorted(rows, key=lambda r: abs(r["velocity_pct"]), reverse=True)[:5]

    # Top 5 by mentions
    top_mentioned = sorted(rows, key=lambda r: r["mentions"], reverse=True)[:5]

    result = {
        "fear_greed": fear_greed,
        "top_movers": top_movers,
        "top_mentioned": top_mentioned,
        "metadata": {
            "timestamp": time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime()),
        },
    }

    with _lock:
        _sentiment_overview_cache = (now, result)

    return result
