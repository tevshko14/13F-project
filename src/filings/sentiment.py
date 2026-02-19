"""Fetch sentiment data from multiple free sources.

Sources:
  1. Finnhub  – per-ticker news sentiment (bull/bear %, buzz metrics)
  2. CNN      – market-wide Fear & Greed Index (0-100)
  3. ApeWisdom – Reddit mention counts (WSB, r/stocks, etc.)
  4. Alpha Vantage – per-ticker NLP-scored news articles

All results are cached in memory with aggressive TTLs to avoid
hitting rate limits and to keep page loads fast.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
import urllib.request

logger = logging.getLogger(__name__)

# ── Thread lock for all cache reads/writes ────────────────────────────
_lock = threading.Lock()

# ── Cache TTLs ──────────────────────────────────────────────────────
_FINNHUB_TTL = 7200       # 2 hours
_CNN_TTL = 3600            # 1 hour
_APEWISDOM_TTL = 3600      # 1 hour
_ALPHAVANTAGE_TTL = 43200  # 12 hours
_GLASSDOOR_TTL = 2_592_000  # 30 days — ratings change very slowly

# ── LRU max entries for per-ticker caches ─────────────────────────────
_MAX_CACHE_ENTRIES = 2000

# ── Per-ticker caches: {TICKER: (timestamp, data)} ─────────────────
_finnhub_cache: dict[str, tuple[float, dict | None]] = {}
_alphavantage_cache: dict[str, tuple[float, dict | None]] = {}
_glassdoor_cache: dict[str, tuple[float, dict | None]] = {}

# ── Global caches: (timestamp, data) ───────────────────────────────
_cnn_cache: tuple[float, dict | None] | None = None
_apewisdom_cache: tuple[float, list[dict]] | None = None

# ── Alpha Vantage daily budget tracker ──────────────────────────────
_av_daily_count = 0
_av_daily_reset: float = 0.0
_AV_DAILY_MAX = 20  # leave 5-call buffer from the 25/day limit


def _evict_oldest(cache: dict, max_size: int = _MAX_CACHE_ENTRIES) -> None:
    """Evict oldest entries if cache exceeds max_size. Must hold _lock."""
    if len(cache) <= max_size:
        return
    sorted_keys = sorted(cache, key=lambda k: cache[k][0])
    for k in sorted_keys[:len(cache) - max_size]:
        del cache[k]


# ── Helpers ─────────────────────────────────────────────────────────

def has_finnhub_key() -> bool:
    return bool(os.environ.get("FINNHUB_API_KEY"))


def has_alphavantage_key() -> bool:
    return bool(os.environ.get("ALPHAVANTAGE_API_KEY"))


def has_glassdoor_key() -> bool:
    return bool(os.environ.get("GLASSDOOR_RAPIDAPI_KEY"))


_BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def _http_get_json(url: str, headers: dict | None = None, timeout: int = 10) -> dict | list | None:
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

    Returns dict with: score, rating, previous_close, one_week_ago,
    one_month_ago, one_year_ago.
    """
    global _cnn_cache

    with _lock:
        if _cnn_cache is not None:
            ts, data = _cnn_cache
            if time.time() - ts < _CNN_TTL:
                return data

    url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
    raw = _http_get_json(url, headers={
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://edition.cnn.com/",
        "Origin": "https://edition.cnn.com",
    })

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
    """Fetch all-stocks ranked list from ApeWisdom (pages 1-5)."""
    global _apewisdom_cache

    with _lock:
        if _apewisdom_cache is not None:
            ts, data = _apewisdom_cache
            if time.time() - ts < _APEWISDOM_TTL:
                return data

    all_results: list[dict] = []
    for page in range(1, 6):  # pages 1-5 (cap for speed)
        url = f"https://apewisdom.io/api/v1.0/filter/all-stocks/page/{page}"
        raw = _http_get_json(url, timeout=8)
        if not raw or not isinstance(raw, dict):
            break
        results = raw.get("results") or []
        if not results:
            break
        all_results.extend(results)

    with _lock:
        _apewisdom_cache = (time.time(), all_results)
    return all_results


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

def _check_av_budget() -> bool:
    """Return True if we can still make Alpha Vantage calls today."""
    global _av_daily_count, _av_daily_reset
    now = time.time()
    if now - _av_daily_reset > 86400:
        _av_daily_count = 0
        _av_daily_reset = now
    return _av_daily_count < _AV_DAILY_MAX


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

    if not _check_av_budget():
        logger.info("Alpha Vantage daily budget exhausted (%d/%d)", _av_daily_count, _AV_DAILY_MAX)
        return None

    url = (
        f"https://www.alphavantage.co/query"
        f"?function=NEWS_SENTIMENT&tickers={key}&apikey={api_key}&limit=10"
    )
    raw = _http_get_json(url, timeout=15)
    _av_daily_count += 1

    if not raw or not isinstance(raw, dict):
        with _lock:
            _alphavantage_cache[key] = (time.time(), None)
            _evict_oldest(_alphavantage_cache)
        return None

    # Check for error/rate-limit responses
    if "Note" in raw or "Error Message" in raw or "Information" in raw:
        logger.warning("Alpha Vantage returned error for %s: %s",
                       key, raw.get("Note") or raw.get("Error Message") or raw.get("Information"))
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
        for ts_item in (item.get("ticker_sentiment") or []):
            if (ts_item.get("ticker") or "").upper() == key:
                ticker_sent = ts_item
                break

        t_score = float(ticker_sent.get("ticker_sentiment_score", 0))
        t_label = ticker_sent.get("ticker_sentiment_label", "Neutral")

        articles.append({
            "title": item.get("title", ""),
            "url": item.get("url", ""),
            "source": item.get("source", ""),
            "published": item.get("time_published", "")[:10],  # YYYYMMDD or similar
            "ticker_sentiment_score": t_score,
            "ticker_sentiment_label": t_label,
        })

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
# 5. Glassdoor Employee Sentiment (via RapidAPI)
# ═══════════════════════════════════════════════════════════════════

def _resolve_company_name(ticker: str) -> str | None:
    """Resolve ticker to company name via yfinance for Glassdoor lookup."""
    try:
        import yfinance as yf
        tk = yf.Ticker(ticker.upper())
        info = tk.info or {}
        return info.get("longName") or info.get("shortName")
    except Exception as exc:
        logger.debug("yfinance company name lookup failed for %s: %s", ticker, exc)
        return None


def _get_glassdoor_data(ticker: str) -> dict | None:
    """Fetch Glassdoor company ratings via RapidAPI.

    Returns dict with: overall_rating, recommend_to_friend_pct,
    ceo_approval_pct, ceo_name, review_count, business_outlook_pct,
    company_name.

    Uses 30-day cache (ratings change very slowly).
    """
    key = ticker.upper()

    # Check cache
    with _lock:
        if key in _glassdoor_cache:
            ts, data = _glassdoor_cache[key]
            if time.time() - ts < _GLASSDOOR_TTL:
                return data

    api_key = os.environ.get("GLASSDOOR_RAPIDAPI_KEY", "")
    if not api_key:
        return None

    # Resolve ticker to company name
    company_name = _resolve_company_name(key)
    if not company_name:
        logger.info("Cannot resolve company name for %s — skipping Glassdoor", key)
        with _lock:
            _glassdoor_cache[key] = (time.time(), None)
            _evict_oldest(_glassdoor_cache)
        return None

    # Query RapidAPI Glassdoor endpoint
    import urllib.parse
    encoded_name = urllib.parse.quote(company_name)
    url = f"https://real-time-glassdoor-data.p.rapidapi.com/company-overview?companyName={encoded_name}"

    raw = _http_get_json(url, headers={
        "X-RapidAPI-Key": api_key,
        "X-RapidAPI-Host": "real-time-glassdoor-data.p.rapidapi.com",
    }, timeout=15)

    if not raw or not isinstance(raw, dict):
        logger.info("Glassdoor returned no data for %s (%s)", key, company_name)
        with _lock:
            _glassdoor_cache[key] = (time.time(), None)
            _evict_oldest(_glassdoor_cache)
        return None

    # Parse response — field names may vary by provider
    # Try common field patterns from the RapidAPI provider
    overall = raw.get("overallRating") or raw.get("overall_rating")
    if overall is None:
        # Try nested data structure
        ratings = raw.get("ratings") or raw.get("data") or {}
        if isinstance(ratings, dict):
            overall = ratings.get("overallRating") or ratings.get("overall_rating")

    if overall is None:
        logger.info("Glassdoor response missing rating for %s: keys=%s", key, list(raw.keys())[:10])
        with _lock:
            _glassdoor_cache[key] = (time.time(), None)
            _evict_oldest(_glassdoor_cache)
        return None

    try:
        overall_float = round(float(overall), 1)
    except (ValueError, TypeError):
        overall_float = 0.0

    # Extract sub-metrics (gracefully handle missing fields)
    def _pct(val: any) -> float | None:
        if val is None:
            return None
        try:
            v = float(str(val).replace("%", ""))
            # If value is 0-1 range, convert to percentage
            if 0 < v <= 1:
                v = round(v * 100)
            return round(v)
        except (ValueError, TypeError):
            return None

    result = {
        "overall_rating": overall_float,
        "recommend_to_friend_pct": _pct(
            raw.get("recommendToFriend") or raw.get("recommend_to_friend")
            or raw.get("recommendPercent")
        ),
        "ceo_approval_pct": _pct(
            raw.get("ceoApproval") or raw.get("ceo_approval")
            or raw.get("ceoApprovalPercent")
        ),
        "ceo_name": (
            raw.get("ceoName") or raw.get("ceo_name")
            or raw.get("ceo") or ""
        ),
        "review_count": int(raw.get("reviewCount") or raw.get("review_count") or raw.get("numberOfRatings") or 0),
        "business_outlook_pct": _pct(
            raw.get("businessOutlook") or raw.get("business_outlook")
            or raw.get("positiveBusinessOutlookPercent")
        ),
        "company_name": raw.get("companyName") or raw.get("name") or company_name,
    }

    logger.info("Glassdoor data for %s: rating=%.1f, reviews=%d",
                key, result["overall_rating"], result["review_count"])

    with _lock:
        _glassdoor_cache[key] = (time.time(), result)
        _evict_oldest(_glassdoor_cache)
    return result


# ═══════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════

def get_sentiment_data(ticker: str) -> dict:
    """Aggregate sentiment from all sources for a ticker.

    Each source is fetched independently; failures in one do not
    affect the others. Returns dict with keys:
        finnhub, cnn_fear_greed, apewisdom, alphavantage, glassdoor
    Each value is either a dict of data or None.
    """
    result: dict[str, dict | None] = {}

    try:
        result["finnhub"] = _get_finnhub_sentiment(ticker)
    except Exception as exc:
        logger.warning("Finnhub sentiment failed for %s: %s", ticker, exc)
        result["finnhub"] = None

    try:
        result["cnn_fear_greed"] = _get_cnn_fear_greed()
    except Exception as exc:
        logger.warning("CNN Fear & Greed failed: %s", exc)
        result["cnn_fear_greed"] = None

    try:
        result["apewisdom"] = _get_apewisdom_for_ticker(ticker)
    except Exception as exc:
        logger.warning("ApeWisdom failed for %s: %s", ticker, exc)
        result["apewisdom"] = None

    try:
        result["alphavantage"] = _get_alphavantage_sentiment(ticker)
    except Exception as exc:
        logger.warning("Alpha Vantage failed for %s: %s", ticker, exc)
        result["alphavantage"] = None

    try:
        result["glassdoor"] = _get_glassdoor_data(ticker)
    except Exception as exc:
        logger.warning("Glassdoor failed for %s: %s", ticker, exc)
        result["glassdoor"] = None

    return result
