"""WallStreetBets / Reddit sentiment data.

Free, no API key required.
Source: https://apewisdom.io/api/v1.0/

Provides:
  - Top mentioned tickers on Reddit (WSB + other finance subs)
  - Per-ticker mention count + rank + upvote ratio
"""

import json
import logging
import urllib.request

from filings.caching import TTLCache

logger = logging.getLogger(__name__)

_APE_URL = "https://apewisdom.io/api/v1.0/filter/all-stocks/page/1"
_TTL = 3600  # 1 hour — sentiment changes fast; kept for signature compat
_cache = TTLCache(ttl=_TTL, max_size=50)


def _cached_or_fetch(cache_key: str, fetcher, ttl: int = _TTL):
    """Generic L1 → L2 → fetcher pattern.

    *ttl* retained for signature compatibility — the shared TTLCache is
    pinned at ``_TTL`` and no caller overrides it.
    """
    cached = _cache.get(cache_key)
    if cached is not None:
        return cached

    try:
        from filings import supabase_cache
        l2 = supabase_cache.get_cached(cache_key)
        if l2:
            _cache.set(cache_key, l2)
            return l2
    except Exception:
        pass

    try:
        result = fetcher()
        if result:
            try:
                from filings import supabase_cache
                supabase_cache.set_cached(cache_key, "wsb", result, 3600 * 2)
            except Exception:
                pass
            _cache.set(cache_key, result)
        return result
    except Exception as exc:
        logger.warning("WSB fetch failed for %s: %s", cache_key, exc)
        return None


def _fetch_wsb_top() -> list[dict]:
    """Fetch top Reddit-mentioned tickers from ApeWisdom API."""
    try:
        req = urllib.request.Request(_APE_URL, headers={
            "User-Agent": "PaperPanda/1.0",
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())

        results = data.get("results", [])
        if not isinstance(results, list):
            return []

        out = []
        for item in results[:50]:  # top 50
            ticker = item.get("ticker", "").upper()
            if not ticker:
                continue
            mentions = int(item.get("mentions", 0))
            upvotes = int(item.get("upvotes", 0))
            rank = int(item.get("rank", 0))
            name = item.get("name", "")

            # Derive sentiment from upvote ratio
            # High upvotes relative to mentions = bullish consensus
            ratio = upvotes / max(mentions, 1)
            if ratio >= 15:
                sentiment = "Bullish"
            elif ratio <= 5:
                sentiment = "Bearish"
            else:
                sentiment = "Neutral"

            out.append({
                "ticker": ticker,
                "name": name,
                "sentiment": sentiment,
                "mentions": mentions,
                "upvotes": upvotes,
                "rank": rank,
            })

        return out
    except Exception as exc:
        logger.warning("WSB sentiment fetch failed: %s", exc)
        return []


def get_wsb_top() -> list[dict]:
    """Get top WSB tickers with mentions (cached)."""
    return _cached_or_fetch("wsb:top", _fetch_wsb_top) or []


def get_ticker_sentiment(ticker: str) -> dict | None:
    """Get WSB sentiment for a specific ticker.

    Returns {ticker, name, sentiment, mentions, upvotes, rank, total_tracked} or None.
    """
    ticker = ticker.upper()
    top = get_wsb_top()
    for item in top:
        if item["ticker"] == ticker:
            return {**item, "total_tracked": len(top)}
    return None


def get_top_movers(limit: int = 10) -> dict:
    """Get top bullish and bearish tickers from WSB."""
    top = get_wsb_top()
    if not top:
        return {"bullish": [], "bearish": [], "total": 0}

    bullish = sorted(
        [t for t in top if t["sentiment"] == "Bullish"],
        key=lambda x: x["mentions"], reverse=True
    )[:limit]

    bearish = sorted(
        [t for t in top if t["sentiment"] == "Bearish"],
        key=lambda x: x["mentions"], reverse=True
    )[:limit]

    return {
        "bullish": bullish,
        "bearish": bearish,
        "total": len(top),
    }
