"""Google Trends data collection — search interest signals for stocks.

Three signal layers:

1. **Per-ticker keywords** — auto-generated intent, product, and comparison
   terms specific to each company (e.g. "Robinhood sign up", "Nike Dunk",
   "Coinbase vs Robinhood").  Generated from company metadata (sector,
   industry, known products) so no manual curation is needed.

2. **Macro trends dashboard** — generic market-sentiment keywords tracked
   over time: "recession", "bitcoin", "best AI stock", "war", etc.

3. **Trending searches → ticker mapping** — Google's daily trending
   searches matched to known tickers/companies for real-time signal.

Data is fetched from Google Trends via its public embed/widget JSON
endpoints (no API key required).  Rate-limited to ~2 req/min with
exponential back-off to avoid 429s.

All results are cached in-memory (24 h) and persisted to the Supabase
``google_trends_history`` cold table.
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from datetime import datetime, timedelta
from urllib.parse import quote

import httpx

try:
    from pytrends.request import TrendReq
    _PYTRENDS_AVAILABLE = True
except ImportError:
    _PYTRENDS_AVAILABLE = False

logger = logging.getLogger(__name__)

# ── Thread lock & caches ────────────────────────────────────────────
_lock = threading.Lock()

_interest_cache: dict[str, tuple[float, dict]] = {}
_INTEREST_TTL = 86_400  # 24 h

_trending_cache: tuple[float, list] | None = None
_TRENDING_TTL = 3_600  # 1 h — trending searches change fast

_macro_cache: dict[str, tuple[float, dict]] = {}
_MACRO_TTL = 86_400  # 24 h

_sector_cache: dict[str, tuple[float, dict | None]] = {}
_SECTOR_TTL = 604_800  # 7 days

# ── Rate-limiter (shared across all Google Trends calls) ────────────
_last_request_time: float = 0.0
_MIN_INTERVAL = 3.0  # seconds between requests

_GT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
}


def _rate_limit() -> None:
    """Block until MIN_INTERVAL seconds have passed since the last request."""
    global _last_request_time
    with _lock:
        elapsed = time.time() - _last_request_time
        if elapsed < _MIN_INTERVAL:
            time.sleep(_MIN_INTERVAL - elapsed)
        _last_request_time = time.time()


def _gt_get(url: str, *, timeout: int = 15, retries: int = 3) -> httpx.Response | None:
    """GET with rate-limiting, back-off, and retry on 429."""
    for attempt in range(retries):
        _rate_limit()
        try:
            resp = httpx.get(url, timeout=timeout, headers=_GT_HEADERS, follow_redirects=True)
            if resp.status_code == 200:
                return resp
            if resp.status_code == 429:
                wait = 2 ** (attempt + 2)  # 4, 8, 16 s
                logger.warning("Google Trends 429 — backing off %ds", wait)
                time.sleep(wait)
                continue
            logger.warning("Google Trends returned %d for %s", resp.status_code, url)
            return None
        except Exception as e:
            logger.warning("Google Trends request failed (attempt %d): %s", attempt + 1, e)
            if attempt < retries - 1:
                time.sleep(2 ** (attempt + 1))
    return None


# ═════════════════════════════════════════════════════════════════════
# 1. Per-ticker keyword generation
# ═════════════════════════════════════════════════════════════════════

# ── Company type classification for keyword templates ────────────────

_COMPANY_TYPE_KEYWORDS: dict[str, dict] = {
    # Brokerages / Fintech
    "brokerage": {
        "intent": ["{name} app", "{name} sign up", "{name} login", "open {name} account"],
        "product": ["{name} stocks", "{name} crypto", "{name} options"],
        "comparison": ["{name} vs Robinhood", "{name} vs Schwab", "{name} fees"],
    },
    "crypto_exchange": {
        "intent": ["{name} app", "{name} sign up", "{name} login", "buy crypto {name}"],
        "product": ["{name} wallet", "{name} staking", "{name} earn"],
        "comparison": ["{name} vs Binance", "{name} vs Robinhood", "{name} fees"],
    },
    "payments": {
        "intent": ["{name} app", "{name} login", "{name} account"],
        "product": ["{name} business", "{name} pay", "send money {name}"],
        "comparison": ["{name} vs Venmo", "{name} vs Zelle", "{name} fees"],
    },
    # E-commerce / Retail
    "ecommerce": {
        "intent": ["{name} deals", "{name} sale", "{name} promo code", "{name} free shipping"],
        "product": ["{name} prime", "{name} returns", "{name} customer service"],
        "comparison": ["{name} vs Walmart", "{name} vs eBay", "{name} price match"],
    },
    "retail_brand": {
        "intent": ["{name} new releases", "{name} sale", "{name} outlet", "buy {name}"],
        "product": ["{name} shoes", "{name} clothing", "{name} latest"],
        "comparison": ["{name} vs Adidas", "{name} vs competitor", "{name} quality"],
    },
    # Streaming / Content
    "streaming": {
        "intent": ["{name} login", "{name} sign up", "{name} free trial", "cancel {name}"],
        "product": ["{name} new shows", "{name} movies", "{name} plan"],
        "comparison": ["{name} vs Netflix", "{name} vs Disney+", "{name} price"],
    },
    # SaaS / Enterprise
    "saas": {
        "intent": ["{name} pricing", "{name} login", "{name} demo", "{name} trial"],
        "product": ["{name} features", "{name} integration", "{name} API"],
        "comparison": ["{name} vs competitor", "{name} alternative", "{name} review"],
    },
    # Social / Ad platforms
    "social_platform": {
        "intent": ["{name} app", "{name} login", "{name} sign up", "{name} down"],
        "product": ["{name} ads", "{name} marketplace", "{name} reels"],
        "comparison": ["{name} vs TikTok", "{name} vs Twitter", "{name} users"],
    },
    # Travel / Marketplace
    "travel_marketplace": {
        "intent": ["{name} deals", "{name} coupon", "{name} login", "book on {name}"],
        "product": ["{name} hotels", "{name} flights", "{name} cancellation"],
        "comparison": ["{name} vs Expedia", "{name} vs VRBO", "{name} fees"],
    },
    # Delivery
    "delivery": {
        "intent": ["{name} app", "{name} promo code", "{name} sign up", "order {name}"],
        "product": ["{name} pass", "{name} driver", "{name} pickup"],
        "comparison": ["{name} vs UberEats", "{name} vs Grubhub", "{name} fees"],
    },
    # Hardware / Devices
    "hardware": {
        "intent": ["buy {name}", "{name} store", "{name} sale", "{name} price"],
        "product": ["{name} laptop", "{name} GPU", "{name} phone"],
        "comparison": ["{name} vs AMD", "{name} vs Intel", "{name} benchmark"],
    },
    # Default fallback
    "generic": {
        "intent": ["{name} stock", "{name} news", "{name} earnings"],
        "product": ["{name} products", "{name} services"],
        "comparison": ["{name} vs competitor", "{name} review"],
    },
}

# ── Ticker → company type + display name mapping ────────────────────
# This drives keyword generation.  The display name is used in templates
# (e.g. "Robinhood" not "HOOD").

_TICKER_PROFILES: dict[str, dict] = {
    # Brokerages / Fintech
    "HOOD": {"name": "Robinhood", "type": "brokerage"},
    "SOFI": {"name": "SoFi", "type": "brokerage"},
    "SCHW": {"name": "Schwab", "type": "brokerage"},
    "IBKR": {"name": "Interactive Brokers", "type": "brokerage"},
    # Crypto
    "COIN": {"name": "Coinbase", "type": "crypto_exchange"},
    "MSTR": {"name": "MicroStrategy", "type": "generic"},
    # Payments
    "PYPL": {"name": "PayPal", "type": "payments"},
    "SQ": {"name": "Cash App", "type": "payments", "alt_names": ["Square", "Block"]},
    "AFRM": {"name": "Affirm", "type": "payments"},
    "V": {"name": "Visa", "type": "payments"},
    "MA": {"name": "Mastercard", "type": "payments"},
    # E-commerce
    "AMZN": {"name": "Amazon", "type": "ecommerce"},
    "SHOP": {"name": "Shopify", "type": "saas"},
    "ETSY": {"name": "Etsy", "type": "ecommerce"},
    "CHWY": {"name": "Chewy", "type": "ecommerce"},
    "W": {"name": "Wayfair", "type": "ecommerce"},
    "BABA": {"name": "Alibaba", "type": "ecommerce"},
    "PDD": {"name": "Temu", "type": "ecommerce", "alt_names": ["Pinduoduo"]},
    "EBAY": {"name": "eBay", "type": "ecommerce"},
    # Retail brands
    "NKE": {"name": "Nike", "type": "retail_brand",
            "products": ["Nike Dunk", "Air Jordan", "Nike Air Max", "Nike running shoes"],
            "comparisons": ["Nike vs Adidas", "Nike vs New Balance", "Nike vs On Running"]},
    "LULU": {"name": "Lululemon", "type": "retail_brand",
             "products": ["Lululemon leggings", "Lululemon Align", "Lululemon belt bag"],
             "comparisons": ["Lululemon vs Athleta", "Lululemon vs Alo"]},
    "UAA": {"name": "Under Armour", "type": "retail_brand"},
    "ADDYY": {"name": "Adidas", "type": "retail_brand"},
    "DECK": {"name": "Hoka", "type": "retail_brand", "alt_names": ["Deckers", "UGG"],
             "products": ["Hoka Clifton", "Hoka Bondi", "UGG boots"]},
    "ONON": {"name": "On Running", "type": "retail_brand",
             "products": ["On Cloud", "On Cloudmonster", "On running shoes"]},
    "TGT": {"name": "Target", "type": "ecommerce"},
    "WMT": {"name": "Walmart", "type": "ecommerce"},
    "COST": {"name": "Costco", "type": "ecommerce"},
    # Streaming
    "NFLX": {"name": "Netflix", "type": "streaming"},
    "DIS": {"name": "Disney+", "type": "streaming", "alt_names": ["Disney"]},
    "SPOT": {"name": "Spotify", "type": "streaming"},
    "ROKU": {"name": "Roku", "type": "streaming"},
    # SaaS
    "CRM": {"name": "Salesforce", "type": "saas"},
    "SNOW": {"name": "Snowflake", "type": "saas"},
    "DDOG": {"name": "Datadog", "type": "saas"},
    "NET": {"name": "Cloudflare", "type": "saas"},
    "PLTR": {"name": "Palantir", "type": "saas"},
    "ZM": {"name": "Zoom", "type": "saas"},
    "ADBE": {"name": "Adobe", "type": "saas"},
    "NOW": {"name": "ServiceNow", "type": "saas"},
    # Social / Ad platforms
    "META": {"name": "Facebook", "type": "social_platform", "alt_names": ["Instagram", "Meta"]},
    "SNAP": {"name": "Snapchat", "type": "social_platform"},
    "PINS": {"name": "Pinterest", "type": "social_platform"},
    "RDDT": {"name": "Reddit", "type": "social_platform"},
    "TTD": {"name": "The Trade Desk", "type": "saas"},
    # Travel
    "ABNB": {"name": "Airbnb", "type": "travel_marketplace"},
    "BKNG": {"name": "Booking.com", "type": "travel_marketplace"},
    "UBER": {"name": "Uber", "type": "delivery"},
    "LYFT": {"name": "Lyft", "type": "delivery"},
    "DASH": {"name": "DoorDash", "type": "delivery"},
    # Hardware / Chips
    "AAPL": {"name": "Apple", "type": "hardware",
             "products": ["iPhone", "MacBook", "iPad", "Apple Vision Pro", "AirPods"],
             "comparisons": ["iPhone vs Samsung", "MacBook vs Dell", "Apple vs Android"]},
    "NVDA": {"name": "Nvidia", "type": "hardware",
             "products": ["RTX 5090", "RTX 5080", "Nvidia GPU", "CUDA"],
             "comparisons": ["Nvidia vs AMD", "RTX vs Radeon"]},
    "AMD": {"name": "AMD", "type": "hardware",
            "products": ["Ryzen", "Radeon", "AMD GPU"],
            "comparisons": ["AMD vs Intel", "AMD vs Nvidia"]},
    "INTC": {"name": "Intel", "type": "hardware",
             "products": ["Intel Core", "Intel Arc"],
             "comparisons": ["Intel vs AMD", "Intel vs Apple M"]},
    "MSFT": {"name": "Microsoft", "type": "saas",
             "products": ["Windows", "Microsoft 365", "Azure", "Copilot", "Xbox"],
             "comparisons": ["Azure vs AWS", "Copilot vs ChatGPT"]},
    "GOOGL": {"name": "Google", "type": "saas",
              "products": ["Google Search", "YouTube", "Gemini AI", "Google Cloud"],
              "comparisons": ["Google vs ChatGPT", "YouTube vs TikTok"]},
    "GOOG": {"name": "Google", "type": "saas"},
    # AI-specific
    "AI": {"name": "C3.ai", "type": "saas"},
    "TSLA": {"name": "Tesla", "type": "hardware",
             "products": ["Model Y", "Model 3", "Cybertruck", "Tesla Powerwall"],
             "comparisons": ["Tesla vs BYD", "Tesla vs Rivian", "Tesla vs BMW"]},
}


def _get_yf_info(ticker: str) -> dict | None:
    """Fetch company name + sector from yfinance (cached 7 days)."""
    with _lock:
        cached = _sector_cache.get(ticker)
        if cached and time.time() - cached[0] < _SECTOR_TTL:
            return cached[1]

    try:
        import yfinance as yf
        info = yf.Ticker(ticker).info
        result = {
            "name": info.get("shortName", ""),
            "sector": info.get("sector", ""),
            "industry": info.get("industry", ""),
        }
    except Exception as e:
        logger.debug("yfinance lookup failed for %s: %s", ticker, e)
        result = None

    with _lock:
        _sector_cache[ticker] = (time.time(), result)
    return result


def generate_keywords(ticker: str) -> dict:
    """Generate search keywords for a ticker.

    Returns dict with:
      - name: display name for the company
      - intent: list of intent-based search terms
      - product: list of product-based search terms
      - comparison: list of comparison search terms
      - all: flattened list of all keywords (for batch fetch)
    """
    ticker = ticker.upper()

    # 1. Check curated profiles first
    profile = _TICKER_PROFILES.get(ticker)
    if profile:
        return _keywords_from_profile(ticker, profile)

    # 2. Fall back to yfinance metadata → generic keywords
    yf_info = _get_yf_info(ticker)
    name = (yf_info or {}).get("name", ticker) if yf_info else ticker
    # Clean up Inc., Corp., etc.
    name = re.sub(r"\s*(Inc\.?|Corp\.?|Ltd\.?|PLC|Holdings?|Group|Co\.?|,?\s*$)", "", name).strip()
    if not name:
        name = ticker

    templates = _COMPANY_TYPE_KEYWORDS["generic"]
    intent = [t.format(name=name) for t in templates["intent"]]
    product = [t.format(name=name) for t in templates["product"]]
    comparison = [t.format(name=name) for t in templates["comparison"]]

    return {
        "ticker": ticker,
        "name": name,
        "type": "generic",
        "intent": intent,
        "product": product,
        "comparison": comparison,
        "all": intent + product + comparison,
    }


def _keywords_from_profile(ticker: str, profile: dict) -> dict:
    """Build keyword lists from a curated ticker profile."""
    name = profile["name"]
    ctype = profile.get("type", "generic")
    templates = _COMPANY_TYPE_KEYWORDS.get(ctype, _COMPANY_TYPE_KEYWORDS["generic"])

    intent = [t.format(name=name) for t in templates["intent"]]
    product = [t.format(name=name) for t in templates["product"]]
    comparison = [t.format(name=name) for t in templates["comparison"]]

    # Override with custom products if specified
    if "products" in profile:
        product = profile["products"]

    # Override with custom comparisons if specified
    if "comparisons" in profile:
        comparison = profile["comparisons"]

    # Add alt_names as additional intent keywords
    for alt in profile.get("alt_names", []):
        intent.append(f"{alt} app")
        intent.append(f"{alt} stock")

    return {
        "ticker": ticker,
        "name": name,
        "type": ctype,
        "intent": intent,
        "product": product,
        "comparison": comparison,
        "all": intent + product + comparison,
    }


# ═════════════════════════════════════════════════════════════════════
# Google Trends data fetching
# ═════════════════════════════════════════════════════════════════════
#
# Uses pytrends (unofficial Google Trends API client) which handles
# session/cookie negotiation.  Falls back to direct HTTP for the
# daily trending endpoint which doesn't require auth.

_GT_TRENDING = "https://trends.google.com/trends/api/dailytrends"

# Reusable pytrends session (created lazily)
_pytrends_instance: TrendReq | None = None


def _get_pytrends() -> TrendReq | None:
    """Get or create a pytrends session."""
    global _pytrends_instance
    if not _PYTRENDS_AVAILABLE:
        logger.warning("pytrends not installed — Google Trends data unavailable")
        return None
    if _pytrends_instance is None:
        try:
            _pytrends_instance = TrendReq(hl="en-US", tz=240, retries=3, backoff_factor=1.0)
        except Exception as e:
            logger.warning("Failed to create pytrends session: %s", e)
            return None
    return _pytrends_instance


def _parse_gt_json(text: str) -> dict | None:
    """Parse Google Trends JSON response (strip XSSI prefix)."""
    cleaned = text.lstrip()
    if cleaned.startswith(")]}'"):
        cleaned = cleaned[cleaned.index("\n") + 1:]
    try:
        return json.loads(cleaned)
    except (json.JSONDecodeError, ValueError) as e:
        logger.debug("Failed to parse GT JSON: %s", e)
        return None


def fetch_interest_over_time(
    keywords: list[str],
    timeframe: str = "today 3-m",
    geo: str = "US",
) -> dict | None:
    """Fetch Google Trends interest-over-time for up to 5 keywords.

    Uses pytrends for session management. Returns dict with:
      - keywords: list of queried keywords
      - timeframe: the timeframe string used
      - data: list of {date, values: {keyword: score}} dicts
      - averages: {keyword: avg_score}
      - fetched_at: ISO timestamp

    Returns None on failure.
    """
    if not keywords:
        return None
    keywords = keywords[:5]  # GT limit

    cache_key = f"{','.join(sorted(keywords))}|{timeframe}|{geo}"
    with _lock:
        cached = _interest_cache.get(cache_key)
        if cached and time.time() - cached[0] < _INTEREST_TTL:
            return cached[1]

    pt = _get_pytrends()
    if not pt:
        return None

    _rate_limit()

    try:
        pt.build_payload(keywords, cat=0, timeframe=timeframe, geo=geo, gprop="")
        df = pt.interest_over_time()
    except Exception as e:
        logger.warning("pytrends interest_over_time failed for %s: %s", keywords, e)
        # Reset session on failure (stale cookie)
        global _pytrends_instance
        _pytrends_instance = None
        return None

    if df is None or df.empty:
        return None

    # Drop the "isPartial" column if present
    if "isPartial" in df.columns:
        df = df.drop(columns=["isPartial"])

    data_points = []
    sums: dict[str, float] = {kw: 0.0 for kw in keywords}
    count = 0

    for idx, row in df.iterrows():
        date_str = idx.strftime("%b %d, %Y")
        values: dict[str, int] = {}
        for kw in keywords:
            val = int(row.get(kw, 0)) if kw in row.index else 0
            values[kw] = val
            sums[kw] += val
        data_points.append({"date": date_str, "values": values})
        count += 1

    if count == 0:
        return None

    averages = {kw: round(s / count, 1) for kw, s in sums.items()}

    result = {
        "keywords": keywords,
        "timeframe": timeframe,
        "geo": geo,
        "data": data_points,
        "averages": averages,
        "fetched_at": datetime.utcnow().isoformat(),
    }

    with _lock:
        _interest_cache[cache_key] = (time.time(), result)
    return result


# ═════════════════════════════════════════════════════════════════════
# 2. Macro trends — generic market-sentiment keywords
# ═════════════════════════════════════════════════════════════════════

MACRO_CATEGORIES: dict[str, list[str]] = {
    "Market Fear": ["stock market crash", "recession", "bear market", "market correction"],
    "Crypto": ["bitcoin", "ethereum", "buy crypto", "crypto crash"],
    "AI Hype": ["best AI stock", "artificial intelligence", "ChatGPT", "AI investing"],
    "Geopolitics": ["war", "tariffs", "sanctions", "trade war"],
    "Retail Investing": ["how to invest", "best stocks to buy", "Robinhood app", "day trading"],
    "Real Estate": ["housing market crash", "mortgage rates", "buy a house", "rent vs buy"],
    "Gold / Commodities": ["buy gold", "gold price", "oil price", "commodity investing"],
    "Jobs / Economy": ["layoffs", "unemployment", "job market", "hiring freeze"],
}

# Flat list of all macro keywords (for the dashboard)
ALL_MACRO_KEYWORDS = [kw for group in MACRO_CATEGORIES.values() for kw in group]


def fetch_macro_trends(
    category: str | None = None,
    timeframe: str = "today 3-m",
) -> dict | None:
    """Fetch macro trends for a specific category or the top keyword from each.

    If category is None, fetches the first keyword from each category
    (one representative per group — stays within the 5-keyword limit).
    """
    if category and category in MACRO_CATEGORIES:
        keywords = MACRO_CATEGORIES[category][:5]
    else:
        # One representative from each category (up to 5)
        keywords = [kws[0] for kws in list(MACRO_CATEGORIES.values())[:5]]

    cache_key = f"macro:{category or 'overview'}:{timeframe}"
    with _lock:
        cached = _macro_cache.get(cache_key)
        if cached and time.time() - cached[0] < _MACRO_TTL:
            return cached[1]

    result = fetch_interest_over_time(keywords, timeframe=timeframe)
    if result:
        result["category"] = category or "overview"
        with _lock:
            _macro_cache[cache_key] = (time.time(), result)

    return result


# ═════════════════════════════════════════════════════════════════════
# 3. Trending searches → ticker mapping
# ═════════════════════════════════════════════════════════════════════

# ── Known company name → ticker for matching trending searches ──────
_NAME_TO_TICKER: dict[str, str] = {}

def _build_name_to_ticker() -> None:
    """Build a reverse index: lowercase company name → ticker."""
    if _NAME_TO_TICKER:
        return
    for ticker, profile in _TICKER_PROFILES.items():
        name = profile["name"].lower()
        _NAME_TO_TICKER[name] = ticker
        for alt in profile.get("alt_names", []):
            _NAME_TO_TICKER[alt.lower()] = ticker
    # Add some additional well-known mappings
    _extra = {
        "apple": "AAPL", "amazon": "AMZN", "google": "GOOGL",
        "microsoft": "MSFT", "tesla": "TSLA", "nvidia": "NVDA",
        "meta": "META", "netflix": "NFLX", "disney": "DIS",
        "uber": "UBER", "airbnb": "ABNB", "coinbase": "COIN",
        "robinhood": "HOOD", "paypal": "PYPL", "spotify": "SPOT",
        "snapchat": "SNAP", "pinterest": "PINS", "reddit": "RDDT",
        "amd": "AMD", "intel": "INTC", "nike": "NKE",
        "lululemon": "LULU", "adidas": "ADDYY", "costco": "COST",
        "walmart": "WMT", "target": "TGT", "doordash": "DASH",
        "bitcoin": "BTC", "ethereum": "ETH",
    }
    for name, tkr in _extra.items():
        _NAME_TO_TICKER.setdefault(name, tkr)


def _match_trending_to_ticker(query: str) -> str | None:
    """Try to match a trending search query to a known ticker."""
    _build_name_to_ticker()
    q = query.lower().strip()

    # Exact match
    if q in _NAME_TO_TICKER:
        return _NAME_TO_TICKER[q]

    # Check if any company name appears in the query
    for name, ticker in _NAME_TO_TICKER.items():
        if len(name) >= 4 and name in q:  # avoid short false positives
            return ticker

    return None


def fetch_trending_searches(geo: str = "US") -> list[dict] | None:
    """Fetch Google's daily trending searches and map them to tickers.

    Returns list of dicts:
      - query: the trending search term
      - traffic: approximate search volume string (e.g. "500K+")
      - related_queries: list of related queries
      - ticker: matched ticker or None
      - article_title: news article title if available
      - article_url: news article URL if available
    """
    global _trending_cache

    with _lock:
        if _trending_cache and time.time() - _trending_cache[0] < _TRENDING_TTL:
            return _trending_cache[1]

    url = f"{_GT_TRENDING}?hl=en-US&tz=240&geo={geo}&ns=15"
    resp = _gt_get(url)
    if not resp:
        return None

    data = _parse_gt_json(resp.text)
    if not data:
        return None

    days = data.get("default", {}).get("trendingSearchesDays", [])
    results: list[dict] = []

    for day in days[:2]:  # Last 2 days
        for search in day.get("trendingSearches", []):
            title_obj = search.get("title", {})
            query = title_obj.get("query", "")
            traffic = search.get("formattedTraffic", "")

            # Get related queries
            related = [
                r.get("query", "")
                for r in search.get("relatedQueries", [])
                if r.get("query")
            ]

            # Get article info
            articles = search.get("articles", [])
            article_title = articles[0].get("title", "") if articles else ""
            article_url = articles[0].get("url", "") if articles else ""

            # Try to match to a ticker
            ticker = _match_trending_to_ticker(query)
            # Also check related queries if main didn't match
            if not ticker:
                for rq in related:
                    ticker = _match_trending_to_ticker(rq)
                    if ticker:
                        break

            results.append({
                "query": query,
                "traffic": traffic,
                "related_queries": related[:5],
                "ticker": ticker,
                "article_title": article_title,
                "article_url": article_url,
            })

    with _lock:
        _trending_cache = (time.time(), results)
    return results


# ═════════════════════════════════════════════════════════════════════
# Combined fetch for a ticker (used on stock detail page)
# ═════════════════════════════════════════════════════════════════════


def get_ticker_trends(ticker: str) -> dict:
    """Fetch Google Trends data for a specific ticker.

    Returns dict with:
      - keywords: generated keyword set for this ticker
      - intent_trend: interest-over-time for top intent keywords (or None)
      - product_trend: interest-over-time for product keywords (or None)
      - comparison_trend: interest-over-time for comparison keywords (or None)
    """
    ticker = ticker.upper()
    kw = generate_keywords(ticker)

    # Fetch interest for each category (top 5 per group)
    intent_data = fetch_interest_over_time(kw["intent"][:5]) if kw["intent"] else None
    product_data = fetch_interest_over_time(kw["product"][:5]) if kw["product"] else None
    comparison_data = fetch_interest_over_time(kw["comparison"][:5]) if kw["comparison"] else None

    return {
        "ticker": ticker,
        "keywords": kw,
        "intent_trend": intent_data,
        "product_trend": product_data,
        "comparison_trend": comparison_data,
    }


def get_trends_summary(ticker: str) -> dict:
    """Lightweight summary — just keywords + top intent trend.

    Use this for the stock detail page card (avoids 3 separate GT calls).
    """
    ticker = ticker.upper()
    kw = generate_keywords(ticker)

    # Just fetch the top 3 intent keywords (most actionable signal)
    top_intent = kw["intent"][:3]
    trend = fetch_interest_over_time(top_intent) if top_intent else None

    return {
        "ticker": ticker,
        "name": kw["name"],
        "type": kw["type"],
        "keywords": kw,
        "trend": trend,
    }
