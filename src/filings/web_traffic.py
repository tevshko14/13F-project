"""Web traffic data collection — revenue-proxy signals for digital-first companies.

Collects web traffic metrics from multiple free sources and classifies
which stocks have high-confidence web-traffic-to-revenue correlation.

Sources:
  1. SimilarWeb (undocumented extension API) — total visits, bounce rate, traffic sources
  2. Wikipedia Page Views — daily page view counts (public attention proxy)

Data is stored in Supabase ``web_traffic_history`` cold table for long-term
historical analysis. Sector-based auto-filtering ensures we only track
companies where web traffic is a meaningful revenue proxy.

Classification uses GICS sectors + sub-industry overrides to determine
which stocks are "digital-first" (online brokerages, e-commerce, SaaS,
ad-driven platforms, fintech, online marketplaces).
"""

from __future__ import annotations

import logging
import threading
import time
import urllib.parse
import urllib.request
import json
from datetime import datetime, timedelta

import httpx

logger = logging.getLogger(__name__)

# ── Thread lock for cache ────────────────────────────────────────────
_lock = threading.Lock()

# ── In-memory caches ─────────────────────────────────────────────────
_traffic_cache: dict[str, tuple[float, dict]] = {}
_TRAFFIC_TTL = 86_400  # 24 hours — traffic data changes slowly

_wiki_cache: dict[str, tuple[float, dict]] = {}
_WIKI_TTL = 86_400  # 24 hours

_sector_cache: dict[str, tuple[float, dict | None]] = {}
_SECTOR_TTL = 604_800  # 7 days — sectors don't change

# ── Sector-based relevance classification ────────────────────────────
# GICS sectors/sub-industries where web traffic is a strong revenue proxy.

# Sectors that are broadly digital-first
_HIGH_SIGNAL_SECTORS = {
    "Information Technology",
    "Communication Services",
}

# Specific sub-industries within other sectors where traffic matters
_HIGH_SIGNAL_INDUSTRIES = {
    # Consumer Discretionary — e-commerce, online retail
    "internet & direct marketing retail",
    "broadline retail",
    "specialty retail",
    "hotels, resorts & cruise lines",
    "restaurants",
    # Financials — fintech, online brokerages, neobanks
    "financial exchanges & data",
    "consumer finance",
    "transaction & payment processing services",
    "investment banking & brokerage",
    "regional banks",  # neobanks like SoFi
    "diversified financial services",
    # Health Care — telehealth
    "health care technology",
    # Industrials — online marketplaces
    "research & consulting services",
    "human resource & employment services",
}

# Manual overrides: ticker → (is_relevant, reason)
# Use this for edge cases the sector filter misses
_TICKER_OVERRIDES: dict[str, tuple[bool, str]] = {
    # Include: digital-first companies in non-obvious sectors
    "HOOD": (True, "Online brokerage — traffic directly drives trading revenue"),
    "SOFI": (True, "Digital bank & brokerage — user engagement drives deposits & trades"),
    "SQ": (True, "CashApp & Square — online payments platform"),
    "PYPL": (True, "PayPal — online payments, traffic = transaction volume"),
    "AFRM": (True, "Affirm — BNPL fintech, online checkout integration"),
    "COIN": (True, "Coinbase — crypto exchange, traffic = trading volume"),
    "DASH": (True, "DoorDash — online food delivery marketplace"),
    "ABNB": (True, "Airbnb — online travel marketplace"),
    "BKNG": (True, "Booking.com — online travel marketplace"),
    "UBER": (True, "Uber — ride-hailing & delivery platform"),
    "LYFT": (True, "Lyft — ride-hailing platform"),
    "ETSY": (True, "Etsy — online marketplace, traffic = GMV"),
    "CHWY": (True, "Chewy — online pet retail"),
    "W": (True, "Wayfair — online furniture retail"),
    "SHOP": (True, "Shopify — e-commerce platform"),
    "AMZN": (True, "Amazon — e-commerce + AWS, traffic = sales"),
    "NFLX": (True, "Netflix — streaming, traffic = subscriber engagement"),
    "SPOT": (True, "Spotify — streaming, traffic = engagement"),
    "ZM": (True, "Zoom — video platform, traffic = usage"),
    "CRM": (True, "Salesforce — SaaS, traffic = enterprise engagement"),
    "SNOW": (True, "Snowflake — cloud data platform"),
    "NET": (True, "Cloudflare — web infrastructure, traffic proxy"),
    "DDOG": (True, "Datadog — cloud monitoring SaaS"),
    "PLTR": (True, "Palantir — enterprise software"),
    "RDDT": (True, "Reddit — ad-driven social platform"),
    "PINS": (True, "Pinterest — ad-driven platform"),
    "SNAP": (True, "Snap — ad-driven social platform"),
    "TTD": (True, "The Trade Desk — programmatic advertising"),
    # Exclude: companies where website is a brochure
    "BRK-B": (False, "Conglomerate — web traffic not meaningful"),
    "XOM": (False, "Oil & gas — no online revenue"),
    "CVX": (False, "Oil & gas — no online revenue"),
    "JNJ": (False, "Pharma — no direct online sales"),
    "PG": (False, "Consumer staples — offline retail distribution"),
    "KO": (False, "Beverages — offline distribution"),
    "PEP": (False, "Beverages — offline distribution"),
}

# ── Domain mappings for companies (ticker → primary domain) ──────────
# SimilarWeb needs the domain, not the ticker.
_TICKER_TO_DOMAIN: dict[str, str] = {
    "HOOD": "robinhood.com",
    "SOFI": "sofi.com",
    "COIN": "coinbase.com",
    "AMZN": "amazon.com",
    "NFLX": "netflix.com",
    "SPOT": "spotify.com",
    "SHOP": "shopify.com",
    "ETSY": "etsy.com",
    "CHWY": "chewy.com",
    "W": "wayfair.com",
    "DASH": "doordash.com",
    "ABNB": "airbnb.com",
    "BKNG": "booking.com",
    "UBER": "uber.com",
    "LYFT": "lyft.com",
    "PYPL": "paypal.com",
    "SQ": "squareup.com",
    "AFRM": "affirm.com",
    "ZM": "zoom.us",
    "CRM": "salesforce.com",
    "SNOW": "snowflake.com",
    "NET": "cloudflare.com",
    "DDOG": "datadoghq.com",
    "PLTR": "palantir.com",
    "META": "facebook.com",
    "GOOGL": "google.com",
    "GOOG": "google.com",
    "MSFT": "microsoft.com",
    "AAPL": "apple.com",
    "NVDA": "nvidia.com",
    "ADBE": "adobe.com",
    "INTC": "intel.com",
    "AMD": "amd.com",
    "RDDT": "reddit.com",
    "PINS": "pinterest.com",
    "SNAP": "snapchat.com",
    "TTD": "thetradedesk.com",
    "NFLX": "netflix.com",
    "DIS": "disneyplus.com",
}

# ── Wikipedia article mappings (ticker → article title) ──────────────
_TICKER_TO_WIKI: dict[str, str] = {
    "HOOD": "Robinhood_Markets",
    "SOFI": "SoFi",
    "COIN": "Coinbase",
    "AMZN": "Amazon_(company)",
    "NFLX": "Netflix",
    "SPOT": "Spotify",
    "SHOP": "Shopify",
    "ETSY": "Etsy",
    "CHWY": "Chewy_(company)",
    "W": "Wayfair",
    "DASH": "DoorDash",
    "ABNB": "Airbnb",
    "BKNG": "Booking_Holdings",
    "UBER": "Uber",
    "LYFT": "Lyft",
    "PYPL": "PayPal",
    "SQ": "Block,_Inc.",
    "AFRM": "Affirm_(company)",
    "ZM": "Zoom_Video_Communications",
    "CRM": "Salesforce",
    "SNOW": "Snowflake_Inc.",
    "NET": "Cloudflare",
    "DDOG": "Datadog",
    "PLTR": "Palantir_Technologies",
    "META": "Meta_Platforms",
    "GOOGL": "Alphabet_Inc.",
    "GOOG": "Alphabet_Inc.",
    "MSFT": "Microsoft",
    "AAPL": "Apple_Inc.",
    "RDDT": "Reddit",
    "PINS": "Pinterest",
    "SNAP": "Snap_Inc.",
    "TTD": "The_Trade_Desk",
}


# ═════════════════════════════════════════════════════════════════════
# Sector classification
# ═════════════════════════════════════════════════════════════════════


def _get_yf_sector(ticker: str) -> dict | None:
    """Fetch sector/industry from yfinance (cached 7 days)."""
    with _lock:
        cached = _sector_cache.get(ticker)
        if cached and time.time() - cached[0] < _SECTOR_TTL:
            return cached[1]

    try:
        import yfinance as yf

        info = yf.Ticker(ticker).info
        result = {
            "sector": info.get("sector", ""),
            "industry": info.get("industry", ""),
        }
    except Exception as e:
        logger.debug("yfinance sector lookup failed for %s: %s", ticker, e)
        result = None

    with _lock:
        _sector_cache[ticker] = (time.time(), result)
    return result


def is_web_traffic_relevant(ticker: str) -> tuple[bool, str]:
    """Determine if web traffic is a meaningful revenue proxy for this stock.

    Returns (is_relevant, reason) where reason explains the classification.
    """
    ticker = ticker.upper()

    # 1. Check manual overrides first
    if ticker in _TICKER_OVERRIDES:
        return _TICKER_OVERRIDES[ticker]

    # 2. Check sector/industry from yfinance
    sector_info = _get_yf_sector(ticker)
    if not sector_info:
        return False, "Unable to determine sector"

    sector = sector_info.get("sector", "")
    industry = sector_info.get("industry", "").lower()

    # High-signal sectors (broadly digital)
    if sector in _HIGH_SIGNAL_SECTORS:
        return True, f"{sector} — digital-first company, traffic correlates with engagement/revenue"

    # High-signal sub-industries in other sectors
    if industry in _HIGH_SIGNAL_INDUSTRIES:
        return True, f"{industry.title()} — web traffic is a revenue proxy"

    return False, f"{sector} / {industry} — web traffic is not a meaningful revenue signal"


def get_relevance_badge(ticker: str) -> dict | None:
    """Return badge info for the UI if ticker is web-traffic-relevant.

    Returns dict with 'label', 'reason', 'confidence' or None.
    """
    relevant, reason = is_web_traffic_relevant(ticker)
    if not relevant:
        return None
    return {
        "label": "Web Traffic = Revenue Proxy",
        "reason": reason,
        "confidence": "high",
    }


# ═════════════════════════════════════════════════════════════════════
# Data source 1: SimilarWeb (undocumented extension API)
# ═════════════════════════════════════════════════════════════════════

_SW_BASE = "https://data.similarweb.com/api/v1/data"
_SW_TIMEOUT = 10


def _get_domain_for_ticker(ticker: str) -> str | None:
    """Resolve ticker to primary domain."""
    return _TICKER_TO_DOMAIN.get(ticker.upper())


def fetch_similarweb(ticker: str) -> dict | None:
    """Fetch traffic data from SimilarWeb's undocumented extension API.

    Returns dict with traffic metrics or None on failure.
    """
    ticker = ticker.upper()

    # Check cache
    with _lock:
        cached = _traffic_cache.get(ticker)
        if cached and time.time() - cached[0] < _TRAFFIC_TTL:
            return cached[1]

    domain = _get_domain_for_ticker(ticker)
    if not domain:
        return None

    url = f"{_SW_BASE}?domain={domain}"
    try:
        resp = httpx.get(url, timeout=_SW_TIMEOUT, headers={
            "User-Agent": "Mozilla/5.0 (compatible; PaperPanda/1.0)",
        })
        if resp.status_code != 200:
            logger.warning("SimilarWeb returned %d for %s", resp.status_code, domain)
            return None

        raw = resp.json()
        result = _parse_similarweb(raw, domain)

        with _lock:
            _traffic_cache[ticker] = (time.time(), result)
        return result

    except Exception as e:
        logger.warning("SimilarWeb fetch failed for %s: %s", domain, e)
        return None


def _parse_similarweb(raw: dict, domain: str) -> dict:
    """Parse SimilarWeb response into a clean dict."""
    engagements = raw.get("Engagments", {})  # note: their typo, not ours
    traffic_sources = raw.get("TrafficSources", {})
    top_countries = raw.get("TopCountryShares", [])

    # Estimate total monthly visits from engagements
    visits = engagements.get("Visits")

    return {
        "source": "similarweb",
        "domain": domain,
        "global_rank": raw.get("GlobalRank", {}).get("Rank"),
        "country_rank": raw.get("CountryRank", {}).get("Rank"),
        "category_rank": raw.get("CategoryRank", {}).get("Rank"),
        "category": raw.get("Category"),
        "total_visits": visits,
        "avg_visit_duration": engagements.get("TimeOnSite"),
        "pages_per_visit": engagements.get("PagePerVisit"),
        "bounce_rate": engagements.get("BounceRate"),
        "traffic_sources": {
            "direct": traffic_sources.get("Direct"),
            "referral": traffic_sources.get("Referral"),
            "organic_search": traffic_sources.get("Search"),  # organic
            "paid_search": traffic_sources.get("Paid Referrals"),
            "social": traffic_sources.get("Social"),
            "mail": traffic_sources.get("Mail"),
            "display_ads": traffic_sources.get("Display Ads"),
        },
        "top_countries": [
            {"country": c.get("CountryCode"), "share": c.get("Value")}
            for c in (top_countries or [])[:5]
        ],
        "fetched_at": datetime.utcnow().isoformat(),
    }


# ═════════════════════════════════════════════════════════════════════
# Data source 2: Wikipedia Page Views (official REST API)
# ═════════════════════════════════════════════════════════════════════

_WIKI_API = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"


def fetch_wikipedia_views(ticker: str, days: int = 90) -> dict | None:
    """Fetch daily Wikipedia page views for the company.

    Returns dict with daily_views list and summary stats, or None.
    """
    ticker = ticker.upper()

    # Check cache
    with _lock:
        cached = _wiki_cache.get(ticker)
        if cached and time.time() - cached[0] < _WIKI_TTL:
            return cached[1]

    article = _TICKER_TO_WIKI.get(ticker)
    if not article:
        return None

    end = datetime.utcnow()
    start = end - timedelta(days=days)
    start_str = start.strftime("%Y%m%d00")
    end_str = end.strftime("%Y%m%d00")

    url = f"{_WIKI_API}/en.wikipedia/all-access/all-agents/{article}/daily/{start_str}/{end_str}"

    try:
        resp = httpx.get(url, timeout=10, headers={
            "User-Agent": "PaperPanda/1.0 (https://paperpanda.io; mailto:contact@paperpanda.io) httpx/0.27",
            "Accept": "application/json",
        })
        if resp.status_code != 200:
            logger.warning("Wikipedia API returned %d for %s", resp.status_code, article)
            return None

        raw = resp.json()
        items = raw.get("items", [])
        if not items:
            return None

        daily_views = [
            {"date": it["timestamp"][:8], "views": it["views"]}
            for it in items
        ]

        views_list = [d["views"] for d in daily_views]
        result = {
            "source": "wikipedia",
            "article": article,
            "days": len(daily_views),
            "daily_views": daily_views,
            "total_views": sum(views_list),
            "avg_daily_views": round(sum(views_list) / len(views_list)) if views_list else 0,
            "max_daily_views": max(views_list) if views_list else 0,
            "min_daily_views": min(views_list) if views_list else 0,
            "fetched_at": datetime.utcnow().isoformat(),
        }

        with _lock:
            _wiki_cache[ticker] = (time.time(), result)
        return result

    except Exception as e:
        logger.warning("Wikipedia pageviews failed for %s: %s", article, e)
        return None


# ═════════════════════════════════════════════════════════════════════
# Combined fetch (for stock detail page)
# ═════════════════════════════════════════════════════════════════════


def get_web_traffic_data(ticker: str) -> dict:
    """Fetch all web traffic data for a ticker.

    Returns a dict with:
      - relevance: badge info or None
      - similarweb: traffic data or None
      - wikipedia: page view data or None
    """
    ticker = ticker.upper()
    relevance = get_relevance_badge(ticker)

    result: dict = {
        "relevance": relevance,
        "similarweb": None,
        "wikipedia": None,
    }

    # Only fetch traffic data for relevant tickers
    if not relevance:
        return result

    result["similarweb"] = fetch_similarweb(ticker)
    result["wikipedia"] = fetch_wikipedia_views(ticker)
    return result
