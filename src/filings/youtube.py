"""YouTube event calendar data layer.

Provides read access to youtube_events from Supabase with a
tiered fallback system so users never see errors.

Data flow:
  L1: thread-safe in-memory cache with short TTL (fresh hit)
  L2: Supabase youtube_events + youtube_channels tables
  L3: Static channel list fallback (always available)
  L4: Stale L1 data (never show empty/error to users)
"""
from __future__ import annotations

import logging
import threading
import time

from filings import supabase_cache

logger = logging.getLogger(__name__)

_lock = threading.Lock()
_events_cache: tuple[float, list[dict]] | None = None
_channels_cache: tuple[float, list[dict]] | None = None
_recent_cache: tuple[float, list[dict]] | None = None
_EVENTS_TTL = 300    # 5 minutes
_CHANNELS_TTL = 600  # 10 minutes
_RECENT_TTL = 300    # 5 minutes


# ── L3 static fallback: always available even when Supabase is down ──

_STATIC_CHANNELS: list[dict] = [
    {"channel_id": "UCnMn36GT_H0X-w5_ckLtlgQ", "channel_name": "Financial Education", "handle": "@FinancialEducation", "subscriber_count": 0, "avg_views_30d": 0, "avg_posts_per_week": 7.0, "thumbnail_url": ""},
    {"channel_id": "UCbta0n8i6Rljh0obO7HzG9A", "channel_name": "Joseph Carlson", "handle": "@JosephCarlsonShow", "subscriber_count": 0, "avg_views_30d": 0, "avg_posts_per_week": 2.0, "thumbnail_url": ""},
    {"channel_id": "UChvd7RCRJS50RWlwbfcwr3A", "channel_name": "Tevis (FunOfInvesting)", "handle": "@FunofInvesting", "subscriber_count": 0, "avg_views_30d": 0, "avg_posts_per_week": 7.0, "thumbnail_url": ""},
    {"channel_id": "UCyZNir5FhvazX5L3_Q77UbA", "channel_name": "MattMoney", "handle": "@RealMattMoney", "subscriber_count": 0, "avg_views_30d": 0, "avg_posts_per_week": 7.0, "thumbnail_url": ""},
    {"channel_id": "UCD0yDGUSqKLyHviB6FUZzzg", "channel_name": "Kross Roads", "handle": "@Kross_Roads-g4j", "subscriber_count": 0, "avg_views_30d": 0, "avg_posts_per_week": 7.0, "thumbnail_url": ""},
    {"channel_id": "UCPss8jtpAyp3k829QVX3jhQ", "channel_name": "Steven Fiorillo", "handle": "@stevenfiorillo1", "subscriber_count": 0, "avg_views_30d": 0, "avg_posts_per_week": 7.0, "thumbnail_url": ""},
    {"channel_id": "UCvWx0-NX-9qVLCSW9yjdX-g", "channel_name": "Futurenvesting", "handle": "@Futurenvesting", "subscriber_count": 0, "avg_views_30d": 0, "avg_posts_per_week": 7.0, "thumbnail_url": ""},
    {"channel_id": "UCjZnbgPb08NFg7MHyPQRZ3Q", "channel_name": "Amit Investing", "handle": "@amitinvesting", "subscriber_count": 0, "avg_views_30d": 0, "avg_posts_per_week": 7.0, "thumbnail_url": ""},
    {"channel_id": "UCrGLm-Drgv0vbbemwwHeXJw", "channel_name": "Couch Investor", "handle": "@CouchInvestor", "subscriber_count": 0, "avg_views_30d": 0, "avg_posts_per_week": 7.0, "thumbnail_url": ""},
    {"channel_id": "UCgYKMfmLTViSE7Qj0Mj_Mhw", "channel_name": "Endicott Invests", "handle": "@EndicottInvests", "subscriber_count": 0, "avg_views_30d": 0, "avg_posts_per_week": 7.0, "thumbnail_url": ""},
    {"channel_id": "UCEXnaoFOX1P-4pyq2LbruYA", "channel_name": "Kris Patel", "handle": "@KrisPatel99", "subscriber_count": 0, "avg_views_30d": 0, "avg_posts_per_week": 7.0, "thumbnail_url": ""},
]


# ── Stale-while-revalidate cache helpers ─────────────────────────────


def _get_cached_with_stale(
    cache_ref: tuple[float, list[dict]] | None,
    ttl: int,
) -> tuple[list[dict] | None, bool]:
    """Return ``(data, is_fresh)`` -- stale data returned instead of None.

    Unlike a strict TTL check that returns None on expiry, this always
    returns the last cached value (even if stale) so users never see an
    empty/error state while upstream sources are refreshing.
    """
    if cache_ref is not None:
        ts, data = cache_ref
        is_fresh = (time.time() - ts) < ttl
        return data, is_fresh
    return None, False


# ── Public API ───────────────────────────────────────────────────────


def get_upcoming_events(limit: int = 50) -> list[dict]:
    """Return upcoming YouTube events with 4-tier fallback.

    L1: Fresh in-memory cache (5 min TTL)
    L2: Supabase youtube_events query
    L3: (no upstream for events -- only populated by sync worker)
    L4: Stale L1 data -- never show empty/error to users
    """
    global _events_cache

    # ── L1: fresh in-memory cache ──
    with _lock:
        stale_data, is_fresh = _get_cached_with_stale(_events_cache, _EVENTS_TTL)
    if stale_data is not None and is_fresh:
        return stale_data[:limit]

    # ── L2: Supabase query ──
    events = supabase_cache.get_youtube_events(limit=limit)
    if events is not None:
        with _lock:
            _events_cache = (time.time(), events)
        return events[:limit]

    logger.warning("YouTube events L2 (Supabase) returned None")

    # ── L4: stale L1 data -- better than nothing ──
    if stale_data is not None:
        logger.info(
            "Returning stale YouTube events (%d items)", len(stale_data),
        )
        return stale_data[:limit]

    return []


def get_channels() -> list[dict]:
    """Return tracked YouTube channels with 4-tier fallback.

    L1: Fresh in-memory cache (10 min TTL)
    L2: Supabase youtube_channels query
    L3: Static channel list (hardcoded, always available)
    L4: Stale L1 data
    """
    global _channels_cache

    # ── L1: fresh in-memory cache ──
    with _lock:
        stale_data, is_fresh = _get_cached_with_stale(
            _channels_cache, _CHANNELS_TTL,
        )
    if stale_data is not None and is_fresh:
        return stale_data

    # ── L2: Supabase query ──
    channels = supabase_cache.get_youtube_channels()
    if channels:
        with _lock:
            _channels_cache = (time.time(), channels)
        return channels

    logger.warning("YouTube channels L2 (Supabase) returned None/empty")

    # ── L3: static fallback -- always available ──
    # This guarantees the channels table always renders even on first
    # deploy before the sync worker has run.
    if not stale_data:
        logger.info("Using static channel list fallback (L3)")
        with _lock:
            _channels_cache = (time.time(), _STATIC_CHANNELS)
        return _STATIC_CHANNELS

    # ── L4: stale L1 data ──
    logger.info(
        "Returning stale YouTube channels (%d items)", len(stale_data),
    )
    return stale_data


def get_recent_uploads(limit: int = 20) -> list[dict]:
    """Return recently posted videos with 3-tier fallback.

    L1: Fresh in-memory cache (5 min TTL)
    L2: Supabase youtube_events query (event_type = 'recent_upload')
    L4: Stale L1 data -- never show empty/error to users
    """
    global _recent_cache

    # ── L1: fresh in-memory cache ──
    with _lock:
        stale_data, is_fresh = _get_cached_with_stale(_recent_cache, _RECENT_TTL)
    if stale_data is not None and is_fresh:
        return stale_data[:limit]

    # ── L2: Supabase query ──
    uploads = supabase_cache.get_recent_youtube_uploads(limit=limit)
    if uploads is not None:
        with _lock:
            _recent_cache = (time.time(), uploads)
        return uploads[:limit]

    logger.warning("Recent uploads L2 (Supabase) returned None")

    # ── L4: stale L1 data -- better than nothing ──
    if stale_data is not None:
        logger.info(
            "Returning stale recent uploads (%d items)", len(stale_data),
        )
        return stale_data[:limit]

    return []


def get_high_impact_events(min_score: int = 9) -> list[dict]:
    """Return upcoming events with impact >= min_score (for toast notifications).

    Wrapped in try/except -- toast failure should never break page load.
    """
    try:
        return supabase_cache.get_high_impact_youtube_events(min_score) or []
    except Exception as exc:
        logger.warning("get_high_impact_events failed: %s", exc)
        return []


def build_calendar_data(events: list[dict], channels: list[dict], recent_uploads: list[dict] | None = None) -> dict:
    """Build enriched calendar data for the frontend.

    Returns:
        {
            "events": [...],
            "channels": [...],
            "recent_uploads": [...],
            "high_impact": [...],
            "stats": {
                "total_upcoming": int,
                "bullish_count": int,
                "bearish_count": int,
                "frequency_alerts": int,
                "recent_count": int,
            }
        }
    """
    recent = recent_uploads or []
    bullish = sum(1 for e in events if e.get("sentiment") == "bullish")
    bearish = sum(1 for e in events if e.get("sentiment") == "bearish")
    freq_alerts = sum(1 for e in events if e.get("frequency_alert"))
    high_impact = [e for e in events if (e.get("impact_score") or 0) >= 9]

    return {
        "events": events,
        "channels": channels,
        "recent_uploads": recent,
        "high_impact": high_impact,
        "stats": {
            "total_upcoming": len(events),
            "bullish_count": bullish,
            "bearish_count": bearish,
            "frequency_alerts": freq_alerts,
            "recent_count": len(recent),
        },
    }
