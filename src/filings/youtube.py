"""YouTube event calendar data layer.

Provides read access to youtube_events from Supabase with an
in-memory cache (L1) for fast page loads.

Data flow:
  L1: thread-safe in-memory dict with short TTL
  L2: Supabase youtube_events + youtube_channels tables
  (No L3 fallback -- data is only populated by youtube_sync worker)
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
_EVENTS_TTL = 300    # 5 minutes
_CHANNELS_TTL = 600  # 10 minutes


def get_upcoming_events(limit: int = 50) -> list[dict]:
    """Return upcoming YouTube events, newest scheduled first. L1-cached."""
    global _events_cache

    with _lock:
        if _events_cache is not None:
            ts, data = _events_cache
            if time.time() - ts < _EVENTS_TTL:
                return data[:limit]

    events = supabase_cache.get_youtube_events(limit=limit) or []

    with _lock:
        _events_cache = (time.time(), events)

    return events[:limit]


def get_channels() -> list[dict]:
    """Return tracked YouTube channels with metadata. L1-cached."""
    global _channels_cache

    with _lock:
        if _channels_cache is not None:
            ts, data = _channels_cache
            if time.time() - ts < _CHANNELS_TTL:
                return data

    channels = supabase_cache.get_youtube_channels() or []

    with _lock:
        _channels_cache = (time.time(), channels)

    return channels


def get_high_impact_events(min_score: int = 9) -> list[dict]:
    """Return upcoming events with impact >= min_score (for toast notifications).

    Not cached -- called infrequently (on page load only).
    """
    return supabase_cache.get_high_impact_youtube_events(min_score) or []


def build_calendar_data(events: list[dict], channels: list[dict]) -> dict:
    """Build enriched calendar data for the frontend.

    Returns:
        {
            "events": [...],
            "channels": [...],
            "high_impact": [...],
            "stats": {
                "total_upcoming": int,
                "bullish_count": int,
                "bearish_count": int,
                "frequency_alerts": int,
            }
        }
    """
    bullish = sum(1 for e in events if e.get("sentiment") == "bullish")
    bearish = sum(1 for e in events if e.get("sentiment") == "bearish")
    freq_alerts = sum(1 for e in events if e.get("frequency_alert"))
    high_impact = [e for e in events if (e.get("impact_score") or 0) >= 9]

    return {
        "events": events,
        "channels": channels,
        "high_impact": high_impact,
        "stats": {
            "total_upcoming": len(events),
            "bullish_count": bullish,
            "bearish_count": bearish,
            "frequency_alerts": freq_alerts,
        },
    }
