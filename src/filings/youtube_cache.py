"""YouTube event / channel persistence helpers — extracted from
``supabase_cache.py`` during audit-sprint-5.

The monolithic ``supabase_cache.py`` mixes generic cache primitives with
domain-specific persistence for 7 different domains (insider trades,
congress trades, youtube, earnings, options, watchlist, admin).  Splitting
them one at a time into dedicated modules reduces the 4,625-line file and
gives each domain a clean import path.

Backward compatibility: ``supabase_cache.py`` re-exports every function
here so existing callers (``supabase_cache.get_youtube_events`` etc.)
keep working without changes.  New code should import from this module
directly:

    from filings.youtube_cache import get_youtube_events
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)


def _get_client():
    """Lazy proxy to ``supabase_cache._get_client``.

    Defers the import until first call so the module load order doesn't
    matter: ``supabase_cache.py`` can import from here at its tail, and
    this module can still reach back into it — without a Python circular
    import at module-load time (``youtube_cache`` doesn't depend on
    ``supabase_cache`` being fully loaded until a function is actually
    invoked, by which point both modules are initialised).
    """
    from filings.supabase_cache import _get_client as _delegate
    return _delegate()


# ── Column projections (keep in sync with Supabase table schemas) ─────

_YOUTUBE_EVENT_COLS = (
    "video_id,channel_id,channel_name,title,scheduled_at,"
    "event_type,sentiment,tickers,impact_score,subscriber_count,"
    "avg_views,frequency_alert,frequency_detail,thumbnail_url,"
    "video_url,duration,content_type"
)

_YOUTUBE_CHANNEL_COLS = (
    "channel_id,channel_name,handle,subscriber_count,"
    "avg_views_30d,avg_posts_per_week,thumbnail_url"
)


# ── Helpers ───────────────────────────────────────────────────────────


def _get_existing_video_ids(days: int = 14) -> set[str]:
    """Fetch video_id values from recent youtube_events (lightweight).

    Only fetches the unique key column — no row data.  Used to skip
    re-uploading events that already exist in Supabase.
    """
    client = _get_client()
    if client is None:
        return set()

    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

    try:
        resp = (
            client.table("youtube_events")
            .select("video_id")
            .gte("created_at", cutoff)
            .execute()
        )
        return {row["video_id"] for row in (resp.data or []) if row.get("video_id")}
    except Exception:
        return set()  # On failure, fall through to full upsert


def upsert_youtube_events(rows: list[dict]) -> int:
    """Batch upsert rows into ``youtube_events``, skipping existing ones.

    First fetches existing video_id keys (lightweight query) and filters
    them out.  Only new events or events needing updates (upcoming streams
    whose status may have changed) are sent to Supabase.

    Uses ``ON CONFLICT (video_id) DO UPDATE`` for deduplication safety.
    Returns the number of rows upserted, or 0 on failure.
    """
    client = _get_client()
    if client is None:
        return 0

    # Filter out rows that already exist — but ALWAYS re-upsert upcoming
    # events since their status/scheduled_at may have changed
    existing_ids = _get_existing_video_ids()
    new_rows = []
    for r in rows:
        vid = r.get("video_id", "")
        if vid not in existing_ids:
            new_rows.append(r)  # Brand new event
        elif r.get("event_type") == "upcoming":
            new_rows.append(r)  # Upcoming may have updated fields

    skipped = len(rows) - len(new_rows)
    if not new_rows:
        logger.info("All %d youtube events already exist — skipping upsert", len(rows))
        return len(rows)

    if skipped > 0:
        logger.info(
            "YouTube events: %d to upsert, %d skipped (already exist)",
            len(new_rows),
            skipped,
        )

    upserted = 0
    CHUNK = 50
    for i in range(0, len(new_rows), CHUNK):
        chunk = new_rows[i : i + CHUNK]
        try:
            client.table("youtube_events").upsert(
                chunk, on_conflict="video_id"
            ).execute()
            upserted += len(chunk)
        except Exception as exc:
            logger.warning("upsert_youtube_events chunk %d failed: %s", i, exc)

    return upserted


def upsert_youtube_channels(rows: list[dict]) -> int:
    """Upsert rows into ``youtube_channels``.

    Uses ``ON CONFLICT (channel_id) DO UPDATE``.
    Returns the number of rows upserted, or 0 on failure.
    """
    client = _get_client()
    if client is None:
        return 0

    upserted = 0
    for row in rows:
        try:
            client.table("youtube_channels").upsert(
                row, on_conflict="channel_id"
            ).execute()
            upserted += 1
        except Exception as exc:
            logger.warning(
                "upsert_youtube_channels failed for %s: %s",
                row.get("channel_id"),
                exc,
            )

    return upserted


def get_youtube_events(
    limit: int = 50,
    sentiment: str | None = None,
    min_impact: int | None = None,
) -> list[dict] | None:
    """Query ``youtube_events`` for upcoming streams, newest scheduled first.

    Filters to ``event_type = 'upcoming'`` so recent uploads are excluded
    (use :func:`get_recent_youtube_uploads` for those).
    """
    client = _get_client()
    if client is None:
        return None
    try:
        # Only return events scheduled within the last 6 hours or in the future
        # so stale "upcoming" rows from years ago never appear.
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=6)).isoformat()
        query = (
            client.table("youtube_events")
            .select(_YOUTUBE_EVENT_COLS)
            .eq("event_type", "upcoming")
            .gte("scheduled_at", cutoff)
            .order("scheduled_at", desc=True)
            .limit(limit)
        )
        if sentiment:
            query = query.eq("sentiment", sentiment)
        if min_impact is not None:
            query = query.gte("impact_score", min_impact)
        resp = query.execute()
        return resp.data
    except Exception as exc:
        logger.warning("get_youtube_events failed: %s", exc)
        return None


def get_youtube_channels() -> list[dict] | None:
    """Return all tracked ``youtube_channels`` rows."""
    client = _get_client()
    if client is None:
        return None
    try:
        resp = client.table("youtube_channels").select(_YOUTUBE_CHANNEL_COLS).execute()
        return resp.data
    except Exception as exc:
        logger.warning("get_youtube_channels failed: %s", exc)
        return None


# ── High-impact events: cached in-memory to spare /retail page loads ──

_high_impact_cache: tuple[float, list[dict]] | None = None
_HIGH_IMPACT_TTL = 300  # 5 minutes


def get_high_impact_youtube_events(min_score: int = 9) -> list[dict] | None:
    """Return upcoming events with ``impact_score >= min_score``.

    Results are cached in-memory for 5 minutes to avoid hitting
    Supabase on every ``/retail`` page load.
    """
    global _high_impact_cache

    # ── L1: in-memory cache ──
    if _high_impact_cache is not None:
        ts, data = _high_impact_cache
        if time.time() - ts < _HIGH_IMPACT_TTL:
            return data

    client = _get_client()
    if client is None:
        return None
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=6)).isoformat()
        resp = (
            client.table("youtube_events")
            .select(_YOUTUBE_EVENT_COLS)
            .gte("impact_score", min_score)
            .eq("event_type", "upcoming")
            .gte("scheduled_at", cutoff)
            .order("scheduled_at", desc=True)
            .limit(10)
            .execute()
        )
        result = resp.data
        _high_impact_cache = (time.time(), result)
        return result
    except Exception as exc:
        logger.warning("get_high_impact_youtube_events failed: %s", exc)
        return None


def get_recent_youtube_uploads(limit: int = 20) -> list[dict] | None:
    """Query ``youtube_events`` for recently posted videos.

    Fetches rows with ``event_type = 'recent_upload'``, ordered by
    ``scheduled_at DESC`` (which stores ``published_at`` for uploads).

    Returns list of row dicts, or ``None`` if Supabase is unavailable.
    """
    client = _get_client()
    if client is None:
        return None
    try:
        resp = (
            client.table("youtube_events")
            .select(_YOUTUBE_EVENT_COLS)
            .eq("event_type", "recent_upload")
            .order("scheduled_at", desc=True)
            .limit(limit)
            .execute()
        )
        return resp.data
    except Exception as exc:
        logger.warning("get_recent_youtube_uploads failed: %s", exc)
        return None
