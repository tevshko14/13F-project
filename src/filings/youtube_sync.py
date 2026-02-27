"""YouTube event calendar sync worker -- polls upcoming live streams.

Designed to run as a Railway Cron Job every 6 hours.
Uses YouTube Data API v3 search.list to find upcoming live streams
from tracked finance YouTubers, parses titles for ticker mentions
and sentiment, computes impact scores, and upserts to Supabase.

Usage:
    uv run filings-youtube-sync

Quota budget (10,000 units/day):
    search.list:     11 channels x 100 units x 4 polls = 4,400
    channels.list:   1 batch call x 1 unit x 4 polls   =     4
    activities.list: 11 channels x 1 unit x 4 polls    =    44
    videos.list:     ~10 batch calls x 1 unit x 4 polls =    40
    Total: ~4,488 units/day (45% of 10,000 quota)
"""

from __future__ import annotations

import logging
import math
import os
import re
import time
import uuid
from datetime import datetime, timedelta, timezone

import httpx

from filings import notifications, supabase_cache

# ── Logging ──────────────────────────────────────────────────────────


def _setup_logging() -> None:
    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    if os.environ.get("RAILWAY_ENVIRONMENT"):
        fmt = (
            '{"time":"%(asctime)s","level":"%(levelname)s",'
            '"name":"%(name)s","msg":"%(message)s"}'
        )
    else:
        fmt = "%(asctime)s %(levelname)-8s %(name)s -- %(message)s"
    logging.basicConfig(level=log_level, format=fmt, force=True)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


logger = logging.getLogger(__name__)


# ── Channel Gold List ────────────────────────────────────────────────
# channel_id -> {name, handle, baseline_posts_per_week}
# Channel IDs resolved manually. baseline is approximate from known schedules.

_CHANNELS: dict[str, dict] = {
    "UCnMn36GT_H0X-w5_ckLtlgQ": {
        "name": "Financial Education",
        "handle": "@FinancialEducation",
        "baseline_posts_per_week": 7.0,
    },
    "UCbta0n8i6Rljh0obO7HzG9A": {
        "name": "Joseph Carlson",
        "handle": "@JosephCarlsonShow",
        "baseline_posts_per_week": 2.0,
    },
    "UChvd7RCRJS50RWlwbfcwr3A": {
        "name": "Tevis (FunOfInvesting)",
        "handle": "@FunofInvesting",
        "baseline_posts_per_week": 7.0,
    },
    "UCyZNir5FhvazX5L3_Q77UbA": {
        "name": "MattMoney",
        "handle": "@RealMattMoney",
        "baseline_posts_per_week": 7.0,
    },
    "UCD0yDGUSqKLyHviB6FUZzzg": {
        "name": "Kross Roads",
        "handle": "@Kross_Roads-g4j",
        "baseline_posts_per_week": 7.0,
    },
    "UCPss8jtpAyp3k829QVX3jhQ": {
        "name": "Steven Fiorillo",
        "handle": "@stevenfiorillo1",
        "baseline_posts_per_week": 7.0,
    },
    "UCvWx0-NX-9qVLCSW9yjdX-g": {
        "name": "Futurenvesting",
        "handle": "@Futurenvesting",
        "baseline_posts_per_week": 7.0,
    },
    "UCjZnbgPb08NFg7MHyPQRZ3Q": {
        "name": "Amit Investing",
        "handle": "@amitinvesting",
        "baseline_posts_per_week": 7.0,
    },
    "UCrGLm-Drgv0vbbemwwHeXJw": {
        "name": "Couch Investor",
        "handle": "@CouchInvestor",
        "baseline_posts_per_week": 7.0,
    },
    "UCgYKMfmLTViSE7Qj0Mj_Mhw": {
        "name": "Endicott Invests",
        "handle": "@EndicottInvests",
        "baseline_posts_per_week": 7.0,
    },
    "UCEXnaoFOX1P-4pyq2LbruYA": {
        "name": "Kris Patel",
        "handle": "@KrisPatel99",
        "baseline_posts_per_week": 7.0,
    },
}


# ── Ticker + Company Name Parsing ────────────────────────────────────

_COMPANY_TO_TICKER: dict[str, str] = {
    "tesla": "TSLA",
    "apple": "AAPL",
    "amazon": "AMZN",
    "google": "GOOGL",
    "alphabet": "GOOGL",
    "microsoft": "MSFT",
    "nvidia": "NVDA",
    "meta": "META",
    "facebook": "META",
    "netflix": "NFLX",
    "amd": "AMD",
    "intel": "INTC",
    "palantir": "PLTR",
    "coinbase": "COIN",
    "sofi": "SOFI",
    "rivian": "RIVN",
    "lucid": "LCID",
    "nio": "NIO",
    "disney": "DIS",
    "berkshire": "BRK.B",
    "boeing": "BA",
    "walmart": "WMT",
    "costco": "COST",
    "jpmorgan": "JPM",
    "goldman": "GS",
    "paypal": "PYPL",
    "shopify": "SHOP",
    "snowflake": "SNOW",
    "crowdstrike": "CRWD",
    "datadog": "DDOG",
    "robinhood": "HOOD",
    "gamestop": "GME",
    "amc": "AMC",
    "spotify": "SPOT",
    "uber": "UBER",
    "airbnb": "ABNB",
    "block": "SQ",
    "square": "SQ",
    "broadcom": "AVGO",
    "salesforce": "CRM",
    "roku": "ROKU",
    "snap": "SNAP",
    "pinterest": "PINS",
    "roblox": "RBLX",
    "draftkings": "DKNG",
    "affirm": "AFRM",
}

# Cashtag regex: $TSLA, $aapl (1-5 letters after $)
_CASHTAG_RE = re.compile(r"\$([A-Za-z]{1,5})\b")

# Company name regex (case-insensitive, word boundaries)
_COMPANY_RE = re.compile(
    r"\b(" + "|".join(re.escape(k) for k in _COMPANY_TO_TICKER) + r")\b",
    re.IGNORECASE,
)


def parse_tickers(title: str) -> list[str]:
    """Extract unique ticker symbols from a video title.

    Detects cashtags ($TSLA) and known company names (Tesla).
    Returns a deduplicated, uppercase, sorted list.
    """
    tickers: set[str] = set()

    for match in _CASHTAG_RE.finditer(title):
        tickers.add(match.group(1).upper())

    for match in _COMPANY_RE.finditer(title):
        company = match.group(1).lower()
        if company in _COMPANY_TO_TICKER:
            tickers.add(_COMPANY_TO_TICKER[company])

    return sorted(tickers)


# ── Sentiment Classification ─────────────────────────────────────────

_BULLISH_KEYWORDS = [
    "buy",
    "buying",
    "bullish",
    "moon",
    "mooning",
    "rocket",
    "surge",
    "surging",
    "rally",
    "breakout",
    "upside",
    "opportunity",
    "undervalued",
    "all-in",
    "going up",
    "massive gains",
    "short squeeze",
    "to the moon",
    "long",
    "accumulate",
    "bargain",
    "dip buy",
    "buy the dip",
    "price target raised",
    "upgrade",
]

_BEARISH_KEYWORDS = [
    "sell",
    "selling",
    "bearish",
    "crash",
    "crashing",
    "dump",
    "dumping",
    "collapse",
    "bubble",
    "overvalued",
    "short",
    "shorting",
    "warning",
    "danger",
    "avoid",
    "panic",
    "plunge",
    "plummeting",
    "downgrade",
    "price target cut",
    "going down",
    "falling",
    "recession",
    "bear market",
    "get out",
    "liquidate",
]


def classify_sentiment(title: str) -> str:
    """Classify a video title as bullish, bearish, or neutral.

    Simple keyword scoring -- count matches in each list, highest wins.
    Tie or no matches -> neutral.
    """
    lower = title.lower()
    bull_score = sum(1 for kw in _BULLISH_KEYWORDS if kw in lower)
    bear_score = sum(1 for kw in _BEARISH_KEYWORDS if kw in lower)

    if bull_score > bear_score:
        return "bullish"
    elif bear_score > bull_score:
        return "bearish"
    return "neutral"


# ── Impact Score ─────────────────────────────────────────────────────


def compute_impact_score(subscriber_count: int, avg_views: int) -> int:
    """Compute Retail-Impact score 1-10.

    Log-scaled: 100K subs ~ 5, 1M ~ 7, 5M ~ 9, 10M = 10.
    Weighted 50/50 between subscriber reach and view engagement.
    """
    if subscriber_count <= 0 and avg_views <= 0:
        return 1

    sub_score = 0.0
    if subscriber_count > 0:
        sub_score = max(0.0, min(10.0, (math.log10(subscriber_count) - 4) * 2.5))

    view_score = 0.0
    if avg_views > 0:
        view_score = max(0.0, min(10.0, (math.log10(avg_views) - 3.5) * 2.5))

    combined = 0.5 * sub_score + 0.5 * view_score
    return max(1, min(10, round(combined)))


# ── Frequency Alert ──────────────────────────────────────────────────


def check_frequency_alert(
    recent_activity_count: int,
    recent_days: int,
    baseline_posts_per_week: float,
) -> tuple[bool, str]:
    """Check if posting frequency exceeds 2x the baseline.

    Returns (is_alert, detail_string).
    """
    if baseline_posts_per_week <= 0 or recent_days <= 0:
        return False, ""

    recent_rate_per_week = (recent_activity_count / recent_days) * 7
    ratio = recent_rate_per_week / baseline_posts_per_week

    if ratio >= 2.0:
        detail = (
            f"{recent_activity_count}x in {recent_days} days "
            f"vs {baseline_posts_per_week:.1f}x/week avg"
        )
        return True, detail

    return False, ""


# ── YouTube API ──────────────────────────────────────────────────────

_YT_API_BASE = "https://www.googleapis.com/youtube/v3"
_DELAY_BETWEEN_CHANNELS = 1.0  # seconds


def _yt_get(endpoint: str, params: dict) -> dict | None:
    """Make an authenticated YouTube Data API request."""
    api_key = os.environ.get("YOUTUBE_API_KEY", "")
    if not api_key:
        logger.error("YOUTUBE_API_KEY not set -- skipping API call")
        return None
    params["key"] = api_key
    try:
        resp = httpx.get(
            f"{_YT_API_BASE}/{endpoint}",
            params=params,
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        logger.warning("YouTube API %s failed: %s", endpoint, exc)
        return None


def _fetch_channel_stats(channel_ids: list[str]) -> dict[str, dict]:
    """Batch fetch channel statistics and thumbnails (1 unit, up to 50 IDs).

    Returns {channel_id: {subscriber_count, view_count, video_count, thumbnail_url}}.
    """
    result: dict[str, dict] = {}
    data = _yt_get(
        "channels",
        {
            "part": "snippet,statistics",
            "id": ",".join(channel_ids),
        },
    )
    if data and "items" in data:
        for item in data["items"]:
            cid = item["id"]
            stats = item.get("statistics", {})
            snippet = item.get("snippet", {})
            thumbnails = snippet.get("thumbnails", {})
            thumb_url = thumbnails.get("default", {}).get("url", "") or thumbnails.get(
                "medium", {}
            ).get("url", "")
            result[cid] = {
                "subscriber_count": int(stats.get("subscriberCount", 0)),
                "view_count": int(stats.get("viewCount", 0)),
                "video_count": int(stats.get("videoCount", 1)),
                "thumbnail_url": thumb_url,
            }
    return result


def _fetch_recent_activities(
    channel_id: str, channel_name: str, days: int = 2
) -> list[dict]:
    """Fetch recent uploads via activities.list (1 unit per call).

    Returns a list of dicts with video metadata for each upload:
    ``{video_id, title, thumbnail_url, published_at, channel_id, channel_name}``

    The ``contentDetails.upload.videoId`` path is the standard activities
    API structure for upload-type activities.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    data = _yt_get(
        "activities",
        {
            "part": "snippet,contentDetails",
            "channelId": channel_id,
            "publishedAfter": cutoff,
            "maxResults": 50,
        },
    )
    results: list[dict] = []
    if data and "items" in data:
        for it in data["items"]:
            snippet = it.get("snippet", {})
            if snippet.get("type") != "upload":
                continue
            content = it.get("contentDetails", {})
            video_id = content.get("upload", {}).get("videoId")
            if not video_id:
                continue
            thumbnails = snippet.get("thumbnails", {})
            thumb_url = (
                thumbnails.get("high", {}).get("url")
                or thumbnails.get("medium", {}).get("url")
                or thumbnails.get("default", {}).get("url", "")
            )
            results.append(
                {
                    "video_id": video_id,
                    "title": snippet.get("title", ""),
                    "thumbnail_url": thumb_url,
                    "published_at": snippet.get("publishedAt", ""),
                    "channel_id": channel_id,
                    "channel_name": channel_name,
                }
            )
    return results


def _fetch_upcoming_videos(channel_id: str) -> list[dict]:
    """Search for upcoming live streams on a channel (100 units per call)."""
    data = _yt_get(
        "search",
        {
            "part": "snippet",
            "channelId": channel_id,
            "type": "video",
            "eventType": "upcoming",
            "maxResults": 10,
            "order": "date",
        },
    )
    if data and "items" in data:
        return data["items"]
    return []


def _parse_iso8601_duration(iso_dur: str) -> str:
    """Convert ISO 8601 duration (PT1H2M30S) to human-readable (1:02:30).

    Returns empty string for unparseable or zero-length durations.
    """
    if not iso_dur:
        return ""
    m = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", iso_dur)
    if not m:
        return ""
    hours = int(m.group(1) or 0)
    minutes = int(m.group(2) or 0)
    seconds = int(m.group(3) or 0)
    if hours == 0 and minutes == 0 and seconds == 0:
        return ""
    if hours > 0:
        return f"{hours}:{minutes:02d}:{seconds:02d}"
    return f"{minutes}:{seconds:02d}"


def _fetch_video_details(video_ids: list[str]) -> dict[str, dict]:
    """Batch fetch video details for scheduled start times (1 unit per batch of 50).

    Returns {video_id: {scheduled_at, view_count, duration, content_type}}.
    """
    if not video_ids:
        return {}
    result: dict[str, dict] = {}
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i : i + 50]
        data = _yt_get(
            "videos",
            {
                "part": "contentDetails,liveStreamingDetails,statistics,snippet",
                "id": ",".join(batch),
            },
        )
        if data and "items" in data:
            for item in data["items"]:
                vid = item["id"]
                live = item.get("liveStreamingDetails", {})
                stats = item.get("statistics", {})
                content = item.get("contentDetails", {})
                snippet = item.get("snippet", {})

                # liveBroadcastContent: "live", "upcoming", or "none"
                broadcast = snippet.get("liveBroadcastContent", "none")
                if broadcast == "none" and live:
                    # Has liveStreamingDetails but not currently live/upcoming
                    content_type = "was_live"
                elif broadcast in ("live", "upcoming"):
                    content_type = broadcast
                else:
                    content_type = "video"

                result[vid] = {
                    "scheduled_at": live.get("scheduledStartTime"),
                    "view_count": int(stats.get("viewCount", 0)),
                    "duration": _parse_iso8601_duration(content.get("duration", "")),
                    "content_type": content_type,
                }
    return result


# ── Main Sync ────────────────────────────────────────────────────────


def sync_youtube_events() -> dict:
    """Poll all tracked channels, compute scores, upsert to Supabase."""
    run_id = f"youtube-sync-{uuid.uuid4().hex[:8]}"
    supabase_cache.create_sync_log(run_id)

    channel_ids = list(_CHANNELS.keys())

    # 1. Batch fetch channel stats (1 API unit total)
    channel_stats = _fetch_channel_stats(channel_ids)
    logger.info(
        "Fetched stats for %d/%d channels", len(channel_stats), len(channel_ids)
    )

    # Update youtube_channels table
    channel_rows: list[dict] = []
    now_iso = datetime.now(timezone.utc).isoformat()
    for cid, info in _CHANNELS.items():
        stats = channel_stats.get(cid, {})
        total_views = stats.get("view_count", 0)
        video_count = max(stats.get("video_count", 1), 1)
        channel_rows.append(
            {
                "channel_id": cid,
                "channel_name": info["name"],
                "handle": info["handle"],
                "subscriber_count": stats.get("subscriber_count", 0),
                "avg_views_30d": total_views // video_count,
                "avg_posts_per_week": info["baseline_posts_per_week"],
                "thumbnail_url": stats.get("thumbnail_url", ""),
                "last_polled_at": now_iso,
                "updated_at": now_iso,
            }
        )
    supabase_cache.upsert_youtube_channels(channel_rows)

    # 2. Per-channel: search upcoming + fetch recent activities
    all_event_rows: list[dict] = []
    all_upload_rows: list[dict] = []
    total_videos_found = 0

    for cid, info in _CHANNELS.items():
        # 2a. Search upcoming live streams (100 units)
        upcoming = _fetch_upcoming_videos(cid)
        logger.info(
            "Channel %s (%s): %d upcoming videos",
            info["name"],
            cid[:8],
            len(upcoming),
        )

        # 2b. Fetch recent activities for frequency alert + recent uploads (1 unit)
        recent_activities = _fetch_recent_activities(cid, info["name"], days=2)
        recent_count = len(recent_activities)
        freq_alert, freq_detail = check_frequency_alert(
            recent_count,
            2,
            info["baseline_posts_per_week"],
        )
        if freq_alert:
            logger.info("Frequency alert for %s: %s", info["name"], freq_detail)

        # 2c. Collect all video IDs (upcoming + recent uploads) for batch detail fetch
        upcoming_vids = [
            it["id"]["videoId"] for it in upcoming if "videoId" in it.get("id", {})
        ]
        upload_vids = [act["video_id"] for act in recent_activities]
        all_vids = list(
            dict.fromkeys(upcoming_vids + upload_vids)
        )  # dedupe, preserve order
        video_details = _fetch_video_details(all_vids) if all_vids else {}

        # 2d. Build event rows (upcoming streams)
        sub_count = channel_stats.get(cid, {}).get("subscriber_count", 0)
        total_views = channel_stats.get(cid, {}).get("view_count", 0)
        video_count = max(channel_stats.get(cid, {}).get("video_count", 1), 1)
        avg_views = total_views // video_count

        for item in upcoming:
            vid = item.get("id", {}).get("videoId")
            if not vid:
                continue

            snippet = item.get("snippet", {})
            title = snippet.get("title", "")
            thumbnail = snippet.get("thumbnails", {}).get("high", {}).get(
                "url"
            ) or snippet.get("thumbnails", {}).get("default", {}).get("url", "")

            details = video_details.get(vid, {})
            scheduled_at = details.get("scheduled_at")

            tickers = parse_tickers(title)
            sent = classify_sentiment(title)
            impact = compute_impact_score(sub_count, avg_views)

            all_event_rows.append(
                {
                    "video_id": vid,
                    "channel_id": cid,
                    "channel_name": info["name"],
                    "title": title,
                    "scheduled_at": scheduled_at,
                    "event_type": "upcoming",
                    "sentiment": sent,
                    "tickers": tickers,
                    "impact_score": impact,
                    "subscriber_count": sub_count,
                    "avg_views": avg_views,
                    "frequency_alert": freq_alert,
                    "frequency_detail": freq_detail if freq_alert else "",
                    "thumbnail_url": thumbnail,
                    "video_url": f"https://www.youtube.com/watch?v={vid}",
                    "duration": details.get("duration", ""),
                    "content_type": details.get("content_type", "upcoming"),
                    "updated_at": now_iso,
                }
            )
            total_videos_found += 1

        # 2e. Build recent upload rows (from activities data)
        for act in recent_activities:
            vid = act["video_id"]
            title = act["title"]
            details = video_details.get(vid, {})
            tickers = parse_tickers(title)
            sent = classify_sentiment(title)
            impact = compute_impact_score(sub_count, avg_views)

            all_upload_rows.append(
                {
                    "video_id": vid,
                    "channel_id": cid,
                    "channel_name": info["name"],
                    "title": title,
                    "scheduled_at": act[
                        "published_at"
                    ],  # reuse column for chronological ordering
                    "event_type": "recent_upload",
                    "sentiment": sent,
                    "tickers": tickers,
                    "impact_score": impact,
                    "subscriber_count": sub_count,
                    "avg_views": avg_views,
                    "frequency_alert": freq_alert,
                    "frequency_detail": freq_detail if freq_alert else "",
                    "thumbnail_url": act["thumbnail_url"],
                    "video_url": f"https://www.youtube.com/watch?v={vid}",
                    "duration": details.get("duration", ""),
                    "content_type": details.get("content_type", "video"),
                    "updated_at": now_iso,
                }
            )

        time.sleep(_DELAY_BETWEEN_CHANNELS)

    # 3. Upsert all events (upcoming + recent uploads)
    combined_rows = all_event_rows + all_upload_rows
    upserted = (
        supabase_cache.upsert_youtube_events(combined_rows) if combined_rows else 0
    )
    total_videos_found += len(all_upload_rows)

    # ── Create notifications for high-impact new events ──
    try:
        notif_rows: list[dict] = []
        for row in combined_rows:
            notif = notifications.create_youtube_notification(row)
            if notif is not None:
                notif_rows.append(notif)
        if notif_rows:
            # upsert_notifications handles dedup via deterministic IDs
            n_inserted = supabase_cache.upsert_notifications(notif_rows)
            if n_inserted:
                logger.info("Created %d YouTube notifications", n_inserted)
    except Exception as notif_exc:
        logger.debug("YouTube notification creation failed: %s", notif_exc)

    errors: list[str] = []
    if total_videos_found > 0 and upserted == 0:
        errors.append("Found videos but upsert returned 0")

    supabase_cache.complete_sync_log(
        run_id,
        funds_updated=upserted,
        funds_failed=0 if not errors else 1,
        funds_skipped=total_videos_found - upserted,
        errors=errors,
    )

    logger.info(
        "YouTube sync complete: %d upcoming + %d recent uploads found, "
        "%d upserted across %d channels",
        len(all_event_rows),
        len(all_upload_rows),
        upserted,
        len(_CHANNELS),
    )
    return {
        "upcoming_found": len(all_event_rows),
        "uploads_found": len(all_upload_rows),
        "upserted": upserted,
        "errors": len(errors),
    }


# ── Entry point ──────────────────────────────────────────────────────


def main() -> None:
    """Entry point for ``uv run filings-youtube-sync``."""
    _setup_logging()
    logger.info("=== PaperPanda YouTube Calendar Sync starting ===")
    start = time.time()
    result = sync_youtube_events()
    elapsed = round(time.time() - start)
    logger.info("=== YouTube sync finished in %ds: %s ===", elapsed, result)


if __name__ == "__main__":
    main()
