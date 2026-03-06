"""Supabase persistent cache wrapper for PaperPanda.

Provides a thin, fault-tolerant interface to the ``api_cache`` table
in Supabase Postgres.  Every public function is safe to call even when
Supabase is not configured -- it will simply return a "miss" so the
caller can fall back to the local disk cache.

On first successful connection the module auto-creates the
``api_cache`` table if it doesn't exist (using the direct Postgres
connection via ``SUPABASE_DB_URL``).  This means you never need to run
the SQL migration manually.

Env vars (set in Railway):
    SUPABASE_URL         -- e.g. https://xxxx.supabase.co
    SUPABASE_SERVICE_KEY -- service-role key (server-side writes)
    SUPABASE_DB_URL      -- direct Postgres connection string (optional,
                            used only for auto-migration)
"""

from __future__ import annotations

import hashlib
import json as _json
import logging
import os
import threading
import time
from collections.abc import Callable
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

# ── Lazy-initialised Supabase client ──────────────────────────────
_client = None  # supabase.Client | None
_initialised = False  # True once we've attempted init (even if it failed)
_table_verified = False  # True once we've confirmed the table exists
_init_lock = threading.Lock()  # Protects one-time client creation

_TABLE = "api_cache"

# ── Column projections (egress optimization — avoid SELECT *) ─────
# Only fetch columns the web layer actually reads.

_INSIDER_COLS = (
    "filing_date,trade_date,ticker,company_name,insider_name,"
    "title,trade_type,price_fmt,qty_fmt,owned_fmt,delta_own_fmt,"
    "value_fmt,sec_url"
)

# Lean projection for the cold history table (purchases only, no _fmt cols)
_HISTORY_COLS = (
    "sec_url,filing_date,trade_date,ticker,company_name,"
    "insider_name,title,price,qty,value,"
    "close_on_trade,close_at_30d,close_at_90d,close_at_180d,close_at_365d,"
    "returns_updated"
)

# Projection for the all-types cold history table (raw numerics, no _fmt cols)
_FULL_HISTORY_COLS = (
    "sec_url,filing_date,trade_date,ticker,company_name,"
    "insider_name,title,trade_type,price,qty,value"
)

# Lean projection for chart aggregation — only the 6 columns needed
# by aggregate_top_tickers().  ~40% less network transfer vs _INSIDER_COLS.
_CHART_COLS = (
    "ticker,company_name,value_fmt,trade_date,trade_type,insider_name"
)

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

_NOTIFICATION_COLS = (
    "id,type,title,message,icon,toast_type,link,metadata,created_at"
)

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS api_cache (
    cache_key     TEXT PRIMARY KEY,
    category      TEXT NOT NULL DEFAULT 'general',
    response_data JSONB NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at    TIMESTAMPTZ,
    ttl_seconds   INTEGER
);
CREATE INDEX IF NOT EXISTS idx_api_cache_category
    ON api_cache (category);
CREATE INDEX IF NOT EXISTS idx_api_cache_expires_at
    ON api_cache (expires_at)
    WHERE expires_at IS NOT NULL;
"""

# Idempotent migrations — safe to run on every startup.
_MIGRATE_SQL = """
-- Sync worker tracking columns on api_cache
ALTER TABLE api_cache ADD COLUMN IF NOT EXISTS last_synced_at TIMESTAMPTZ;
ALTER TABLE api_cache ADD COLUMN IF NOT EXISTS sync_status TEXT DEFAULT 'pending';
-- Content hash for change detection (avoids re-uploading identical data)
ALTER TABLE api_cache ADD COLUMN IF NOT EXISTS content_hash TEXT DEFAULT '';

-- Sync run log table
CREATE TABLE IF NOT EXISTS sync_logs (
    run_id          TEXT PRIMARY KEY,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ,
    funds_updated   INTEGER DEFAULT 0,
    funds_failed    INTEGER DEFAULT 0,
    funds_skipped   INTEGER DEFAULT 0,
    error_messages  JSONB DEFAULT '[]'::jsonb
);

-- ── Dedicated insider_trades table ──
CREATE TABLE IF NOT EXISTS insider_trades (
    id              BIGSERIAL PRIMARY KEY,
    sec_url         TEXT NOT NULL UNIQUE,
    filing_date     DATE NOT NULL,
    trade_date      DATE NOT NULL,
    ticker          TEXT NOT NULL,
    company_name    TEXT NOT NULL DEFAULT '',
    insider_name    TEXT NOT NULL,
    title           TEXT NOT NULL DEFAULT '',
    trade_type      TEXT NOT NULL,
    price           NUMERIC(12,4),
    qty             INTEGER,
    owned           BIGINT,
    delta_own_pct   NUMERIC(8,4),
    value           NUMERIC(16,2),
    price_fmt       TEXT NOT NULL DEFAULT '',
    qty_fmt         TEXT NOT NULL DEFAULT '',
    owned_fmt       TEXT NOT NULL DEFAULT '',
    delta_own_fmt   TEXT NOT NULL DEFAULT '',
    value_fmt       TEXT NOT NULL DEFAULT '',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_insider_trades_ticker
    ON insider_trades (ticker, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_insider_trades_trade_date
    ON insider_trades (trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_insider_trades_type_date
    ON insider_trades (trade_type, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_insider_trades_filing_date
    ON insider_trades (filing_date DESC);

-- ── YouTube calendar events ──
CREATE TABLE IF NOT EXISTS youtube_events (
    id                BIGSERIAL PRIMARY KEY,
    video_id          TEXT NOT NULL UNIQUE,
    channel_id        TEXT NOT NULL,
    channel_name      TEXT NOT NULL DEFAULT '',
    title             TEXT NOT NULL DEFAULT '',
    scheduled_at      TIMESTAMPTZ,
    event_type        TEXT NOT NULL DEFAULT 'upcoming',
    sentiment         TEXT NOT NULL DEFAULT 'neutral',
    tickers           JSONB NOT NULL DEFAULT '[]'::jsonb,
    impact_score      SMALLINT NOT NULL DEFAULT 0,
    subscriber_count  BIGINT DEFAULT 0,
    avg_views         BIGINT DEFAULT 0,
    frequency_alert   BOOLEAN NOT NULL DEFAULT FALSE,
    frequency_detail  TEXT NOT NULL DEFAULT '',
    thumbnail_url     TEXT NOT NULL DEFAULT '',
    video_url         TEXT NOT NULL DEFAULT '',
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_youtube_events_scheduled
    ON youtube_events (scheduled_at DESC);
CREATE INDEX IF NOT EXISTS idx_youtube_events_channel
    ON youtube_events (channel_id, scheduled_at DESC);
CREATE INDEX IF NOT EXISTS idx_youtube_events_impact
    ON youtube_events (impact_score DESC)
    WHERE impact_score >= 9;
CREATE INDEX IF NOT EXISTS idx_youtube_events_sentiment
    ON youtube_events (sentiment, scheduled_at DESC);
CREATE INDEX IF NOT EXISTS idx_youtube_events_type_scheduled
    ON youtube_events (event_type, scheduled_at DESC);

-- ── YouTube channel metadata cache ──
CREATE TABLE IF NOT EXISTS youtube_channels (
    channel_id          TEXT PRIMARY KEY,
    channel_name        TEXT NOT NULL DEFAULT '',
    handle              TEXT NOT NULL DEFAULT '',
    subscriber_count    BIGINT DEFAULT 0,
    avg_views_30d       BIGINT DEFAULT 0,
    avg_posts_per_week  NUMERIC(5,2) DEFAULT 0,
    last_polled_at      TIMESTAMPTZ,
    thumbnail_url       TEXT NOT NULL DEFAULT '',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Backfill: add thumbnail_url if table already exists
ALTER TABLE youtube_channels ADD COLUMN IF NOT EXISTS thumbnail_url TEXT NOT NULL DEFAULT '';

-- ── Panda Fund supporters ──
-- One row per Stripe payment/subscription event. The web layer aggregates
-- these to compute the monthly total shown on /support.
CREATE TABLE IF NOT EXISTS supporters (
    id              BIGSERIAL PRIMARY KEY,
    stripe_event_id TEXT NOT NULL UNIQUE,   -- idempotency key (Stripe event.id)
    session_id      TEXT NOT NULL DEFAULT '', -- Stripe checkout session ID
    customer_email  TEXT NOT NULL DEFAULT '',
    amount_cents    INTEGER NOT NULL DEFAULT 0,
    currency        TEXT NOT NULL DEFAULT 'usd',
    mode            TEXT NOT NULL DEFAULT 'payment',  -- 'payment' | 'subscription'
    month           TEXT NOT NULL,           -- 'YYYY-MM' for easy grouping
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_supporters_month
    ON supporters (month DESC);
CREATE INDEX IF NOT EXISTS idx_supporters_event
    ON supporters (stripe_event_id);

-- ── Notifications table (global, server-generated) ──
CREATE TABLE IF NOT EXISTS notifications (
    id              TEXT PRIMARY KEY,
    type            TEXT NOT NULL,
    title           TEXT NOT NULL,
    message         TEXT NOT NULL DEFAULT '',
    icon            TEXT NOT NULL DEFAULT '🔔',
    toast_type      TEXT NOT NULL DEFAULT 'alert',
    link            TEXT NOT NULL DEFAULT '',
    metadata        JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_notifications_created
    ON notifications (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_notifications_type_created
    ON notifications (type, created_at DESC);

-- Backfill: add duration and content_type columns for video metadata
ALTER TABLE youtube_events ADD COLUMN IF NOT EXISTS duration TEXT NOT NULL DEFAULT '';
ALTER TABLE youtube_events ADD COLUMN IF NOT EXISTS content_type TEXT NOT NULL DEFAULT 'video';

-- ── Additional indexes for high-traffic query patterns ──
-- Sync worker: WHERE category = '13f' AND last_synced_at IS NULL / < cutoff
CREATE INDEX IF NOT EXISTS idx_api_cache_category_synced
    ON api_cache (category, last_synced_at);
-- YouTube sync dedup: WHERE created_at >= cutoff
CREATE INDEX IF NOT EXISTS idx_youtube_events_created_at
    ON youtube_events (created_at DESC);

-- ── Insider Insights: forward-return columns on insider_trades ──
ALTER TABLE insider_trades ADD COLUMN IF NOT EXISTS close_on_trade   NUMERIC(12,4);
ALTER TABLE insider_trades ADD COLUMN IF NOT EXISTS close_at_30d     NUMERIC(12,4);
ALTER TABLE insider_trades ADD COLUMN IF NOT EXISTS close_at_90d     NUMERIC(12,4);
ALTER TABLE insider_trades ADD COLUMN IF NOT EXISTS close_at_180d    NUMERIC(12,4);
ALTER TABLE insider_trades ADD COLUMN IF NOT EXISTS close_at_365d    NUMERIC(12,4);
ALTER TABLE insider_trades ADD COLUMN IF NOT EXISTS returns_updated  TIMESTAMPTZ;

-- Partial index: purchases still needing forward-return computation
CREATE INDEX IF NOT EXISTS idx_insider_purchases_pending
    ON insider_trades (ticker, trade_date)
    WHERE trade_type = 'Purchase' AND returns_updated IS NULL;

-- ── Cold history table: delete-protected, purchases only ──
CREATE TABLE IF NOT EXISTS insider_purchases_history (
    id              BIGSERIAL PRIMARY KEY,
    sec_url         TEXT NOT NULL UNIQUE,
    filing_date     DATE NOT NULL,
    trade_date      DATE NOT NULL,
    ticker          TEXT NOT NULL,
    company_name    TEXT NOT NULL DEFAULT '',
    insider_name    TEXT NOT NULL,
    title           TEXT NOT NULL DEFAULT '',
    price           NUMERIC(12,4),
    qty             INTEGER,
    value           NUMERIC(16,2),
    close_on_trade  NUMERIC(12,4),
    close_at_30d    NUMERIC(12,4),
    close_at_90d    NUMERIC(12,4),
    close_at_180d   NUMERIC(12,4),
    close_at_365d   NUMERIC(12,4),
    returns_updated TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_iph_ticker_trade_date
    ON insider_purchases_history (ticker, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_iph_pending_returns
    ON insider_purchases_history (ticker, trade_date)
    WHERE returns_updated IS NULL;

-- ── Congress member headshots (mirrors ticker_logos pattern) ──────
CREATE TABLE IF NOT EXISTS congress_headshots (
    member_id    TEXT PRIMARY KEY,
    photo_b64    TEXT NOT NULL DEFAULT '',
    content_type TEXT NOT NULL DEFAULT 'image/jpeg',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── RLS: protect ticker_logos & congress_headshots from public mutation ──
-- Both tables are written only by the backend (service_role key).
-- The anon / authenticated roles can SELECT but not INSERT/UPDATE/DELETE.
ALTER TABLE ticker_logos ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS ticker_logos_read_only ON ticker_logos;
CREATE POLICY ticker_logos_read_only ON ticker_logos
    FOR SELECT USING (true);

ALTER TABLE congress_headshots ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS congress_headshots_read_only ON congress_headshots;
CREATE POLICY congress_headshots_read_only ON congress_headshots
    FOR SELECT USING (true);

-- ── Feature announcements (manually edited in Supabase dashboard) ──
-- Net worth estimates for congress members (scraped from public sources)
ALTER TABLE congress_members ADD COLUMN IF NOT EXISTS net_worth_estimate BIGINT;
ALTER TABLE congress_members ADD COLUMN IF NOT EXISTS net_worth_source TEXT NOT NULL DEFAULT '';
ALTER TABLE congress_members ADD COLUMN IF NOT EXISTS net_worth_year INT;

CREATE TABLE IF NOT EXISTS feature_announcements (
    id          TEXT PRIMARY KEY,
    title       TEXT NOT NULL,
    message     TEXT NOT NULL DEFAULT '',
    icon        TEXT NOT NULL DEFAULT '🐼',
    toast_type  TEXT NOT NULL DEFAULT 'alert',
    link        TEXT NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── Short interest history (FINRA bi-monthly reports, archived over time) ──
CREATE TABLE IF NOT EXISTS short_interest_history (
    id                  BIGSERIAL PRIMARY KEY,
    ticker              TEXT NOT NULL,
    report_date         DATE NOT NULL,
    shares_short        BIGINT NOT NULL,
    shares_short_prior  BIGINT DEFAULT 0,
    short_pct_float     NUMERIC(8,6) DEFAULT 0,
    short_ratio         NUMERIC(8,2) DEFAULT 0,
    float_shares        BIGINT DEFAULT 0,
    shares_outstanding  BIGINT DEFAULT 0,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(ticker, report_date)
);
CREATE INDEX IF NOT EXISTS idx_si_ticker_date
    ON short_interest_history (ticker, report_date DESC);
CREATE INDEX IF NOT EXISTS idx_si_pct_float
    ON short_interest_history (short_pct_float DESC)
    WHERE short_pct_float > 0;
CREATE INDEX IF NOT EXISTS idx_si_date_pct
    ON short_interest_history (report_date DESC, short_pct_float DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_si_report_date
    ON short_interest_history (report_date DESC);

-- ── Earnings history (quarterly EPS & revenue results, cached from yfinance) ──
CREATE TABLE IF NOT EXISTS earnings_history (
    id                   BIGSERIAL PRIMARY KEY,
    ticker               TEXT NOT NULL,
    report_date          DATE NOT NULL,
    fiscal_quarter       TEXT DEFAULT '',
    eps_estimate         NUMERIC(10,4),
    eps_actual           NUMERIC(10,4),
    eps_surprise_pct     NUMERIC(8,4),
    revenue_estimate     BIGINT,
    revenue_actual       BIGINT,
    revenue_surprise_pct NUMERIC(8,4),
    beat_eps             BOOLEAN,
    beat_revenue         BOOLEAN,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(ticker, report_date)
);
CREATE INDEX IF NOT EXISTS idx_earnings_ticker_date
    ON earnings_history (ticker, report_date DESC);
"""


def _auto_migrate() -> None:
    """Create the api_cache table if it doesn't exist.

    Uses ``SUPABASE_DB_URL`` (direct Postgres connection string) to
    execute DDL.  Falls back to deriving the connection string from
    ``SUPABASE_URL`` + ``SUPABASE_DB_PASSWORD`` if available.

    This is a one-time operation; failures are non-fatal.
    """
    global _table_verified
    if _table_verified:
        return

    db_url = os.environ.get("SUPABASE_DB_URL", "").strip()

    # Build a list of connection strings to try
    urls_to_try: list[str] = []
    if db_url:
        urls_to_try.append(db_url)
    else:
        supa_url = os.environ.get("SUPABASE_URL", "").strip()
        db_pass = os.environ.get("SUPABASE_DB_PASSWORD", "").strip()
        if supa_url and db_pass:
            ref = supa_url.replace("https://", "").split(".")[0]
            # Try pooler first (works across cloud boundaries), then direct
            urls_to_try.append(
                f"postgresql://postgres.{ref}:{db_pass}@aws-0-us-east-1.pooler.supabase.com:5432/postgres"
            )
            urls_to_try.append(
                f"postgresql://postgres:{db_pass}@db.{ref}.supabase.co:5432/postgres"
            )

    if not urls_to_try:
        logger.debug("No SUPABASE_DB_URL -- skipping auto-migration")
        return

    try:
        import psycopg2  # noqa: F811
    except ImportError:
        logger.debug("psycopg2 not installed -- skipping auto-migration")
        return

    for url in urls_to_try:
        try:
            conn = psycopg2.connect(url, connect_timeout=5)
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute(_CREATE_TABLE_SQL)
            cur.execute(_MIGRATE_SQL)
            cur.close()
            conn.close()
            _table_verified = True
            logger.info(
                "Supabase api_cache + sync_logs + insider_trades tables verified via auto-migration"
            )
            # Create cold storage bucket (non-fatal)
            try:
                from filings import cold_storage

                cold_storage.ensure_bucket()
            except Exception:
                logger.debug("Cold storage bucket creation skipped (non-fatal)")
            return  # Success — stop trying other URLs
        except Exception as exc:
            logger.debug("Auto-migration attempt failed (%s...): %s", url[:40], exc)

    logger.warning("Auto-migration failed on all connection strings")


def _get_client():
    """Return the Supabase client, lazily creating it on first call.

    Uses double-checked locking so concurrent threads on first request
    don't each create a separate client or run migrations in parallel.

    Returns ``None`` when either env var is missing or the client
    could not be created.
    """
    global _client, _initialised

    # Fast path: already initialised (no lock needed)
    if _initialised:
        return _client

    with _init_lock:
        # Re-check after acquiring lock (another thread may have finished)
        if _initialised:
            return _client

        _initialised = True

        url = os.environ.get("SUPABASE_URL", "").strip()
        key = os.environ.get("SUPABASE_SERVICE_KEY", "").strip()

        if not url or not key:
            logger.info(
                "Supabase not configured (SUPABASE_URL / SUPABASE_SERVICE_KEY missing)"
            )
            return None

        try:
            from supabase import create_client

            _client = create_client(url, key)
            logger.info("Supabase client initialised (%s)", url)
            # Try auto-migration (non-fatal)
            _auto_migrate()
        except Exception as exc:
            logger.warning("Supabase client init failed: %s", exc)
            _client = None

    return _client


# ── Public helpers ────────────────────────────────────────────────


def is_available() -> bool:
    """Return True if Supabase is configured and the client was created."""
    return _get_client() is not None


def _handle_table_missing(exc: Exception) -> None:
    """Log an appropriate message depending on whether the table is missing."""
    err = str(exc)
    if "PGRST205" in err or "api_cache" in err:
        logger.info(
            "Supabase api_cache table not found -- run the SQL migration "
            "or set SUPABASE_DB_URL for auto-migration. Falling back to disk cache."
        )
    else:
        logger.warning("Supabase operation failed: %s", exc)


# ── Single-key read / write ──────────────────────────────────────


def get_cached(cache_key: str) -> dict | None:
    """Fetch a single cache row's ``response_data``.

    Returns the JSON payload (as a Python dict) on hit, or ``None``
    on miss / error / not configured / table missing.
    """
    client = _get_client()
    if client is None:
        return None

    try:
        resp = (
            client.table(_TABLE)
            .select("response_data, expires_at")
            .eq("cache_key", cache_key)
            .maybe_single()
            .execute()
        )
        if resp.data is None:
            return None

        # Check expiry
        expires_at = resp.data.get("expires_at")
        if expires_at is not None:
            exp_dt = datetime.fromisoformat(expires_at)
            if exp_dt < datetime.now(timezone.utc):
                return None  # Expired

        return resp.data.get("response_data")
    except Exception as exc:
        _handle_table_missing(exc)
        return None


def get_cached_with_stale(cache_key: str) -> tuple[dict | list | None, bool]:
    """Fetch a cache row, distinguishing fresh hits from stale data.

    Returns a tuple ``(data, is_fresh)``:
      - ``(data, True)``  -- cache hit, data is not expired
      - ``(data, False)`` -- cache hit but expired (stale; useful for fallback)
      - ``(None, False)`` -- complete miss (key not found, or Supabase unavailable)
    """
    client = _get_client()
    if client is None:
        return None, False

    try:
        resp = (
            client.table(_TABLE)
            .select("response_data, expires_at")
            .eq("cache_key", cache_key)
            .maybe_single()
            .execute()
        )
        if resp.data is None:
            return None, False

        response_data = resp.data.get("response_data")

        # Check expiry
        expires_at = resp.data.get("expires_at")
        if expires_at is not None:
            exp_dt = datetime.fromisoformat(expires_at)
            if exp_dt < datetime.now(timezone.utc):
                return response_data, False  # Stale but available

        return response_data, True  # Fresh hit
    except Exception as exc:
        _handle_table_missing(exc)
        return None, False


def _compute_hash(data: dict | list) -> str:
    """Compute a stable SHA-256 hash of a JSON-serialisable payload.

    Used for change detection: skip Supabase upserts when the data
    hasn't actually changed (saves egress on both write and subsequent reads).
    """
    raw = _json.dumps(data, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def get_content_hash(cache_key: str) -> str:
    """Fetch only the content_hash for a cache row (very lightweight).

    Returns the hash string, or ``""`` on miss / error / column missing.
    """
    client = _get_client()
    if client is None:
        return ""

    try:
        resp = (
            client.table(_TABLE)
            .select("content_hash")
            .eq("cache_key", cache_key)
            .maybe_single()
            .execute()
        )
        if resp.data is None:
            return ""
        return resp.data.get("content_hash") or ""
    except Exception:
        return ""


def get_all_content_hashes(category: str) -> dict[str, str]:
    """Fetch {cache_key: content_hash} for an entire category.

    Very lightweight query — only two small text columns, no JSONB.
    Used on startup to detect which funds changed since last deploy.
    """
    client = _get_client()
    if client is None:
        return {}

    try:
        resp = (
            client.table(_TABLE)
            .select("cache_key, content_hash")
            .eq("category", category)
            .execute()
        )
        return {
            row["cache_key"]: (row.get("content_hash") or "")
            for row in (resp.data or [])
        }
    except Exception as exc:
        _handle_table_missing(exc)
        return {}


def set_cached(
    cache_key: str,
    category: str,
    data: dict,
    ttl_seconds: int | None = None,
) -> bool:
    """Upsert a cache row with content-hash change detection.

    Computes a SHA-256 hash of *data* and compares it to the stored
    ``content_hash``.  If identical, only the TTL / timestamps are
    bumped (no JSONB rewrite) — saving significant Supabase egress.

    Args:
        cache_key:   e.g. ``"glassdoor:AAPL"``
        category:    e.g. ``"glassdoor"``
        data:        the JSON-serialisable payload
        ttl_seconds: optional TTL; ``None`` means never expires

    Returns ``True`` on success, ``False`` on error / not configured.
    """
    client = _get_client()
    if client is None:
        return False

    new_hash = _compute_hash(data)

    # Check if data actually changed (lightweight: fetches only the hash)
    existing_hash = get_content_hash(cache_key)
    if existing_hash and existing_hash == new_hash:
        # Data unchanged — just bump the TTL and sync timestamp
        expires_at = None
        if ttl_seconds is not None:
            expires_at = (
                datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)
            ).isoformat()
        try:
            update_row: dict = {"expires_at": expires_at}
            client.table(_TABLE).update(update_row).eq("cache_key", cache_key).execute()
            logger.debug(
                "Cache unchanged for %s (hash=%s), bumped TTL only", cache_key, new_hash
            )
            return True
        except Exception:
            pass  # Fall through to full upsert

    expires_at = None
    if ttl_seconds is not None:
        expires_at = (
            datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)
        ).isoformat()

    row = {
        "cache_key": cache_key,
        "category": category,
        "response_data": data,
        "expires_at": expires_at,
        "content_hash": new_hash,
    }

    try:
        client.table(_TABLE).upsert(row, on_conflict="cache_key").execute()
        logger.debug("Cache updated for %s (new hash=%s)", cache_key, new_hash)
        return True
    except Exception as exc:
        _handle_table_missing(exc)
        return False


# ── Bulk read by category ────────────────────────────────────────


def get_category_keys(category: str) -> list[str]:
    """Return all ``cache_key`` values for a given category (lightweight).

    Only fetches the key column — no response_data. Used on startup to
    discover which funds are cached without downloading 28MB of JSON.
    """
    client = _get_client()
    if client is None:
        return []

    try:
        resp = (
            client.table(_TABLE).select("cache_key").eq("category", category).execute()
        )
        return [row["cache_key"] for row in (resp.data or [])]
    except Exception as exc:
        _handle_table_missing(exc)
        return []


def get_cache_keys_by_category(category: str) -> list[str]:
    """Fetch only cache_key values for a category (lightweight, no data blobs).

    Used by the sync worker to determine which CIKs already exist
    without loading 20-30MB of response_data.
    """
    client = _get_client()
    if client is None:
        return []
    try:
        resp = (
            client.table(_TABLE).select("cache_key").eq("category", category).execute()
        )
        return [row["cache_key"] for row in (resp.data or [])]
    except Exception as exc:
        _handle_table_missing(exc)
        return []


def get_all_by_category(category: str, page_size: int = 10) -> list[dict] | None:
    """Fetch all rows for a given category, paginated to avoid timeouts.

    Returns a list of row dicts (each with ``cache_key``,
    ``response_data``, etc.) on success, or ``None`` on error.

    Large categories (e.g. 84 × 300KB 13f rows ≈ 28MB) are fetched
    in pages of *page_size* to stay within Supabase/network limits.
    """
    client = _get_client()
    if client is None:
        return None

    all_rows: list[dict] = []
    offset = 0

    try:
        while True:
            resp = (
                client.table(_TABLE)
                .select("cache_key, response_data, created_at")
                .eq("category", category)
                .range(offset, offset + page_size - 1)
                .execute()
            )
            batch = resp.data or []
            all_rows.extend(batch)
            if len(batch) < page_size:
                break  # Last page
            offset += page_size

        return all_rows
    except Exception as exc:
        _handle_table_missing(exc)
        # Return whatever we collected so far (partial > nothing)
        return all_rows if all_rows else None


# ── Quota helpers (stored as a special cache row) ────────────────


def get_quota(category: str, month: str) -> dict | None:
    """Read a quota row.

    The cache_key convention is ``"{category}_quota:{month}"``,
    e.g. ``"glassdoor_quota:2026-02"``.

    Returns the ``response_data`` dict (expected shape:
    ``{"month": "2026-02", "count": 14}``) or ``None``.
    """
    cache_key = f"{category}_quota:{month}"
    return get_cached(cache_key)


def increment_quota(category: str, month: str) -> int:
    """Increment a monthly quota counter.

    Reads the current count, increments by 1, and upserts.
    Returns the new count, or ``-1`` on error / not configured.
    """
    client = _get_client()
    if client is None:
        return -1

    cache_key = f"{category}_quota:{month}"

    try:
        # Read current value
        current = get_cached(cache_key)
        if current is None:
            current = {"month": month, "count": 0}

        new_count = current.get("count", 0) + 1
        current["count"] = new_count

        # Upsert (no TTL -- quota rows are managed manually)
        ok = set_cached(cache_key, f"{category}_quota", current, ttl_seconds=None)
        if not ok:
            return -1

        return new_count
    except Exception as exc:
        logger.warning(
            "Supabase increment_quota(%s, %s) failed: %s", category, month, exc
        )
        return -1


# ── High-level cache-first fetch utilities ───────────────────────


def fetch_with_cache(
    cache_key: str,
    category: str,
    ttl_days: float,
    api_fetch_fn: Callable[[], dict | list | None],
    symbol: str | None = None,
) -> dict | list | None:
    """Centralized cache-first fetching utility.

    Wraps any API call with Supabase-backed caching:
      1. Check Supabase for *cache_key* -- if fresh, return immediately
      2. If miss or expired, call *api_fetch_fn()*
      3. On success: upsert result to Supabase with new ``expires_at``
      4. On failure: return stale data from Supabase as fallback
      5. All wrapped in try/except -- **never** raises

    Args:
        cache_key:    Unique identifier, e.g. ``"insider_global:p:100"``
        category:     Grouping label, e.g. ``"insider"``, ``"sentiment"``
        ttl_days:     How many days before data is considered stale
        api_fetch_fn: Zero-argument callable that returns fresh data
                      (dict, list, or ``None`` on failure)
        symbol:       Optional ticker for log messages

    Returns:
        Cached or freshly-fetched data, or ``None`` if both fail.
    """
    log_label = f"{category}:{symbol}" if symbol else cache_key

    try:
        # Step 1: Check Supabase cache
        cached_data, is_fresh = get_cached_with_stale(cache_key)

        if cached_data is not None and is_fresh:
            logger.debug("Cache HIT (fresh) for %s", log_label)
            return cached_data

        # Step 2: Cache miss or stale -- call the API
        if cached_data is not None:
            logger.debug("Cache STALE for %s -- calling API", log_label)
        else:
            logger.debug("Cache MISS for %s -- calling API", log_label)

        fresh_data = None
        try:
            fresh_data = api_fetch_fn()
        except Exception as exc:
            logger.warning("API call failed for %s: %s", log_label, exc)

        # Step 3: On success -- upsert to Supabase
        if fresh_data is not None:
            ttl_seconds = int(ttl_days * 86_400)
            set_cached(cache_key, category, fresh_data, ttl_seconds=ttl_seconds)
            logger.info("Cache SET for %s (ttl=%.2fd)", log_label, ttl_days)
            return fresh_data

        # Step 4: On failure -- return stale data as fallback
        if cached_data is not None:
            logger.info("API failed for %s -- returning stale cached data", log_label)
            return cached_data

        # Step 5: Both cache and API failed
        logger.warning("No data available for %s (cache miss + API failure)", log_label)
        return None

    except Exception as exc:
        logger.error("fetch_with_cache failed for %s: %s", log_label, exc)
        return None


def fetch_with_cache_and_quota(
    cache_key: str,
    category: str,
    ttl_days: float,
    api_fetch_fn: Callable[[], dict | list | None],
    quota_category: str,
    max_monthly: int,
    symbol: str | None = None,
) -> dict | list | None:
    """Like :func:`fetch_with_cache`, but with a monthly quota guard.

    Before calling the API, checks whether the monthly call count for
    *quota_category* has been exceeded.  If so, returns stale data (if
    available) or ``None`` without touching the external API.

    Args:
        quota_category: Quota bucket name, e.g. ``"glassdoor"``
        max_monthly:    Hard cap on API calls per calendar month
        (all other args identical to :func:`fetch_with_cache`)
    """
    log_label = f"{category}:{symbol}" if symbol else cache_key

    try:
        # Step 1: Check Supabase cache
        cached_data, is_fresh = get_cached_with_stale(cache_key)

        if cached_data is not None and is_fresh:
            return cached_data

        # Step 2: Check quota before calling API
        current_month = datetime.now(timezone.utc).strftime("%Y-%m")
        quota_data = get_quota(quota_category, current_month)
        current_count = (quota_data or {}).get("count", 0)

        if current_count >= max_monthly:
            logger.warning(
                "Quota exhausted for %s (%d/%d) -- returning %s data for %s",
                quota_category,
                current_count,
                max_monthly,
                "stale" if cached_data else "no",
                log_label,
            )
            return cached_data  # May be None

        # Step 3: Increment quota and call API
        fresh_data = None
        try:
            fresh_data = api_fetch_fn()
            increment_quota(quota_category, current_month)
        except Exception as exc:
            logger.warning("API call failed for %s: %s", log_label, exc)

        # Step 4: On success -- upsert
        if fresh_data is not None:
            ttl_seconds = int(ttl_days * 86_400)
            set_cached(cache_key, category, fresh_data, ttl_seconds=ttl_seconds)
            return fresh_data

        # Step 5: Fallback to stale
        if cached_data is not None:
            logger.info("API failed for %s -- returning stale data", log_label)
            return cached_data

        return None

    except Exception as exc:
        logger.error("fetch_with_cache_and_quota failed for %s: %s", log_label, exc)
        return None


# ── Insider trades queries ───────────────────────────────────────


_VALID_INSIDER_TRADE_TYPES = frozenset({"Purchase", "Sale"})


def get_insider_trades(
    trade_type: str = "",
    limit: int = 100,
    since_date: str = "",
) -> list[dict] | None:
    """Query ``insider_trades`` table for latest trades.

    Args:
        trade_type: ``"Purchase"`` for buys, ``"Sale"`` for sells,
                    ``""`` for all.  Must be one of the values in
                    ``_VALID_INSIDER_TRADE_TYPES`` or empty.
        limit: Max rows to return.
        since_date: If set, only return trades with ``trade_date >= since_date``
                    (ISO format ``YYYY-MM-DD``).

    Returns list of row dicts, or ``None`` if Supabase is unavailable.
    """
    client = _get_client()
    if client is None:
        return None

    # Reject unexpected trade_type values (defense-in-depth).
    if trade_type and trade_type not in _VALID_INSIDER_TRADE_TYPES:
        logger.warning("Invalid trade_type rejected: %r", trade_type)
        return None

    try:
        query = (
            client.table("insider_trades")
            .select(_INSIDER_COLS)
            .order("trade_date", desc=True)
            .order("filing_date", desc=True)
            .limit(limit)
        )
        if trade_type:
            query = query.eq("trade_type", trade_type)
        if since_date:
            query = query.gte("trade_date", since_date)

        resp = query.execute()
        return resp.data
    except Exception as exc:
        logger.warning("get_insider_trades failed: %s", exc)
        return None


def get_insider_trades_for_chart(
    days: int = 30,
    limit_per_type: int = 250,
    since_date: str = "",
) -> list[dict] | None:
    """Query ``insider_trades`` over a time window for chart aggregation.

    Fetches purchases and sales **separately** to guarantee both types
    appear in the result set, then merges them.  A single mixed query
    is always dominated by sales due to volume disparity (~90 %+ of
    insider activity), so a LIMIT on a combined query excludes purchases.

    Args:
        days: How many days back to look (default 30).
        limit_per_type: Max rows per trade type (default 250 →
            up to 500 total).
        since_date: If set, overrides ``days`` with an explicit cutoff
                    (ISO ``YYYY-MM-DD``).

    Returns list of row dicts, or ``None`` if Supabase is unavailable.
    """
    client = _get_client()
    if client is None:
        return None

    try:
        cutoff = since_date or (
            datetime.now(timezone.utc) - timedelta(days=days)
        ).strftime("%Y-%m-%d")

        buys = (
            client.table("insider_trades")
            .select(_CHART_COLS)
            .eq("trade_type", "Purchase")
            .gte("trade_date", cutoff)
            .order("trade_date", desc=True)
            .limit(limit_per_type)
            .execute()
        ).data or []

        sells = (
            client.table("insider_trades")
            .select(_CHART_COLS)
            .in_("trade_type", ["Sale", "Sale+OE"])
            .gte("trade_date", cutoff)
            .order("trade_date", desc=True)
            .limit(limit_per_type)
            .execute()
        ).data or []

        combined = buys + sells
        return combined if combined else None
    except Exception as exc:
        logger.warning("get_insider_trades_for_chart failed: %s", exc)
        return None


def get_insider_trades_by_ticker(
    ticker: str,
    limit: int = 100,
) -> list[dict] | None:
    """Query ``insider_trades`` for a specific ticker.

    Returns list of row dicts, or ``None`` if Supabase is unavailable.
    """
    client = _get_client()
    if client is None:
        return None

    try:
        resp = (
            client.table("insider_trades")
            .select(_INSIDER_COLS)
            .eq("ticker", ticker.upper())
            .order("trade_date", desc=True)
            .limit(limit)
            .execute()
        )
        return resp.data
    except Exception as exc:
        logger.warning("get_insider_trades_by_ticker(%s) failed: %s", ticker, exc)
        return None


def get_history_purchases_by_ticker(
    ticker: str,
    limit: int = 500,
) -> list[dict] | None:
    """Query ``insider_purchases_history`` for a specific ticker (display use).

    Returns cold-table rows with raw numeric columns (price, qty, value).
    Caller should use ``InsiderTrade.from_history_row()`` to format for display.
    """
    client = _get_client()
    if client is None:
        return None

    try:
        resp = (
            client.table("insider_purchases_history")
            .select(
                "sec_url,filing_date,trade_date,ticker,company_name,"
                "insider_name,title,price,qty,value"
            )
            .eq("ticker", ticker.upper())
            .order("trade_date", desc=True)
            .limit(limit)
            .execute()
        )
        return resp.data
    except Exception as exc:
        logger.warning("get_history_purchases_by_ticker(%s) failed: %s", ticker, exc)
        return None


def get_existing_insider_urls(days: int = 7) -> set[str]:
    """Fetch sec_url values from recent insider_trades (lightweight).

    Only fetches the unique key column — no row data.  Used to skip
    re-uploading trades that already exist in Supabase, and to identify
    genuinely new trades for notification emission.
    """
    client = _get_client()
    if client is None:
        return set()

    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

    try:
        resp = (
            client.table("insider_trades")
            .select("sec_url")
            .gte("filing_date", cutoff[:10])  # date only
            .execute()
        )
        return {row["sec_url"] for row in (resp.data or []) if row.get("sec_url")}
    except Exception:
        return set()  # On failure, fall through to full upsert


def upsert_insider_trades(rows: list[dict]) -> int:
    """Batch upsert rows into ``insider_trades``, skipping existing ones.

    First fetches existing sec_url keys (lightweight query) and filters
    them out to avoid re-uploading identical rows.  Only genuinely new
    trades are sent to Supabase, saving egress.

    Deduplicates by sec_url within each batch to avoid Postgres
    ``ON CONFLICT DO UPDATE cannot affect row a second time`` errors
    (a single Form 4 filing can contain multiple transactions that
    share the same sec_url).  When duplicates exist, purchases are
    preferred over sales/other types.

    Uses ``ON CONFLICT (sec_url) DO UPDATE`` for deduplication safety.
    Returns the number of rows upserted, or 0 on failure.
    """
    client = _get_client()
    if client is None:
        return 0

    # Filter out rows that already exist in Supabase
    existing_urls = get_existing_insider_urls()
    new_rows = [r for r in rows if r.get("sec_url") not in existing_urls]

    if not new_rows:
        logger.info("All %d insider trades already exist — skipping upsert", len(rows))
        return len(rows)  # All accounted for

    # Deduplicate by sec_url within the batch — a single Form 4 filing
    # can contain multiple transactions (e.g. purchase + sale) sharing
    # the same URL.  Prefer purchases over other trade types.
    seen: dict[str, dict] = {}
    for row in new_rows:
        url = row.get("sec_url", "")
        if not url:
            continue
        if url not in seen:
            seen[url] = row
        else:
            # Keep the purchase if one of the duplicates is a purchase
            existing_type = seen[url].get("trade_type", "")
            new_type = row.get("trade_type", "")
            if "Purchase" in new_type and "Purchase" not in existing_type:
                seen[url] = row
    deduped = list(seen.values())

    if len(deduped) < len(new_rows):
        logger.info(
            "Deduplicated %d → %d rows (removed %d duplicate sec_urls)",
            len(new_rows), len(deduped), len(new_rows) - len(deduped),
        )

    logger.info(
        "Insider trades: %d new out of %d total (skipping %d existing)",
        len(deduped),
        len(rows),
        len(rows) - len(deduped),
    )

    upserted = 0
    CHUNK = 50
    for i in range(0, len(deduped), CHUNK):
        chunk = deduped[i : i + CHUNK]
        try:
            client.table("insider_trades").upsert(
                chunk, on_conflict="sec_url"
            ).execute()
            upserted += len(chunk)
        except Exception as exc:
            logger.warning("upsert_insider_trades chunk %d failed: %s", i, exc)

    return upserted


def upsert_history_purchases(rows: list[dict]) -> int:
    """Insert rows into ``insider_purchases_history`` (cold, write-once).

    Uses ``ON CONFLICT (sec_url) DO NOTHING`` — existing rows are never
    overwritten.  Only purchases should be passed in; caller is responsible
    for filtering.

    Deduplicates by sec_url within the batch before inserting.
    Returns the number of rows inserted, or 0 on failure.
    """
    client = _get_client()
    if client is None:
        return 0

    # Deduplicate by sec_url within the batch
    seen: dict[str, dict] = {}
    for row in rows:
        url = row.get("sec_url", "")
        if url and url not in seen:
            seen[url] = row
    deduped = list(seen.values())

    if not deduped:
        return 0

    inserted = 0
    CHUNK = 50
    for i in range(0, len(deduped), CHUNK):
        chunk = deduped[i : i + CHUNK]
        try:
            # DO NOTHING on conflict — write-once cold archive.
            # ignore_duplicates=True → PostgREST sends
            # "Prefer: resolution=ignore-duplicates" → ON CONFLICT DO NOTHING.
            client.table("insider_purchases_history").upsert(
                chunk,
                on_conflict="sec_url",
                ignore_duplicates=True,
            ).execute()
            inserted += len(chunk)
        except Exception as exc:
            logger.warning("upsert_history_purchases chunk %d failed: %s", i, exc)

    logger.info(
        "History purchases: %d inserted out of %d provided", inserted, len(rows)
    )
    return inserted


# ── Insider trades history (all-types cold table) ────────────────


def upsert_history_trades(rows: list[dict]) -> int:
    """Insert rows into ``insider_trades_history`` (cold, write-once).

    Uses ``ON CONFLICT (sec_url) DO NOTHING`` — existing rows are never
    overwritten.  Archives ALL trade types (purchases + sales).

    Deduplicates by sec_url within the batch before inserting.
    Returns the number of rows inserted, or 0 on failure.
    """
    client = _get_client()
    if client is None:
        return 0

    # Deduplicate by sec_url within the batch
    seen: dict[str, dict] = {}
    for row in rows:
        url = row.get("sec_url", "")
        if url and url not in seen:
            seen[url] = row
    deduped = list(seen.values())

    if not deduped:
        return 0

    inserted = 0
    CHUNK = 50
    for i in range(0, len(deduped), CHUNK):
        chunk = deduped[i : i + CHUNK]
        try:
            client.table("insider_trades_history").upsert(
                chunk,
                on_conflict="sec_url",
                ignore_duplicates=True,
            ).execute()
            inserted += len(chunk)
        except Exception as exc:
            logger.warning("upsert_history_trades chunk %d failed: %s", i, exc)

    logger.info(
        "History trades (all types): %d inserted out of %d provided",
        inserted, len(rows),
    )
    return inserted


def get_history_trades(
    trade_type: str = "",
    limit: int = 100,
    since_date: str = "",
) -> list[dict] | None:
    """Query ``insider_trades_history`` cold table.

    Same interface as :func:`get_insider_trades` but reads from the
    permanent cold archive.  Used when the date filter extends beyond
    the 30-day hot table window.

    Args:
        trade_type: ``"Purchase"`` for buys, ``"Sale"`` for sells,
                    ``""`` for all.
        limit: Max rows to return.
        since_date: If set, only return trades with ``trade_date >= since_date``.

    Returns list of row dicts, or ``None`` if Supabase is unavailable.
    """
    client = _get_client()
    if client is None:
        return None

    try:
        query = (
            client.table("insider_trades_history")
            .select(_FULL_HISTORY_COLS)
            .order("trade_date", desc=True)
            .order("filing_date", desc=True)
            .limit(limit)
        )
        if trade_type:
            if trade_type == "Sale":
                query = query.in_("trade_type", ["Sale", "Sale+OE"])
            else:
                query = query.eq("trade_type", trade_type)
        if since_date:
            query = query.gte("trade_date", since_date)

        resp = query.execute()
        return resp.data
    except Exception as exc:
        logger.warning("get_history_trades failed: %s", exc)
        return None


# ── Insider Insights: forward-return queries ─────────────────────

_INSIGHT_COLS = (
    "sec_url,trade_date,ticker,insider_name,title,price,value,"
    "close_on_trade,close_at_30d,close_at_90d,close_at_180d,close_at_365d,"
    "returns_updated"
)


def get_insider_purchases(ticker: str, limit: int = 500) -> list[dict] | None:
    """Get open market purchases for a ticker (for insights computation).

    Reads from ``insider_purchases_history`` (cold, delete-protected table).
    All rows are purchases — no trade_type filter needed.
    Returns numeric columns needed for analysis, ordered by trade_date DESC.
    """
    client = _get_client()
    if client is None:
        return None
    try:
        resp = (
            client.table("insider_purchases_history")
            .select(_INSIGHT_COLS)
            .eq("ticker", ticker.upper())
            .order("trade_date", desc=True)
            .limit(limit)
            .execute()
        )
        return resp.data if resp.data else None
    except Exception as exc:
        logger.warning("get_insider_purchases(%s) failed: %s", ticker, exc)
        return None


def get_purchases_pending_returns(limit: int = 2000) -> list[dict] | None:
    """Get purchases where returns have not yet been computed.

    Reads from ``insider_purchases_history`` (cold table).
    Returns rows with returns_updated IS NULL, ordered by ticker then trade_date.
    Used by the insider_returns.py background job.
    """
    client = _get_client()
    if client is None:
        return None
    try:
        resp = (
            client.table("insider_purchases_history")
            .select("sec_url,trade_date,ticker,price")
            .is_("returns_updated", "null")
            .order("ticker")
            .order("trade_date", desc=True)
            .limit(limit)
            .execute()
        )
        return resp.data if resp.data else None
    except Exception as exc:
        logger.warning("get_purchases_pending_returns failed: %s", exc)
        return None


def get_purchases_with_open_windows() -> list[dict] | None:
    """Get purchases where a forward window has closed but price is still NULL.

    Reads from ``insider_purchases_history`` (cold table).
    For example, a trade from 100 days ago should have close_at_90d filled in,
    but it may still be NULL if it was processed before the 90-day mark.
    """
    client = _get_client()
    if client is None:
        return None
    try:
        cutoff_90 = (datetime.now(timezone.utc) - timedelta(days=95)).strftime("%Y-%m-%d")
        cutoff_180 = (datetime.now(timezone.utc) - timedelta(days=185)).strftime("%Y-%m-%d")
        cutoff_365 = (datetime.now(timezone.utc) - timedelta(days=370)).strftime("%Y-%m-%d")

        # Trades old enough for 90d but missing it
        rows_90 = (
            client.table("insider_purchases_history")
            .select("sec_url,trade_date,ticker")
            .is_("close_at_90d", "null")
            .lte("trade_date", cutoff_90)
            .not_.is_("returns_updated", "null")  # already processed once
            .limit(500)
            .execute()
        ).data or []

        rows_180 = (
            client.table("insider_purchases_history")
            .select("sec_url,trade_date,ticker")
            .is_("close_at_180d", "null")
            .lte("trade_date", cutoff_180)
            .not_.is_("returns_updated", "null")
            .limit(500)
            .execute()
        ).data or []

        rows_365 = (
            client.table("insider_purchases_history")
            .select("sec_url,trade_date,ticker")
            .is_("close_at_365d", "null")
            .lte("trade_date", cutoff_365)
            .not_.is_("returns_updated", "null")
            .limit(500)
            .execute()
        ).data or []

        # Deduplicate by sec_url
        seen: set[str] = set()
        result: list[dict] = []
        for row in rows_90 + rows_180 + rows_365:
            if row["sec_url"] not in seen:
                seen.add(row["sec_url"])
                result.append(row)
        return result if result else None
    except Exception as exc:
        logger.warning("get_purchases_with_open_windows failed: %s", exc)
        return None


def bulk_update_forward_returns(updates: list[dict]) -> int:
    """Batch update forward-return columns on ``insider_purchases_history``.

    Each dict in ``updates`` must have ``sec_url`` plus any of:
    close_on_trade, close_at_30d, close_at_90d, close_at_180d,
    close_at_365d, returns_updated.

    Uses individual UPDATE (not upsert) to avoid NOT NULL constraint
    violations on columns like filing_date.  The write-once trigger on
    the history table silently preserves existing non-NULL return values.

    Returns count of rows updated.
    """
    client = _get_client()
    if client is None:
        return 0

    # Batch upsert in chunks of 50 — avoids N+1 individual UPDATEs.
    # Uses upsert with on_conflict="sec_url" so the DB handles matching.
    CHUNK = 50
    updated = 0

    # Filter out rows with no sec_url or no payload
    valid_rows = []
    for row in updates:
        sec_url = row.get("sec_url")
        if not sec_url:
            continue
        payload = {k: v for k, v in row.items()}
        if len(payload) <= 1:  # only sec_url, nothing to update
            continue
        valid_rows.append(payload)

    for i in range(0, len(valid_rows), CHUNK):
        chunk = valid_rows[i : i + CHUNK]
        try:
            client.table("insider_purchases_history").upsert(
                chunk, on_conflict="sec_url"
            ).execute()
            updated += len(chunk)
        except Exception as exc:
            logger.warning(
                "bulk_update_forward_returns chunk %d failed: %s", i // CHUNK, exc
            )
            # Fall back to individual updates for this chunk
            for row in chunk:
                sec_url = row.get("sec_url")
                payload = {k: v for k, v in row.items() if k != "sec_url"}
                try:
                    client.table("insider_purchases_history").update(payload).eq(
                        "sec_url", sec_url
                    ).execute()
                    updated += 1
                except Exception as exc2:
                    logger.warning(
                        "bulk_update_forward_returns fallback failed for %s: %s",
                        sec_url, exc2,
                    )

    return updated


def admin_force_update_returns(sec_url: str, updates: dict) -> bool:
    """Force-update forward return values (admin override of write-once).

    DANGER: Bypasses the write-once trigger by NULLing ``returns_updated``
    first, then re-applying the corrected values.  Use only for data
    corrections (e.g., stock-split adjustments, bad price data).

    ``updates`` should contain only forward-return columns, e.g.::

        {"close_at_90d": 123.45, "close_at_180d": 130.00}

    Returns True on success, False on failure.
    """
    client = _get_client()
    if client is None:
        return False
    try:
        # Step 1: NULL out returns_updated to unlock the row
        # (the write-once trigger allows all changes when returns_updated IS NULL)
        client.table("insider_purchases_history").update(
            {"returns_updated": None}
        ).eq("sec_url", sec_url).execute()

        # Step 2: Apply corrected values + re-set returns_updated
        updates["returns_updated"] = datetime.now(timezone.utc).isoformat()
        client.table("insider_purchases_history").update(
            updates
        ).eq("sec_url", sec_url).execute()

        logger.info("admin_force_update_returns: corrected %s", sec_url)
        return True
    except Exception as exc:
        logger.warning("admin_force_update_returns failed for %s: %s", sec_url, exc)
        return False


def get_distinct_insider_tickers() -> list[str]:
    """Return sorted list of unique tickers in ``insider_purchases_history``.

    Used by the per-ticker backfill to know which tickers to scrape
    historical data for from OpenInsider.
    """
    client = _get_client()
    if client is None:
        return []
    try:
        # Fetch all unique tickers — Supabase doesn't support DISTINCT,
        # so we select ticker column and deduplicate in Python.
        resp = (
            client.table("insider_purchases_history")
            .select("ticker")
            .limit(10000)
            .execute()
        )
        if not resp.data:
            return []
        tickers = sorted({row["ticker"] for row in resp.data if row.get("ticker")})
        return tickers
    except Exception as exc:
        logger.warning("get_distinct_insider_tickers failed: %s", exc)
        return []


# ── Sync worker helpers ──────────────────────────────────────────


def update_sync_status(cache_key: str, status: str) -> bool:
    """Update ``last_synced_at`` and ``sync_status`` for a cache row.

    Called by the sync worker after each fund refresh attempt.
    """
    client = _get_client()
    if client is None:
        return False
    now = datetime.now(timezone.utc).isoformat()
    try:
        client.table(_TABLE).update(
            {
                "last_synced_at": now,
                "sync_status": status,
            }
        ).eq("cache_key", cache_key).execute()
        return True
    except Exception as exc:
        logger.warning("update_sync_status failed for %s: %s", cache_key, exc)
        return False


def get_stale_sync_keys(max_age_hours: int = 24) -> list[str]:
    """Return ``cache_key`` values for 13f rows needing a sync.

    A row is considered stale if ``last_synced_at`` is NULL or older
    than *max_age_hours*.
    """
    client = _get_client()
    if client is None:
        return []

    cutoff = (datetime.now(timezone.utc) - timedelta(hours=max_age_hours)).isoformat()

    try:
        # Keys where never synced
        resp_null = (
            client.table(_TABLE)
            .select("cache_key")
            .eq("category", "13f")
            .is_("last_synced_at", "null")
            .execute()
        )
        # Keys where sync is stale
        resp_old = (
            client.table(_TABLE)
            .select("cache_key")
            .eq("category", "13f")
            .lt("last_synced_at", cutoff)
            .execute()
        )
        keys: set[str] = set()
        for row in resp_null.data or []:
            keys.add(row["cache_key"])
        for row in resp_old.data or []:
            keys.add(row["cache_key"])
        return sorted(keys)
    except Exception as exc:
        logger.warning("get_stale_sync_keys failed: %s", exc)
        return []


def create_sync_log(run_id: str) -> bool:
    """Insert a new sync_logs row when a sync run starts."""
    client = _get_client()
    if client is None:
        return False
    try:
        client.table("sync_logs").insert(
            {
                "run_id": run_id,
            }
        ).execute()
        return True
    except Exception as exc:
        logger.warning("create_sync_log failed: %s", exc)
        return False


def complete_sync_log(
    run_id: str,
    funds_updated: int,
    funds_failed: int,
    funds_skipped: int,
    errors: list[str],
) -> bool:
    """Finalise a sync_logs row when the run completes."""
    client = _get_client()
    if client is None:
        return False
    now = datetime.now(timezone.utc).isoformat()
    try:
        client.table("sync_logs").update(
            {
                "completed_at": now,
                "funds_updated": funds_updated,
                "funds_failed": funds_failed,
                "funds_skipped": funds_skipped,
                "error_messages": errors[:50],  # Cap stored errors
            }
        ).eq("run_id", run_id).execute()
        return True
    except Exception as exc:
        logger.warning("complete_sync_log failed: %s", exc)
        return False


# ── YouTube helpers ──────────────────────────────────────────────


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


# ── Retention cleanup ───────────────────────────────────────────


def run_retention_cleanup() -> dict:
    """Run all retention policies -- delete old data to keep DB small.

    Policies:
      1. insider_trades: delete rows with trade_date older than 30 days
         (hot table only — historical purchases live in delete-protected
         insider_purchases_history cold table)
      2. sync_logs: delete rows with started_at older than 30 days
      3. youtube_events: delete rows with scheduled_at older than 30 days
      4. api_cache: physically delete expired rows (excluding 13f which
         uses stale-while-revalidate)

    Each table cleanup is independent -- failure on one doesn't block others.
    Returns summary dict with deletion counts (-1 means error).
    """
    client = _get_client()
    if client is None:
        return {"status": "skipped", "reason": "supabase_unavailable"}

    now = datetime.now(timezone.utc)
    results: dict[str, int] = {}

    # 1. Insider trades (hot table): keep 30 days of recent data.
    # Historical purchases live in insider_purchases_history (cold table,
    # delete-protected) — so this hot table can be aggressively cleaned.
    cutoff_insider = (now - timedelta(days=30)).strftime("%Y-%m-%d")
    try:
        resp = (
            client.table("insider_trades")
            .delete(count="exact")
            .lt("trade_date", cutoff_insider)
            .execute()
        )
        results["insider_trades_deleted"] = resp.count if resp.count is not None else 0
    except Exception as exc:
        logger.warning("Retention: insider_trades cleanup failed: %s", exc)
        results["insider_trades_deleted"] = -1

    # 2. Sync logs: delete > 30 days
    cutoff_30d_ts = (now - timedelta(days=30)).isoformat()
    try:
        resp = (
            client.table("sync_logs")
            .delete(count="exact")
            .lt("started_at", cutoff_30d_ts)
            .execute()
        )
        results["sync_logs_deleted"] = resp.count if resp.count is not None else 0
    except Exception as exc:
        logger.warning("Retention: sync_logs cleanup failed: %s", exc)
        results["sync_logs_deleted"] = -1

    # 3. YouTube events: delete > 30 days
    try:
        resp = (
            client.table("youtube_events")
            .delete(count="exact")
            .lt("scheduled_at", cutoff_30d_ts)
            .execute()
        )
        results["youtube_events_deleted"] = resp.count if resp.count is not None else 0
    except Exception as exc:
        logger.warning("Retention: youtube_events cleanup failed: %s", exc)
        results["youtube_events_deleted"] = -1

    # 4. Expired api_cache rows: physical delete (skip 13f -- stale-while-revalidate)
    now_iso = now.isoformat()
    try:
        resp = (
            client.table("api_cache")
            .delete(count="exact")
            .lt("expires_at", now_iso)
            .neq("category", "13f")
            .execute()
        )
        results["expired_cache_deleted"] = resp.count if resp.count is not None else 0
    except Exception as exc:
        logger.warning("Retention: api_cache cleanup failed: %s", exc)
        results["expired_cache_deleted"] = -1

    logger.info("Retention cleanup: %s", results)
    return results


# ── Panda Fund supporters ─────────────────────────────────────────────────────

def record_supporter(
    stripe_event_id: str,
    session_id: str,
    customer_email: str,
    amount_cents: int,
    currency: str,
    mode: str,
    month: str,
) -> bool:
    """Insert a supporter row for a completed Stripe payment.

    Uses ``stripe_event_id`` as the unique key so the same Stripe webhook
    event delivered more than once is silently ignored (idempotent).

    Returns True on success, False on error or when Supabase is unavailable.
    """
    client = _get_client()
    if client is None:
        logger.warning("record_supporter: Supabase unavailable — payment not persisted")
        return False

    row = {
        "stripe_event_id": stripe_event_id,
        "session_id": session_id,
        "customer_email": customer_email,
        "amount_cents": amount_cents,
        "currency": currency,
        "mode": mode,
        "month": month,
    }
    try:
        client.table("supporters").upsert(row, on_conflict="stripe_event_id").execute()
        logger.info(
            "record_supporter: recorded %s cents (%s) for event %s",
            amount_cents,
            mode,
            stripe_event_id,
        )
        return True
    except Exception as exc:
        logger.exception("record_supporter: failed to write supporter row: %s", exc)
        return False


def get_monthly_raised_cents(month: str) -> int:
    """Return total amount raised (in cents) for the given month ('YYYY-MM').

    Returns 0 when Supabase is unavailable or the table is empty.
    """
    client = _get_client()
    if client is None:
        return 0

    try:
        resp = (
            client.table("supporters")
            .select("amount_cents")
            .eq("month", month)
            .execute()
        )
        return sum(row["amount_cents"] for row in (resp.data or []))
    except Exception as exc:
        logger.warning("get_monthly_raised_cents: query failed: %s", exc)
        return 0


def get_funding_history(num_months: int = 6) -> list[dict]:
    """Return a list of {month, raised} dicts from the launch month to today.

    Always starts from February 2025 (launch month) and runs through the
    current month, so the chart never shows a sliding window that drops
    early months.

    ``month`` is the short month name (e.g. 'Feb'), ``raised`` is in dollars.
    Returns an empty list when Supabase is unavailable.
    """
    from calendar import month_abbr as _abbr
    from datetime import date

    client = _get_client()
    history: list[dict] = []

    # Build the list of YYYY-MM strings from launch → today, oldest → newest
    launch_year, launch_month = 2026, 2  # February 2026
    today = date.today()
    months: list[str] = []
    y, m = launch_year, launch_month
    while (y, m) <= (today.year, today.month):
        months.append(f"{y}-{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1

    if client is None:
        return [{"month": _abbr[int(m.split("-")[1])], "raised": 0} for m in months]

    try:
        resp = (
            client.table("supporters")
            .select("month, amount_cents")
            .in_("month", months)
            .execute()
        )
        totals: dict[str, int] = {}
        for row in resp.data or []:
            totals[row["month"]] = totals.get(row["month"], 0) + row["amount_cents"]

        for ym in months:
            mon_idx = int(ym.split("-")[1])
            history.append({
                "month": _abbr[mon_idx],
                "raised": totals.get(ym, 0) // 100,  # cents → dollars
            })
    except Exception as exc:
        logger.warning("get_funding_history: query failed: %s", exc)
        history = [{"month": _abbr[int(m.split("-")[1])], "raised": 0} for m in months]

    return history


# ── Notifications ────────────────────────────────────────────────


def upsert_notifications(rows: list[dict]) -> int:
    """Batch upsert notification rows, skipping existing IDs.

    Uses deterministic ``id`` for deduplication — the database PRIMARY KEY
    constraint handles conflicts (``ON CONFLICT DO NOTHING`` via
    ``ignore_duplicates=True``).  No pre-fetch needed.

    Returns the number of rows sent, or 0 on failure.
    """
    client = _get_client()
    if client is None or not rows:
        return 0

    inserted = 0
    CHUNK = 50
    for i in range(0, len(rows), CHUNK):
        chunk = rows[i : i + CHUNK]
        try:
            client.table("notifications").upsert(
                chunk, on_conflict="id", ignore_duplicates=True
            ).execute()
            inserted += len(chunk)
        except Exception as exc:
            logger.warning("upsert_notifications chunk %d failed: %s", i, exc)

    if inserted:
        logger.info("Upserted %d notifications (dupes skipped by DB)", inserted)
    return inserted


def get_recent_notifications(
    limit: int = 20,
    types: list[str] | None = None,
    offset: int = 0,
) -> list[dict]:
    """Fetch the most recent notifications, newest first.

    *types* — optional list of notification type strings to include
    (e.g. ``["13f_change", "youtube"]``).  ``None`` means all types.
    *offset* — number of rows to skip (for pagination).

    Returns a list of notification dicts, or [] on failure.
    """
    client = _get_client()
    if client is None:
        return []

    try:
        q = (
            client.table("notifications")
            .select(_NOTIFICATION_COLS)
        )
        if types:
            q = q.in_("type", types)
        q = q.order("created_at", desc=True)
        if offset > 0:
            q = q.range(offset, offset + limit - 1)
        else:
            q = q.limit(limit)
        resp = q.execute()
        return resp.data or []
    except Exception as exc:
        logger.warning("get_recent_notifications failed: %s", exc)
        return []


def get_bell_state(since_iso: str) -> tuple[int, dict | None]:
    """Return (count, latest_notification) for notifications after *since_iso*.

    Performs a single database query that returns both the count (via
    ``count="exact"``) and the most recent notification row.  This
    replaces the old ``get_notification_count_since`` + ``get_latest_notification``
    pair, halving the number of round-trips for the bell poll.
    """
    client = _get_client()
    if client is None:
        return 0, None

    try:
        resp = (
            client.table("notifications")
            .select(_NOTIFICATION_COLS, count="exact")
            .gt("created_at", since_iso)
            .order("created_at", desc=True)
            .limit(1)
            .execute()
        )
        count = resp.count or 0
        latest = resp.data[0] if resp.data else None
        return count, latest
    except Exception as exc:
        logger.warning("get_bell_state failed: %s", exc)
        return 0, None


def cleanup_old_notifications(days: int = 2) -> int:
    """Delete notifications older than *days* days.

    Called at the start of each sync cycle to keep the table small.
    Returns the number of rows deleted, or 0 on failure.
    """
    client = _get_client()
    if client is None:
        return 0

    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    try:
        resp = (
            client.table("notifications")
            .delete(count="exact")
            .lt("created_at", cutoff)
            .execute()
        )
        deleted = resp.count or 0
        if deleted:
            logger.info("Cleaned up %d old notifications (>%dd)", deleted, days)
        return deleted
    except Exception as exc:
        logger.warning("cleanup_old_notifications failed: %s", exc)
        return 0


# ── Feature announcements ────────────────────────────────────────────

_FEATURE_ANNOUNCEMENT_COLS = "id,title,message,icon,toast_type,link,created_at"


def get_recent_feature_announcements(limit: int = 20) -> list[dict]:
    """Fetch recent feature announcements (newest first, last 7 days).

    Only returns announcements from the last 7 days to avoid
    re-syncing old entries whose derived notifications have already
    been cleaned up by ``cleanup_old_notifications``.
    """
    client = _get_client()
    if client is None:
        return []
    try:
        cutoff = (
            datetime.now(timezone.utc) - timedelta(days=7)
        ).isoformat()
        resp = (
            client.table("feature_announcements")
            .select(_FEATURE_ANNOUNCEMENT_COLS)
            .gt("created_at", cutoff)
            .order("created_at", desc=True)
            .limit(limit)
            .execute()
        )
        return resp.data or []
    except Exception as exc:
        logger.warning("get_recent_feature_announcements failed: %s", exc)
        return []


# ── Congress Trading: member + trade CRUD ─────────────────────────────

_CONGRESS_MEMBER_COLS = (
    "member_id,full_name,first_name,last_name,party,chamber,"
    "state,state_abbr,district,is_current,"
    "first_trade_date,last_trade_date,total_trades,"
    "net_worth_estimate,net_worth_source,net_worth_year,"
    "created_at,updated_at"
)

_CONGRESS_TRADE_COLS = (
    "trade_id,member_id,politician_name,party,chamber,state,"
    "ticker,asset_name,trade_type,trade_date,filing_date,"
    "amount_low,amount_high,amount_display,owner,cap_trades_url,created_at"
)


def upsert_congress_members(rows: list[dict]) -> int:
    """Batch upsert politician profiles into ``congress_members``.

    Uses ``ON CONFLICT (member_id) DO UPDATE`` to refresh metadata
    (last_trade_date, total_trades, etc.) on re-scrapes.
    Returns the number of rows upserted, or 0 on failure.
    """
    client = _get_client()
    if client is None:
        return 0

    if not rows:
        return 0

    # Deduplicate by member_id
    seen: dict[str, dict] = {}
    for row in rows:
        mid = row.get("member_id", "")
        if mid:
            seen[mid] = row
    deduped = list(seen.values())

    upserted = 0
    CHUNK = 50
    for i in range(0, len(deduped), CHUNK):
        chunk = deduped[i : i + CHUNK]
        try:
            client.table("congress_members").upsert(
                chunk, on_conflict="member_id"
            ).execute()
            upserted += len(chunk)
        except Exception as exc:
            logger.warning("upsert_congress_members chunk %d failed: %s", i, exc)

    logger.info("Congress members: %d upserted out of %d provided", upserted, len(rows))
    return upserted


def upsert_congress_trades(rows: list[dict]) -> int:
    """Batch insert trades into ``congress_trades`` (cold, write-once).

    Uses ``ON CONFLICT (trade_id) DO NOTHING`` — existing rows are never
    overwritten.  Follows the ``upsert_history_purchases()`` pattern.
    Returns the number of rows inserted, or 0 on failure.
    """
    client = _get_client()
    if client is None:
        return 0

    if not rows:
        return 0

    # Deduplicate by trade_id within the batch
    seen: dict[str, dict] = {}
    for row in rows:
        tid = row.get("trade_id", "")
        if tid and tid not in seen:
            seen[tid] = row
    deduped = list(seen.values())

    if len(deduped) < len(rows):
        logger.info(
            "Congress trades: deduplicated %d → %d rows",
            len(rows), len(deduped),
        )

    inserted = 0
    CHUNK = 50
    for i in range(0, len(deduped), CHUNK):
        chunk = deduped[i : i + CHUNK]
        try:
            # DO NOTHING on conflict — write-once cold archive.
            # ignore_duplicates=True → PostgREST sends
            # "Prefer: resolution=ignore-duplicates" which translates
            # to INSERT … ON CONFLICT (trade_id) DO NOTHING.
            client.table("congress_trades").upsert(
                chunk,
                on_conflict="trade_id",
                ignore_duplicates=True,
            ).execute()
            inserted += len(chunk)
        except Exception as exc:
            logger.warning("upsert_congress_trades chunk %d failed: %s", i, exc)

    logger.info(
        "Congress trades: %d inserted out of %d provided", inserted, len(rows)
    )
    return inserted


def get_congress_member(member_id: str) -> dict | None:
    """Get a single politician's profile from ``congress_members``."""
    client = _get_client()
    if client is None:
        return None
    try:
        resp = (
            client.table("congress_members")
            .select(_CONGRESS_MEMBER_COLS)
            .eq("member_id", member_id)
            .limit(1)
            .execute()
        )
        rows = resp.data or []
        return rows[0] if rows else None
    except Exception as exc:
        logger.warning("get_congress_member(%s) failed: %s", member_id, exc)
        return None


def get_all_congress_members() -> list[dict] | None:
    """Get all politician profiles (for search index and listing page).

    Returns list sorted by total_trades DESC (most active first).
    """
    client = _get_client()
    if client is None:
        return None
    try:
        resp = (
            client.table("congress_members")
            .select(_CONGRESS_MEMBER_COLS)
            .order("total_trades", desc=True)
            .limit(500)
            .execute()
        )
        return resp.data or []
    except Exception as exc:
        logger.warning("get_all_congress_members failed: %s", exc)
        return None


def get_congress_trades_by_member(
    member_id: str, limit: int = 500
) -> list[dict] | None:
    """Get all trades for a specific politician (for profile page).

    Returns trades sorted by trade_date DESC (newest first).
    """
    client = _get_client()
    if client is None:
        return None
    try:
        resp = (
            client.table("congress_trades")
            .select(_CONGRESS_TRADE_COLS)
            .eq("member_id", member_id)
            .order("trade_date", desc=True)
            .limit(limit)
            .execute()
        )
        return resp.data or []
    except Exception as exc:
        logger.warning("get_congress_trades_by_member(%s) failed: %s", member_id, exc)
        return None


def get_congress_trades_by_ticker(
    ticker: str, limit: int = 500
) -> list[dict] | None:
    """Get all congressional trades for a stock ticker (for Congress subtab).

    Returns trades sorted by trade_date DESC (newest first).
    """
    client = _get_client()
    if client is None:
        return None
    try:
        resp = (
            client.table("congress_trades")
            .select(_CONGRESS_TRADE_COLS)
            .eq("ticker", ticker.upper())
            .order("trade_date", desc=True)
            .limit(limit)
            .execute()
        )
        return resp.data or []
    except Exception as exc:
        logger.warning("get_congress_trades_by_ticker(%s) failed: %s", ticker, exc)
        return None


def get_congress_recent_trades(limit: int = 100) -> list[dict] | None:
    """Get most recent trades across all politicians (for notifications).

    Returns trades sorted by filing_date DESC (most recently disclosed).
    """
    client = _get_client()
    if client is None:
        return None
    try:
        resp = (
            client.table("congress_trades")
            .select(_CONGRESS_TRADE_COLS)
            .order("filing_date", desc=True)
            .limit(limit)
            .execute()
        )
        return resp.data or []
    except Exception as exc:
        logger.warning("get_congress_recent_trades failed: %s", exc)
        return None


# ── Congress Trades Prices (forward-return enrichment) ─────────────────

_CONGRESS_PRICE_COLS = (
    "trade_id,ticker,trade_date,close_on_trade,"
    "close_at_30d,close_at_90d,close_at_180d,close_at_365d,"
    "return_30d,return_90d,return_180d,return_365d,prices_updated"
)


def get_congress_trades_missing_prices(limit: int = 5000) -> list[dict] | None:
    """Get congress trades that don't have price data yet.

    Fetches trades with a non-null ticker that do NOT have a corresponding
    row in ``congress_trades_prices``.  Used by the backfill script.
    """
    client = _get_client()
    if client is None:
        return None
    try:
        # Use a LEFT JOIN via PostgREST embedding syntax:
        # Select from congress_trades with a left join to congress_trades_prices
        # where the prices row is null (i.e., not yet backfilled).
        # PostgREST doesn't directly support "WHERE joined IS NULL" with
        # embedding, so we fetch trades and filter client-side, or use RPC.
        # Simpler approach: fetch all trade_ids that DO have prices, then
        # fetch trades NOT in that set.

        # Step 1: get existing price trade_ids
        price_resp = (
            client.table("congress_trades_prices")
            .select("trade_id")
            .limit(50000)
            .execute()
        )
        existing_ids = {r["trade_id"] for r in (price_resp.data or [])}

        # Step 2: get trades with tickers, not in existing set
        resp = (
            client.table("congress_trades")
            .select("trade_id,member_id,politician_name,ticker,trade_date,trade_type,amount_low,amount_high")
            .not_.is_("ticker", "null")
            .order("trade_date", desc=True)
            .limit(limit)
            .execute()
        )
        all_trades = resp.data or []

        # Filter out those already priced
        missing = [t for t in all_trades if t["trade_id"] not in existing_ids]
        logger.info(
            "Congress trades missing prices: %d of %d (existing: %d)",
            len(missing), len(all_trades), len(existing_ids),
        )
        return missing
    except Exception as exc:
        logger.warning("get_congress_trades_missing_prices failed: %s", exc)
        return None


def upsert_congress_trade_prices(rows: list[dict]) -> int:
    """Batch upsert price/return data into ``congress_trades_prices``.

    Uses ON CONFLICT (trade_id) DO UPDATE so prices can be refreshed
    as new forward windows close.
    """
    client = _get_client()
    if client is None:
        return 0
    if not rows:
        return 0

    upserted = 0
    CHUNK = 50
    for i in range(0, len(rows), CHUNK):
        chunk = rows[i : i + CHUNK]
        try:
            client.table("congress_trades_prices").upsert(
                chunk, on_conflict="trade_id"
            ).execute()
            upserted += len(chunk)
        except Exception as exc:
            logger.warning("upsert_congress_trade_prices chunk %d failed: %s", i, exc)

    logger.info("Congress trade prices: %d upserted", upserted)
    return upserted


def get_congress_trades_recent_months(
    months: int = 6, limit: int = 5000
) -> list[dict] | None:
    """Get trades from the last N months for trending/momentum calculations.

    Returns trades sorted by trade_date DESC with a non-null ticker.
    """
    client = _get_client()
    if client is None:
        return None
    try:
        from datetime import datetime, timedelta, timezone

        cutoff = (datetime.now(timezone.utc) - timedelta(days=months * 30)).strftime(
            "%Y-%m-%d"
        )
        resp = (
            client.table("congress_trades")
            .select(_CONGRESS_TRADE_COLS)
            .gte("trade_date", cutoff)
            .not_.is_("ticker", "null")
            .order("trade_date", desc=True)
            .limit(limit)
            .execute()
        )
        return resp.data or []
    except Exception as exc:
        logger.warning("get_congress_trades_recent_months failed: %s", exc)
        return None


def get_congress_all_ticker_trades(limit: int = 50000) -> list[dict] | None:
    """Get all trades with a non-null ticker (for consensus calculation).

    Returns a slim projection for aggregation — avoids transferring
    large text fields.  Paginated to handle the full ~35K dataset.
    """
    client = _get_client()
    if client is None:
        return None
    try:
        slim_cols = (
            "trade_id,member_id,politician_name,party,chamber,"
            "ticker,asset_name,trade_type,trade_date,"
            "amount_low,amount_high"
        )
        all_rows: list[dict] = []
        page_size = 1000
        offset = 0

        while offset < limit:
            resp = (
                client.table("congress_trades")
                .select(slim_cols)
                .not_.is_("ticker", "null")
                .order("trade_date", desc=True)
                .range(offset, offset + page_size - 1)
                .execute()
            )
            batch = resp.data or []
            all_rows.extend(batch)
            if len(batch) < page_size:
                break
            offset += page_size

        logger.info("Congress all ticker trades: fetched %d rows", len(all_rows))
        return all_rows
    except Exception as exc:
        logger.warning("get_congress_all_ticker_trades failed: %s", exc)
        return None


# ── Congress sync log ─────────────────────────────────────────────────


def get_latest_congress_sync(limit: int = 5) -> list[dict] | None:
    """Get the most recent sync log entries for health monitoring.

    Returns rows from ``congress_sync_log`` sorted by started_at DESC.
    """
    client = _get_client()
    if client is None:
        return None
    try:
        resp = (
            client.table("congress_sync_log")
            .select("*")
            .order("started_at", desc=True)
            .limit(limit)
            .execute()
        )
        return resp.data or []
    except Exception as exc:
        logger.warning("get_latest_congress_sync failed: %s", exc)
        return None


# ── Ticker logos ─────────────────────────────────────────────────────


def get_all_logos() -> list[dict]:
    """Bulk-read every ticker logo from ``ticker_logos``.

    Returns list of ``{"ticker": ..., "logo_b64": ...}`` dicts.
    Used at startup to populate the in-memory logo cache.

    Paginates in pages of 1000 to work around Supabase PostgREST
    default row limit.
    """
    client = _get_client()
    if client is None:
        return []
    try:
        all_rows: list[dict] = []
        page_size = 1000
        offset = 0
        while True:
            resp = (
                client.table("ticker_logos")
                .select("ticker,logo_b64")
                .neq("logo_b64", "")
                .range(offset, offset + page_size - 1)
                .execute()
            )
            rows = resp.data or []
            all_rows.extend(rows)
            if len(rows) < page_size:
                break
            offset += page_size
        return all_rows
    except Exception as exc:
        logger.warning("get_all_logos failed: %s", exc)
        return []


def get_existing_logo_tickers() -> set[str]:
    """Return the set of tickers that already have a logo stored.

    Paginates in pages of 1000 to work around Supabase PostgREST
    default row limit.
    """
    client = _get_client()
    if client is None:
        return set()
    try:
        result: set[str] = set()
        page_size = 1000
        offset = 0
        while True:
            resp = (
                client.table("ticker_logos")
                .select("ticker")
                .range(offset, offset + page_size - 1)
                .execute()
            )
            rows = resp.data or []
            result.update(row["ticker"] for row in rows if row.get("ticker"))
            if len(rows) < page_size:
                break
            offset += page_size
        return result
    except Exception as exc:
        logger.warning("get_existing_logo_tickers failed: %s", exc)
        return set()


def insert_logos(rows: list[dict]) -> int:
    """Insert-only: add new logos to ``ticker_logos``, never overwrite existing.

    Uses upsert with ``ignore_duplicates=True`` so rows whose ticker
    already exists are silently skipped — existing data is NEVER modified.

    Each row should have: ``ticker``, ``logo_b64``, ``content_type``, ``logo_domain``.
    """
    client = _get_client()
    if client is None:
        return 0

    inserted = 0
    CHUNK = 50
    for i in range(0, len(rows), CHUNK):
        chunk = rows[i : i + CHUNK]
        try:
            client.table("ticker_logos").upsert(
                chunk, on_conflict="ticker", ignore_duplicates=True
            ).execute()
            inserted += len(chunk)
        except Exception as exc:
            logger.warning("insert_logos chunk %d failed: %s", i, exc)

    return inserted


# ── Congress headshots (mirrors ticker_logos pattern) ─────────────


def get_all_headshots() -> list[dict]:
    """Bulk-read every congress headshot from ``congress_headshots``.

    Returns list of ``{"member_id": ..., "photo_b64": ...}`` dicts.
    Used at startup to populate the in-memory headshot cache.

    Paginates in pages of 1000 to work around Supabase PostgREST
    default row limit.
    """
    client = _get_client()
    if client is None:
        return []
    try:
        all_rows: list[dict] = []
        page_size = 1000
        offset = 0
        while True:
            resp = (
                client.table("congress_headshots")
                .select("member_id,photo_b64")
                .neq("photo_b64", "")
                .range(offset, offset + page_size - 1)
                .execute()
            )
            rows = resp.data or []
            all_rows.extend(rows)
            if len(rows) < page_size:
                break
            offset += page_size
        return all_rows
    except Exception as exc:
        logger.warning("get_all_headshots failed: %s", exc)
        return []


def get_existing_headshot_members() -> set[str]:
    """Return the set of member_ids that already have a headshot stored.

    Paginates in pages of 1000 to work around Supabase PostgREST
    default row limit.
    """
    client = _get_client()
    if client is None:
        return set()
    try:
        result: set[str] = set()
        page_size = 1000
        offset = 0
        while True:
            resp = (
                client.table("congress_headshots")
                .select("member_id")
                .range(offset, offset + page_size - 1)
                .execute()
            )
            rows = resp.data or []
            result.update(row["member_id"] for row in rows if row.get("member_id"))
            if len(rows) < page_size:
                break
            offset += page_size
        return result
    except Exception as exc:
        logger.warning("get_existing_headshot_members failed: %s", exc)
        return set()


def insert_headshots(rows: list[dict]) -> int:
    """Insert-only: add new headshots to ``congress_headshots``, never overwrite.

    Uses upsert with ``ignore_duplicates=True`` so rows whose member_id
    already exists are silently skipped — existing data is NEVER modified.

    Each row should have: ``member_id``, ``photo_b64``, ``content_type``.
    """
    client = _get_client()
    if client is None:
        return 0

    inserted = 0
    CHUNK = 50
    for i in range(0, len(rows), CHUNK):
        chunk = rows[i : i + CHUNK]
        try:
            client.table("congress_headshots").upsert(
                chunk, on_conflict="member_id", ignore_duplicates=True
            ).execute()
            inserted += len(chunk)
        except Exception as exc:
            logger.warning("insert_headshots chunk %d failed: %s", i, exc)

    return inserted


# ── Short Interest History ──────────────────────────────────────────


def upsert_short_interest_rows(rows: list[dict]) -> int:
    """Batch upsert rows into ``short_interest_history``.

    Each row should have at minimum ``ticker`` and ``report_date`` (ISO
    date string).  On conflict (same ticker + report_date) the row is
    updated with the latest values.

    Returns the number of rows upserted, or 0 on failure.
    """
    client = _get_client()
    if client is None:
        return 0

    if not rows:
        return 0

    upserted = 0
    CHUNK = 100
    for i in range(0, len(rows), CHUNK):
        chunk = rows[i : i + CHUNK]
        try:
            client.table("short_interest_history").upsert(
                chunk, on_conflict="ticker,report_date"
            ).execute()
            upserted += len(chunk)
        except Exception as exc:
            logger.warning("upsert_short_interest_rows chunk %d failed: %s", i, exc)

    return upserted


def get_short_interest_history(ticker: str, limit: int = 24) -> list[dict]:
    """Fetch historical short interest data for *ticker*, newest first.

    Returns up to *limit* rows (default 24 = ~12 months of bi-monthly
    FINRA reports) as a list of dicts.  Returns empty list on error.
    """
    client = _get_client()
    if client is None:
        return []

    try:
        resp = (
            client.table("short_interest_history")
            .select(
                "report_date,shares_short,short_pct_float,"
                "short_ratio,float_shares,shares_outstanding"
            )
            .eq("ticker", ticker.upper())
            .order("report_date", desc=True)
            .limit(limit)
            .execute()
        )
        return resp.data or []
    except Exception as exc:
        logger.warning("get_short_interest_history(%s) failed: %s", ticker, exc)
        return []


def get_latest_short_interest_all(limit: int = 1600) -> list[dict]:
    """Fetch the most recent short interest row per ticker.

    Used by the sync worker to build the leaderboard.
    Returns list of dicts sorted by short_pct_float DESC.

    Uses a single SQL query via RPC to avoid two sequential round-trips.
    Falls back to the two-query approach if the RPC function isn't available.
    """
    client = _get_client()
    if client is None:
        return []

    try:
        # Single-query approach via RPC (requires get_latest_short_interest function).
        # .limit(limit) overrides PostgREST's default 1000-row ceiling on SETOF RPCs.
        resp = client.rpc(
            "get_latest_short_interest",
            {"row_limit": limit},
        ).limit(limit).execute()
        return resp.data or []
    except Exception:
        pass  # RPC not available — fall back to two-query approach

    try:
        # Fallback: fetch recent rows (last 7 days) and pick each ticker's
        # most-recent entry in Python.  This mirrors the DISTINCT ON (ticker)
        # logic in the RPC and avoids the single-date bug where FINRA data
        # lands on different dates for NYSE vs NASDAQ tickers.
        from datetime import date, timedelta

        cutoff = (date.today() - timedelta(days=7)).isoformat()
        resp = (
            client.table("short_interest_history")
            .select(
                "ticker,report_date,shares_short,shares_short_prior,"
                "short_pct_float,short_ratio,float_shares,shares_outstanding"
            )
            .gte("report_date", cutoff)
            .order("report_date", desc=True)
            .limit(limit * 5)  # over-fetch; multiple dates per ticker
            .execute()
        )
        rows = resp.data or []
        # Keep only the most-recent row per ticker (rows already sorted by date DESC)
        seen: set[str] = set()
        latest_per_ticker: list[dict] = []
        for row in rows:
            t = row.get("ticker", "")
            if t and t not in seen:
                seen.add(t)
                latest_per_ticker.append(row)
        # Sort by short % float descending (matches RPC output order)
        latest_per_ticker.sort(
            key=lambda r: r.get("short_pct_float") or 0, reverse=True
        )
        return latest_per_ticker[:limit]
    except Exception as exc:
        logger.warning("get_latest_short_interest_all failed: %s", exc)
        return []


def build_leaderboard_from_db(
    ticker_map: dict[str, list[str]] | None = None,
    limit: int = 1600,
) -> dict | None:
    """Build a short-interest leaderboard snapshot directly from the DB.

    Used as a fallback when the cron worker hasn't run yet and the
    ``short_interest_leaderboard`` cache key is empty.  Computes
    ``short_change`` / ``short_change_pct`` from the stored
    ``shares_short`` / ``shares_short_prior`` columns.

    Returns the same dict shape that ``short_interest_sync._build_leaderboard``
    produces, or ``None`` if no data is available.
    """
    import time

    rows = get_latest_short_interest_all(limit=limit)
    if not rows:
        return None

    ticker_map = ticker_map or {}
    enriched = []
    for r in rows:
        ss = r.get("shares_short") or 0
        ss_prior = r.get("shares_short_prior") or 0
        short_change = ss - ss_prior
        short_change_pct = round((short_change / ss_prior * 100), 1) if ss_prior else 0.0
        guru_names = ticker_map.get(r["ticker"], [])
        enriched.append({
            "ticker": r["ticker"],
            "short_pct_float": r.get("short_pct_float") or 0,
            "short_ratio": r.get("short_ratio") or 0,
            "shares_short": ss,
            "shares_short_prior": ss_prior,
            "short_change": short_change,
            "short_change_pct": short_change_pct,
            "float_shares": r.get("float_shares") or 0,
            "report_date": r.get("report_date", ""),
            "guru_count": len(guru_names),
            "guru_names": guru_names[:5],
        })

    highest = sorted(
        enriched,
        key=lambda x: x.get("short_pct_float") or 0,
        reverse=True,
    )[:50]

    trending = sorted(
        [e for e in enriched if e.get("short_change_pct", 0) > 0],
        key=lambda x: x.get("short_change_pct") or 0,
        reverse=True,
    )[:50]

    return {
        "highest_short": highest,
        "trending_short": trending,
        "metadata": {
            "count": len(rows),
            "timestamp": time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime()),
        },
    }


# ── Earnings history helpers ─────────────────────────────────────

_EARNINGS_COLS = (
    "ticker,report_date,fiscal_quarter,eps_estimate,eps_actual,"
    "eps_surprise_pct,revenue_estimate,revenue_actual,"
    "revenue_surprise_pct,beat_eps,beat_revenue,updated_at"
)


def upsert_earnings_history(rows: list[dict]) -> int:
    """Batch upsert rows into ``earnings_history``."""
    client = _get_client()
    if client is None or not rows:
        return 0

    upserted = 0
    CHUNK = 50
    for i in range(0, len(rows), CHUNK):
        chunk = rows[i : i + CHUNK]
        try:
            client.table("earnings_history").upsert(
                chunk, on_conflict="ticker,report_date"
            ).execute()
            upserted += len(chunk)
        except Exception as exc:
            logger.warning("upsert_earnings_history chunk %d failed: %s", i, exc)

    return upserted


def get_earnings_history(ticker: str, limit: int = 100) -> list[dict]:
    """Fetch historical earnings for *ticker*, newest first."""
    client = _get_client()
    if client is None:
        return []

    try:
        resp = (
            client.table("earnings_history")
            .select(_EARNINGS_COLS)
            .eq("ticker", ticker.upper())
            .order("report_date", desc=True)
            .limit(limit)
            .execute()
        )
        return resp.data or []
    except Exception as exc:
        logger.warning("get_earnings_history(%s) failed: %s", ticker, exc)
        return []
