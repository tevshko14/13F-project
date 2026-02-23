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
from collections.abc import Callable
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

# ── Lazy-initialised Supabase client ──────────────────────────────
_client = None           # supabase.Client | None
_initialised = False     # True once we've attempted init (even if it failed)
_table_verified = False  # True once we've confirmed the table exists
_init_lock = threading.Lock()  # Protects one-time client creation

_TABLE = "api_cache"

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

-- Backfill: add duration and content_type columns for video metadata
ALTER TABLE youtube_events ADD COLUMN IF NOT EXISTS duration TEXT NOT NULL DEFAULT '';
ALTER TABLE youtube_events ADD COLUMN IF NOT EXISTS content_type TEXT NOT NULL DEFAULT 'video';
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
            logger.info("Supabase api_cache + sync_logs + insider_trades tables verified via auto-migration")
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
            logger.info("Supabase not configured (SUPABASE_URL / SUPABASE_SERVICE_KEY missing)")
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
            client
            .table(_TABLE)
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
            client
            .table(_TABLE)
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
            client
            .table(_TABLE)
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
            client
            .table(_TABLE)
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
            logger.debug("Cache unchanged for %s (hash=%s), bumped TTL only", cache_key, new_hash)
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
            client
            .table(_TABLE)
            .select("cache_key")
            .eq("category", category)
            .execute()
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
            client
            .table(_TABLE)
            .select("cache_key")
            .eq("category", category)
            .execute()
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
                client
                .table(_TABLE)
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
        logger.warning("Supabase increment_quota(%s, %s) failed: %s", category, month, exc)
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
                quota_category, current_count, max_monthly,
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


def get_insider_trades(
    trade_type: str = "",
    limit: int = 100,
) -> list[dict] | None:
    """Query ``insider_trades`` table for latest trades.

    Args:
        trade_type: ``"Purchase"`` for buys, ``"Sale"`` for sells,
                    ``""`` for all.
        limit: Max rows to return.

    Returns list of row dicts, or ``None`` if Supabase is unavailable.
    """
    client = _get_client()
    if client is None:
        return None

    try:
        query = (
            client.table("insider_trades")
            .select("*")
            .order("filing_date", desc=True)
            .order("trade_date", desc=True)
            .limit(limit)
        )
        if trade_type:
            query = query.ilike("trade_type", f"%{trade_type}%")

        resp = query.execute()
        return resp.data
    except Exception as exc:
        logger.warning("get_insider_trades failed: %s", exc)
        return None


def get_insider_trades_for_chart(
    days: int = 30,
    limit_per_type: int = 250,
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

    Returns list of row dicts, or ``None`` if Supabase is unavailable.
    """
    client = _get_client()
    if client is None:
        return None

    try:
        cutoff = (
            datetime.now(timezone.utc) - timedelta(days=days)
        ).strftime("%Y-%m-%d")

        buys = (
            client.table("insider_trades")
            .select("*")
            .ilike("trade_type", "%Purchase%")
            .gte("trade_date", cutoff)
            .order("trade_date", desc=True)
            .limit(limit_per_type)
            .execute()
        ).data or []

        sells = (
            client.table("insider_trades")
            .select("*")
            .ilike("trade_type", "%Sale%")
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
            .select("*")
            .eq("ticker", ticker.upper())
            .order("trade_date", desc=True)
            .limit(limit)
            .execute()
        )
        return resp.data
    except Exception as exc:
        logger.warning("get_insider_trades_by_ticker(%s) failed: %s", ticker, exc)
        return None


def upsert_insider_trades(rows: list[dict]) -> int:
    """Batch upsert rows into ``insider_trades``.

    Uses ``ON CONFLICT (sec_url) DO UPDATE`` for deduplication.
    Returns the number of rows upserted, or 0 on failure.
    """
    client = _get_client()
    if client is None:
        return 0

    upserted = 0
    CHUNK = 50
    for i in range(0, len(rows), CHUNK):
        chunk = rows[i : i + CHUNK]
        try:
            client.table("insider_trades").upsert(
                chunk, on_conflict="sec_url"
            ).execute()
            upserted += len(chunk)
        except Exception as exc:
            logger.warning("upsert_insider_trades chunk %d failed: %s", i, exc)

    return upserted


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
        client.table(_TABLE).update({
            "last_synced_at": now,
            "sync_status": status,
        }).eq("cache_key", cache_key).execute()
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

    cutoff = (
        datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
    ).isoformat()

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
        client.table("sync_logs").insert({
            "run_id": run_id,
        }).execute()
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
        client.table("sync_logs").update({
            "completed_at": now,
            "funds_updated": funds_updated,
            "funds_failed": funds_failed,
            "funds_skipped": funds_skipped,
            "error_messages": errors[:50],  # Cap stored errors
        }).eq("run_id", run_id).execute()
        return True
    except Exception as exc:
        logger.warning("complete_sync_log failed: %s", exc)
        return False


# ── YouTube helpers ──────────────────────────────────────────────


def upsert_youtube_events(rows: list[dict]) -> int:
    """Batch upsert rows into ``youtube_events``.

    Uses ``ON CONFLICT (video_id) DO UPDATE`` for deduplication.
    Returns the number of rows upserted, or 0 on failure.
    """
    client = _get_client()
    if client is None:
        return 0

    upserted = 0
    CHUNK = 50
    for i in range(0, len(rows), CHUNK):
        chunk = rows[i : i + CHUNK]
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
                row.get("channel_id"), exc,
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
            .select("*")
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
        resp = client.table("youtube_channels").select("*").execute()
        return resp.data
    except Exception as exc:
        logger.warning("get_youtube_channels failed: %s", exc)
        return None


def get_high_impact_youtube_events(min_score: int = 9) -> list[dict] | None:
    """Return upcoming events with ``impact_score >= min_score``."""
    client = _get_client()
    if client is None:
        return None
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=6)).isoformat()
        resp = (
            client.table("youtube_events")
            .select("*")
            .gte("impact_score", min_score)
            .eq("event_type", "upcoming")
            .gte("scheduled_at", cutoff)
            .order("scheduled_at", desc=True)
            .limit(10)
            .execute()
        )
        return resp.data
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
            .select("*")
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

    # 1. Insider trades: delete > 30 days
    cutoff_30d = (now - timedelta(days=30)).strftime("%Y-%m-%d")
    try:
        resp = (
            client.table("insider_trades")
            .delete()
            .lt("trade_date", cutoff_30d)
            .execute()
        )
        results["insider_trades_deleted"] = len(resp.data) if resp.data else 0
    except Exception as exc:
        logger.warning("Retention: insider_trades cleanup failed: %s", exc)
        results["insider_trades_deleted"] = -1

    # 2. Sync logs: delete > 30 days
    cutoff_30d_ts = (now - timedelta(days=30)).isoformat()
    try:
        resp = (
            client.table("sync_logs")
            .delete()
            .lt("started_at", cutoff_30d_ts)
            .execute()
        )
        results["sync_logs_deleted"] = len(resp.data) if resp.data else 0
    except Exception as exc:
        logger.warning("Retention: sync_logs cleanup failed: %s", exc)
        results["sync_logs_deleted"] = -1

    # 3. YouTube events: delete > 30 days
    try:
        resp = (
            client.table("youtube_events")
            .delete()
            .lt("scheduled_at", cutoff_30d_ts)
            .execute()
        )
        results["youtube_events_deleted"] = len(resp.data) if resp.data else 0
    except Exception as exc:
        logger.warning("Retention: youtube_events cleanup failed: %s", exc)
        results["youtube_events_deleted"] = -1

    # 4. Expired api_cache rows: physical delete (skip 13f -- stale-while-revalidate)
    now_iso = now.isoformat()
    try:
        resp = (
            client.table("api_cache")
            .delete()
            .lt("expires_at", now_iso)
            .neq("category", "13f")
            .execute()
        )
        results["expired_cache_deleted"] = len(resp.data) if resp.data else 0
    except Exception as exc:
        logger.warning("Retention: api_cache cleanup failed: %s", exc)
        results["expired_cache_deleted"] = -1

    logger.info("Retention cleanup: %s", results)
    return results
