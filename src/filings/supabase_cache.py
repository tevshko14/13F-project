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

import logging
import os
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

# ── Lazy-initialised Supabase client ──────────────────────────────
_client = None           # supabase.Client | None
_initialised = False     # True once we've attempted init (even if it failed)
_table_verified = False  # True once we've confirmed the table exists

_TABLE = "api_cache"

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS api_cache (
    cache_key     TEXT PRIMARY KEY,
    category      TEXT NOT NULL,
    response_data JSONB NOT NULL,
    expires_at    TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_api_cache_category
    ON api_cache (category);
CREATE INDEX IF NOT EXISTS idx_api_cache_expires_at
    ON api_cache (expires_at)
    WHERE expires_at IS NOT NULL;
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

    # Try to derive a connection string if not explicitly set
    if not db_url:
        supa_url = os.environ.get("SUPABASE_URL", "").strip()
        db_pass = os.environ.get("SUPABASE_DB_PASSWORD", "").strip()
        if supa_url and db_pass:
            # Extract project ref from URL: https://<ref>.supabase.co
            ref = supa_url.replace("https://", "").split(".")[0]
            db_url = f"postgresql://postgres:{db_pass}@db.{ref}.supabase.co:5432/postgres"

    if not db_url:
        logger.debug("No SUPABASE_DB_URL -- skipping auto-migration")
        return

    try:
        import psycopg2  # noqa: F811
        conn = psycopg2.connect(db_url, connect_timeout=5)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(_CREATE_TABLE_SQL)
        cur.close()
        conn.close()
        _table_verified = True
        logger.info("Supabase api_cache table verified/created via auto-migration")
    except ImportError:
        logger.debug("psycopg2 not installed -- skipping auto-migration")
    except Exception as exc:
        logger.debug("Supabase auto-migration skipped: %s", exc)


def _get_client():
    """Return the Supabase client, lazily creating it on first call.

    Returns ``None`` when either env var is missing or the client
    could not be created.
    """
    global _client, _initialised

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


def set_cached(
    cache_key: str,
    category: str,
    data: dict,
    ttl_seconds: int | None = None,
) -> bool:
    """Upsert a cache row.

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

    now = datetime.now(timezone.utc).isoformat()
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
        "updated_at": now,
    }

    try:
        client.table(_TABLE).upsert(row, on_conflict="cache_key").execute()
        return True
    except Exception as exc:
        _handle_table_missing(exc)
        return False


# ── Bulk read by category ────────────────────────────────────────


def get_all_by_category(category: str) -> list[dict] | None:
    """Fetch all rows for a given category.

    Returns a list of row dicts (each with ``cache_key``,
    ``response_data``, etc.) on success, or ``None`` on error.
    """
    client = _get_client()
    if client is None:
        return None

    try:
        resp = (
            client
            .table(_TABLE)
            .select("cache_key, response_data, updated_at")
            .eq("category", category)
            .execute()
        )
        return resp.data  # list[dict]
    except Exception as exc:
        _handle_table_missing(exc)
        return None


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
