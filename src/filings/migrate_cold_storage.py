"""One-time migration: archive historical data to Supabase Storage.

Safe to re-run (idempotent):
  - Uploads use upsert (overwrite existing files)
  - Only trims quarterly_changes if ALL archive uploads succeed
  - Insider trades deletion uses a date cutoff

Usage:
    uv run filings-migrate-cold

Steps:
  1. Ensure the Supabase Storage bucket exists
  2. For each 13f:{cik} in api_cache:
     a. Read the full response_data blob
     b. Archive quarters 3+ to Supabase Storage
     c. Trim quarterly_changes to 2 quarters
     d. Upsert the trimmed blob back to api_cache
  3. Delete insider_trades rows older than 30 days
  4. Delete expired api_cache rows (physical cleanup)
  5. Log summary
"""

from __future__ import annotations

import logging
import os
import time

logger = logging.getLogger(__name__)


def _setup_logging() -> None:
    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    if os.environ.get("RAILWAY_ENVIRONMENT"):
        fmt = '{"time":"%(asctime)s","level":"%(levelname)s","name":"%(name)s","msg":"%(message)s"}'
    else:
        fmt = "%(asctime)s %(levelname)-8s %(name)s -- %(message)s"
    logging.basicConfig(level=log_level, format=fmt, force=True)


def migrate_13f_to_cold_storage() -> dict:
    """Archive older 13F quarters to Supabase Storage.

    For each fund in api_cache with category=13f:
      1. Read full blob
      2. If quarterly_changes has > 2 entries, archive extras to Storage
      3. Trim to 2 quarters and re-save

    Returns stats dict: {archived, trimmed, skipped, failed}.
    """
    from filings import cold_storage, supabase_cache

    # Ensure bucket exists before uploading
    if not cold_storage.ensure_bucket():
        logger.error("Could not ensure archive bucket -- aborting 13F migration")
        return {"archived": 0, "trimmed": 0, "skipped": 0, "failed": 0, "error": "bucket_creation_failed"}

    keys = supabase_cache.get_category_keys("13f")
    if not keys:
        logger.info("No 13f keys found in api_cache -- nothing to migrate")
        return {"archived": 0, "trimmed": 0, "skipped": 0, "failed": 0}

    stats = {"archived": 0, "trimmed": 0, "skipped": 0, "failed": 0}

    for key in keys:
        cik = key.replace("13f:", "", 1) if key.startswith("13f:") else None
        if not cik:
            stats["skipped"] += 1
            continue

        # Read the full blob (including stale data -- we want all quarters)
        data, _is_fresh = supabase_cache.get_cached_with_stale(key)
        if not data or not isinstance(data, dict):
            logger.warning("No data for %s -- skipping", key)
            stats["skipped"] += 1
            continue

        quarterly = data.get("quarterly_changes", [])
        if len(quarterly) <= 2:
            logger.debug("%s has only %d quarters -- no archiving needed", key, len(quarterly))
            stats["skipped"] += 1
            continue

        # Archive quarters 3+ to cold storage
        to_archive = quarterly[2:]
        all_ok = True
        archived_count = 0
        for q in to_archive:
            period = q.get("period", "").replace(" ", "_")
            if not period:
                logger.warning("Quarter missing period label in %s -- skipping quarter", key)
                continue
            path = f"13f/{cik}/quarterly/{period}.json"
            if cold_storage.upload_json(path, q):
                archived_count += 1
            else:
                logger.error("Failed to archive %s -- aborting trim for %s", path, key)
                all_ok = False
                break

        if not all_ok:
            stats["failed"] += 1
            continue

        stats["archived"] += archived_count

        # Trim to 2 most recent quarters and re-save
        data["quarterly_changes"] = quarterly[:2]
        ok = supabase_cache.set_cached(
            cache_key=key,
            category="13f",
            data=data,
            ttl_seconds=None,  # Preserve existing TTL behavior
        )
        if ok:
            stats["trimmed"] += 1
            logger.info(
                "Archived %d quarters and trimmed %s (kept %d)",
                archived_count, key, len(data["quarterly_changes"]),
            )
        else:
            logger.error("Failed to write trimmed blob for %s", key)
            stats["failed"] += 1

    return stats


def cleanup_old_insider_trades(days: int = 30) -> int:
    """Delete insider_trades rows with trade_date older than N days.

    Returns count of deleted rows, or 0 on failure.
    """
    from filings.supabase_cache import _get_client
    from datetime import datetime, timedelta, timezone

    client = _get_client()
    if client is None:
        logger.warning("Supabase unavailable -- skipping insider trades cleanup")
        return 0

    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")

    try:
        resp = (
            client.table("insider_trades")
            .delete()
            .lt("trade_date", cutoff)
            .execute()
        )
        count = len(resp.data) if resp.data else 0
        logger.info("Deleted %d insider trades older than %s", count, cutoff)
        return count
    except Exception as exc:
        logger.warning("cleanup_old_insider_trades failed: %s", exc)
        return 0


def cleanup_expired_cache_rows() -> int:
    """Physically delete api_cache rows where expires_at < now().

    Excludes 13f rows (they use stale-while-revalidate and should
    only be trimmed, not deleted).

    Returns count of deleted rows, or 0 on failure.
    """
    from filings.supabase_cache import _get_client
    from datetime import datetime, timezone

    client = _get_client()
    if client is None:
        return 0

    now_iso = datetime.now(timezone.utc).isoformat()

    try:
        resp = (
            client.table("api_cache")
            .delete()
            .lt("expires_at", now_iso)
            .neq("category", "13f")
            .execute()
        )
        count = len(resp.data) if resp.data else 0
        logger.info("Deleted %d expired api_cache rows", count)
        return count
    except Exception as exc:
        logger.warning("cleanup_expired_cache_rows failed: %s", exc)
        return 0


def main() -> None:
    """Entry point for ``uv run filings-migrate-cold``."""
    _setup_logging()
    logger.info("=== PaperPanda Cold Storage Migration starting ===")
    start = time.time()

    # Step 1: Archive + trim 13F data
    logger.info("--- Step 1: Migrating 13F quarterly data to cold storage ---")
    stats_13f = migrate_13f_to_cold_storage()
    logger.info("13F migration: %s", stats_13f)

    # Step 2: Clean up old insider trades
    logger.info("--- Step 2: Cleaning up old insider trades (>30 days) ---")
    insider_deleted = cleanup_old_insider_trades(days=30)

    # Step 3: Clean up expired cache rows
    logger.info("--- Step 3: Cleaning up expired api_cache rows ---")
    cache_deleted = cleanup_expired_cache_rows()

    elapsed = round(time.time() - start)
    logger.info(
        "=== Migration complete in %ds: 13f=%s, insider_deleted=%d, cache_deleted=%d ===",
        elapsed, stats_13f, insider_deleted, cache_deleted,
    )


if __name__ == "__main__":
    main()
