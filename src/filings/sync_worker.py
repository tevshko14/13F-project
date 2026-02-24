"""Standalone SEC EDGAR sync worker for PaperPanda.

Designed to run as a Railway Cron Job (separate service) on a schedule
(e.g. every 12 hours, or every 2 hours during filing season).

Usage:
    uv run filings-sync

The worker:
  1. Queries Supabase api_cache for 13f entries needing refresh
  2. For each stale fund: calls client.get_fund_summary(cik)
  3. Upserts result to api_cache, updates last_synced_at / sync_status
  4. Logs results to sync_logs table
  5. On failure: logs error, continues to next fund

The web frontend never calls the SEC API directly -- it reads
exclusively from Supabase.  This worker is the only process that
writes fresh 13F data.
"""

import os as _os

# Must be set before edgartools is imported anywhere.
_os.environ.setdefault("EDGAR_RATE_LIMIT_PER_SEC", "5")

import gc
import logging
import resource
import time
import uuid

from filings import cache, supabase_cache
from filings.superinvestors import SUPERINVESTORS

# ── Logging ──────────────────────────────────────────────────────────


def _setup_logging() -> None:
    log_level = _os.environ.get("LOG_LEVEL", "INFO").upper()
    if _os.environ.get("RAILWAY_ENVIRONMENT"):
        fmt = '{"time":"%(asctime)s","level":"%(levelname)s","name":"%(name)s","msg":"%(message)s"}'
    else:
        fmt = "%(asctime)s %(levelname)-8s %(name)s -- %(message)s"
    logging.basicConfig(level=log_level, format=fmt, force=True)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


logger = logging.getLogger(__name__)


# ── Sync logic ───────────────────────────────────────────────────────


def sync_all_funds(max_age_hours: int = 24, delay_between: float = 2.0) -> dict:
    """Sync all stale superinvestor funds from SEC EDGAR to Supabase.

    Returns a summary dict ``{"updated": N, "failed": N, "skipped": N}``.
    """
    run_id = f"sync-{uuid.uuid4().hex[:8]}"
    supabase_cache.create_sync_log(run_id)

    all_ciks = {si.cik for si in SUPERINVESTORS}

    # ── Identify which CIKs need a sync ──────────────────────────
    # 1. Stale keys already in Supabase (never synced or >max_age_hours)
    stale_keys = supabase_cache.get_stale_sync_keys(max_age_hours=max_age_hours)
    stale_ciks_from_db = {k.replace("13f:", "") for k in stale_keys}

    # 2. CIKs with no Supabase entry at all (new investors)
    # Use lightweight query (cache_key only) to avoid loading ~30MB of fund data
    existing_keys = supabase_cache.get_cache_keys_by_category("13f")
    existing_ciks = {
        k.replace("13f:", "") for k in existing_keys if k.startswith("13f:")
    }

    missing_ciks = all_ciks - existing_ciks
    ciks_to_sync = sorted(stale_ciks_from_db | missing_ciks)

    if not ciks_to_sync:
        logger.info("All %d funds are fresh -- nothing to sync", len(all_ciks))
        supabase_cache.complete_sync_log(run_id, 0, 0, len(all_ciks), [])
        return {"updated": 0, "failed": 0, "skipped": len(all_ciks)}

    logger.info(
        "Syncing %d/%d funds (stale=%d, missing=%d)",
        len(ciks_to_sync),
        len(all_ciks),
        len(stale_ciks_from_db),
        len(missing_ciks),
    )

    # ── Iterate and refresh ──────────────────────────────────────
    updated = 0
    failed = 0
    errors: list[str] = []

    for idx, cik in enumerate(ciks_to_sync):
        cache_key = f"13f:{cik}"

        try:
            logger.info("[%d/%d] Fetching CIK %s ...", idx + 1, len(ciks_to_sync), cik)
            data = cache.refresh_single_fund(cik)

            if data is not None:
                supabase_cache.update_sync_status(cache_key, "success")
                updated += 1
                logger.info(
                    "[%d/%d] OK: CIK %s (%s)",
                    idx + 1,
                    len(ciks_to_sync),
                    cik,
                    data.get("name", ""),
                )
            else:
                # refresh_single_fund returned None (rate limit or SEC error)
                supabase_cache.update_sync_status(cache_key, "failed")
                failed += 1
                error_msg = f"CIK {cik}: refresh returned None"
                errors.append(error_msg)
                logger.warning("[%d/%d] FAILED: %s", idx + 1, len(ciks_to_sync), cik)
        except Exception as e:
            supabase_cache.update_sync_status(cache_key, "failed")
            failed += 1
            error_msg = f"CIK {cik}: {str(e)[:200]}"
            errors.append(error_msg)
            logger.warning(
                "[%d/%d] FAILED: CIK %s -- %s",
                idx + 1,
                len(ciks_to_sync),
                cik,
                e,
            )

        # ── Cleanup + Rate limiting ───────────────────────────────
        data = None  # free the large dict immediately
        gc.collect()

        if idx < len(ciks_to_sync) - 1:
            time.sleep(delay_between)

        # Batch pause every 10 funds + log memory usage
        if (idx + 1) % 10 == 0 and idx < len(ciks_to_sync) - 1:
            mem_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1024 * 1024)
            logger.info("Batch pause (10 funds done) -- peak RSS: %.0f MB", mem_mb)
            time.sleep(5)

    skipped = len(all_ciks) - len(ciks_to_sync)
    supabase_cache.complete_sync_log(run_id, updated, failed, skipped, errors)

    logger.info(
        "Sync complete: %d updated, %d failed, %d skipped",
        updated,
        failed,
        skipped,
    )
    return {"updated": updated, "failed": failed, "skipped": skipped}


# ── Entry point ──────────────────────────────────────────────────────


def main() -> None:
    """Entry point for ``uv run filings-sync``."""
    _setup_logging()
    mem_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1024 * 1024)
    logger.info("=== PaperPanda SEC Sync Worker starting (RSS: %.0f MB) ===", mem_mb)
    start = time.time()

    result = sync_all_funds()

    elapsed = round(time.time() - start)
    mem_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1024 * 1024)
    logger.info(
        "=== Sync finished in %ds (RSS: %.0f MB): %s ===", elapsed, mem_mb, result
    )


if __name__ == "__main__":
    main()
