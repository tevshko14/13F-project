"""Short interest sync worker -- fetches FINRA short interest data via yfinance.

Designed to run as a Railway Cron Job every 12-24 hours.
Fetches short interest for all tickers held by tracked superinvestors,
upserts into the ``short_interest_history`` table, and caches the
leaderboard snapshot in ``api_cache``.

Usage:
    uv run filings-short-interest-sync
"""

import logging
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date as _date

from filings import cache, client, supabase_cache
from filings.superinvestors import SUPERINVESTORS_BY_CIK

# ── Logging ──────────────────────────────────────────────────────────


def _setup_logging() -> None:
    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    if os.environ.get("RAILWAY_ENVIRONMENT"):
        fmt = '{"time":"%(asctime)s","level":"%(levelname)s","name":"%(name)s","msg":"%(message)s"}'
    else:
        fmt = "%(asctime)s %(levelname)-8s %(name)s -- %(message)s"
    logging.basicConfig(level=log_level, format=fmt, force=True)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("yfinance").setLevel(logging.WARNING)
    logging.getLogger("peewee").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


logger = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────────────

_BATCH_SIZE = 3  # Concurrent yfinance calls per batch
_BATCH_DELAY = 1.5  # Seconds between batches (rate limiting)
_MAX_RETRIES = 2
_LEADERBOARD_TTL = 43200  # 12 hours

# Persistent pool (avoids per-batch pool creation/teardown)
_si_pool = ThreadPoolExecutor(max_workers=_BATCH_SIZE, thread_name_prefix="si-sync")


# ── Fetch logic ──────────────────────────────────────────────────────


def _fetch_one(ticker: str) -> dict | None:
    """Fetch short interest for a single ticker from yfinance.

    Returns a dict with the fields needed for ``short_interest_history``
    upsert plus leaderboard enrichment, or None on failure.
    """
    try:
        import yfinance as yf

        t = yf.Ticker(ticker)
        info = t.info or {}
    except Exception as exc:
        logger.debug("yfinance fetch failed for %s: %s", ticker, exc)
        return None

    shares_short = info.get("sharesShort")
    if not shares_short:
        return None

    report_date_epoch = info.get("dateShortInterest")
    if not report_date_epoch:
        return None

    shares_short_prior = info.get("sharesShortPriorMonth") or 0
    short_change = shares_short - shares_short_prior
    short_change_pct = (
        (short_change / shares_short_prior * 100) if shares_short_prior else 0.0
    )

    return {
        "ticker": ticker.upper(),
        "report_date": _date.fromtimestamp(report_date_epoch).isoformat(),
        "shares_short": shares_short,
        "shares_short_prior": shares_short_prior,
        "short_pct_float": info.get("shortPercentOfFloat") or 0,
        "short_ratio": info.get("shortRatio") or 0,
        "float_shares": info.get("floatShares") or 0,
        "shares_outstanding": info.get("sharesOutstanding") or 0,
        # Extra fields for leaderboard (not stored in history table)
        "_short_change": short_change,
        "_short_change_pct": round(short_change_pct, 1),
        "_prior_date": info.get("sharesShortPreviousMonthDate"),
    }


def _build_leaderboard(
    results: list[dict], ticker_map: dict[str, list[str]]
) -> dict:
    """Build the two leaderboard tables from fetched data.

    Returns a dict with ``highest_short``, ``trending_short``, and ``metadata``.
    """
    # Enrich with guru overlap
    enriched = []
    for r in results:
        guru_names = ticker_map.get(r["ticker"], [])
        enriched.append({
            "ticker": r["ticker"],
            "short_pct_float": r["short_pct_float"],
            "short_ratio": r["short_ratio"],
            "shares_short": r["shares_short"],
            "shares_short_prior": r["shares_short_prior"],
            "short_change": r["_short_change"],
            "short_change_pct": r["_short_change_pct"],
            "float_shares": r["float_shares"],
            "report_date": r["report_date"],
            "guru_count": len(guru_names),
            "guru_names": guru_names[:5],
        })

    # Table 1: Highest short interest (top 50 by % of float)
    highest = sorted(
        enriched,
        key=lambda x: x.get("short_pct_float") or 0,
        reverse=True,
    )[:50]

    # Table 2: Biggest trending (top 50 by absolute change %, increasing)
    trending = sorted(
        [e for e in enriched if e.get("short_change_pct", 0) > 0],
        key=lambda x: x.get("short_change_pct") or 0,
        reverse=True,
    )[:50]

    return {
        "highest_short": highest,
        "trending_short": trending,
        "metadata": {
            "count": len(results),
            "timestamp": time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime()),
        },
    }


# ── Main sync ────────────────────────────────────────────────────────


def sync_all_short_interest() -> dict:
    """Main sync: fetch all superinvestor tickers, upsert, build leaderboard."""

    run_id = f"short-interest-sync-{uuid.uuid4().hex[:8]}"
    supabase_cache.create_sync_log(run_id)

    # 1. Get ticker universe from cached 13F data
    logger.info("Loading 13F cache to discover ticker universe...")
    fund_cache = cache.load_cache_from_supabase() or cache.load_cache()
    ticker_map = client.build_ticker_ownership_map(fund_cache, SUPERINVESTORS_BY_CIK)
    tickers = sorted(ticker_map.keys())
    logger.info("Discovered %d unique tickers from superinvestor holdings", len(tickers))

    if not tickers:
        logger.warning("No tickers found — aborting sync")
        supabase_cache.complete_sync_log(
            run_id, 0, 1, 0, ["No tickers found in cache"]
        )
        return {"fetched": 0, "total": 0, "errors": 1}

    # 2. Batch fetch with rate limiting
    all_results: list[dict] = []
    failed = 0
    total_batches = (len(tickers) + _BATCH_SIZE - 1) // _BATCH_SIZE

    for batch_idx in range(0, len(tickers), _BATCH_SIZE):
        batch = tickers[batch_idx : batch_idx + _BATCH_SIZE]
        batch_num = batch_idx // _BATCH_SIZE + 1

        futures = {_si_pool.submit(_fetch_one, t): t for t in batch}
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                result = future.result()
                if result:
                    all_results.append(result)
                else:
                    failed += 1
            except Exception as exc:
                logger.debug("Batch fetch error for %s: %s", ticker, exc)
                failed += 1

        if batch_num % 20 == 0 or batch_num == total_batches:
            logger.info(
                "Progress: batch %d/%d — %d fetched, %d failed",
                batch_num, total_batches, len(all_results), failed,
            )

        # Rate limit between batches
        if batch_idx + _BATCH_SIZE < len(tickers):
            time.sleep(_BATCH_DELAY)

    logger.info(
        "Fetch complete: %d succeeded, %d failed out of %d tickers",
        len(all_results), failed, len(tickers),
    )

    # 3. Upsert history rows to Supabase
    history_rows = [
        {
            "ticker": r["ticker"],
            "report_date": r["report_date"],
            "shares_short": r["shares_short"],
            "shares_short_prior": r["shares_short_prior"],
            "short_pct_float": r["short_pct_float"],
            "short_ratio": r["short_ratio"],
            "float_shares": r["float_shares"],
            "shares_outstanding": r["shares_outstanding"],
        }
        for r in all_results
    ]

    # Also upsert prior-month data points (backfill)
    for r in all_results:
        if r.get("_prior_date") and r.get("shares_short_prior"):
            try:
                history_rows.append({
                    "ticker": r["ticker"],
                    "report_date": _date.fromtimestamp(r["_prior_date"]).isoformat(),
                    "shares_short": r["shares_short_prior"],
                })
            except Exception:
                pass

    upserted = supabase_cache.upsert_short_interest_rows(history_rows)
    logger.info("Upserted %d rows into short_interest_history", upserted)

    # 4. Build and cache the leaderboard
    leaderboard = _build_leaderboard(all_results, ticker_map)
    supabase_cache.set_cached(
        "short_interest_leaderboard",
        "alt_signals",
        leaderboard,
        ttl_seconds=_LEADERBOARD_TTL,
    )
    logger.info(
        "Cached leaderboard: %d highest, %d trending",
        len(leaderboard["highest_short"]),
        len(leaderboard["trending_short"]),
    )

    # 5. Log completion
    supabase_cache.complete_sync_log(
        run_id,
        funds_updated=len(all_results),
        funds_failed=failed,
        funds_skipped=0,
        errors=[],
    )

    return {
        "fetched": len(all_results),
        "total": len(tickers),
        "failed": failed,
        "upserted": upserted,
    }


def main() -> None:
    _setup_logging()
    logger.info("=== Short Interest Sync starting ===")
    t0 = time.time()
    result = sync_all_short_interest()
    elapsed = time.time() - t0
    logger.info(
        "=== Short Interest Sync complete in %.1fs: %s ===",
        elapsed,
        result,
    )


if __name__ == "__main__":
    main()
