#!/usr/bin/env python3
"""Backfill Google Trends data into Supabase cold storage.

Collects three types of data:

1. **Per-ticker interest** — fetches interest-over-time for the top
   intent keywords of each profiled ticker (3 months default).

2. **Macro trends** — fetches interest for each macro sentiment
   category (Market Fear, Crypto, AI Hype, etc.).

3. **Trending searches** — fetches today's trending searches with
   automatic ticker matching.

Usage:
    uv run python scripts/backfill_google_trends.py [--source ticker|macro|trending|all]
        [--limit N] [--dry-run]

Examples:
    # Backfill everything
    uv run python scripts/backfill_google_trends.py

    # Just macro trends
    uv run python scripts/backfill_google_trends.py --source macro

    # First 5 tickers only (for testing)
    uv run python scripts/backfill_google_trends.py --source ticker --limit 5

    # Dry run
    uv run python scripts/backfill_google_trends.py --dry-run

Environment:
    SUPABASE_URL, SUPABASE_SERVICE_KEY must be set.

Note: Google Trends rate-limits aggressively. The script enforces a 5-second
delay between requests and backs off on 429s.  A full backfill of ~60 tickers
takes approximately 15-20 minutes.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

# Ensure the src directory is on the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from filings import supabase_cache, google_trends

# ── Logging ──────────────────────────────────────────────────────────


def _setup_logging() -> None:
    fmt = "%(asctime)s %(levelname)-8s %(name)s -- %(message)s"
    logging.basicConfig(level=logging.INFO, format=fmt, force=True)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


logger = logging.getLogger(__name__)

# Extra delay on top of the module's built-in rate limiter
_EXTRA_DELAY = 2.0


# ── Upsert helper ───────────────────────────────────────────────────


def _upsert_snapshot(
    ticker: str | None,
    category: str,
    keywords: list[str],
    data: dict,
    snapshot_date: str,
    *,
    dry_run: bool = False,
) -> bool:
    """Upsert a single snapshot row into google_trends_history."""
    if dry_run:
        logger.info("  [DRY RUN] Would insert %s / %s / %s", ticker or "NULL", category, snapshot_date)
        return True

    client = supabase_cache._get_client()
    if client is None:
        logger.error("No Supabase client — cannot store snapshot")
        return False

    try:
        client.table("google_trends_history").upsert(
            {
                "ticker": ticker,
                "category": category,
                "keywords": keywords,
                "snapshot_date": snapshot_date,
                "data": json.dumps(data) if isinstance(data, dict) else data,
            },
            on_conflict="ticker,category,snapshot_date",
        ).execute()
        return True
    except Exception as e:
        logger.warning("Upsert failed for %s/%s/%s: %s", ticker, category, snapshot_date, e)
        return False


# ═════════════════════════════════════════════════════════════════════
# Per-ticker backfill
# ═════════════════════════════════════════════════════════════════════


def backfill_tickers(limit: int = 0, *, dry_run: bool = False) -> int:
    """Fetch interest-over-time for each profiled ticker's top intent keywords.

    Returns number of snapshots inserted.
    """
    all_tickers = sorted(google_trends._TICKER_PROFILES.keys())
    if limit > 0:
        all_tickers = all_tickers[:limit]

    inserted = 0
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    for i, ticker in enumerate(all_tickers):
        kw = google_trends.generate_keywords(ticker)
        top_intent = kw["intent"][:3]  # Top 3 intent keywords

        if not top_intent:
            logger.info("  %s: no intent keywords, skipping", ticker)
            continue

        logger.info(
            "  [%d/%d] %s (%s): fetching interest for %s",
            i + 1, len(all_tickers), ticker, kw["name"], top_intent,
        )

        trend = google_trends.fetch_interest_over_time(top_intent)
        if trend:
            ok = _upsert_snapshot(
                ticker, "intent", top_intent, trend, today, dry_run=dry_run,
            )
            if ok:
                inserted += 1
            logger.info("  %s: %s (avg scores: %s)",
                        ticker, "OK" if ok else "FAIL",
                        trend.get("averages", {}))
        else:
            logger.warning("  %s: no data returned (rate limited?)", ticker)

        time.sleep(_EXTRA_DELAY)

    return inserted


# ═════════════════════════════════════════════════════════════════════
# Macro trends backfill
# ═════════════════════════════════════════════════════════════════════


def backfill_macro(*, dry_run: bool = False) -> int:
    """Fetch interest for each macro category."""
    inserted = 0
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    for cat_name, keywords in google_trends.MACRO_CATEGORIES.items():
        top_kws = keywords[:5]
        logger.info("  Macro '%s': fetching %s", cat_name, top_kws)

        trend = google_trends.fetch_interest_over_time(top_kws)
        if trend:
            ok = _upsert_snapshot(
                None, "macro", top_kws, {**trend, "macro_category": cat_name},
                today, dry_run=dry_run,
            )
            if ok:
                inserted += 1
            logger.info("  '%s': %s (avg: %s)",
                        cat_name, "OK" if ok else "FAIL",
                        trend.get("averages", {}))
        else:
            logger.warning("  '%s': no data returned", cat_name)

        time.sleep(_EXTRA_DELAY)

    return inserted


# ═════════════════════════════════════════════════════════════════════
# Trending searches backfill
# ═════════════════════════════════════════════════════════════════════


def backfill_trending(*, dry_run: bool = False) -> int:
    """Fetch today's trending searches and store with ticker matches."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    logger.info("  Fetching trending searches...")
    trending = google_trends.fetch_trending_searches()

    if not trending:
        logger.warning("  No trending data returned")
        return 0

    matched = [t for t in trending if t.get("ticker")]
    logger.info("  Got %d trending searches, %d matched to tickers", len(trending), len(matched))

    data = {
        "total_results": len(trending),
        "ticker_matches": len(matched),
        "searches": trending,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }

    ok = _upsert_snapshot(
        None, "trending", [t["query"] for t in trending[:10]],
        data, today, dry_run=dry_run,
    )
    return 1 if ok else 0


# ═════════════════════════════════════════════════════════════════════
# Main
# ═════════════════════════════════════════════════════════════════════


def main() -> None:
    _setup_logging()

    parser = argparse.ArgumentParser(description="Backfill Google Trends data")
    parser.add_argument(
        "--source",
        choices=["ticker", "macro", "trending", "all"],
        default="all",
        help="Which source to backfill (default: all)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limit to first N tickers for ticker source (0 = all, default: 0)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log actions without writing to Supabase",
    )
    args = parser.parse_args()

    logger.info("=== Google Trends backfill starting ===")
    logger.info("Source: %s | Ticker limit: %d | Dry run: %s",
                args.source, args.limit, args.dry_run)

    t0 = time.time()
    ticker_count = 0
    macro_count = 0
    trending_count = 0

    if args.source in ("trending", "all"):
        logger.info("--- Trending searches ---")
        trending_count = backfill_trending(dry_run=args.dry_run)
        logger.info("Trending: %d snapshot(s) stored", trending_count)

    if args.source in ("macro", "all"):
        logger.info("--- Macro trends ---")
        macro_count = backfill_macro(dry_run=args.dry_run)
        logger.info("Macro: %d snapshots stored", macro_count)

    if args.source in ("ticker", "all"):
        logger.info("--- Per-ticker interest ---")
        ticker_count = backfill_tickers(limit=args.limit, dry_run=args.dry_run)
        logger.info("Ticker: %d snapshots stored", ticker_count)

    elapsed = time.time() - t0
    logger.info(
        "=== Backfill complete in %.1fs: Trending %d, Macro %d, Ticker %d ===",
        elapsed, trending_count, macro_count, ticker_count,
    )


if __name__ == "__main__":
    main()
