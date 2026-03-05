#!/usr/bin/env python3
"""Daily web traffic data sync into Supabase cold storage.

Designed to run as a Railway Cron Service every 24 hours.
Fetches SimilarWeb + Wikipedia page views for all web-traffic-relevant
tickers found in the superinvestor portfolio, then upserts snapshots
into the ``web_traffic_history`` cold table.

The script is idempotent:
  - Uses ON CONFLICT (ticker, snapshot_date, source) DO UPDATE
  - Safe to re-run multiple times per day

Usage:
    uv run python scripts/sync_web_traffic.py [--dry-run]

Environment:
    SUPABASE_URL, SUPABASE_SERVICE_KEY must be set.
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

from filings import supabase_cache, web_traffic

# ── Logging ──────────────────────────────────────────────────────────


def _setup_logging() -> None:
    fmt = "%(asctime)s %(levelname)-8s %(name)s -- %(message)s"
    logging.basicConfig(level=logging.INFO, format=fmt, force=True)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


logger = logging.getLogger(__name__)

# Polite delay between API calls (seconds)
_FETCH_DELAY = 2.0


# ── Discover relevant tickers ───────────────────────────────────────


def _get_all_portfolio_tickers() -> list[str]:
    """Get unique tickers from all superinvestor portfolios + overrides."""
    tickers: set[str] = set()

    # 1. All manually-mapped tickers (always relevant)
    tickers.update(web_traffic._TICKER_TO_DOMAIN.keys())

    # 2. Try to pull tickers from cached superinvestor portfolios
    try:
        client = supabase_cache._get_client()
        if client:
            resp = (
                client.table("api_cache")
                .select("key")
                .like("key", "fund:%")
                .execute()
            )
            for row in resp.data or []:
                key = row.get("key", "")
                # fund cache keys look like "fund:{cik}" — we'd need to
                # read each to extract tickers, which is expensive.
                # For now, rely on the manual domain mappings.
                pass
    except Exception as e:
        logger.debug("Could not read fund cache: %s", e)

    return sorted(tickers)


# ── Upsert to Supabase ──────────────────────────────────────────────


def _upsert_snapshot(
    ticker: str, source: str, data: dict, snapshot_date: str, *, dry_run: bool = False
) -> bool:
    """Upsert a traffic snapshot into web_traffic_history. Returns True on success."""
    if dry_run:
        logger.info("[DRY RUN] Would upsert %s/%s for %s", source, snapshot_date, ticker)
        return True

    client = supabase_cache._get_client()
    if client is None:
        logger.error("No Supabase client — cannot store snapshot")
        return False

    try:
        client.table("web_traffic_history").upsert(
            {
                "ticker": ticker,
                "snapshot_date": snapshot_date,
                "source": source,
                "data": json.dumps(data) if isinstance(data, dict) else data,
            },
            on_conflict="ticker,snapshot_date,source",
        ).execute()
        return True
    except Exception as e:
        logger.warning("Upsert failed for %s/%s: %s", ticker, source, e)
        return False


# ── Main sync logic ─────────────────────────────────────────────────


def sync_all(*, dry_run: bool = False) -> dict:
    """Run the full sync cycle. Returns summary stats."""
    tickers = _get_all_portfolio_tickers()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    stats = {
        "tickers_checked": 0,
        "tickers_relevant": 0,
        "similarweb_ok": 0,
        "similarweb_fail": 0,
        "wikipedia_ok": 0,
        "wikipedia_fail": 0,
    }

    for ticker in tickers:
        stats["tickers_checked"] += 1

        # Check relevance
        relevant, reason = web_traffic.is_web_traffic_relevant(ticker)
        if not relevant:
            continue

        stats["tickers_relevant"] += 1
        logger.info("Syncing %s — %s", ticker, reason)

        # SimilarWeb
        try:
            sw_data = web_traffic.fetch_similarweb(ticker)
            if sw_data:
                ok = _upsert_snapshot(ticker, "similarweb", sw_data, today, dry_run=dry_run)
                if ok:
                    stats["similarweb_ok"] += 1
                else:
                    stats["similarweb_fail"] += 1
            else:
                stats["similarweb_fail"] += 1
        except Exception as e:
            logger.warning("SimilarWeb error for %s: %s", ticker, e)
            stats["similarweb_fail"] += 1

        time.sleep(_FETCH_DELAY)

        # Wikipedia
        try:
            wiki_data = web_traffic.fetch_wikipedia_views(ticker)
            if wiki_data:
                ok = _upsert_snapshot(ticker, "wikipedia", wiki_data, today, dry_run=dry_run)
                if ok:
                    stats["wikipedia_ok"] += 1
                else:
                    stats["wikipedia_fail"] += 1
            else:
                stats["wikipedia_fail"] += 1
        except Exception as e:
            logger.warning("Wikipedia error for %s: %s", ticker, e)
            stats["wikipedia_fail"] += 1

        time.sleep(_FETCH_DELAY)

    return stats


# ── Entry point ─────────────────────────────────────────────────────


def main() -> None:
    _setup_logging()
    parser = argparse.ArgumentParser(description="Sync web traffic data")
    parser.add_argument("--dry-run", action="store_true", help="Log but don't write")
    args = parser.parse_args()

    logger.info("=== Web traffic sync starting ===")
    t0 = time.time()

    stats = sync_all(dry_run=args.dry_run)

    elapsed = time.time() - t0
    logger.info(
        "=== Sync complete in %.1fs: %d tickers checked, %d relevant, "
        "SW %d ok / %d fail, Wiki %d ok / %d fail ===",
        elapsed,
        stats["tickers_checked"],
        stats["tickers_relevant"],
        stats["similarweb_ok"],
        stats["similarweb_fail"],
        stats["wikipedia_ok"],
        stats["wikipedia_fail"],
    )


if __name__ == "__main__":
    main()
