#!/usr/bin/env python3
"""Bulk-populate ``company_about`` for the covered ticker universe.

Universe = (S&P 500 fallback list) ∪ (every ticker that any superinvestor
holds in fund_cache).  Resumable: by default we only fetch tickers whose
row is missing or > 180 days old.  Sleeps between yfinance calls to dodge
Yahoo rate-limits; on 429 we exponential-backoff and retry the same ticker.

Examples:
    uv run python scripts/backfill_about.py                 # full pass
    uv run python scripts/backfill_about.py --ticker NVDA   # one ticker
    uv run python scripts/backfill_about.py --limit 25      # smoke test
    uv run python scripts/backfill_about.py --force         # ignore freshness
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

# Project src on path so this can run standalone (no entry point shim).
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

# .env loading — keeps SUPABASE_URL / FINNHUB_API_KEY available when invoked
# from a stock shell, mirroring the pattern in scripts/full_sync.py.
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
except Exception:
    pass

from filings import company_about  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("backfill_about")


def build_universe() -> list[str]:
    """Build the ticker universe from S&P 500 + cached fund holdings."""
    tickers: set[str] = set()

    try:
        from filings.market_data import _FALLBACK_SP500
        for entry in _FALLBACK_SP500:
            t = (entry.get("ticker") or "").strip().upper()
            if t and t.isalnum():
                tickers.add(t)
        logger.info("S&P 500 fallback: %d tickers", len(tickers))
    except Exception as exc:
        logger.warning("could not load S&P 500 fallback: %s", exc)

    try:
        from filings import cache
        fund_cache = cache.load_cache()
        added = 0
        for fund_data in fund_cache.values():
            for h in fund_data.get("all_holdings") or []:
                t = (h.get("ticker") or "").strip().upper()
                # Skip blank, mutual-fund-shaped, or cusip-only entries.
                if t and t.isalnum() and 1 <= len(t) <= 5:
                    if t not in tickers:
                        added += 1
                    tickers.add(t)
        logger.info("fund_cache holdings added: %d new (%d total)",
                    added, len(tickers))
    except Exception as exc:
        logger.warning("could not load fund_cache: %s", exc)

    return sorted(tickers)


def backfill_one(ticker: str, *, retries: int = 3) -> str:
    """Fetch + upsert one ticker.

    Returns one of: ``"ok"`` (row saved with at least one source),
    ``"empty"`` (both upstreams returned nothing — no row written),
    ``"error"`` (upstream failure after retries — no row written).
    """
    backoff = 5.0
    for attempt in range(1, retries + 1):
        try:
            row = company_about.fetch_live(ticker)
            if row.get("source") == "empty":
                logger.warning("[%s] empty — no upstream returned data", ticker)
                return "empty"
            ok = company_about.upsert_row(row)
            if ok:
                logger.info(
                    "[%s] %s · ceo=%s · emp=%s · hq=%s",
                    ticker, row.get("source"),
                    row.get("ceo") or "—",
                    f"{row['employees']:,}" if row.get("employees") else "—",
                    row.get("hq_city") or row.get("hq_country") or "—",
                )
                return "ok"
            logger.error("[%s] upsert failed", ticker)
            return "error"
        except Exception as exc:
            msg = str(exc).lower()
            if "rate" in msg or "429" in msg or "too many" in msg:
                logger.warning(
                    "[%s] rate-limited, sleeping %.0fs (attempt %d/%d)",
                    ticker, backoff, attempt, retries,
                )
                time.sleep(backoff)
                backoff *= 3
                continue
            logger.error("[%s] fetch failed: %s", ticker, exc)
            return "error"
    logger.error("[%s] gave up after %d retries", ticker, retries)
    return "error"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--ticker", help="Backfill a single ticker (skip universe scan)")
    ap.add_argument("--limit", type=int, default=None,
                    help="Cap how many tickers to process this run")
    ap.add_argument("--sleep", type=float, default=2.5,
                    help="Sleep between calls to avoid yfinance rate-limits")
    ap.add_argument("--force", action="store_true",
                    help="Ignore freshness check; refetch every ticker")
    args = ap.parse_args()

    if args.ticker:
        targets = [args.ticker.upper()]
    else:
        universe = build_universe()
        if not universe:
            logger.error("empty universe — nothing to backfill")
            return 1
        if args.force:
            targets = universe
        else:
            targets = company_about.get_stale_or_missing(universe)
        logger.info("%d / %d tickers need refresh", len(targets), len(universe))

    if args.limit:
        targets = targets[:args.limit]
    if not targets:
        logger.info("nothing to do — all rows fresh")
        return 0

    counts = {"ok": 0, "empty": 0, "error": 0}
    for i, ticker in enumerate(targets, 1):
        logger.info("(%d/%d) %s", i, len(targets), ticker)
        counts[backfill_one(ticker)] += 1
        if i < len(targets):
            time.sleep(args.sleep)

    logger.info("Done. ok=%d empty=%d error=%d",
                counts["ok"], counts["empty"], counts["error"])
    return 0 if counts["error"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
