#!/usr/bin/env python3
"""Backfill stock prices for congressional trades.

For each congress trade with a ticker, downloads historical daily prices
from yfinance and computes forward returns at +30d, +90d, +180d, +365d.
Results go into ``congress_trades_prices`` (a separate table since the
main ``congress_trades`` table has BEFORE UPDATE triggers blocking all
updates).

Usage:
    uv run python scripts/backfill_congress_prices.py [--limit N] [--dry-run]

The full backfill (~30K trades across ~2K tickers) takes 20-30 minutes.
Use --limit for testing.  Use --dry-run to compute without writing.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import pandas as pd
import yfinance as yf

# Ensure the src directory is on the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from filings import supabase_cache


def _setup_logging() -> None:
    fmt = "%(asctime)s %(levelname)-8s %(name)s -- %(message)s"
    logging.basicConfig(level=logging.INFO, format=fmt, force=True)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("yfinance").setLevel(logging.WARNING)


logger = logging.getLogger(__name__)

# Forward-return windows (calendar days)
_WINDOWS = [30, 90, 180, 365]
_WINDOW_COLS = {
    0: "close_on_trade",
    30: "close_at_30d",
    90: "close_at_90d",
    180: "close_at_180d",
    365: "close_at_365d",
}


# ── Price lookup helpers ────────────────────────────────────────────


def _nearest_close(
    prices: pd.Series,
    target_date: str,
    tolerance_days: int = 5,
) -> float | None:
    """Find the closing price nearest to target_date (within tolerance).

    Markets are closed on weekends/holidays, so we look for the nearest
    trading day within ±tolerance_days of the target date.
    """
    try:
        target = pd.Timestamp(target_date)
    except Exception:
        return None

    if prices.empty:
        return None

    idx = prices.index
    if not isinstance(idx, pd.DatetimeIndex):
        return None

    diffs = abs(idx - target)
    min_diff = diffs.min()
    if min_diff > pd.Timedelta(days=tolerance_days):
        return None

    nearest_idx = diffs.argmin()
    val = prices.iloc[nearest_idx]
    if pd.isna(val):
        return None
    return round(float(val), 4)


def _download_prices(
    ticker: str,
    start_date: str,
    end_date: str,
) -> pd.Series | None:
    """Download daily closing prices for a ticker via yfinance."""
    try:
        df = yf.download(
            ticker,
            start=start_date,
            end=end_date,
            progress=False,
            threads=False,
            auto_adjust=True,
        )
        if df is None or df.empty:
            return None
        if isinstance(df.columns, pd.MultiIndex):
            close = (
                df["Close"].iloc[:, 0]
                if "Close" in df.columns.get_level_values(0)
                else None
            )
        else:
            close = df["Close"] if "Close" in df.columns else None
        return close
    except Exception as exc:
        logger.warning("yfinance download failed for %s: %s", ticker, exc)
        return None


# ── Main computation ────────────────────────────────────────────────


def _compute_returns_for_ticker(
    ticker: str,
    trades: list[dict],
) -> list[dict]:
    """Compute forward returns for a batch of trades of the same ticker.

    Returns list of row dicts ready for upsert into congress_trades_prices.
    """
    if not trades:
        return []

    trade_dates = []
    for t in trades:
        try:
            trade_dates.append(pd.Timestamp(t["trade_date"]))
        except Exception:
            pass

    if not trade_dates:
        return []

    earliest = min(trade_dates) - pd.Timedelta(days=5)
    latest = max(trade_dates) + pd.Timedelta(days=370)
    today = pd.Timestamp.now(tz=None)
    end = min(latest, today)

    prices = _download_prices(
        ticker,
        earliest.strftime("%Y-%m-%d"),
        end.strftime("%Y-%m-%d"),
    )
    if prices is None or prices.empty:
        logger.debug("No price data for %s — skipping %d trades", ticker, len(trades))
        return []

    now = datetime.now(timezone.utc)
    results = []

    for trade in trades:
        trade_date_str = trade["trade_date"]
        try:
            trade_dt = pd.Timestamp(trade_date_str)
        except Exception:
            continue

        row: dict = {
            "trade_id": trade["trade_id"],
            "ticker": ticker,
            "trade_date": trade_date_str,
            "prices_updated": now.isoformat(),
        }

        # Close on trade date
        close_trade = _nearest_close(prices, trade_date_str)
        if close_trade is not None:
            row["close_on_trade"] = close_trade

        # Forward windows
        for window_days, col_name in _WINDOW_COLS.items():
            if window_days == 0:
                continue
            target_date = trade_dt + pd.Timedelta(days=window_days)
            if target_date <= today:
                fwd_close = _nearest_close(prices, target_date.strftime("%Y-%m-%d"))
                if fwd_close is not None:
                    row[col_name] = fwd_close
                    # Compute return percentage
                    if close_trade and close_trade > 0:
                        ret = ((fwd_close - close_trade) / close_trade) * 100
                        row[f"return_{window_days}d"] = round(ret, 4)

        results.append(row)

    return results


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill stock prices for congressional trades"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50000,
        help="Max trades to process (default: 50000)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute prices without writing to Supabase",
    )
    args = parser.parse_args()

    _setup_logging()

    logger.info("=== Congress Trades Price Backfill ===")
    logger.info("Settings: limit=%d, dry_run=%s", args.limit, args.dry_run)

    # ── Phase 1: Fetch trades missing prices ──
    if not supabase_cache.is_available():
        logger.error("Supabase not available — check env vars")
        sys.exit(1)

    trades = supabase_cache.get_congress_trades_missing_prices(limit=args.limit)
    if not trades:
        logger.info("No trades missing prices — all up to date!")
        return

    logger.info("Found %d trades missing prices", len(trades))

    # ── Phase 2: Group by ticker ──
    by_ticker: dict[str, list[dict]] = defaultdict(list)
    for t in trades:
        ticker = t.get("ticker")
        if ticker:
            by_ticker[ticker].append(t)

    logger.info(
        "Processing %d trades across %d unique tickers",
        len(trades),
        len(by_ticker),
    )

    # ── Phase 3: Process in batches of 50 tickers ──
    t0 = time.time()
    total_computed = 0
    total_written = 0
    tickers_processed = 0

    ticker_list = list(by_ticker.keys())
    BATCH = 50

    for batch_start in range(0, len(ticker_list), BATCH):
        batch_tickers = ticker_list[batch_start : batch_start + BATCH]
        all_results: list[dict] = []

        for ticker in batch_tickers:
            ticker_trades = by_ticker[ticker]
            results = _compute_returns_for_ticker(ticker, ticker_trades)
            all_results.extend(results)
            tickers_processed += 1

        total_computed += len(all_results)

        if all_results and not args.dry_run:
            written = supabase_cache.upsert_congress_trade_prices(all_results)
            total_written += written

        elapsed = time.time() - t0
        logger.info(
            "Progress: %d/%d tickers (%.0fs) — %d prices computed, %d written",
            tickers_processed,
            len(ticker_list),
            elapsed,
            total_computed,
            total_written,
        )

        # Polite sleep between yfinance batches
        if batch_start + BATCH < len(ticker_list):
            time.sleep(2.0)

    # ── Summary ──
    total_time = time.time() - t0
    logger.info("=== Backfill Complete ===")
    logger.info("  Tickers:  %d", tickers_processed)
    logger.info("  Computed: %d price rows", total_computed)
    logger.info("  Written:  %d rows%s", total_written, " (DRY RUN)" if args.dry_run else "")
    logger.info("  Time:     %.1f seconds", total_time)


if __name__ == "__main__":
    main()
