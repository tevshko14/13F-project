"""Compute forward stock returns for insider purchases.

For each open market purchase in ``insider_trades`` that is missing forward
price data, this job:
  1. Downloads historical daily prices from yfinance
  2. Looks up closing prices at trade date, +30d, +90d, +180d, +365d
  3. Updates the row in Supabase with the forward prices

Two modes:
  - **Initial fill**: processes rows with ``returns_updated IS NULL``
  - **Window sweep**: re-processes rows where a forward window has newly
    closed (e.g. a 100-day-old trade now qualifies for its 90d price)

Usage:
    uv run filings-insider-returns                # process all pending
    uv run filings-insider-returns --sweep        # also re-check open windows
    uv run filings-insider-returns --limit 500    # cap rows processed
"""

from __future__ import annotations

import argparse
import logging
import time
from collections import defaultdict
from datetime import datetime, timezone

import yfinance as yf
import pandas as pd

from filings import supabase_cache

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

    # Ensure index is datetime
    idx = prices.index
    if not isinstance(idx, pd.DatetimeIndex):
        return None

    # Find nearest date within tolerance
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
    """Download daily closing prices for a ticker via yfinance.

    Returns a Series indexed by date with closing prices, or None on failure.
    """
    try:
        from filings.market_data import _YF_TIMEOUT
        df = yf.download(
            ticker,
            start=start_date,
            end=end_date,
            progress=False,
            threads=False,
            auto_adjust=True,
            timeout=_YF_TIMEOUT,
        )
        if df is None or df.empty:
            return None
        # yf.download returns MultiIndex columns for single ticker too
        if isinstance(df.columns, pd.MultiIndex):
            close = df["Close"].iloc[:, 0] if "Close" in df.columns.get_level_values(0) else None
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

    Args:
        ticker: Stock ticker symbol
        trades: List of dicts with keys: sec_url, trade_date, price

    Returns list of update dicts (sec_url + forward price columns).
    """
    if not trades:
        return []

    # Determine date range needed
    trade_dates = []
    for t in trades:
        try:
            trade_dates.append(pd.Timestamp(t["trade_date"]))
        except Exception:
            pass

    if not trade_dates:
        return []

    earliest = min(trade_dates) - pd.Timedelta(days=5)  # buffer for weekends
    latest = max(trade_dates) + pd.Timedelta(days=370)   # 365 + buffer
    today = pd.Timestamp.now(tz=None)
    end = min(latest, today)

    # Single yfinance call for all trades of this ticker
    prices = _download_prices(
        ticker,
        earliest.strftime("%Y-%m-%d"),
        end.strftime("%Y-%m-%d"),
    )
    if prices is None or prices.empty:
        logger.info("No price data for %s — skipping %d trades", ticker, len(trades))
        return []

    now = datetime.now(timezone.utc)
    updates = []

    for trade in trades:
        trade_date_str = trade["trade_date"]
        try:
            trade_dt = pd.Timestamp(trade_date_str)
        except Exception:
            continue

        update: dict = {
            "sec_url": trade["sec_url"],
            "returns_updated": now.isoformat(),
        }

        # Close on trade date
        close_trade = _nearest_close(prices, trade_date_str)
        if close_trade is not None:
            update["close_on_trade"] = close_trade

        # Forward windows
        for window_days, col_name in _WINDOW_COLS.items():
            if window_days == 0:
                continue  # already handled above
            target_date = trade_dt + pd.Timedelta(days=window_days)
            # Only fill if the window has closed
            if target_date <= today:
                fwd_close = _nearest_close(prices, target_date.strftime("%Y-%m-%d"))
                if fwd_close is not None:
                    update[col_name] = fwd_close

        updates.append(update)

    return updates


def process_pending_returns(limit: int = 2000) -> dict:
    """Process all purchases with returns_updated IS NULL.

    Returns stats dict.
    """
    rows = supabase_cache.get_purchases_pending_returns(limit=limit)
    if not rows:
        logger.info("No pending purchases to process")
        return {"tickers": 0, "trades": 0, "updated": 0}

    # Group by ticker
    by_ticker: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        by_ticker[row["ticker"]].append(row)

    logger.info(
        "Processing %d pending purchases across %d tickers",
        len(rows), len(by_ticker),
    )

    total_updated = 0
    tickers_processed = 0

    # Process in batches of ~50 tickers
    ticker_list = list(by_ticker.keys())
    BATCH = 50

    for batch_start in range(0, len(ticker_list), BATCH):
        batch_tickers = ticker_list[batch_start : batch_start + BATCH]
        all_updates: list[dict] = []

        for ticker in batch_tickers:
            trades = by_ticker[ticker]
            updates = _compute_returns_for_ticker(ticker, trades)
            all_updates.extend(updates)
            tickers_processed += 1

        if all_updates:
            updated = supabase_cache.bulk_update_forward_returns(all_updates)
            total_updated += updated
            logger.info(
                "Batch %d: %d tickers → %d rows updated",
                batch_start // BATCH + 1,
                len(batch_tickers),
                updated,
            )

        # Brief pause between batches to be kind to yfinance
        if batch_start + BATCH < len(ticker_list):
            time.sleep(2)

    return {
        "tickers": tickers_processed,
        "trades": len(rows),
        "updated": total_updated,
    }


def process_open_windows() -> dict:
    """Re-process trades where a forward window has newly closed.

    For example, a trade from 100 days ago was initially processed at day 20
    (only close_on_trade and close_at_30d were available). Now the 90d window
    has closed, so we can fill in close_at_90d.
    """
    rows = supabase_cache.get_purchases_with_open_windows()
    if not rows:
        logger.info("No open windows to process")
        return {"tickers": 0, "trades": 0, "updated": 0}

    # Group by ticker
    by_ticker: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        by_ticker[row["ticker"]].append(row)

    logger.info(
        "Re-processing %d trades with open windows across %d tickers",
        len(rows), len(by_ticker),
    )

    total_updated = 0

    for ticker, trades in by_ticker.items():
        updates = _compute_returns_for_ticker(ticker, trades)
        if updates:
            updated = supabase_cache.bulk_update_forward_returns(updates)
            total_updated += updated

    return {
        "tickers": len(by_ticker),
        "trades": len(rows),
        "updated": total_updated,
    }


# ── CLI entry point ──────────────────────────────────────────────────


def main() -> None:
    # Load .env for local development (CLI scripts don't get it from web.py)
    import os
    from pathlib import Path
    if not os.environ.get("RAILWAY_ENVIRONMENT"):
        env_file = Path(__file__).resolve().parents[2] / ".env"
        if env_file.exists():
            from dotenv import load_dotenv
            load_dotenv(env_file)

    parser = argparse.ArgumentParser(
        description="Compute forward stock returns for insider purchases"
    )
    parser.add_argument(
        "--limit", type=int, default=2000,
        help="Max pending rows to process (default: 2000)",
    )
    parser.add_argument(
        "--sweep", action="store_true",
        help="Also re-process trades with newly-closed forward windows",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    start = time.time()

    # Phase 1: process new trades with no returns data
    pending_result = process_pending_returns(limit=args.limit)
    print(f"\nPending returns: {pending_result}")

    # Phase 2 (optional): re-check open windows
    sweep_result = {"tickers": 0, "trades": 0, "updated": 0}
    if args.sweep:
        sweep_result = process_open_windows()
        print(f"Window sweep:    {sweep_result}")

    elapsed = round(time.time() - start, 1)
    total = pending_result["updated"] + sweep_result["updated"]

    print(f"\n{'='*50}")
    print(f"Forward Returns Summary ({elapsed}s)")
    print(f"{'='*50}")
    print(f"Rows updated: {total}")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()
