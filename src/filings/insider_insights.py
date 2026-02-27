"""Compute insider trading insights for a stock ticker.

Pure computation module — takes a list of historical insider purchases
(with optional forward-return data) and returns a structured dict of
4 insight metrics:

  1. **Buy Frequency** — purchases in last 12mo vs historical average
  2. **Insider Buying Zone** — price range where insiders commonly buy
  3. **Win Rate** — % of purchases currently in profit
  4. **Forward 90-Day Return** — median return 90 days after insider buys
"""

from __future__ import annotations

import statistics
from datetime import datetime, timedelta, timezone


def compute_insider_insights(
    ticker: str,
    trades: list[dict],
    current_price: float | None = None,
) -> dict | None:
    """Compute insider insights for a single ticker from its purchase history.

    Args:
        ticker: Stock ticker symbol.
        trades: List of dicts from ``get_insider_purchases()``, each with keys:
            trade_date, price, value, close_on_trade, close_at_30d,
            close_at_90d, close_at_180d, close_at_365d.
        current_price: Current stock price (from yfinance). If None, win rate
            uses close_on_trade + latest forward price as proxy.

    Returns:
        Dict of insight metrics, or None if insufficient data (< 2 trades).
    """
    if not trades or len(trades) < 2:
        return None

    now = datetime.now(timezone.utc)
    one_year_ago = now - timedelta(days=365)
    two_years_ago = now - timedelta(days=730)

    # Parse trade dates and buy prices
    parsed: list[dict] = []
    for t in trades:
        try:
            td = datetime.strptime(str(t["trade_date"]), "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
        except (ValueError, TypeError):
            continue

        buy_price = _to_float(t.get("price"))
        if buy_price is None or buy_price <= 0:
            # Fall back to close_on_trade if OpenInsider price is missing
            buy_price = _to_float(t.get("close_on_trade"))
        if buy_price is None or buy_price <= 0:
            continue

        parsed.append({
            "trade_date": td,
            "buy_price": buy_price,
            "value": _to_float(t.get("value")) or 0,
            "insider_name": t.get("insider_name", ""),
            "close_on_trade": _to_float(t.get("close_on_trade")),
            "close_at_30d": _to_float(t.get("close_at_30d")),
            "close_at_90d": _to_float(t.get("close_at_90d")),
            "close_at_180d": _to_float(t.get("close_at_180d")),
            "close_at_365d": _to_float(t.get("close_at_365d")),
        })

    if len(parsed) < 2:
        return None

    # Sort by date (newest first)
    parsed.sort(key=lambda x: x["trade_date"], reverse=True)

    # ── 1. Buy Frequency ────────────────────────────────────────────

    purchases_12m = sum(1 for p in parsed if p["trade_date"] >= one_year_ago)
    purchases_prior_12m = sum(
        1 for p in parsed
        if two_years_ago <= p["trade_date"] < one_year_ago
    )

    # Date range of all trades
    oldest = min(p["trade_date"] for p in parsed)
    newest = max(p["trade_date"] for p in parsed)
    span_years = max((newest - oldest).days / 365.25, 0.5)  # at least 6 months
    avg_annual = len(parsed) / span_years

    # Frequency label
    if purchases_12m > avg_annual * 1.5:
        frequency_label = "Elevated"
    elif purchases_12m < avg_annual * 0.5:
        frequency_label = "Quiet"
    else:
        frequency_label = "Normal"

    # ── 2. Insider Buying Zone ──────────────────────────────────────

    buy_prices = [p["buy_price"] for p in parsed]
    avg_buy_price = round(statistics.mean(buy_prices), 2)
    median_buy_price = round(statistics.median(buy_prices), 2)
    min_buy_price = round(min(buy_prices), 2)
    max_buy_price = round(max(buy_prices), 2)

    vs_avg_buy_pct = None
    if current_price and avg_buy_price > 0:
        vs_avg_buy_pct = round(
            (current_price - avg_buy_price) / avg_buy_price * 100, 1
        )

    # ── 3. Win Rate ─────────────────────────────────────────────────

    wins = 0
    losses = 0
    pending = 0
    returns: list[float] = []

    for p in parsed:
        # Use current_price if available, otherwise best available forward price
        ref_price = current_price
        if ref_price is None:
            # Use the furthest forward price available
            for key in ["close_at_365d", "close_at_180d", "close_at_90d", "close_at_30d"]:
                ref_price = p.get(key)
                if ref_price is not None:
                    break

        if ref_price is None or ref_price <= 0:
            pending += 1
            continue

        ret = (ref_price - p["buy_price"]) / p["buy_price"] * 100
        returns.append(ret)
        if ret >= 0:
            wins += 1
        else:
            losses += 1

    win_rate_pct = None
    avg_return_pct = None
    median_return_pct = None
    total_decided = wins + losses

    if total_decided >= 2:
        win_rate_pct = round(wins / total_decided * 100, 1)
        avg_return_pct = round(statistics.mean(returns), 1)
        median_return_pct = round(statistics.median(returns), 1)

    # ── 4. Forward 90-Day Return ────────────────────────────────────

    fwd_90d_returns: list[float] = []
    for p in parsed:
        close_trade = p["close_on_trade"] or p["buy_price"]
        close_90d = p.get("close_at_90d")
        if close_trade and close_90d and close_trade > 0:
            ret = (close_90d - close_trade) / close_trade * 100
            fwd_90d_returns.append(ret)

    fwd_90d_count = len(fwd_90d_returns)
    fwd_90d_median_return = None
    fwd_90d_avg_return = None
    fwd_90d_win_rate = None

    if fwd_90d_count >= 3:
        fwd_90d_median_return = round(statistics.median(fwd_90d_returns), 1)
        fwd_90d_avg_return = round(statistics.mean(fwd_90d_returns), 1)
        fwd_90d_wins = sum(1 for r in fwd_90d_returns if r >= 0)
        fwd_90d_win_rate = round(fwd_90d_wins / fwd_90d_count * 100, 1)

    # ── Assemble result ─────────────────────────────────────────────

    return {
        "ticker": ticker.upper(),
        "total_purchases": len(parsed),
        "date_range_start": oldest.strftime("%Y-%m-%d"),
        "date_range_end": newest.strftime("%Y-%m-%d"),
        # 1. Buy Frequency
        "purchases_12m": purchases_12m,
        "purchases_prior_12m": purchases_prior_12m,
        "avg_annual_purchases": round(avg_annual, 1),
        "frequency_label": frequency_label,
        # 2. Insider Buying Zone
        "avg_buy_price": avg_buy_price,
        "min_buy_price": min_buy_price,
        "max_buy_price": max_buy_price,
        "median_buy_price": median_buy_price,
        "current_price": round(current_price, 2) if current_price else None,
        "vs_avg_buy_pct": vs_avg_buy_pct,
        # 3. Win Rate
        "wins": wins,
        "losses": losses,
        "pending": pending,
        "win_rate_pct": win_rate_pct,
        "avg_return_pct": avg_return_pct,
        "median_return_pct": median_return_pct,
        # 4. Forward 90-Day Return
        "fwd_90d_count": fwd_90d_count,
        "fwd_90d_median_return": fwd_90d_median_return,
        "fwd_90d_avg_return": fwd_90d_avg_return,
        "fwd_90d_win_rate": fwd_90d_win_rate,
    }


# ── Helpers ──────────────────────────────────────────────────────────


def _to_float(val) -> float | None:
    """Safely convert a value to float, returning None on failure."""
    if val is None:
        return None
    try:
        f = float(val)
        return f if f == f else None  # NaN check
    except (ValueError, TypeError):
        return None
