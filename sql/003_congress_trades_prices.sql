-- ================================================================
-- Congress Trades Prices — Forward Return Enrichment Table
-- ================================================================
--
-- Since `congress_trades` has BEFORE UPDATE triggers that block ALL
-- updates (write-once cold archive), price enrichment data lives in
-- a **separate join table** that CAN be freely updated/backfilled.
--
-- Each row stores the stock's closing price at the trade date and at
-- forward windows (+30d, +90d, +180d, +365d), plus computed returns.
--
-- Run this ONCE in the Supabase SQL Editor after congress_trades exists.
-- It is safe to re-run (CREATE TABLE IF NOT EXISTS).
-- ================================================================

CREATE TABLE IF NOT EXISTS congress_trades_prices (
    trade_id       TEXT PRIMARY KEY REFERENCES congress_trades(trade_id),
    ticker         TEXT,                          -- denormalized for quick lookups
    trade_date     DATE,                          -- denormalized
    close_on_trade NUMERIC(12, 4),                -- closing price on trade_date
    close_at_30d   NUMERIC(12, 4),                -- closing price ~30 days after
    close_at_90d   NUMERIC(12, 4),
    close_at_180d  NUMERIC(12, 4),
    close_at_365d  NUMERIC(12, 4),
    return_30d     NUMERIC(8, 4),                 -- % return (e.g., 12.34 = +12.34%)
    return_90d     NUMERIC(8, 4),
    return_180d    NUMERIC(8, 4),
    return_365d    NUMERIC(8, 4),
    prices_updated TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Index for joining back to congress_trades
CREATE INDEX IF NOT EXISTS idx_ctp_trade_id ON congress_trades_prices (trade_id);

-- Index for per-ticker price lookups
CREATE INDEX IF NOT EXISTS idx_ctp_ticker ON congress_trades_prices (ticker, trade_date DESC);

-- ── Verify ──
SELECT count(*) AS total_rows FROM congress_trades_prices;
