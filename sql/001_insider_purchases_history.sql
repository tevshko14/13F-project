-- ============================================================
-- Migration: insider_purchases_history (cold, delete-protected)
-- Run this in the Supabase SQL Editor BEFORE deploying code.
-- ============================================================

-- ── 1. Create the cold history table ──────────────────────────

CREATE TABLE IF NOT EXISTS insider_purchases_history (
    id              BIGSERIAL PRIMARY KEY,
    sec_url         TEXT NOT NULL UNIQUE,
    filing_date     DATE NOT NULL,
    trade_date      DATE NOT NULL,
    ticker          TEXT NOT NULL,
    company_name    TEXT NOT NULL DEFAULT '',
    insider_name    TEXT NOT NULL,
    title           TEXT NOT NULL DEFAULT '',
    price           NUMERIC(12,4),
    qty             INTEGER,
    value           NUMERIC(16,2),
    close_on_trade  NUMERIC(12,4),
    close_at_30d    NUMERIC(12,4),
    close_at_90d    NUMERIC(12,4),
    close_at_180d   NUMERIC(12,4),
    close_at_365d   NUMERIC(12,4),
    returns_updated TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);


-- ── 2. Indexes ────────────────────────────────────────────────

-- Primary lookup: per-ticker queries sorted by trade date
CREATE INDEX IF NOT EXISTS idx_iph_ticker_trade_date
    ON insider_purchases_history (ticker, trade_date DESC);

-- Forward returns job: find rows not yet processed
CREATE INDEX IF NOT EXISTS idx_iph_pending_returns
    ON insider_purchases_history (ticker, trade_date)
    WHERE returns_updated IS NULL;

-- Open-window queries: rows that need later-window fills
CREATE INDEX IF NOT EXISTS idx_iph_open_90d
    ON insider_purchases_history (trade_date)
    WHERE close_at_90d IS NULL AND returns_updated IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_iph_open_180d
    ON insider_purchases_history (trade_date)
    WHERE close_at_180d IS NULL AND returns_updated IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_iph_open_365d
    ON insider_purchases_history (trade_date)
    WHERE close_at_365d IS NULL AND returns_updated IS NOT NULL;


-- ── 3. Delete protection trigger ──────────────────────────────
-- Prevents ANY row from being deleted via SQL or REST API.

CREATE OR REPLACE FUNCTION prevent_history_delete() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'DELETE on insider_purchases_history is prohibited.';
    RETURN NULL;
END; $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_no_delete_history ON insider_purchases_history;
CREATE TRIGGER trg_no_delete_history
    BEFORE DELETE ON insider_purchases_history
    FOR EACH ROW EXECUTE FUNCTION prevent_history_delete();


-- ── 4. Write-once trigger for forward returns ─────────────────
-- Once a forward-return value is filled, it can never be overwritten.
-- New windows (NULL → value) are always allowed.

CREATE OR REPLACE FUNCTION enforce_write_once_returns() RETURNS trigger AS $$
BEGIN
    -- If returns_updated was never set, this is the first fill — allow everything
    IF OLD.returns_updated IS NULL THEN RETURN NEW; END IF;
    -- Preserve existing non-NULL forward return values
    IF OLD.close_on_trade IS NOT NULL THEN NEW.close_on_trade := OLD.close_on_trade; END IF;
    IF OLD.close_at_30d IS NOT NULL THEN NEW.close_at_30d := OLD.close_at_30d; END IF;
    IF OLD.close_at_90d IS NOT NULL THEN NEW.close_at_90d := OLD.close_at_90d; END IF;
    IF OLD.close_at_180d IS NOT NULL THEN NEW.close_at_180d := OLD.close_at_180d; END IF;
    IF OLD.close_at_365d IS NOT NULL THEN NEW.close_at_365d := OLD.close_at_365d; END IF;
    RETURN NEW;
END; $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_write_once_returns ON insider_purchases_history;
CREATE TRIGGER trg_write_once_returns
    BEFORE UPDATE ON insider_purchases_history
    FOR EACH ROW EXECUTE FUNCTION enforce_write_once_returns();


-- ── 5. Migrate existing purchase data from hot → cold ─────────

INSERT INTO insider_purchases_history (
    sec_url, filing_date, trade_date, ticker, company_name,
    insider_name, title, price, qty, value,
    close_on_trade, close_at_30d, close_at_90d, close_at_180d, close_at_365d,
    returns_updated, created_at
)
SELECT
    sec_url, filing_date, trade_date, ticker, company_name,
    insider_name, title, price, qty, value,
    close_on_trade, close_at_30d, close_at_90d, close_at_180d, close_at_365d,
    returns_updated, created_at
FROM insider_trades
WHERE trade_type = 'Purchase'
ON CONFLICT (sec_url) DO NOTHING;


-- ── 6. Verify counts match ───────────────────────────────────

SELECT 'hot_purchases' AS source, COUNT(*) AS cnt
FROM insider_trades WHERE trade_type = 'Purchase'
UNION ALL
SELECT 'cold_history', COUNT(*)
FROM insider_purchases_history;
