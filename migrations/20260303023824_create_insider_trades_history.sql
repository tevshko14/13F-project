-- Cold archive for ALL insider trades (purchases + sales).
-- Write-once, delete-protected. Raw numeric values (no _fmt columns).
-- The hot table (insider_trades) keeps 30 days; this table keeps everything.

CREATE TABLE IF NOT EXISTS insider_trades_history (
    id              BIGSERIAL PRIMARY KEY,
    sec_url         TEXT NOT NULL UNIQUE,
    filing_date     DATE NOT NULL,
    trade_date      DATE NOT NULL,
    ticker          TEXT NOT NULL,
    company_name    TEXT NOT NULL DEFAULT '',
    insider_name    TEXT NOT NULL,
    title           TEXT NOT NULL DEFAULT '',
    trade_type      TEXT NOT NULL,
    price           NUMERIC(12,4),
    qty             INTEGER,
    value           NUMERIC(16,2),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_insider_history_ticker_trade_date
    ON insider_trades_history(ticker, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_insider_history_filing_date
    ON insider_trades_history(filing_date DESC);
CREATE INDEX IF NOT EXISTS idx_insider_history_trade_type_filing_date
    ON insider_trades_history(trade_type, filing_date DESC);

-- ── Delete protection ──
CREATE OR REPLACE FUNCTION prevent_insider_history_delete()
RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION
        'DELETE on insider_trades_history is prohibited — this is a write-once cold archive. '
        'sec_url = %', OLD.sec_url;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_no_delete_insider_history ON insider_trades_history;
CREATE TRIGGER trg_no_delete_insider_history
    BEFORE DELETE ON insider_trades_history
    FOR EACH ROW EXECUTE FUNCTION prevent_insider_history_delete();

-- ── Update protection ──
CREATE OR REPLACE FUNCTION prevent_insider_history_update()
RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION
        'UPDATE on insider_trades_history is prohibited — rows are immutable. '
        'sec_url = %', OLD.sec_url;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_no_update_insider_history ON insider_trades_history;
CREATE TRIGGER trg_no_update_insider_history
    BEFORE UPDATE ON insider_trades_history
    FOR EACH ROW EXECUTE FUNCTION prevent_insider_history_update();
