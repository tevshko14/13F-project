-- Google Trends search interest history
-- Stores per-ticker keyword trends + macro trends + trending searches
-- Write-once cold table (same pattern as web_traffic_history)

CREATE TABLE IF NOT EXISTS google_trends_history (
    id              BIGSERIAL PRIMARY KEY,
    ticker          TEXT,                       -- NULL for macro/trending entries
    category        TEXT NOT NULL,              -- 'intent', 'product', 'comparison', 'macro', 'trending'
    keywords        TEXT[] NOT NULL,            -- keywords queried
    snapshot_date   DATE NOT NULL,
    data            JSONB NOT NULL,             -- full response payload
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(ticker, category, snapshot_date)     -- one snapshot per category per day
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_gth_ticker_date
    ON google_trends_history (ticker, snapshot_date DESC);

CREATE INDEX IF NOT EXISTS idx_gth_category_date
    ON google_trends_history (category, snapshot_date DESC);

CREATE INDEX IF NOT EXISTS idx_gth_trending
    ON google_trends_history (category, snapshot_date DESC)
    WHERE category = 'trending';

-- Write-once protection (same as other cold tables)
CREATE OR REPLACE FUNCTION prevent_gth_delete()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'DELETE not allowed on google_trends_history (cold table)';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_gth_no_delete ON google_trends_history;
CREATE TRIGGER trg_gth_no_delete
    BEFORE DELETE ON google_trends_history
    FOR EACH ROW
    EXECUTE FUNCTION prevent_gth_delete();
