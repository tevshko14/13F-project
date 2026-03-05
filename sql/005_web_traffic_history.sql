-- ============================================================
-- Migration: web_traffic_history (cold, delete-protected)
-- Stores daily web traffic snapshots for digital-first stocks.
-- Run this in the Supabase SQL Editor BEFORE deploying code.
-- ============================================================

-- ── 1. Create the cold history table ──────────────────────────

CREATE TABLE IF NOT EXISTS web_traffic_history (
    id              BIGSERIAL PRIMARY KEY,
    ticker          TEXT NOT NULL,
    snapshot_date   DATE NOT NULL,
    source          TEXT NOT NULL,        -- 'cloudflare_radar', 'tranco', 'wikipedia'
    data            JSONB NOT NULL,       -- full response payload
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(ticker, snapshot_date, source) -- one snapshot per source per day
);


-- ── 2. Indexes ────────────────────────────────────────────────

-- Primary lookup: per-ticker queries sorted by date
CREATE INDEX IF NOT EXISTS idx_wth_ticker_date
    ON web_traffic_history (ticker, snapshot_date DESC);

-- Source-specific queries
CREATE INDEX IF NOT EXISTS idx_wth_source_date
    ON web_traffic_history (source, snapshot_date DESC);


-- ── 3. Delete protection (cold table — write-once) ────────────

CREATE OR REPLACE FUNCTION prevent_web_traffic_delete()
RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'DELETE on web_traffic_history is blocked (cold table). '
                    'Contact admin to remove data.';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_web_traffic_no_delete ON web_traffic_history;
CREATE TRIGGER trg_web_traffic_no_delete
    BEFORE DELETE ON web_traffic_history
    FOR EACH ROW
    EXECUTE FUNCTION prevent_web_traffic_delete();


-- ── 4. RLS (optional — enable if using Supabase auth) ─────────

-- ALTER TABLE web_traffic_history ENABLE ROW LEVEL SECURITY;
-- CREATE POLICY "service_role_full" ON web_traffic_history
--     FOR ALL USING (auth.role() = 'service_role');
