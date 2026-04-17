-- Tracks which tickers have had full historical XBRL data
-- archived to cold storage (Supabase Storage JSON blobs).

CREATE TABLE IF NOT EXISTS fundamentals_backfill_meta (
    ticker            TEXT PRIMARY KEY,
    cik               TEXT,
    annual_periods    INTEGER NOT NULL DEFAULT 0,
    quarterly_periods INTEGER NOT NULL DEFAULT 0,
    cold_storage_path TEXT NOT NULL DEFAULT '',
    status            TEXT NOT NULL DEFAULT 'pending',
    error_message     TEXT,
    backfilled_at     TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_fbm_status
    ON fundamentals_backfill_meta (status);
CREATE INDEX IF NOT EXISTS idx_fbm_backfilled_at
    ON fundamentals_backfill_meta (backfilled_at DESC NULLS LAST);

-- Delete protection (cold table)
CREATE OR REPLACE FUNCTION prevent_fundamentals_meta_delete()
RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'DELETE on fundamentals_backfill_meta is blocked (cold table). Contact admin to remove data.';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_fundamentals_meta_no_delete ON fundamentals_backfill_meta;
CREATE TRIGGER trg_fundamentals_meta_no_delete
    BEFORE DELETE ON fundamentals_backfill_meta
    FOR EACH ROW
    EXECUTE FUNCTION prevent_fundamentals_meta_delete();

-- RLS
ALTER TABLE fundamentals_backfill_meta ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow service role full access on fundamentals_backfill_meta"
    ON fundamentals_backfill_meta
    FOR ALL
    USING (true)
    WITH CHECK (true);
