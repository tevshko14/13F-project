CREATE TABLE IF NOT EXISTS ticker_logos (
    ticker        TEXT PRIMARY KEY,
    logo_b64      TEXT NOT NULL DEFAULT '',
    content_type  TEXT NOT NULL DEFAULT 'image/png',
    logo_domain   TEXT NOT NULL DEFAULT '',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Allow service-role full access (matches existing RLS pattern)
ALTER TABLE ticker_logos ENABLE ROW LEVEL SECURITY;
CREATE POLICY "service_role_all" ON ticker_logos
    FOR ALL
    TO service_role
    USING (true)
    WITH CHECK (true);

-- Allow anon/authenticated to read (for public logo serving if needed)
CREATE POLICY "public_read" ON ticker_logos
    FOR SELECT
    TO anon, authenticated
    USING (true);
