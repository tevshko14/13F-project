-- Protect ticker_logos: read-only for anon/authenticated, service_role can still write
ALTER TABLE ticker_logos ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS ticker_logos_read_only ON ticker_logos;
CREATE POLICY ticker_logos_read_only ON ticker_logos
    FOR SELECT USING (true);

-- Protect congress_headshots: read-only for anon/authenticated, service_role can still write
ALTER TABLE congress_headshots ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS congress_headshots_read_only ON congress_headshots;
CREATE POLICY congress_headshots_read_only ON congress_headshots
    FOR SELECT USING (true);
