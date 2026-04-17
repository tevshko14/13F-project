CREATE TABLE IF NOT EXISTS cc_settings (
    key TEXT PRIMARY KEY,
    value TEXT,
    updated_at TIMESTAMPTZ DEFAULT now()
);
ALTER TABLE cc_settings ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow all on cc_settings" ON cc_settings FOR ALL USING (true) WITH CHECK (true);

-- Add auto_detected to rooms (youtube_url already exists from N2 sprint)
ALTER TABLE cc_rooms ADD COLUMN IF NOT EXISTS auto_detected BOOLEAN DEFAULT false;
