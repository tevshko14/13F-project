-- Topics for pre-stream planning
CREATE TABLE cc_topics (
    id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    room_id TEXT REFERENCES cc_rooms(id) ON DELETE CASCADE,
    label TEXT NOT NULL,
    sort_order INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT now()
);
ALTER TABLE cc_topics REPLICA IDENTITY FULL;
ALTER TABLE cc_topics ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow all on cc_topics" ON cc_topics FOR ALL USING (true) WITH CHECK (true);

-- AI suggestions
CREATE TABLE cc_suggestions (
    id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    room_id TEXT REFERENCES cc_rooms(id) ON DELETE CASCADE,
    type TEXT NOT NULL CHECK (type IN ('timestamp', 'short')),
    note TEXT NOT NULL,
    elapsed_seconds INTEGER NOT NULL,
    reasoning TEXT,
    confidence TEXT DEFAULT 'medium',
    status TEXT DEFAULT 'pending',
    matched_topic TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);
ALTER TABLE cc_suggestions REPLICA IDENTITY FULL;
ALTER TABLE cc_suggestions ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow all on cc_suggestions" ON cc_suggestions FOR ALL USING (true) WITH CHECK (true);

-- Add monitoring fields to rooms
ALTER TABLE cc_rooms ADD COLUMN youtube_url TEXT;
ALTER TABLE cc_rooms ADD COLUMN ai_monitoring_enabled BOOLEAN DEFAULT false;
ALTER TABLE cc_rooms ADD COLUMN last_caption_offset INTEGER DEFAULT 0;

-- Enable realtime
ALTER PUBLICATION supabase_realtime ADD TABLE cc_topics;
ALTER PUBLICATION supabase_realtime ADD TABLE cc_suggestions;
