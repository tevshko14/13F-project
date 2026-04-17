CREATE TABLE cc_rooms (
    id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    title TEXT NOT NULL,
    host_name TEXT NOT NULL,
    started_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT now()
);

ALTER TABLE cc_rooms REPLICA IDENTITY FULL;

CREATE TABLE cc_entries (
    id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    room_id TEXT REFERENCES cc_rooms(id) ON DELETE CASCADE,
    type TEXT NOT NULL CHECK (type IN ('timestamp', 'short')),
    note TEXT DEFAULT '',
    elapsed_seconds INTEGER NOT NULL,
    author_name TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
);

ALTER TABLE cc_entries REPLICA IDENTITY FULL;

ALTER TABLE cc_rooms ENABLE ROW LEVEL SECURITY;
ALTER TABLE cc_entries ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Allow all on cc_rooms" ON cc_rooms FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all on cc_entries" ON cc_entries FOR ALL USING (true) WITH CHECK (true);
