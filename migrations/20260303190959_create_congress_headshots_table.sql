CREATE TABLE IF NOT EXISTS congress_headshots (
    member_id    TEXT PRIMARY KEY,
    photo_b64    TEXT NOT NULL DEFAULT '',
    content_type TEXT NOT NULL DEFAULT 'image/jpeg',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
