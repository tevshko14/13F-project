CREATE TABLE IF NOT EXISTS feature_announcements (
    id          TEXT PRIMARY KEY,
    title       TEXT NOT NULL,
    message     TEXT NOT NULL DEFAULT '',
    icon        TEXT NOT NULL DEFAULT '🚀',
    toast_type  TEXT NOT NULL DEFAULT 'alert',
    link        TEXT NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
