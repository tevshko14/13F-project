CREATE TABLE IF NOT EXISTS earnings_scorecard_cache (
    id          BIGSERIAL PRIMARY KEY,
    cache_key   TEXT NOT NULL UNIQUE,
    index_key   TEXT NOT NULL,
    quarter     TEXT NOT NULL,
    sector      TEXT,
    payload     JSONB NOT NULL,
    fetched_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_esc_key
    ON earnings_scorecard_cache (cache_key);

ALTER TABLE earnings_scorecard_cache ENABLE ROW LEVEL SECURITY;
