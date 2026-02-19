-- PaperPanda Supabase Schema: api_cache
-- Run this in your Supabase SQL Editor (Dashboard > SQL Editor > New Query)
--
-- This table acts as a universal persistent cache for expensive API data
-- (Glassdoor ratings, quota tracking, and future cacheable sources).
-- It replaces the local JSON file cache that wipes on Railway deploys.

-- 1. Create the api_cache table
CREATE TABLE IF NOT EXISTS api_cache (
    cache_key     TEXT PRIMARY KEY,           -- e.g. "glassdoor:AAPL", "glassdoor_quota:2026-02"
    category      TEXT NOT NULL,              -- e.g. "glassdoor", "glassdoor_quota"
    response_data JSONB NOT NULL,             -- the cached payload
    expires_at    TIMESTAMPTZ,                -- NULL = never expires (manually managed)
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 2. Indexes for fast lookups
CREATE INDEX IF NOT EXISTS idx_api_cache_category
    ON api_cache (category);

CREATE INDEX IF NOT EXISTS idx_api_cache_expires_at
    ON api_cache (expires_at)
    WHERE expires_at IS NOT NULL;

-- 3. Enable Row Level Security
ALTER TABLE api_cache ENABLE ROW LEVEL SECURITY;

-- 4. RLS Policy: service role can do everything (server-side only)
-- The app uses the service_role key, which bypasses RLS entirely.
-- This policy allows authenticated users to read cached data if needed
-- in the future (e.g., from a frontend client using the anon key).
CREATE POLICY "Allow authenticated read access"
    ON api_cache
    FOR SELECT
    TO authenticated
    USING (true);

-- No INSERT/UPDATE/DELETE policies for authenticated users.
-- Only the service_role key (used by the Python backend) can write.
