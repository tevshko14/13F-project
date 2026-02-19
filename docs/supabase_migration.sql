-- PaperPanda Supabase Schema: api_cache
-- Run this in your Supabase SQL Editor (Dashboard > SQL Editor > New Query)
-- NOTE: Auto-applied on first deploy via _auto_migrate() in supabase_cache.py
--
-- This table acts as a universal persistent cache (L2) for all API data.
-- It survives Railway deploys (unlike the disk JSON cache).
--
-- Categories stored:
--   "13f"              — ~100 superinvestor fund data (holdings, changes, quarterly history)
--   "glassdoor"        — Company culture ratings per ticker
--   "glassdoor_quota"  — Monthly Glassdoor API call counter
--   "insider"          — Form 4 insider trades (global + per-ticker)

-- 1. Create the api_cache table
CREATE TABLE IF NOT EXISTS api_cache (
    cache_key     TEXT PRIMARY KEY,           -- e.g. "13f:1067983", "glassdoor:AAPL", "insider_global:p:25"
    category      TEXT NOT NULL,              -- e.g. "13f", "glassdoor", "glassdoor_quota", "insider"
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


-- ═══════════════════════════════════════════════════════════════════════
-- PaperPanda Supabase Schema: profiles (Authentication)
-- ═══════════════════════════════════════════════════════════════════════
--
-- User profiles table for authentication.
-- Extends Supabase auth.users with app-specific fields.
-- Auto-created via trigger when a new user signs up.
--
-- Tiers: "free" (default) | "premium"

-- 5. Create the profiles table
CREATE TABLE IF NOT EXISTS profiles (
    id           UUID PRIMARY KEY REFERENCES auth.users(id) ON DELETE CASCADE,
    email        TEXT,
    display_name TEXT,
    tier         TEXT NOT NULL DEFAULT 'free',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 6. Enable Row Level Security on profiles
ALTER TABLE profiles ENABLE ROW LEVEL SECURITY;

-- 7. RLS Policies for profiles
-- Users can read their own profile
CREATE POLICY "Users can read own profile"
    ON profiles FOR SELECT
    TO authenticated
    USING (auth.uid() = id);

-- Service role can do everything (backend user management)
CREATE POLICY "Service role full access on profiles"
    ON profiles FOR ALL
    TO service_role
    USING (true);

-- 8. Auto-create profile on user signup
CREATE OR REPLACE FUNCTION public.handle_new_user()
RETURNS trigger AS $$
BEGIN
    INSERT INTO public.profiles (id, email, display_name)
    VALUES (
        NEW.id,
        NEW.email,
        COALESCE(NEW.raw_user_meta_data->>'display_name', split_part(NEW.email, '@', 1))
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

DROP TRIGGER IF EXISTS on_auth_user_created ON auth.users;
CREATE TRIGGER on_auth_user_created
    AFTER INSERT ON auth.users
    FOR EACH ROW EXECUTE FUNCTION public.handle_new_user();
