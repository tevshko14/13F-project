-- PaperPanda Supabase Schema
-- Run this in your Supabase SQL Editor (Dashboard > SQL Editor > New Query)
-- NOTE: Auto-applied on first deploy via _auto_migrate() in supabase_cache.py
--
-- Tables:
--   api_cache       — Universal persistent cache (L2) for API data
--   insider_trades  — Dedicated table for SEC Form 4 insider trading data
--   profiles        — User authentication / tier tracking
--
-- api_cache categories:
--   "13f"              — ~100 superinvestor fund data (holdings, changes, quarterly history)
--   "glassdoor"        — Company culture ratings per ticker
--   "glassdoor_quota"  — Monthly Glassdoor API call counter

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


-- ═══════════════════════════════════════════════════════════════════════
-- PaperPanda Supabase Schema: insider_trades (SEC Form 4 Data)
-- ═══════════════════════════════════════════════════════════════════════
--
-- Dedicated table for insider trading data scraped from OpenInsider.
-- Populated by the insider_sync cron worker (filings-insider-sync).
-- The web server reads from this table instead of scraping on-demand.
--
-- Each row is a single SEC Form 4 filing transaction, deduplicated
-- by sec_url (the unique SEC filing URL).
--
-- Numeric columns (price, qty, value, etc.) enable SQL-level
-- aggregation.  The *_fmt columns preserve the original formatted
-- strings for display.

-- 9. Create the insider_trades table
CREATE TABLE IF NOT EXISTS insider_trades (
    id              BIGSERIAL PRIMARY KEY,
    sec_url         TEXT NOT NULL UNIQUE,           -- dedup key (SEC Form 4 filing URL)
    filing_date     DATE NOT NULL,                  -- when the Form 4 was filed with SEC
    trade_date      DATE NOT NULL,                  -- when the trade actually occurred
    ticker          TEXT NOT NULL,                   -- e.g. "AAPL"
    company_name    TEXT NOT NULL DEFAULT '',        -- e.g. "Apple Inc"
    insider_name    TEXT NOT NULL,                   -- e.g. "Tim Cook"
    title           TEXT NOT NULL DEFAULT '',        -- e.g. "CEO", "CFO", "Director", "10%"
    trade_type      TEXT NOT NULL,                   -- "Purchase", "Sale", "Sale+OE"
    price           NUMERIC(12,4),                   -- parsed numeric price (150.5000)
    qty             INTEGER,                         -- parsed signed quantity (+75000 or -3752)
    owned           BIGINT,                          -- shares owned after trade
    delta_own_pct   NUMERIC(8,4),                    -- ownership change as percentage (13.0, -4.0)
    value           NUMERIC(16,2),                   -- parsed signed dollar value (+150000.00)
    price_fmt       TEXT NOT NULL DEFAULT '',        -- "$150.50" (display string)
    qty_fmt         TEXT NOT NULL DEFAULT '',        -- "+75,000" (display string)
    owned_fmt       TEXT NOT NULL DEFAULT '',        -- "1,500,000" (display string)
    delta_own_fmt   TEXT NOT NULL DEFAULT '',        -- "+13%" (display string)
    value_fmt       TEXT NOT NULL DEFAULT '',        -- "+$150,000" (display string)
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 10. Indexes for insider_trades
CREATE INDEX IF NOT EXISTS idx_insider_trades_ticker
    ON insider_trades (ticker, trade_date DESC);

CREATE INDEX IF NOT EXISTS idx_insider_trades_trade_date
    ON insider_trades (trade_date DESC);

CREATE INDEX IF NOT EXISTS idx_insider_trades_type_date
    ON insider_trades (trade_type, trade_date DESC);

CREATE INDEX IF NOT EXISTS idx_insider_trades_filing_date
    ON insider_trades (filing_date DESC);

-- 11. Enable Row Level Security on insider_trades
ALTER TABLE insider_trades ENABLE ROW LEVEL SECURITY;

-- 12. RLS Policies for insider_trades
CREATE POLICY "Allow authenticated read access on insider_trades"
    ON insider_trades
    FOR SELECT
    TO authenticated
    USING (true);

-- Service role (Python backend / sync worker) has full access via bypass.
