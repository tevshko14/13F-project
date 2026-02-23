-- PaperPanda Supabase Schema
-- Run this in your Supabase SQL Editor (Dashboard > SQL Editor > New Query)
-- NOTE: Most tables are auto-applied on first deploy via _auto_migrate() in supabase_cache.py
--
-- Tables:
--   api_cache        — Universal persistent cache (L2) for API data
--   sync_logs        — Cron worker run tracking and error logging
--   insider_trades   — Dedicated table for SEC Form 4 insider trading data
--   youtube_events   — YouTube calendar: upcoming livestreams + recent uploads
--   youtube_channels — YouTube channel metadata (subscribers, views, frequency)
--   profiles         — User authentication / tier tracking
--
-- Supabase Storage:
--   paperpanda-archive  — Private bucket for cold storage (archived 13F quarters)
--
-- api_cache categories:
--   "13f"              — ~84 superinvestor fund data (holdings, changes, quarterly history)
--   "glassdoor"        — Company culture ratings per ticker
--   "glassdoor_quota"  — Monthly Glassdoor API call counter
--   "pdl"              — People Data Labs employee data per ticker
--   "pdl_quota"        — Monthly PDL API call counter
--   "appstore"         — Apple App Store ratings per ticker

-- ═══════════════════════════════════════════════════════════════════════
-- 1. api_cache — Universal persistent cache
-- ═══════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS api_cache (
    cache_key     TEXT PRIMARY KEY,           -- e.g. "13f:1067983", "glassdoor:AAPL"
    category      TEXT NOT NULL,              -- e.g. "13f", "glassdoor", "pdl", "appstore"
    response_data JSONB NOT NULL,             -- the cached payload
    expires_at    TIMESTAMPTZ,                -- NULL = never expires
    ttl_seconds   INTEGER,                    -- original TTL for reference
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Sync worker tracking columns
    last_synced_at TIMESTAMPTZ,               -- when the sync worker last refreshed this key
    sync_status   TEXT DEFAULT 'pending',     -- "pending", "success", "failed"
    -- Content-hash for change detection (avoids re-uploading identical data)
    content_hash  TEXT DEFAULT ''              -- SHA-256 hash prefix for delta detection
);

CREATE INDEX IF NOT EXISTS idx_api_cache_category
    ON api_cache (category);

CREATE INDEX IF NOT EXISTS idx_api_cache_expires_at
    ON api_cache (expires_at)
    WHERE expires_at IS NOT NULL;

-- RLS
ALTER TABLE api_cache ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Allow authenticated read access"
    ON api_cache FOR SELECT TO authenticated USING (true);


-- ═══════════════════════════════════════════════════════════════════════
-- 2. sync_logs — Cron worker run tracking
-- ═══════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS sync_logs (
    run_id          TEXT PRIMARY KEY,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ,
    funds_updated   INTEGER DEFAULT 0,
    funds_failed    INTEGER DEFAULT 0,
    funds_skipped   INTEGER DEFAULT 0,
    error_messages  JSONB DEFAULT '[]'::jsonb
);


-- ═══════════════════════════════════════════════════════════════════════
-- 3. insider_trades — SEC Form 4 insider trading data
-- ═══════════════════════════════════════════════════════════════════════
--
-- Populated by insider_sync cron worker (filings-insider-sync) every 30 min.
-- Each row is a single SEC Form 4 filing transaction, deduplicated by sec_url.

CREATE TABLE IF NOT EXISTS insider_trades (
    id              BIGSERIAL PRIMARY KEY,
    sec_url         TEXT NOT NULL UNIQUE,           -- dedup key (SEC Form 4 filing URL)
    filing_date     DATE NOT NULL,
    trade_date      DATE NOT NULL,
    ticker          TEXT NOT NULL,
    company_name    TEXT NOT NULL DEFAULT '',
    insider_name    TEXT NOT NULL,
    title           TEXT NOT NULL DEFAULT '',
    trade_type      TEXT NOT NULL,                   -- "Purchase", "Sale", "Sale+OE"
    price           NUMERIC(12,4),
    qty             INTEGER,
    owned           BIGINT,
    delta_own_pct   NUMERIC(8,4),
    value           NUMERIC(16,2),
    price_fmt       TEXT NOT NULL DEFAULT '',
    qty_fmt         TEXT NOT NULL DEFAULT '',
    owned_fmt       TEXT NOT NULL DEFAULT '',
    delta_own_fmt   TEXT NOT NULL DEFAULT '',
    value_fmt       TEXT NOT NULL DEFAULT '',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_insider_trades_ticker
    ON insider_trades (ticker, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_insider_trades_trade_date
    ON insider_trades (trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_insider_trades_type_date
    ON insider_trades (trade_type, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_insider_trades_filing_date
    ON insider_trades (filing_date DESC);

ALTER TABLE insider_trades ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Allow authenticated read access on insider_trades"
    ON insider_trades FOR SELECT TO authenticated USING (true);


-- ═══════════════════════════════════════════════════════════════════════
-- 4. youtube_events — YouTube calendar data
-- ═══════════════════════════════════════════════════════════════════════
--
-- Populated by youtube_sync cron worker (filings-youtube-sync) every 6 hours.
-- Stores upcoming livestreams and recent uploads from 11 finance channels.

CREATE TABLE IF NOT EXISTS youtube_events (
    id                BIGSERIAL PRIMARY KEY,
    video_id          TEXT NOT NULL UNIQUE,           -- YouTube video ID (dedup key)
    channel_id        TEXT NOT NULL,
    channel_name      TEXT NOT NULL DEFAULT '',
    title             TEXT NOT NULL DEFAULT '',
    scheduled_at      TIMESTAMPTZ,                    -- for upcoming livestreams
    event_type        TEXT NOT NULL DEFAULT 'upcoming', -- "upcoming", "upload"
    sentiment         TEXT NOT NULL DEFAULT 'neutral',
    tickers           JSONB NOT NULL DEFAULT '[]'::jsonb,
    impact_score      SMALLINT NOT NULL DEFAULT 0,    -- 1-10 retail impact score
    subscriber_count  BIGINT DEFAULT 0,
    avg_views         BIGINT DEFAULT 0,
    frequency_alert   BOOLEAN NOT NULL DEFAULT FALSE,
    frequency_detail  TEXT NOT NULL DEFAULT '',
    thumbnail_url     TEXT NOT NULL DEFAULT '',
    video_url         TEXT NOT NULL DEFAULT '',
    duration          TEXT NOT NULL DEFAULT '',        -- formatted: "1:02:30" or "12:45"
    content_type      TEXT NOT NULL DEFAULT 'video',   -- "video", "live", "upcoming", "was_live"
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_youtube_events_scheduled
    ON youtube_events (scheduled_at DESC);
CREATE INDEX IF NOT EXISTS idx_youtube_events_channel
    ON youtube_events (channel_id, scheduled_at DESC);
CREATE INDEX IF NOT EXISTS idx_youtube_events_impact
    ON youtube_events (impact_score DESC) WHERE impact_score >= 9;
CREATE INDEX IF NOT EXISTS idx_youtube_events_sentiment
    ON youtube_events (sentiment, scheduled_at DESC);
CREATE INDEX IF NOT EXISTS idx_youtube_events_type_scheduled
    ON youtube_events (event_type, scheduled_at DESC);


-- ═══════════════════════════════════════════════════════════════════════
-- 5. youtube_channels — Channel metadata cache
-- ═══════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS youtube_channels (
    channel_id          TEXT PRIMARY KEY,
    channel_name        TEXT NOT NULL DEFAULT '',
    handle              TEXT NOT NULL DEFAULT '',
    subscriber_count    BIGINT DEFAULT 0,
    avg_views_30d       BIGINT DEFAULT 0,
    avg_posts_per_week  NUMERIC(5,2) DEFAULT 0,
    last_polled_at      TIMESTAMPTZ,
    thumbnail_url       TEXT NOT NULL DEFAULT '',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);


-- ═══════════════════════════════════════════════════════════════════════
-- 6. profiles — User authentication
-- ═══════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS profiles (
    id           UUID PRIMARY KEY REFERENCES auth.users(id) ON DELETE CASCADE,
    email        TEXT,
    display_name TEXT,
    tier         TEXT NOT NULL DEFAULT 'free',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE profiles ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Users can read own profile"
    ON profiles FOR SELECT TO authenticated USING (auth.uid() = id);

CREATE POLICY "Service role full access on profiles"
    ON profiles FOR ALL TO service_role USING (true);

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
-- Supabase Storage: Cold Storage Bucket
-- ═══════════════════════════════════════════════════════════════════════
--
-- Create the paperpanda-archive bucket in Supabase Dashboard:
--   Dashboard > Storage > New Bucket > "paperpanda-archive" > Private
--
-- Used by cold_storage.py to archive older 13F quarterly data as JSON files.
-- Bucket structure: paperpanda-archive/13f/{cik}/quarterly/{period}.json
-- Auto-created by ensure_bucket() if permissions allow, otherwise create manually.
