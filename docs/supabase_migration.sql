-- PaperPanda Supabase Schema
-- Run this in your Supabase SQL Editor (Dashboard > SQL Editor > New Query)
-- NOTE: Most tables are auto-applied on first deploy via _auto_migrate() in supabase_cache.py
--
-- Tables:
--   api_cache                  — Universal persistent cache (L2) for API data
--   sync_logs                  — Cron worker run tracking and error logging
--   insider_trades             — Dedicated table for SEC Form 4 insider trading data (hot, 30-day)
--   insider_trades_history     — Cold archive of ALL insider trades (delete/update-protected, permanent)
--   insider_purchases_history  — Cold archive of insider buys only (delete-protected, write-once returns)
--   youtube_events             — YouTube calendar: upcoming livestreams + recent uploads
--   youtube_channels           — YouTube channel metadata (subscribers, views, frequency)
--   supporters                 — Stripe donation / subscription tracking (Panda Fund)
--   notifications              — Server-generated alerts (13F, YouTube, Reddit, Congress)
--   congress_trades            — STOCK Act disclosures from Capitol Trades (cold, write-once)
--   congress_members           — Politician profiles derived from trade data
--   congress_trades_prices     — Forward-return enrichment for Congress trades
--   profiles                   — User authentication / tier tracking
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
-- Hot table: populated by insider_sync cron worker (filings-insider-sync) every 30 min.
-- 30-day retention window — older trades pruned by run_retention_cleanup().
-- Each row is a single SEC Form 4 filing transaction, deduplicated by sec_url.
-- All trades are also bridged to insider_trades_history (permanent cold archive).

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
-- 7. supporters — Panda Fund donation tracking
-- ═══════════════════════════════════════════════════════════════════════
--
-- One row per Stripe payment/subscription event. The web layer aggregates
-- these to compute the monthly total shown on /support.

CREATE TABLE IF NOT EXISTS supporters (
    id              BIGSERIAL PRIMARY KEY,
    stripe_event_id TEXT NOT NULL UNIQUE,        -- idempotency key (Stripe event.id)
    session_id      TEXT NOT NULL DEFAULT '',     -- Stripe checkout session ID
    customer_email  TEXT NOT NULL DEFAULT '',
    amount_cents    INTEGER NOT NULL DEFAULT 0,
    currency        TEXT NOT NULL DEFAULT 'usd',
    mode            TEXT NOT NULL DEFAULT 'payment',  -- 'payment' | 'subscription'
    month           TEXT NOT NULL,                -- 'YYYY-MM' for easy grouping
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_supporters_month
    ON supporters (month DESC);
CREATE INDEX IF NOT EXISTS idx_supporters_event
    ON supporters (stripe_event_id);


-- ═══════════════════════════════════════════════════════════════════════
-- 8. notifications — Server-generated alerts
-- ═══════════════════════════════════════════════════════════════════════
--
-- Global, server-generated notifications for 13F changes, YouTube uploads,
-- Reddit velocity spikes, and Congress trades. 48-hour retention with
-- background cleanup.

CREATE TABLE IF NOT EXISTS notifications (
    id              TEXT PRIMARY KEY,
    type            TEXT NOT NULL,                -- "13f", "youtube", "reddit", "congress", "insider_trade"
    title           TEXT NOT NULL,
    message         TEXT NOT NULL DEFAULT '',
    icon            TEXT NOT NULL DEFAULT '',
    toast_type      TEXT NOT NULL DEFAULT 'alert',
    link            TEXT NOT NULL DEFAULT '',
    metadata        JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_notifications_created
    ON notifications (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_notifications_type_created
    ON notifications (type, created_at DESC);


-- ═══════════════════════════════════════════════════════════════════════
-- 9. insider_purchases_history — Cold archive of insider buys
-- ═══════════════════════════════════════════════════════════════════════
--
-- Write-once cold archive of insider purchase transactions with forward
-- returns. Protected by DELETE and write-once UPDATE triggers.
-- See sql/001_insider_purchases_history.sql for triggers and migration.

CREATE TABLE IF NOT EXISTS insider_purchases_history (
    id              BIGSERIAL PRIMARY KEY,
    sec_url         TEXT NOT NULL UNIQUE,
    filing_date     DATE NOT NULL,
    trade_date      DATE NOT NULL,
    ticker          TEXT NOT NULL,
    company_name    TEXT NOT NULL DEFAULT '',
    insider_name    TEXT NOT NULL,
    title           TEXT NOT NULL DEFAULT '',
    price           NUMERIC(12,4),
    qty             INTEGER,
    value           NUMERIC(16,2),
    close_on_trade  NUMERIC(12,4),
    close_at_30d    NUMERIC(12,4),
    close_at_90d    NUMERIC(12,4),
    close_at_180d   NUMERIC(12,4),
    close_at_365d   NUMERIC(12,4),
    returns_updated TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_iph_ticker_trade_date
    ON insider_purchases_history (ticker, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_iph_pending_returns
    ON insider_purchases_history (ticker, trade_date)
    WHERE returns_updated IS NULL;


-- ═══════════════════════════════════════════════════════════════════════
-- 10. insider_trades_history — Cold archive of ALL insider trades
-- ═══════════════════════════════════════════════════════════════════════
--
-- Permanent write-once archive of all insider trades (buys + sells).
-- Bridged from the hot insider_trades table by the insider_sync cron worker.
-- Protected by DELETE and UPDATE triggers. ON CONFLICT DO NOTHING on insert.
-- Used for date-filtered queries beyond the 30-day hot table window.

CREATE TABLE IF NOT EXISTS insider_trades_history (
    id              BIGSERIAL PRIMARY KEY,
    sec_url         TEXT NOT NULL UNIQUE,
    filing_date     DATE NOT NULL,
    trade_date      DATE NOT NULL,
    ticker          TEXT NOT NULL,
    company_name    TEXT NOT NULL DEFAULT '',
    insider_name    TEXT NOT NULL,
    title           TEXT NOT NULL DEFAULT '',
    trade_type      TEXT NOT NULL DEFAULT '',     -- "Purchase", "Sale", "Sale+OE"
    price           NUMERIC(12,4),
    qty             INTEGER,
    value           NUMERIC(16,2),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ith_ticker_trade_date
    ON insider_trades_history (ticker, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_ith_filing_date
    ON insider_trades_history (filing_date DESC);
CREATE INDEX IF NOT EXISTS idx_ith_trade_type_filing
    ON insider_trades_history (trade_type, filing_date DESC);

-- Delete protection trigger
CREATE OR REPLACE FUNCTION prevent_insider_history_delete()
RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'DELETE on insider_trades_history is not allowed (cold archive)';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_no_delete_insider_history ON insider_trades_history;
CREATE TRIGGER trg_no_delete_insider_history
    BEFORE DELETE ON insider_trades_history
    FOR EACH ROW EXECUTE FUNCTION prevent_insider_history_delete();

-- Update protection trigger
CREATE OR REPLACE FUNCTION prevent_insider_history_update()
RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'UPDATE on insider_trades_history is not allowed (write-once archive)';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_no_update_insider_history ON insider_trades_history;
CREATE TRIGGER trg_no_update_insider_history
    BEFORE UPDATE ON insider_trades_history
    FOR EACH ROW EXECUTE FUNCTION prevent_insider_history_update();


-- ═══════════════════════════════════════════════════════════════════════
-- 11. congress_trades — STOCK Act disclosures (cold, write-once)
-- ═══════════════════════════════════════════════════════════════════════
--
-- Scraped from Capitol Trades (~35K trades, 200+ politicians).
-- Write-once cold archive: ON CONFLICT DO NOTHING on insert.
-- Protected by DELETE and UPDATE triggers (see sql/002_congress_cold_table_protection.sql).

CREATE TABLE IF NOT EXISTS congress_trades (
    trade_id        TEXT PRIMARY KEY,
    member_id       TEXT NOT NULL,
    politician_name TEXT NOT NULL,
    party           TEXT NOT NULL DEFAULT '',
    chamber         TEXT NOT NULL DEFAULT '',     -- "Senate" or "House"
    state           TEXT NOT NULL DEFAULT '',
    ticker          TEXT,
    asset_name      TEXT NOT NULL DEFAULT '',
    trade_type      TEXT NOT NULL DEFAULT '',     -- "buy", "sell", "exchange"
    trade_date      DATE,
    filing_date     DATE,
    amount_low      NUMERIC(16,2),
    amount_high     NUMERIC(16,2),
    amount_display  TEXT NOT NULL DEFAULT '',
    owner           TEXT NOT NULL DEFAULT '',     -- "Self", "Spouse", "Child", "Joint"
    cap_trades_url  TEXT NOT NULL DEFAULT '',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ct_member_id
    ON congress_trades (member_id, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_ct_ticker
    ON congress_trades (ticker, trade_date DESC) WHERE ticker IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_ct_trade_date
    ON congress_trades (trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_ct_filing_date
    ON congress_trades (filing_date DESC);


-- ═══════════════════════════════════════════════════════════════════════
-- 12. congress_members — Politician profiles
-- ═══════════════════════════════════════════════════════════════════════
--
-- Derived from Capitol Trades data. Updatable (metadata refresh on re-scrapes),
-- but DELETE-protected (see sql/002_congress_cold_table_protection.sql).

CREATE TABLE IF NOT EXISTS congress_members (
    member_id        TEXT PRIMARY KEY,
    full_name        TEXT NOT NULL,
    first_name       TEXT NOT NULL DEFAULT '',
    last_name        TEXT NOT NULL DEFAULT '',
    party            TEXT NOT NULL DEFAULT '',
    chamber          TEXT NOT NULL DEFAULT '',
    state            TEXT NOT NULL DEFAULT '',
    state_abbr       TEXT NOT NULL DEFAULT '',
    district         TEXT NOT NULL DEFAULT '',
    is_current       BOOLEAN NOT NULL DEFAULT TRUE,
    first_trade_date DATE,
    last_trade_date  DATE,
    total_trades     INTEGER NOT NULL DEFAULT 0,
    net_worth_estimate BIGINT,
    net_worth_source TEXT NOT NULL DEFAULT '',
    net_worth_year   INT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_cm_total_trades
    ON congress_members (total_trades DESC);


-- ═══════════════════════════════════════════════════════════════════════
-- 13. congress_trades_prices — Forward-return enrichment
-- ═══════════════════════════════════════════════════════════════════════
--
-- Separate join table for price data because congress_trades has UPDATE
-- triggers that block all modifications. See sql/003_congress_trades_prices.sql.

CREATE TABLE IF NOT EXISTS congress_trades_prices (
    trade_id       TEXT PRIMARY KEY REFERENCES congress_trades(trade_id),
    ticker         TEXT,
    trade_date     DATE,
    close_on_trade NUMERIC(12, 4),
    close_at_30d   NUMERIC(12, 4),
    close_at_90d   NUMERIC(12, 4),
    close_at_180d  NUMERIC(12, 4),
    close_at_365d  NUMERIC(12, 4),
    return_30d     NUMERIC(8, 4),
    return_90d     NUMERIC(8, 4),
    return_180d    NUMERIC(8, 4),
    return_365d    NUMERIC(8, 4),
    prices_updated TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ctp_trade_id ON congress_trades_prices (trade_id);
CREATE INDEX IF NOT EXISTS idx_ctp_ticker ON congress_trades_prices (ticker, trade_date DESC);


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
