-- ================================================================
-- Congress Cold Table Protection Triggers
-- ================================================================
--
-- Both `congress_trades` and `congress_members` are COLD archival
-- tables — they hold scraped historical data from Capitol Trades
-- that is expensive/slow to re-acquire (~35K trades across 350+
-- paginated pages).
--
-- This migration adds database-level guardrails so the data cannot
-- be accidentally deleted or overwritten by application bugs,
-- manual SQL, or PostgREST API calls.
--
-- Run this ONCE in the Supabase SQL Editor after the tables exist.
-- It is safe to re-run (CREATE OR REPLACE / DROP IF EXISTS).
-- ================================================================


-- ── 1. congress_trades — Prevent DELETE ──────────────────────────
-- This is a write-once cold archive.  Rows must never be deleted.

CREATE OR REPLACE FUNCTION prevent_congress_trades_delete()
RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION
        'DELETE on congress_trades is prohibited — this is a write-once cold archive. '
        'trade_id = %', OLD.trade_id;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_no_delete_congress_trades ON congress_trades;
CREATE TRIGGER trg_no_delete_congress_trades
    BEFORE DELETE ON congress_trades
    FOR EACH ROW EXECUTE FUNCTION prevent_congress_trades_delete();


-- ── 2. congress_trades — Prevent UPDATE ─────────────────────────
-- Trade rows are immutable once inserted.  No field should ever
-- change — the data reflects a point-in-time STOCK Act disclosure.

CREATE OR REPLACE FUNCTION prevent_congress_trades_update()
RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION
        'UPDATE on congress_trades is prohibited — rows are immutable. '
        'trade_id = %', OLD.trade_id;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_no_update_congress_trades ON congress_trades;
CREATE TRIGGER trg_no_update_congress_trades
    BEFORE UPDATE ON congress_trades
    FOR EACH ROW EXECUTE FUNCTION prevent_congress_trades_update();


-- ── 3. congress_members — Prevent DELETE ────────────────────────
-- Member profiles are derived from trade data and should never
-- be deleted.  They CAN be updated (to refresh last_trade_date,
-- total_trades, etc. on re-scrapes).

CREATE OR REPLACE FUNCTION prevent_congress_members_delete()
RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION
        'DELETE on congress_members is prohibited — this is a cold archive. '
        'member_id = %', OLD.member_id;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_no_delete_congress_members ON congress_members;
CREATE TRIGGER trg_no_delete_congress_members
    BEFORE DELETE ON congress_members
    FOR EACH ROW EXECUTE FUNCTION prevent_congress_members_delete();


-- ── 4. Verify protection is active ──────────────────────────────
-- Quick sanity check — these should show the triggers we just created.

SELECT tgname, tgrelid::regclass, tgenabled
FROM pg_trigger
WHERE tgname LIKE 'trg_%congress%'
ORDER BY tgrelid::regclass, tgname;
