-- ================================================================
-- Congress Trades Indexes — Performance Optimization
-- ================================================================
--
-- The congress_trades table (~35K rows) is queried by several access
-- patterns in the web app.  Without indexes, every query requires a
-- full sequential scan.  These indexes cover all hot query paths.
--
-- Run this ONCE in the Supabase SQL Editor.
-- Safe to re-run (CREATE INDEX IF NOT EXISTS).
-- ================================================================

-- ── Per-politician lookups (politician profile page) ──
-- Used by: get_congress_trades_by_member()
CREATE INDEX IF NOT EXISTS idx_ct_member_id
    ON congress_trades (member_id, trade_date DESC);

-- ── Per-ticker lookups (stock page Congress subtab) ──
-- Used by: get_congress_trades_by_ticker()
-- Partial index: only rows with a non-null ticker
CREATE INDEX IF NOT EXISTS idx_ct_ticker
    ON congress_trades (ticker, trade_date DESC)
    WHERE ticker IS NOT NULL;

-- ── Recent trades by trade_date (trending / momentum calculations) ──
-- Used by: get_congress_trades_recent_months()
CREATE INDEX IF NOT EXISTS idx_ct_trade_date
    ON congress_trades (trade_date DESC);

-- ── Recent filings by filing_date (activity feed) ──
-- Used by: get_congress_recent_trades()
CREATE INDEX IF NOT EXISTS idx_ct_filing_date
    ON congress_trades (filing_date DESC);

-- ── Congress members by activity (listing page sort) ──
-- Used by: get_all_congress_members() ORDER BY total_trades DESC
CREATE INDEX IF NOT EXISTS idx_cm_total_trades
    ON congress_members (total_trades DESC);

-- ── Verify ──
SELECT indexname, tablename
  FROM pg_indexes
 WHERE tablename IN ('congress_trades', 'congress_members')
 ORDER BY tablename, indexname;
