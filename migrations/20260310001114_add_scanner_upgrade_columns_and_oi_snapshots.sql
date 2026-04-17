-- ── Phase 1: Scanner upgrade columns on unusual_options_activity ──────────
-- OI delta tracking (Phase 1B)
ALTER TABLE unusual_options_activity ADD COLUMN IF NOT EXISTS oi_prev          INTEGER;
ALTER TABLE unusual_options_activity ADD COLUMN IF NOT EXISTS oi_delta         INTEGER;
ALTER TABLE unusual_options_activity ADD COLUMN IF NOT EXISTS oi_delta_pct     NUMERIC(8,1);
ALTER TABLE unusual_options_activity ADD COLUMN IF NOT EXISTS is_new_positioning BOOLEAN DEFAULT FALSE;

-- Urgency score (Phase 1C)
ALTER TABLE unusual_options_activity ADD COLUMN IF NOT EXISTS urgency_score    NUMERIC(4,2) DEFAULT 1.0;

-- Moneyness / OTM bias (Phase 1D)
ALTER TABLE unusual_options_activity ADD COLUMN IF NOT EXISTS moneyness        NUMERIC(8,4);
ALTER TABLE unusual_options_activity ADD COLUMN IF NOT EXISTS moneyness_label  TEXT NOT NULL DEFAULT '';
ALTER TABLE unusual_options_activity ADD COLUMN IF NOT EXISTS otm_score        NUMERIC(4,2) DEFAULT 1.0;

-- Greeks (Phase 3 — Tradier integration)
ALTER TABLE unusual_options_activity ADD COLUMN IF NOT EXISTS delta            NUMERIC(8,6);
ALTER TABLE unusual_options_activity ADD COLUMN IF NOT EXISTS gamma            NUMERIC(8,6);
ALTER TABLE unusual_options_activity ADD COLUMN IF NOT EXISTS theta            NUMERIC(8,6);
ALTER TABLE unusual_options_activity ADD COLUMN IF NOT EXISTS vega             NUMERIC(8,6);

-- ── OI snapshots: daily open interest for delta computation ──────────────
CREATE TABLE IF NOT EXISTS options_oi_snapshots (
    id                BIGSERIAL PRIMARY KEY,
    contract_symbol   TEXT NOT NULL,
    scan_date         DATE NOT NULL DEFAULT CURRENT_DATE,
    open_interest     INTEGER NOT NULL DEFAULT 0,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(contract_symbol, scan_date)
);
CREATE INDEX IF NOT EXISTS idx_oi_snap_symbol_date
    ON options_oi_snapshots (contract_symbol, scan_date DESC);
