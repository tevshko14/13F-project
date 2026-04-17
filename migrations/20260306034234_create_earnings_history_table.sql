CREATE TABLE IF NOT EXISTS earnings_history (
    id                   BIGSERIAL PRIMARY KEY,
    ticker               TEXT NOT NULL,
    report_date          DATE NOT NULL,
    fiscal_quarter       TEXT DEFAULT '',
    eps_estimate         NUMERIC(10,4),
    eps_actual           NUMERIC(10,4),
    eps_surprise_pct     NUMERIC(8,4),
    revenue_estimate     BIGINT,
    revenue_actual       BIGINT,
    revenue_surprise_pct NUMERIC(8,4),
    beat_eps             BOOLEAN,
    beat_revenue         BOOLEAN,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(ticker, report_date)
);
CREATE INDEX IF NOT EXISTS idx_earnings_ticker_date
    ON earnings_history (ticker, report_date DESC);

ALTER TABLE earnings_history ENABLE ROW LEVEL SECURITY;
CREATE POLICY "earnings_history_public_read" ON earnings_history FOR SELECT USING (true);
CREATE POLICY "earnings_history_service_write" ON earnings_history FOR ALL USING (true);
