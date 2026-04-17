CREATE TABLE IF NOT EXISTS analyst_estimates (
    id               BIGSERIAL PRIMARY KEY,
    ticker           TEXT NOT NULL,
    estimate_type    TEXT NOT NULL,
    period_key       TEXT NOT NULL,
    period_label     TEXT NOT NULL,
    num_analysts     INT,
    avg_estimate     NUMERIC(18,4),
    low_estimate     NUMERIC(18,4),
    high_estimate    NUMERIC(18,4),
    year_ago_value   NUMERIC(18,4),
    growth_pct       NUMERIC(10,4),
    fetched_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(ticker, estimate_type, period_key)
);
CREATE INDEX IF NOT EXISTS idx_analyst_estimates_ticker
    ON analyst_estimates (ticker);
