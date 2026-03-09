-- Unusual Options Activity screener table
-- Stores contracts where daily volume >= 5x open interest,
-- classified by sentiment (bullish calls / bearish puts).
-- 7-day rolling retention; synced every 30 min during market hours.

CREATE TABLE IF NOT EXISTS unusual_options_activity (
    id                BIGSERIAL PRIMARY KEY,
    contract_symbol   TEXT NOT NULL,
    ticker            TEXT NOT NULL,
    company_name      TEXT NOT NULL DEFAULT '',
    sector            TEXT NOT NULL DEFAULT '',

    option_type       TEXT NOT NULL,
    strike            NUMERIC(12,2) NOT NULL,
    expiry            DATE NOT NULL,
    dte               INTEGER NOT NULL DEFAULT 0,

    volume            INTEGER NOT NULL DEFAULT 0,
    open_interest     INTEGER NOT NULL DEFAULT 0,
    vol_oi_ratio      NUMERIC(10,2) NOT NULL DEFAULT 0,

    bid               NUMERIC(10,4),
    ask               NUMERIC(10,4),
    last_price        NUMERIC(10,4),
    implied_vol       NUMERIC(8,6),
    underlying_price  NUMERIC(12,4),

    premium_est       NUMERIC(16,2),
    sentiment         TEXT NOT NULL DEFAULT 'neutral',

    scan_date         DATE NOT NULL DEFAULT CURRENT_DATE,
    fetched_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE(contract_symbol, scan_date)
);

CREATE INDEX IF NOT EXISTS idx_uoa_fetched_at
    ON unusual_options_activity (fetched_at DESC);
CREATE INDEX IF NOT EXISTS idx_uoa_ticker_fetched
    ON unusual_options_activity (ticker, fetched_at DESC);
CREATE INDEX IF NOT EXISTS idx_uoa_sector_fetched
    ON unusual_options_activity (sector, fetched_at DESC);
CREATE INDEX IF NOT EXISTS idx_uoa_sentiment
    ON unusual_options_activity (sentiment, fetched_at DESC);
CREATE INDEX IF NOT EXISTS idx_uoa_premium_desc
    ON unusual_options_activity (premium_est DESC NULLS LAST)
    WHERE premium_est > 0;
CREATE INDEX IF NOT EXISTS idx_uoa_ratio_desc
    ON unusual_options_activity (vol_oi_ratio DESC)
    WHERE vol_oi_ratio >= 5;
