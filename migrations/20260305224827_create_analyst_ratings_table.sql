-- Analyst ratings table: persists firm-level upgrade/downgrade history per ticker
CREATE TABLE IF NOT EXISTS public.analyst_ratings (
    id             bigserial PRIMARY KEY,
    ticker         text          NOT NULL,
    firm           text          NOT NULL,
    action         text,
    from_grade     text,
    to_grade       text,
    grade_date     date          NOT NULL,
    current_price_target  numeric(10, 2),
    prior_price_target    numeric(10, 2),
    price_target_action   text,
    source         text          NOT NULL DEFAULT 'yfinance',
    fetched_at     timestamptz   NOT NULL DEFAULT now(),
    created_at     timestamptz   NOT NULL DEFAULT now(),
    CONSTRAINT analyst_ratings_unique UNIQUE (ticker, firm, grade_date)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_analyst_ratings_ticker_date
    ON public.analyst_ratings (ticker, grade_date DESC);

CREATE INDEX IF NOT EXISTS idx_analyst_ratings_firm
    ON public.analyst_ratings (firm);

-- RLS
ALTER TABLE public.analyst_ratings ENABLE ROW LEVEL SECURITY;

CREATE POLICY "analyst_ratings_public_read"
    ON public.analyst_ratings FOR SELECT
    USING (true);
