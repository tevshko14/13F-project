-- Individual analyst profiles (from TipRanks)
CREATE TABLE IF NOT EXISTS public.analyst_profiles (
    analyst_id   text PRIMARY KEY,
    name         text NOT NULL,
    firm         text NOT NULL,
    photo_b64    text,
    content_type text NOT NULL DEFAULT 'image/jpeg',
    star_rating  numeric(3,1),
    success_rate numeric(5,2),
    total_ratings int,
    tipranks_rank int,
    fetched_at   timestamptz,
    created_at   timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE public.analyst_profiles ENABLE ROW LEVEL SECURITY;
CREATE POLICY "analyst_profiles_public_read"
    ON public.analyst_profiles FOR SELECT USING (true);

-- Per-analyst, per-ticker ratings (from TipRanks — more granular than analyst_ratings)
CREATE TABLE IF NOT EXISTS public.analyst_stock_ratings (
    id                 bigserial PRIMARY KEY,
    ticker             text NOT NULL,
    analyst_id         text NOT NULL REFERENCES public.analyst_profiles(analyst_id),
    firm               text NOT NULL,
    analyst_name       text NOT NULL,
    action             text,
    to_grade           text,
    from_grade         text,
    price_target       numeric(10,2),
    prior_price_target numeric(10,2),
    grade_date         date NOT NULL,
    fetched_at         timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT analyst_stock_ratings_unique UNIQUE (ticker, analyst_id, grade_date)
);
CREATE INDEX IF NOT EXISTS idx_asr_ticker_date
    ON public.analyst_stock_ratings (ticker, grade_date DESC);
CREATE INDEX IF NOT EXISTS idx_asr_analyst
    ON public.analyst_stock_ratings (analyst_id);
ALTER TABLE public.analyst_stock_ratings ENABLE ROW LEVEL SECURITY;
CREATE POLICY "analyst_stock_ratings_public_read"
    ON public.analyst_stock_ratings FOR SELECT USING (true);
