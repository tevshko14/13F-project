-- Fix: get each ticker's most-recent row (DISTINCT ON ticker),
-- not all tickers for a single latest date.
-- This ensures we get ~1581 tickers (629 + 952) instead of just 629,
-- because FINRA data lands on different dates for NYSE vs NASDAQ.
CREATE OR REPLACE FUNCTION public.get_latest_short_interest(row_limit integer DEFAULT 1600)
RETURNS SETOF short_interest_history
LANGUAGE sql
STABLE
AS $function$
  SELECT * FROM (
    SELECT DISTINCT ON (ticker) *
    FROM short_interest_history
    ORDER BY ticker, report_date DESC NULLS LAST
  ) latest
  ORDER BY short_pct_float DESC NULLS LAST
  LIMIT row_limit;
$function$;
