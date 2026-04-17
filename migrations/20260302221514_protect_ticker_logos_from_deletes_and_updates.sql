-- Drop the overly permissive service_role policy
DROP POLICY IF EXISTS "service_role_all" ON ticker_logos;

-- Service role can only SELECT and INSERT (never UPDATE or DELETE)
CREATE POLICY "service_role_read" ON ticker_logos
    FOR SELECT TO service_role USING (true);

CREATE POLICY "service_role_insert" ON ticker_logos
    FOR INSERT TO service_role WITH CHECK (true);

-- Explicitly deny UPDATE and DELETE by not creating policies for them.
-- With RLS enabled + no matching policy, UPDATE/DELETE will be rejected.
