-- Enable RLS
ALTER TABLE public.profiles ENABLE ROW LEVEL SECURITY;

-- Users can read their own profile
CREATE POLICY "Users read own profile" ON public.profiles
    FOR SELECT USING (auth.jwt()->>'sub' = id);

-- Users can update their own profile (display_name, avatar_url)
CREATE POLICY "Users update own profile" ON public.profiles
    FOR UPDATE USING (auth.jwt()->>'sub' = id)
    WITH CHECK (auth.jwt()->>'sub' = id);

-- Service role bypasses RLS for webhook inserts
CREATE POLICY "Service role full access" ON public.profiles
    FOR ALL USING (auth.role() = 'service_role');
