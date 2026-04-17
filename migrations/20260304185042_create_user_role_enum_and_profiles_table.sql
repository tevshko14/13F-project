-- User role enum
CREATE TYPE public.user_role AS ENUM ('FREE', 'PRO');

-- Profiles table keyed on Clerk user ID
CREATE TABLE public.profiles (
    id           TEXT PRIMARY KEY,
    email        TEXT,
    display_name TEXT,
    avatar_url   TEXT,
    user_role    public.user_role NOT NULL DEFAULT 'FREE',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Index for email lookups
CREATE INDEX idx_profiles_email ON public.profiles (email);
