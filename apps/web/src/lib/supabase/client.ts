"use client";

import { createClient } from "@supabase/supabase-js";
import { useSession } from "@clerk/nextjs";
import { useMemo } from "react";

/**
 * Browser-side Supabase client that authenticates via Clerk session tokens.
 * Uses the `accessToken` callback so Supabase attaches the Clerk JWT
 * as a Bearer token on every request. RLS evaluates `auth.jwt()->>'sub'`.
 */
export function useSupabaseClient() {
  const { session } = useSession();

  return useMemo(() => {
    return createClient(
      process.env.NEXT_PUBLIC_SUPABASE_URL!,
      process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!,
      {
        accessToken: async () => {
          return (await session?.getToken()) ?? "";
        },
      }
    );
  }, [session]);
}
