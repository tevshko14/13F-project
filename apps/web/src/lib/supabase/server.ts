import { createClient } from "@supabase/supabase-js";
import { auth } from "@clerk/nextjs/server";

/**
 * Server-side Supabase client that authenticates via Clerk session tokens.
 * Use in Server Components and Route Handlers.
 * RLS policies evaluate `auth.jwt()->>'sub'` against the Clerk user ID.
 */
export async function createServerSupabaseClient() {
  const { getToken } = await auth();

  return createClient(
    process.env.NEXT_PUBLIC_SUPABASE_URL!,
    process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!,
    {
      accessToken: async () => {
        const token = await getToken();
        return token ?? "";
      },
    }
  );
}
