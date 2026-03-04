import { authAdapter } from "@/lib/auth";
import { createServerSupabaseClient } from "@/lib/supabase/server";

export default async function DashboardPage() {
  const user = await authAdapter.getCurrentUser();
  const supabase = await createServerSupabaseClient();

  // Fetch profile from Supabase (RLS scoped to current user)
  const { data: profile } = await supabase
    .from("profiles")
    .select("*")
    .single();

  return (
    <div className="space-y-8">
      <div>
        <h1 className="text-3xl font-bold tracking-tight">Dashboard</h1>
        <p className="mt-1 text-zinc-500">
          Welcome back, {user?.displayName ?? user?.email ?? "investor"}.
        </p>
      </div>

      {/* Profile card */}
      <div className="rounded-xl border border-zinc-200 bg-white p-6 dark:border-zinc-800 dark:bg-zinc-900">
        <h2 className="mb-4 text-lg font-semibold">Your Profile</h2>
        <dl className="grid grid-cols-1 gap-4 sm:grid-cols-2">
          <div>
            <dt className="text-sm text-zinc-500">Email</dt>
            <dd className="mt-1 font-medium">
              {profile?.email ?? user?.email ?? "--"}
            </dd>
          </div>
          <div>
            <dt className="text-sm text-zinc-500">Display Name</dt>
            <dd className="mt-1 font-medium">
              {profile?.display_name ?? user?.displayName ?? "--"}
            </dd>
          </div>
          <div>
            <dt className="text-sm text-zinc-500">Plan</dt>
            <dd className="mt-1">
              <span
                className={`inline-flex rounded-full px-2.5 py-0.5 text-xs font-semibold ${
                  profile?.user_role === "PRO"
                    ? "bg-emerald-100 text-emerald-800 dark:bg-emerald-900/30 dark:text-emerald-400"
                    : "bg-zinc-100 text-zinc-600 dark:bg-zinc-800 dark:text-zinc-400"
                }`}
              >
                {profile?.user_role ?? "FREE"}
              </span>
            </dd>
          </div>
          <div>
            <dt className="text-sm text-zinc-500">Member Since</dt>
            <dd className="mt-1 font-medium">
              {profile?.created_at
                ? new Date(profile.created_at).toLocaleDateString("en-US", {
                    year: "numeric",
                    month: "long",
                    day: "numeric",
                  })
                : "--"}
            </dd>
          </div>
        </dl>
      </div>

      {/* Quick links */}
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
        <a
          href="/"
          className="rounded-xl border border-zinc-200 bg-white p-5 transition-shadow hover:shadow-md dark:border-zinc-800 dark:bg-zinc-900"
        >
          <h3 className="font-semibold">Superinvestor Funds</h3>
          <p className="mt-1 text-sm text-zinc-500">
            Track 13F filings from top investors
          </p>
        </a>
        <a
          href="/insider-trading"
          className="rounded-xl border border-zinc-200 bg-white p-5 transition-shadow hover:shadow-md dark:border-zinc-800 dark:bg-zinc-900"
        >
          <h3 className="font-semibold">Insider Trading</h3>
          <p className="mt-1 text-sm text-zinc-500">
            Real-time SEC Form 4 filings
          </p>
        </a>
        <a
          href="/congress"
          className="rounded-xl border border-zinc-200 bg-white p-5 transition-shadow hover:shadow-md dark:border-zinc-800 dark:bg-zinc-900"
        >
          <h3 className="font-semibold">Congress Trading</h3>
          <p className="mt-1 text-sm text-zinc-500">
            STOCK Act disclosures tracker
          </p>
        </a>
      </div>
    </div>
  );
}
