import { currentUser } from "@clerk/nextjs/server";
import { redirect } from "next/navigation";
import type { UserRole } from "@/lib/auth";

interface ProtectProps {
  children: React.ReactNode;
  /** Require a specific role. If the user lacks it, show fallback or upgrade prompt. */
  role?: UserRole;
  /** Rendered when the user lacks the required role. */
  fallback?: React.ReactNode;
}

/**
 * Server Component wrapper for route/feature gating.
 *
 * Usage:
 *   <Protect>             -- requires any authenticated user
 *   <Protect role="PRO">  -- requires PRO tier, shows fallback for FREE users
 */
export async function Protect({ children, role, fallback }: ProtectProps) {
  const user = await currentUser();

  if (!user) {
    redirect("/auth/sign-in");
  }

  if (role) {
    const userRole =
      ((user.publicMetadata as Record<string, unknown>)?.role as string) ??
      "FREE";
    if (userRole !== role) {
      return (
        fallback ?? (
          <div className="flex flex-col items-center justify-center gap-4 py-16 text-center">
            <h2 className="text-2xl font-semibold">Upgrade to PRO</h2>
            <p className="max-w-md text-zinc-500">
              This feature requires a PRO subscription. Upgrade to unlock full
              access.
            </p>
          </div>
        )
      );
    }
  }

  return <>{children}</>;
}
