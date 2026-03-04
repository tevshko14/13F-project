import { Protect } from "@/components/protect";
import { UserButton } from "@/components/user-button";
import Link from "next/link";

export default function DashboardLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <Protect>
      <div className="min-h-screen bg-zinc-50 dark:bg-zinc-950">
        {/* Top nav */}
        <header className="sticky top-0 z-40 border-b border-zinc-200 bg-white/80 backdrop-blur dark:border-zinc-800 dark:bg-zinc-950/80">
          <div className="mx-auto flex h-14 max-w-5xl items-center justify-between px-4">
            <div className="flex items-center gap-6">
              <Link
                href="/dashboard"
                className="text-lg font-semibold tracking-tight"
              >
                PaperPanda
              </Link>
              <nav className="hidden gap-4 text-sm sm:flex">
                <Link
                  href="/dashboard"
                  className="text-zinc-600 transition-colors hover:text-zinc-900 dark:text-zinc-400 dark:hover:text-zinc-100"
                >
                  Dashboard
                </Link>
                <Link
                  href="/settings"
                  className="text-zinc-600 transition-colors hover:text-zinc-900 dark:text-zinc-400 dark:hover:text-zinc-100"
                >
                  Settings
                </Link>
              </nav>
            </div>
            <UserButton />
          </div>
        </header>

        {/* Page content */}
        <main className="mx-auto max-w-5xl px-4 py-8">{children}</main>
      </div>
    </Protect>
  );
}
