import { UserProfile } from "@clerk/nextjs";
import { Protect } from "@/components/protect";

export default function SettingsPage() {
  return (
    <Protect>
      <div className="min-h-screen bg-zinc-50 dark:bg-zinc-950">
        <div className="mx-auto max-w-3xl px-4 py-8">
          <h1 className="mb-6 text-3xl font-bold tracking-tight">Settings</h1>
          <UserProfile
            appearance={{
              elements: {
                rootBox: "w-full",
                cardBox: "shadow-none border border-zinc-200 dark:border-zinc-800",
              },
            }}
          />
        </div>
      </div>
    </Protect>
  );
}
