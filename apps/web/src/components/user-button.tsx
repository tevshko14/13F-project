"use client";

import { UserButton as ClerkUserButton } from "@clerk/nextjs";

/**
 * Wrapper around Clerk's UserButton with PaperPanda-specific config.
 * Abstracted so we can swap providers without touching every page.
 */
export function UserButton() {
  return (
    <ClerkUserButton
      appearance={{
        elements: {
          avatarBox: "h-9 w-9",
        },
      }}
    />
  );
}
