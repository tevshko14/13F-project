import { auth, currentUser } from "@clerk/nextjs/server";
import type { AuthAdapter, AuthUser, UserRole } from "./adapter";

/**
 * Clerk implementation of the AuthAdapter interface.
 * Server-side only -- uses Clerk's server helpers.
 */
export class ClerkAuthAdapter implements AuthAdapter {
  async getCurrentUser(): Promise<AuthUser | null> {
    const user = await currentUser();
    if (!user) return null;

    return {
      id: user.id,
      email: user.primaryEmailAddress?.emailAddress ?? null,
      displayName:
        [user.firstName, user.lastName].filter(Boolean).join(" ") || null,
      avatarUrl: user.imageUrl ?? null,
      role: ((user.publicMetadata as Record<string, unknown>)?.role as UserRole) ?? "FREE",
    };
  }

  async getToken(): Promise<string | null> {
    const { getToken } = await auth();
    return getToken();
  }

  async hasRole(role: UserRole): Promise<boolean> {
    const user = await this.getCurrentUser();
    if (!user) return false;
    return user.role === role;
  }
}
