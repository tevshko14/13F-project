/**
 * Auth provider interface -- swap implementations without changing consumers.
 * Currently backed by Clerk. To switch providers, implement this interface
 * and update the singleton export in ./index.ts.
 */

export type UserRole = "FREE" | "PRO";

export interface AuthUser {
  id: string;
  email: string | null;
  displayName: string | null;
  avatarUrl: string | null;
  role: UserRole;
}

export interface AuthAdapter {
  /** Get the currently authenticated user (server-side). */
  getCurrentUser(): Promise<AuthUser | null>;

  /** Get a session token for downstream services (Supabase, FastAPI). */
  getToken(): Promise<string | null>;

  /** Check if current user has a specific role. */
  hasRole(role: UserRole): Promise<boolean>;
}
