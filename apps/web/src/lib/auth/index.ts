import { ClerkAuthAdapter } from "./clerk";
import type { AuthAdapter } from "./adapter";

/**
 * Singleton auth adapter. To switch providers, swap the implementation here.
 * All consumers import from this file, never directly from ./clerk.ts.
 */
export const authAdapter: AuthAdapter = new ClerkAuthAdapter();

export type { AuthAdapter, AuthUser, UserRole } from "./adapter";
