/**
 * Shared user types used across the web app and Chrome extension.
 * Keep in sync with the Supabase `profiles` table schema.
 */

export type UserRole = "FREE" | "PRO";

export interface Profile {
  id: string;
  email: string | null;
  display_name: string | null;
  avatar_url: string | null;
  user_role: UserRole;
  created_at: string;
  updated_at: string;
}
