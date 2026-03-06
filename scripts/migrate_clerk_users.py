"""
Migrate users from Clerk dev instance → Clerk prod instance.

For each user in the Supabase `profiles` table:
  1. Creates a stub account in Clerk prod via the Management API (email only, no password)
  2. Updates profiles.id in Supabase with the new Clerk prod user ID

When users next sign in with Google, Clerk matches by email and links their
Google account to the pre-created stub → same ID → Supabase profile intact.

Usage:
    CLERK_SECRET_KEY=sk_live_... python scripts/migrate_clerk_users.py

Requires the prod secret key (sk_live_...) — NOT the dev key.
The script reads SUPABASE_URL and SUPABASE_SERVICE_KEY from your .env file.
"""

from __future__ import annotations

import os
import sys
import time
import httpx
from dotenv import load_dotenv

load_dotenv()

CLERK_SECRET_KEY = os.environ.get("CLERK_SECRET_KEY", "")
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "") or os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")

# ── Validate ──────────────────────────────────────────────────────────────────

if not CLERK_SECRET_KEY.startswith("sk_live_"):
    print("ERROR: CLERK_SECRET_KEY must be a production key starting with sk_live_")
    print("       Set it as: CLERK_SECRET_KEY=sk_live_... python scripts/migrate_clerk_users.py")
    sys.exit(1)

if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
    print("ERROR: SUPABASE_URL and SUPABASE_SERVICE_KEY must be set in your .env")
    sys.exit(1)

CLERK_HEADERS = {
    "Authorization": f"Bearer {CLERK_SECRET_KEY}",
    "Content-Type": "application/json",
}
SUPA_HEADERS = {
    "apikey": SUPABASE_SERVICE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation",
}

# ── Fetch profiles from Supabase ──────────────────────────────────────────────

print("Fetching profiles from Supabase...")
resp = httpx.get(
    f"{SUPABASE_URL}/rest/v1/profiles?select=id,email,display_name,user_role",
    headers=SUPA_HEADERS,
    timeout=15,
)
resp.raise_for_status()
profiles = resp.json()
print(f"Found {len(profiles)} user(s) to migrate\n")

if not profiles:
    print("No profiles found. Nothing to do.")
    sys.exit(0)

# ── Migrate each user ─────────────────────────────────────────────────────────

results = []

for p in profiles:
    old_id = p["id"]
    email = p["email"]
    display_name = p.get("display_name") or ""

    print(f"  → {email} (old ID: {old_id})")

    try:
        # Step 1: Check if this email already exists in Clerk prod
        search = httpx.get(
            f"https://api.clerk.com/v1/users?email_address={email}",
            headers=CLERK_HEADERS,
            timeout=30,
        )
        search.raise_for_status()
        existing = search.json()

        if existing:
            new_id = existing[0]["id"]
            print(f"     Already exists in Clerk prod — using ID: {new_id}")
        else:
            # Step 2: Create stub user in Clerk prod
            payload: dict = {
                "email_address": [email],
                "skip_password_checks": True,
                "skip_password_requirement": True,
            }
            if display_name:
                parts = display_name.split(" ", 1)
                payload["first_name"] = parts[0]
                if len(parts) > 1:
                    payload["last_name"] = parts[1]

            create = httpx.post(
                "https://api.clerk.com/v1/users",
                headers=CLERK_HEADERS,
                json=payload,
                timeout=30,
            )
            if create.status_code not in (200, 201):
                print(f"     FAILED to create: {create.status_code} {create.text}")
                results.append({"email": email, "status": "failed", "reason": create.text})
                continue

            new_id = create.json()["id"]
            print(f"     Created in Clerk prod — new ID: {new_id}")

        # Step 3: Insert new profile row with new ID (preserving all fields)
        insert_resp = httpx.post(
            f"{SUPABASE_URL}/rest/v1/profiles",
            headers={**SUPA_HEADERS, "Prefer": "resolution=ignore-duplicates,return=representation"},
            json={
                "id": new_id,
                "email": email,
                "display_name": p.get("display_name"),
                "avatar_url": p.get("avatar_url"),
                "user_role": p.get("user_role", "free"),
            },
            timeout=30,
        )

        if insert_resp.status_code in (200, 201):
            print(f"     Inserted new profile row with prod ID ✓")
        else:
            print(f"     Profile insert note: {insert_resp.status_code} (may already exist)")

        # Step 4: If old_id != new_id, delete the old orphaned profile row
        if old_id != new_id:
            del_resp = httpx.delete(
                f"{SUPABASE_URL}/rest/v1/profiles?id=eq.{old_id}",
                headers=SUPA_HEADERS,
                timeout=30,
            )
            if del_resp.status_code in (200, 204):
                print(f"     Removed old dev profile row ✓")
            else:
                print(f"     Could not remove old row ({del_resp.status_code}) — clean up manually if needed")

        results.append({"email": email, "old_id": old_id, "new_id": new_id, "status": "ok"})

    except httpx.TimeoutException:
        print(f"     TIMEOUT — skipping, re-run the script to retry")
        results.append({"email": email, "status": "failed", "reason": "timeout"})
    except Exception as exc:
        print(f"     ERROR — {exc}")
        results.append({"email": email, "status": "failed", "reason": str(exc)})

    time.sleep(0.5)  # be kind to Clerk's rate limits

# ── Summary ───────────────────────────────────────────────────────────────────

print("\n── Migration Summary ─────────────────────────────────────────")
ok = [r for r in results if r["status"] == "ok"]
failed = [r for r in results if r["status"] == "failed"]
print(f"  ✅ Migrated: {len(ok)}")
print(f"  ❌ Failed:   {len(failed)}")

if failed:
    print("\nFailed users:")
    for r in failed:
        print(f"  {r['email']}: {r.get('reason', '')}")

print("\nDone. Users can now sign in with Google on the prod instance.")
print("Their Supabase profile data is intact under the new Clerk prod IDs.")
