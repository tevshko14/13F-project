# Database Migrations

This directory contains versioned SQL files representing every migration that
has been applied to the production PaperPanda Supabase database.

## Purpose

**Disaster recovery.** If the production database is ever lost or needs to be
rebuilt in a new project, these files are the authoritative record of the
schema. Re-apply them in version order to recreate the full schema.

They are also a human-readable audit trail of how the schema evolved.

## File Naming

```
<YYYYMMDDHHMMSS>_<descriptive_name>.sql
```

The version prefix matches the `version` column in Supabase's
`supabase_migrations.schema_migrations` table. Files sort lexicographically
in application order.

## Workflow for New Migrations

Migrations are applied through the Supabase MCP `apply_migration` tool
(or the Supabase dashboard SQL editor for one-offs). To keep this directory
in sync:

1. **Apply** the migration to Supabase via MCP or dashboard.
2. **Copy** the applied SQL verbatim into a new file here with the
   timestamp + name that Supabase assigned.
3. **Commit** alongside the code change that depends on the new schema.

Never edit a committed migration file after it has been applied —
migrations are immutable once deployed. Use a new migration to change the
schema further.

## Re-Creating the Schema from Scratch

Against an empty Supabase project:

```sh
# Apply every migration in version order
for f in migrations/*.sql; do
  psql "$DATABASE_URL" -f "$f"
done
```

Then insert the rows into `supabase_migrations.schema_migrations` so
Supabase's migration tracker is aware they've been applied.

## Known Caveats

- **`20260312225659_initial_schema.sql`** defines tables from a different
  project (fitness/steps-betting domain: `pools`, `commitments`, `wallets`,
  etc.). These tables exist in the DB with 0 rows — likely applied in
  error. Left in place to preserve historical accuracy. Safe to drop if
  never needed.
- **Tables not covered by any migration** (applied directly via dashboard
  SQL editor before migration tracking began): `api_cache`, `sync_logs`,
  `notifications`, `congress_trades`, `congress_members`, `congress_sync_log`,
  `youtube_channels`, `youtube_events`, `supporters`, `short_interest_history`,
  `insider_trades`, `insider_purchases_history`, `user_watchlist`,
  `user_notification_preferences`, `watchlist_digest_log`, `admin_users`,
  `crypto_*`. Schema for these lives only in production. If you rebuild,
  capture their DDL with `pg_dump --schema-only --table=<name>` before
  the rebuild.
