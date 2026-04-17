-- Add net worth estimate columns to congress_members
ALTER TABLE congress_members ADD COLUMN IF NOT EXISTS net_worth_estimate BIGINT;
ALTER TABLE congress_members ADD COLUMN IF NOT EXISTS net_worth_source TEXT NOT NULL DEFAULT '';
ALTER TABLE congress_members ADD COLUMN IF NOT EXISTS net_worth_year INT;
