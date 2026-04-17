-- Sprint 1: Initial Schema — All tables, enums, RLS, indexes
-- Applied atomically. Never modify this file after applying.
--
-- NOTE: This migration was applied to the PaperPanda Supabase instance but
-- appears to belong to a different project (fitness/steps-betting domain).
-- These tables are present in the DB but with 0 rows. Retained here for
-- historical accuracy of the applied-migration ledger.

-- ============================================================
-- ENUMS
-- ============================================================

CREATE TYPE pool_type AS ENUM ('steps', 'gym');
CREATE TYPE pool_status AS ENUM ('open', 'active', 'completed');
CREATE TYPE commitment_status AS ENUM ('active', 'completed', 'hardship_exception');
CREATE TYPE commitment_mode AS ENUM ('standard', 'hardcore');
CREATE TYPE stake_source AS ENUM ('wallet', 'fresh_payment', 'mixed');
CREATE TYPE daily_record_status AS ENUM ('pending', 'success', 'failed', 'shielded');
CREATE TYPE transaction_type AS ENUM (
  'stake_charge', 'stake_from_wallet', 'rake', 'shield',
  'settlement_recovery', 'settlement_forfeit',
  'wallet_topup', 'wallet_withdrawal'
);
CREATE TYPE transaction_direction AS ENUM ('debit', 'credit');
CREATE TYPE shield_credit_source AS ENUM ('stake_tier', 'referral', 'perfect_streak_bonus');
CREATE TYPE referral_status AS ENUM ('pending', 'rewarded');

-- ============================================================
-- TABLES
-- ============================================================

-- 1. users (extends Supabase auth.users)
CREATE TABLE users (
  id            UUID PRIMARY KEY REFERENCES auth.users (id) ON DELETE CASCADE,
  email         TEXT UNIQUE NOT NULL,
  full_name     TEXT,
  stripe_customer_id TEXT,
  healthkit_authorized BOOLEAN NOT NULL DEFAULT false,
  push_token    TEXT,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  timezone      TEXT NOT NULL,
  referral_code TEXT UNIQUE,
  referred_by   UUID REFERENCES users (id)
);

-- 2. pools
CREATE TABLE pools (
  id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name                TEXT,
  type                pool_type NOT NULL DEFAULT 'steps',
  min_duration_days   INTEGER NOT NULL DEFAULT 14,
  max_duration_days   INTEGER NOT NULL DEFAULT 90,
  min_daily_stake_cents INTEGER NOT NULL DEFAULT 100,
  max_daily_stake_cents INTEGER NOT NULL DEFAULT 1000,
  daily_goal          INTEGER NOT NULL DEFAULT 10000,
  rake_pct            DECIMAL(5,2) NOT NULL DEFAULT 7.5,
  shield_pct          DECIMAL(5,2) NOT NULL DEFAULT 65.0,
  starts_at           TIMESTAMPTZ NOT NULL,
  ends_at             TIMESTAMPTZ NOT NULL,
  status              pool_status NOT NULL DEFAULT 'open',
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 3. commitments
CREATE TABLE commitments (
  id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id                 UUID NOT NULL REFERENCES users (id),
  pool_id                 UUID NOT NULL REFERENCES pools (id),
  status                  commitment_status NOT NULL DEFAULT 'active',
  daily_stake_cents       INTEGER NOT NULL CHECK (daily_stake_cents BETWEEN 100 AND 1000),
  duration_days           INTEGER NOT NULL CHECK (duration_days BETWEEN 14 AND 90),
  total_stake_cents       INTEGER GENERATED ALWAYS AS (daily_stake_cents * duration_days) STORED,
  stake_source            stake_source,
  shield_credits_awarded  INTEGER NOT NULL DEFAULT 0,
  recovered_cents         INTEGER NOT NULL DEFAULT 0,
  forfeited_cents         INTEGER NOT NULL DEFAULT 0,
  stripe_payment_intent_id TEXT,
  no_cancel_acknowledged  BOOLEAN NOT NULL DEFAULT false,
  settled_at              TIMESTAMPTZ,
  withdrawal_available_at TIMESTAMPTZ,
  joined_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
  completed_at            TIMESTAMPTZ,
  perfect_streak          BOOLEAN NOT NULL DEFAULT false,
  last_chance_expires_at  TIMESTAMPTZ,
  mode                    commitment_mode NOT NULL DEFAULT 'standard'
);

-- 4. daily_records
CREATE TABLE daily_records (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  commitment_id     UUID NOT NULL REFERENCES commitments (id),
  day_number        INTEGER NOT NULL,
  date              DATE NOT NULL,
  steps_recorded    INTEGER NOT NULL DEFAULT 0,
  goal_met          BOOLEAN,
  status            daily_record_status NOT NULL DEFAULT 'pending',
  shield_purchased  BOOLEAN NOT NULL DEFAULT false,
  shield_cost_cents INTEGER,
  stake_cents       INTEGER NOT NULL,
  evaluated_at      TIMESTAMPTZ,
  UNIQUE (commitment_id, day_number)
);

-- 5. transactions
CREATE TABLE transactions (
  id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id                 UUID NOT NULL REFERENCES users (id),
  commitment_id           UUID REFERENCES commitments (id),
  type                    transaction_type NOT NULL,
  amount_cents            INTEGER NOT NULL CHECK (amount_cents > 0),
  direction               transaction_direction NOT NULL,
  stripe_payment_intent_id TEXT,
  created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
  notes                   TEXT
);

-- 6. wallets
CREATE TABLE wallets (
  id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id                 UUID NOT NULL REFERENCES users (id) UNIQUE,
  balance_cents           INTEGER NOT NULL DEFAULT 0 CHECK (balance_cents >= 0),
  lifetime_earned_cents   INTEGER NOT NULL DEFAULT 0,
  lifetime_forfeited_cents INTEGER NOT NULL DEFAULT 0,
  stripe_connect_account_id TEXT,
  updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 7. shield_credits
CREATE TABLE shield_credits (
  id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id                 UUID NOT NULL REFERENCES users (id),
  amount                  INTEGER NOT NULL,
  source                  shield_credit_source NOT NULL,
  source_commitment_id    UUID REFERENCES commitments (id),
  used_on_commitment_id   UUID REFERENCES commitments (id),
  used_on_day             INTEGER,
  expires_at              TIMESTAMPTZ,
  issued_at               TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 8. processed_operations (idempotency)
CREATE TABLE processed_operations (
  key        TEXT PRIMARY KEY,
  result     JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 9. referrals
CREATE TABLE referrals (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  referrer_user_id  UUID NOT NULL REFERENCES users (id),
  referee_user_id   UUID NOT NULL REFERENCES users (id),
  referral_code     TEXT NOT NULL,
  status            referral_status NOT NULL DEFAULT 'pending',
  rewarded_at       TIMESTAMPTZ,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ============================================================
-- INDEXES
-- ============================================================

CREATE INDEX idx_daily_records_commitment_date ON daily_records (commitment_id, date);
CREATE INDEX idx_commitments_user_status ON commitments (user_id, status);
CREATE INDEX idx_transactions_user_created ON transactions (user_id, created_at DESC);
CREATE INDEX idx_shield_credits_user_expires ON shield_credits (user_id, expires_at);

-- ============================================================
-- ROW LEVEL SECURITY
-- ============================================================

-- Enable RLS on every table
ALTER TABLE users ENABLE ROW LEVEL SECURITY;
ALTER TABLE pools ENABLE ROW LEVEL SECURITY;
ALTER TABLE commitments ENABLE ROW LEVEL SECURITY;
ALTER TABLE daily_records ENABLE ROW LEVEL SECURITY;
ALTER TABLE transactions ENABLE ROW LEVEL SECURITY;
ALTER TABLE wallets ENABLE ROW LEVEL SECURITY;
ALTER TABLE shield_credits ENABLE ROW LEVEL SECURITY;
ALTER TABLE processed_operations ENABLE ROW LEVEL SECURITY;
ALTER TABLE referrals ENABLE ROW LEVEL SECURITY;

-- users: read/update own row
CREATE POLICY users_select_own ON users
  FOR SELECT TO authenticated
  USING (id = auth.uid());

CREATE POLICY users_update_own ON users
  FOR UPDATE TO authenticated
  USING (id = auth.uid())
  WITH CHECK (id = auth.uid());

CREATE POLICY users_insert_own ON users
  FOR INSERT TO authenticated
  WITH CHECK (id = auth.uid());

-- pools: public read for authenticated, write via service_role only
CREATE POLICY pools_select_all ON pools
  FOR SELECT TO authenticated
  USING (true);

-- commitments: read/insert own rows
CREATE POLICY commitments_select_own ON commitments
  FOR SELECT TO authenticated
  USING (user_id = auth.uid());

CREATE POLICY commitments_insert_own ON commitments
  FOR INSERT TO authenticated
  WITH CHECK (user_id = auth.uid());

-- daily_records: read own (via commitment ownership)
CREATE POLICY daily_records_select_own ON daily_records
  FOR SELECT TO authenticated
  USING (
    EXISTS (
      SELECT 1 FROM commitments
      WHERE commitments.id = daily_records.commitment_id
        AND commitments.user_id = auth.uid()
    )
  );

-- transactions: read own rows
CREATE POLICY transactions_select_own ON transactions
  FOR SELECT TO authenticated
  USING (user_id = auth.uid());

-- wallets: read own row only
CREATE POLICY wallets_select_own ON wallets
  FOR SELECT TO authenticated
  USING (user_id = auth.uid());

-- shield_credits: read own rows
CREATE POLICY shield_credits_select_own ON shield_credits
  FOR SELECT TO authenticated
  USING (user_id = auth.uid());

-- processed_operations: no user access (service_role only)
-- No policies needed — RLS enabled with no authenticated policies = deny all

-- referrals: read own rows (as referrer or referee)
CREATE POLICY referrals_select_own ON referrals
  FOR SELECT TO authenticated
  USING (referrer_user_id = auth.uid() OR referee_user_id = auth.uid());
