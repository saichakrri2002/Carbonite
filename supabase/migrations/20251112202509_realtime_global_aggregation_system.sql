/*
  # Realtime Global Aggregation and Leaderboard System
  
  1. New Tables
    - `users`: User authentication and profile data
    - `user_actions`: Action logs with verification status
    - `user_stats`: Pre-aggregated user statistics
    - `global_emissions`: Single-row global aggregate
    - `sync_audit_log`: Tracks sync checks and corrections
  
  2. Key Features
    - Formula: SUM(user_stats.total_emissions_lbs) = global_emissions.total_lbs_saved
    - Real-time updates via Supabase Realtime
    - Automatic sync on verified actions
    - Leaderboard with tie-breaking logic
    - Active users count
    - Hourly sync validation
  
  3. Security
    - RLS enabled on all tables
    - Authenticated users can read all data
    - Users can only insert their own actions
    - Only system can update aggregates via triggers
  
  4. Performance
    - Indexed columns for fast queries
    - Pre-aggregated statistics
    - Optimized trigger functions
*/

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_cron";

-- Users table
CREATE TABLE IF NOT EXISTS users (
  user_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  email TEXT UNIQUE NOT NULL,
  username TEXT UNIQUE,
  password_hash TEXT NOT NULL,
  country TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Enable RLS
ALTER TABLE users ENABLE ROW LEVEL SECURITY;

-- RLS Policies for users
CREATE POLICY "Users can view all profiles"
  ON users FOR SELECT
  TO authenticated
  USING (true);

CREATE POLICY "Users can update own profile"
  ON users FOR UPDATE
  TO authenticated
  USING (auth.uid() = user_id)
  WITH CHECK (auth.uid() = user_id);

-- User actions table
CREATE TABLE IF NOT EXISTS user_actions (
  action_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  action_type TEXT NOT NULL,
  quantity NUMERIC(12,3) NOT NULL CHECK (quantity > 0),
  unit TEXT NOT NULL,
  emissions_saved_lbs NUMERIC(14,4) NOT NULL CHECK (emissions_saved_lbs >= 0),
  verified BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  verified_at TIMESTAMPTZ
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_user_actions_user_id ON user_actions(user_id);
CREATE INDEX IF NOT EXISTS idx_user_actions_verified ON user_actions(verified) WHERE verified = TRUE;
CREATE INDEX IF NOT EXISTS idx_user_actions_created_at ON user_actions(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_user_actions_verified_at ON user_actions(verified_at DESC) WHERE verified = TRUE;

-- Enable RLS
ALTER TABLE user_actions ENABLE ROW LEVEL SECURITY;

-- RLS Policies for user_actions
CREATE POLICY "Users can view all actions"
  ON user_actions FOR SELECT
  TO authenticated
  USING (true);

CREATE POLICY "Users can insert own actions"
  ON user_actions FOR INSERT
  TO authenticated
  WITH CHECK (auth.uid() = user_id);

CREATE POLICY "Users can update own unverified actions"
  ON user_actions FOR UPDATE
  TO authenticated
  USING (auth.uid() = user_id AND verified = FALSE)
  WITH CHECK (auth.uid() = user_id);

-- User stats table (pre-aggregated)
CREATE TABLE IF NOT EXISTS user_stats (
  user_id UUID PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE,
  total_emissions_lbs NUMERIC(16,4) NOT NULL DEFAULT 0 CHECK (total_emissions_lbs >= 0),
  action_count INTEGER NOT NULL DEFAULT 0 CHECK (action_count >= 0),
  monthly_lbs JSONB NOT NULL DEFAULT '{}'::JSONB,
  yearly_lbs JSONB NOT NULL DEFAULT '{}'::JSONB,
  last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for leaderboard queries
CREATE INDEX IF NOT EXISTS idx_user_stats_total_desc ON user_stats(total_emissions_lbs DESC) WHERE total_emissions_lbs > 0;

-- Enable RLS
ALTER TABLE user_stats ENABLE ROW LEVEL SECURITY;

-- RLS Policies for user_stats
CREATE POLICY "Anyone can view user stats"
  ON user_stats FOR SELECT
  TO authenticated
  USING (true);

-- Global emissions table (singleton)
CREATE TABLE IF NOT EXISTS global_emissions (
  id INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
  total_lbs_saved NUMERIC(20,4) NOT NULL DEFAULT 0 CHECK (total_lbs_saved >= 0),
  total_actions BIGINT NOT NULL DEFAULT 0 CHECK (total_actions >= 0),
  last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Initialize global_emissions
INSERT INTO global_emissions (id, total_lbs_saved, total_actions)
VALUES (1, 0, 0)
ON CONFLICT (id) DO NOTHING;

-- Enable RLS
ALTER TABLE global_emissions ENABLE ROW LEVEL SECURITY;

-- RLS Policies for global_emissions
CREATE POLICY "Anyone can view global emissions"
  ON global_emissions FOR SELECT
  TO authenticated
  USING (true);

-- Sync audit log table
CREATE TABLE IF NOT EXISTS sync_audit_log (
  log_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  check_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  was_in_sync BOOLEAN NOT NULL,
  user_stats_sum NUMERIC(20,4) NOT NULL,
  global_total NUMERIC(20,4) NOT NULL,
  discrepancy NUMERIC(20,4) NOT NULL,
  fixed BOOLEAN NOT NULL,
  details JSONB
);

-- Enable RLS
ALTER TABLE sync_audit_log ENABLE ROW LEVEL SECURITY;

-- RLS Policies for sync_audit_log
CREATE POLICY "Authenticated users can view sync logs"
  ON sync_audit_log FOR SELECT
  TO authenticated
  USING (true);

-- Trigger function: Automatic sync on verified actions
CREATE OR REPLACE FUNCTION global_sync_trigger_function()
RETURNS TRIGGER AS $$
BEGIN
  -- Only process when verified changes from false to true
  IF NEW.verified = TRUE AND (OLD.verified IS NULL OR OLD.verified = FALSE) THEN
    -- Update user_stats atomically
    INSERT INTO user_stats (user_id, total_emissions_lbs, action_count, last_updated)
    VALUES (NEW.user_id, NEW.emissions_saved_lbs, 1, NOW())
    ON CONFLICT (user_id) DO UPDATE
    SET 
      total_emissions_lbs = user_stats.total_emissions_lbs + EXCLUDED.total_emissions_lbs,
      action_count = user_stats.action_count + 1,
      last_updated = NOW();
    
    -- Update global_emissions atomically
    UPDATE global_emissions
    SET 
      total_lbs_saved = total_lbs_saved + NEW.emissions_saved_lbs,
      total_actions = total_actions + 1,
      last_updated = NOW()
    WHERE id = 1;
    
    -- Set verified_at timestamp
    NEW.verified_at := NOW();
  END IF;
  
  RETURN NEW;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Create trigger
DROP TRIGGER IF EXISTS global_sync_trigger ON user_actions;
CREATE TRIGGER global_sync_trigger
  AFTER UPDATE ON user_actions
  FOR EACH ROW
  WHEN (NEW.verified = TRUE AND (OLD.verified IS NULL OR OLD.verified = FALSE))
  EXECUTE FUNCTION global_sync_trigger_function();

-- Also trigger on INSERT if already verified
DROP TRIGGER IF EXISTS global_sync_insert_trigger ON user_actions;
CREATE TRIGGER global_sync_insert_trigger
  AFTER INSERT ON user_actions
  FOR EACH ROW
  WHEN (NEW.verified = TRUE)
  EXECUTE FUNCTION global_sync_trigger_function();

-- Function: Get active users count
CREATE OR REPLACE FUNCTION get_active_users_count()
RETURNS INTEGER AS $$
BEGIN
  RETURN (
    SELECT COUNT(DISTINCT user_id)::INTEGER
    FROM user_actions 
    WHERE verified = TRUE
  );
END;
$$ LANGUAGE plpgsql STABLE SECURITY DEFINER;

-- Function: Get leaderboard with tie-breaking
CREATE OR REPLACE FUNCTION get_leaderboard(limit_count INTEGER DEFAULT 100)
RETURNS TABLE(
  rank INTEGER,
  user_id UUID,
  username TEXT,
  total_emissions_lbs NUMERIC,
  action_count INTEGER,
  last_update_timestamp TIMESTAMPTZ
) AS $$
BEGIN
  RETURN QUERY
  WITH ranked_users AS (
    SELECT 
      us.user_id,
      COALESCE(u.username, u.email) AS username,
      us.total_emissions_lbs,
      us.action_count,
      us.last_updated AS last_update_timestamp,
      COALESCE(
        (SELECT MAX(ua.verified_at) 
         FROM user_actions ua 
         WHERE ua.user_id = us.user_id AND ua.verified = TRUE),
        us.last_updated
      ) AS last_verified_action_at
    FROM user_stats us
    JOIN users u ON u.user_id = us.user_id
    WHERE us.total_emissions_lbs > 0
  )
  SELECT 
    ROW_NUMBER() OVER (
      ORDER BY 
        ranked_users.total_emissions_lbs DESC,
        ranked_users.last_verified_action_at DESC
    )::INTEGER AS rank,
    ranked_users.user_id,
    ranked_users.username,
    ROUND(ranked_users.total_emissions_lbs::NUMERIC, 2) AS total_emissions_lbs,
    ranked_users.action_count,
    ranked_users.last_update_timestamp
  FROM ranked_users
  ORDER BY 
    ranked_users.total_emissions_lbs DESC,
    ranked_users.last_verified_action_at DESC
  LIMIT limit_count;
END;
$$ LANGUAGE plpgsql STABLE SECURITY DEFINER;

-- Function: Sync validation and auto-fix
CREATE OR REPLACE FUNCTION sync_global_emissions_from_user_stats()
RETURNS TABLE(
  was_in_sync BOOLEAN,
  user_stats_sum NUMERIC,
  global_total NUMERIC,
  fixed BOOLEAN
) AS $$
DECLARE
  stats_sum NUMERIC;
  global_val NUMERIC;
  is_synced BOOLEAN;
BEGIN
  -- Calculate sum from user_stats
  SELECT COALESCE(SUM(total_emissions_lbs), 0) INTO stats_sum FROM user_stats;
  
  -- Get current global total
  SELECT total_lbs_saved INTO global_val FROM global_emissions WHERE id = 1;
  
  -- Check if in sync (with small tolerance for floating point)
  is_synced := ABS(stats_sum - global_val) < 0.01;
  
  -- If not in sync, fix it
  IF NOT is_synced THEN
    UPDATE global_emissions
    SET 
      total_lbs_saved = stats_sum,
      last_updated = NOW()
    WHERE id = 1;
    
    -- Log the correction
    INSERT INTO sync_audit_log (was_in_sync, user_stats_sum, global_total, discrepancy, fixed)
    VALUES (FALSE, stats_sum, global_val, ABS(stats_sum - global_val), TRUE);
  END IF;
  
  -- Return result
  RETURN QUERY SELECT is_synced, stats_sum, global_val, NOT is_synced;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Function: Hourly sync check
CREATE OR REPLACE FUNCTION hourly_sync_check()
RETURNS JSONB AS $$
DECLARE
  check_result RECORD;
  result JSONB;
  discrepancy NUMERIC;
BEGIN
  -- Run sync check
  SELECT * INTO check_result FROM sync_global_emissions_from_user_stats();
  
  -- Calculate discrepancy
  discrepancy := ABS(check_result.user_stats_sum - check_result.global_total);
  
  -- Build result JSON
  result := jsonb_build_object(
    'timestamp', NOW(),
    'was_in_sync', check_result.was_in_sync,
    'user_stats_sum', ROUND(check_result.user_stats_sum::NUMERIC, 2),
    'global_total', ROUND(check_result.global_total::NUMERIC, 2),
    'discrepancy', ROUND(discrepancy::NUMERIC, 2),
    'fixed', check_result.fixed,
    'in_sync', ABS(check_result.user_stats_sum - check_result.global_total) < 0.01,
    'alert_channel', 'System admin or monitoring dashboard'
  );
  
  -- Log warning if not in sync
  IF NOT check_result.was_in_sync THEN
    RAISE WARNING 'Sync check found inconsistency: user_stats_sum=%, global_total=%, discrepancy=%, fixed=%', 
      ROUND(check_result.user_stats_sum::NUMERIC, 2),
      ROUND(check_result.global_total::NUMERIC, 2),
      ROUND(discrepancy::NUMERIC, 2),
      check_result.fixed;
  END IF;
  
  RETURN result;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- View: Consistency check
CREATE OR REPLACE VIEW consistency_check AS
SELECT 
  (SELECT COALESCE(SUM(total_emissions_lbs), 0) FROM user_stats) AS user_stats_sum_lbs,
  (SELECT total_lbs_saved FROM global_emissions WHERE id = 1) AS global_total_lbs,
  ABS((SELECT COALESCE(SUM(total_emissions_lbs), 0) FROM user_stats) - 
      (SELECT total_lbs_saved FROM global_emissions WHERE id = 1)) < 0.01 AS in_sync;

-- Schedule hourly sync check (if pg_cron is available)
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_cron') THEN
    PERFORM cron.schedule(
      'hourly-sync-check',
      '0 * * * *',
      'SELECT hourly_sync_check();'
    );
  END IF;
END $$;

-- Enable Realtime for key tables
ALTER PUBLICATION supabase_realtime ADD TABLE global_emissions;
ALTER PUBLICATION supabase_realtime ADD TABLE user_stats;
ALTER PUBLICATION supabase_realtime ADD TABLE user_actions;

-- Grant necessary permissions
GRANT SELECT ON global_emissions TO authenticated;
GRANT SELECT ON user_stats TO authenticated;
GRANT SELECT ON user_actions TO authenticated;
GRANT SELECT ON sync_audit_log TO authenticated;
GRANT EXECUTE ON FUNCTION get_active_users_count() TO authenticated;
GRANT EXECUTE ON FUNCTION get_leaderboard(INTEGER) TO authenticated;
GRANT EXECUTE ON FUNCTION sync_global_emissions_from_user_stats() TO authenticated;
GRANT EXECUTE ON FUNCTION hourly_sync_check() TO authenticated;
