CREATE POLICY "System can insert user_stats" ON user_stats
  FOR INSERT
  WITH CHECK (true);

CREATE POLICY "System can update user_stats" ON user_stats
  FOR UPDATE
  USING (true)
  WITH CHECK (true);

ALTER FUNCTION sync_user_stats_on_action_insert() SECURITY DEFINER;
ALTER FUNCTION sync_global_emissions_on_stats_update() SECURITY DEFINER;
