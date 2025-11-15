/**
 * Climate Stats Service
 * Integrates with production-grade database schema:
 * - Production: user_stats_summary, global_stats_rollup, user_action_events, leaderboard_snapshots
 * - Previous: user_statistics, global_statistics, user_action_logs, leaderboards
 * - Legacy: user_stats, global_emissions, user_actions
 * Works with all schemas for backward compatibility
 */

import { supabase } from '../lib/supabase';

// Production schema type definitions
export interface UserStatsSummary {
  user_id: string;
  total_co2_saved_kg: number;
  total_actions: number;
  current_month_co2: number;
  current_year_co2: number;
  streak_days: number;
  last_action_date: string | null;
  rank_global: number | null;
  rank_country: number | null;
  updated_at: string;
}

// Previous schema type definitions (for compatibility)
export interface UserStatistics {
  user_id: string;
  total_co2_saved_kg: number;
  total_actions_count: number;
  current_month_co2_saved: number;
  current_year_co2_saved: number;
  streak_days: number;
  last_action_date: string | null;
  updated_at: string;
}

// Production schema: global_stats_rollup uses key-based rollups
export interface GlobalStatsRollup {
  rollup_key: string; // 'global_all_time', 'country_US_2025', etc.
  total_users: number;
  total_co2_saved_kg: number;
  total_actions: number;
  computed_at: string;
}

// Previous schema type definitions (for compatibility)
export interface GlobalStatistics {
  id: number;
  total_users: number;
  total_co2_saved_kg: number;
  total_actions_count: number;
  countries_count: number;
  last_updated: string;
}

// Production schema: user_action_events with idempotency
export interface UserActionEvent {
  event_id: number;
  user_id: string;
  action_id: number;
  quantity: number;
  co2_saved_kg: number;
  event_timestamp: string;
  action_date: string;
  idempotency_key: string;
}

// Previous schema type definitions (for compatibility)
export interface UserActionLog {
  log_id: number;
  user_id: string;
  action_id: number;
  quantity: number;
  co2_saved_kg: number;
  logged_at: string;
  action_date: string;
  notes: string | null;
  location: string | null;
}

// Production schema: leaderboard_snapshots
export interface LeaderboardSnapshot {
  scope: string; // 'global', 'country', 'organization'
  scope_value: string; // 'US', 'org_123', etc.
  period: string; // 'all_time', 'yearly', 'monthly', 'weekly'
  user_id: string;
  rank: number;
  co2_saved_kg: number;
  snapshot_time: string;
}

export interface LeaderboardEntry {
  rank: number;
  user_id: string;
  username: string;
  co2_saved_kg: number;
  period: 'all_time' | 'yearly' | 'monthly' | 'weekly';
  updated_at: string;
}

/**
 * Get user statistics - supports production, previous, and legacy schemas
 * Priority: user_stats_summary > user_statistics > user_stats > calculated
 */
export async function getUserStatistics(userId: string): Promise<UserStatistics | null> {
  try {
    // Try production schema first: user_stats_summary
    const { data: prodData, error: prodError } = await supabase
      .from('user_stats_summary')
      .select('*')
      .eq('user_id', userId)
      .maybeSingle();

    if (!prodError && prodData) {
      return {
        user_id: prodData.user_id,
        total_co2_saved_kg: parseFloat(prodData.total_co2_saved_kg || 0),
        total_actions_count: prodData.total_actions || 0,
        current_month_co2_saved: parseFloat(prodData.current_month_co2 || 0),
        current_year_co2_saved: parseFloat(prodData.current_year_co2 || 0),
        streak_days: prodData.streak_days || 0,
        last_action_date: prodData.last_action_date,
        updated_at: prodData.updated_at,
      };
    }

    // Try previous schema: user_statistics
    const { data: prevData, error: prevError } = await supabase
      .from('user_statistics')
      .select('*')
      .eq('user_id', userId)
      .maybeSingle();

    if (!prevError && prevData) {
      return {
        user_id: prevData.user_id,
        total_co2_saved_kg: parseFloat(prevData.total_co2_saved_kg || 0),
        total_actions_count: prevData.total_actions_count || 0,
        current_month_co2_saved: parseFloat(prevData.current_month_co2_saved || 0),
        current_year_co2_saved: parseFloat(prevData.current_year_co2_saved || 0),
        streak_days: prevData.streak_days || 0,
        last_action_date: prevData.last_action_date,
        updated_at: prevData.updated_at,
      };
    }

    // Fallback: calculate from existing user_actions table
    return await calculateUserStatisticsFromActions(userId);
  } catch (error) {
    console.error('Error getting user statistics:', error);
    return await calculateUserStatisticsFromActions(userId);
  }
}

/**
 * Get user stats summary with ranks (production schema)
 */
export async function getUserStatsSummary(userId: string): Promise<UserStatsSummary | null> {
  try {
    const { data, error } = await supabase
      .from('user_stats_summary')
      .select('*')
      .eq('user_id', userId)
      .maybeSingle();

    if (error || !data) return null;

    return {
      user_id: data.user_id,
      total_co2_saved_kg: parseFloat(data.total_co2_saved_kg || 0),
      total_actions: data.total_actions || 0,
      current_month_co2: parseFloat(data.current_month_co2 || 0),
      current_year_co2: parseFloat(data.current_year_co2 || 0),
      streak_days: data.streak_days || 0,
      last_action_date: data.last_action_date,
      rank_global: data.rank_global,
      rank_country: data.rank_country,
      updated_at: data.updated_at,
    };
  } catch (error) {
    console.error('Error getting user stats summary:', error);
    return null;
  }
}

/**
 * Calculate user statistics from user_actions (fallback for old schema)
 */
async function calculateUserStatisticsFromActions(userId: string): Promise<UserStatistics | null> {
  try {
    const { data: actions, error } = await supabase
      .from('user_actions')
      .select('custom_emissions_saved, action_template_id, logged_at, action_templates(emissions_saved)')
      .eq('user_id', userId)
      .order('logged_at', { ascending: false });

    if (error) {
      console.error('Error fetching actions:', error);
      return null;
    }

    const now = new Date();
    const thisMonth = new Date(now.getFullYear(), now.getMonth(), 1);
    const thisYear = new Date(now.getFullYear(), 0, 1);

    let totalCo2Kg = 0;
    let monthCo2Kg = 0;
    let yearCo2Kg = 0;
    let actionCount = 0;
    let lastActionDate: string | null = null;

    (actions || []).forEach((action: any) => {
      // Convert lbs to kg (1 lb = 0.453592 kg)
      const emissionsLbs = action.custom_emissions_saved || 
        ((action.action_templates?.emissions_saved || 0) * 2.20462);
      const emissionsKg = emissionsLbs * 0.453592;

      totalCo2Kg += emissionsKg;
      actionCount++;

      const actionDate = new Date(action.logged_at);
      if (actionDate >= thisMonth) {
        monthCo2Kg += emissionsKg;
      }
      if (actionDate >= thisYear) {
        yearCo2Kg += emissionsKg;
      }

      if (!lastActionDate) {
        lastActionDate = action.logged_at;
      }
    });

    // Calculate streak (simplified - would need proper streak calculation)
    const streakDays = 0; // TODO: Implement proper streak calculation

    return {
      user_id: userId,
      total_co2_saved_kg: totalCo2Kg,
      total_actions_count: actionCount,
      current_month_co2_saved: monthCo2Kg,
      current_year_co2_saved: yearCo2Kg,
      streak_days: streakDays,
      last_action_date: lastActionDate,
      updated_at: new Date().toISOString(),
    };
  } catch (error) {
    console.error('Error calculating user statistics:', error);
    return null;
  }
}

/**
 * Get global statistics - supports production, previous, and legacy schemas
 * Priority: global_stats_rollup > global_statistics > global_emissions > calculated
 */
export async function getGlobalStatistics(): Promise<GlobalStatistics | null> {
  try {
    // Try production schema first: global_stats_rollup (key-based)
    const { data: rollupData, error: rollupError } = await supabase
      .from('global_stats_rollup')
      .select('*')
      .eq('rollup_key', 'global_all_time')
      .single();

    if (!rollupError && rollupData) {
      return {
        id: 1,
        total_users: rollupData.total_users || 0,
        total_co2_saved_kg: parseFloat(rollupData.total_co2_saved_kg || 0),
        total_actions_count: rollupData.total_actions || 0,
        countries_count: 0, // Would need separate query for countries
        last_updated: rollupData.computed_at,
      };
    }

    // Try previous schema: global_statistics
    const { data: prevData, error: prevError } = await supabase
      .from('global_statistics')
      .select('*')
      .eq('id', 1)
      .single();

    if (!prevError && prevData) {
      return {
        id: prevData.id,
        total_users: prevData.total_users || 0,
        total_co2_saved_kg: parseFloat(prevData.total_co2_saved_kg || 0),
        total_actions_count: prevData.total_actions_count || 0,
        countries_count: prevData.countries_count || 0,
        last_updated: prevData.last_updated,
      };
    }

    // Fallback: calculate from existing tables
    return await calculateGlobalStatisticsFromExisting();
  } catch (error) {
    console.error('Error getting global statistics:', error);
    return await calculateGlobalStatisticsFromExisting();
  }
}

/**
 * Get global stats rollup for specific key (production schema)
 * Keys: 'global_all_time', 'country_US_2025', 'country_GB_2025', etc.
 */
export async function getGlobalStatsRollup(rollupKey: string = 'global_all_time'): Promise<GlobalStatsRollup | null> {
  try {
    const { data, error } = await supabase
      .from('global_stats_rollup')
      .select('*')
      .eq('rollup_key', rollupKey)
      .maybeSingle();

    if (error || !data) return null;

    return {
      rollup_key: data.rollup_key,
      total_users: data.total_users || 0,
      total_co2_saved_kg: parseFloat(data.total_co2_saved_kg || 0),
      total_actions: data.total_actions || 0,
      computed_at: data.computed_at,
    };
  } catch (error) {
    console.error('Error getting global stats rollup:', error);
    return null;
  }
}

/**
 * Calculate global statistics from existing tables (fallback)
 */
async function calculateGlobalStatisticsFromExisting(): Promise<GlobalStatistics | null> {
  try {
    // Try global_emissions table
    const { data: globalEmissions } = await supabase
      .from('global_emissions')
      .select('total_lbs_saved')
      .eq('id', 1)
      .maybeSingle();

    // Get user count
    const { count: userCount } = await supabase
      .from('profiles')
      .select('*', { count: 'exact', head: true });

    // Get total actions
    const { count: actionCount } = await supabase
      .from('user_actions')
      .select('*', { count: 'exact', head: true });

    // Convert lbs to kg
    const totalLbs = parseFloat(globalEmissions?.total_lbs_saved || '0');
    const totalKg = totalLbs * 0.453592;

    return {
      id: 1,
      total_users: userCount || 0,
      total_co2_saved_kg: totalKg,
      total_actions_count: actionCount || 0,
      countries_count: 0, // Would need location data
      last_updated: new Date().toISOString(),
    };
  } catch (error) {
    console.error('Error calculating global statistics:', error);
    return null;
  }
}

/**
 * Get leaderboard entries - supports production, previous, and legacy schemas
 * Priority: leaderboard_snapshots > leaderboards > calculated from stats
 */
export async function getLeaderboardEntries(
  period: 'all_time' | 'yearly' | 'monthly' | 'weekly' = 'all_time',
  scope: 'global' | 'country' = 'global',
  scopeId?: string,
  limit: number = 100
): Promise<LeaderboardEntry[]> {
  try {
    // Try production schema first: leaderboard_snapshots
    let query = supabase
      .from('leaderboard_snapshots')
      .select(`
        user_id,
        rank,
        co2_saved_kg,
        period,
        snapshot_time,
        users!inner(username)
      `)
      .eq('scope', scope)
      .eq('period', period)
      .order('rank', { ascending: true })
      .limit(limit);

    if (scope === 'country' && scopeId) {
      query = query.eq('scope_value', scopeId);
    } else if (scope === 'global') {
      query = query.eq('scope_value', 'global');
    }

    const { data: snapshotData, error: snapshotError } = await query;

    if (!snapshotError && snapshotData && snapshotData.length > 0) {
      return snapshotData.map((entry: any) => ({
        rank: entry.rank,
        user_id: entry.user_id,
        username: entry.users?.username || 'Anonymous',
        co2_saved_kg: parseFloat(entry.co2_saved_kg || 0),
        period: entry.period as any,
        updated_at: entry.snapshot_time,
      }));
    }

    // Try previous schema: leaderboards
    let prevQuery = supabase
      .from('leaderboards')
      .select(`
        user_id,
        rank,
        co2_saved_kg,
        period,
        updated_at,
        users!inner(username)
      `)
      .eq('scope', scope)
      .eq('period', period)
      .order('rank', { ascending: true })
      .limit(limit);

    if (scope === 'country' && scopeId) {
      prevQuery = prevQuery.eq('scope_id', scopeId);
    }

    const { data: prevData, error: prevError } = await prevQuery;

    if (!prevError && prevData && prevData.length > 0) {
      return prevData.map((entry: any) => ({
        rank: entry.rank,
        user_id: entry.user_id,
        username: entry.users?.username || 'Anonymous',
        co2_saved_kg: parseFloat(entry.co2_saved_kg || 0),
        period: entry.period as any,
        updated_at: entry.updated_at,
      }));
    }

    // Fallback: calculate from user_statistics or user_stats_summary
    return await calculateLeaderboardFromStats(period, limit);
  } catch (error) {
    console.error('Error getting leaderboard:', error);
    return await calculateLeaderboardFromStats(period, limit);
  }
}

/**
 * Calculate leaderboard from stats tables (fallback)
 * Tries: user_stats_summary > user_statistics > user_stats
 */
async function calculateLeaderboardFromStats(
  period: 'all_time' | 'yearly' | 'monthly' | 'weekly',
  limit: number
): Promise<LeaderboardEntry[]> {
  try {
    // Try production schema: user_stats_summary
    let statsQuery = supabase
      .from('user_stats_summary')
      .select(`
        user_id,
        total_co2_saved_kg,
        current_month_co2,
        current_year_co2,
        users!inner(username)
      `)
      .order('total_co2_saved_kg', { ascending: false })
      .limit(limit);

    let { data: stats, error } = await statsQuery;

    // If production schema fails, try previous schema
    if (error || !stats || stats.length === 0) {
      statsQuery = supabase
        .from('user_statistics')
        .select(`
          user_id,
          total_co2_saved_kg,
          current_month_co2_saved,
          current_year_co2_saved,
          users!inner(username)
        `)
        .order('total_co2_saved_kg', { ascending: false })
        .limit(limit);
      
      const result = await statsQuery;
      stats = result.data;
      error = result.error;
    }

    if (error || !stats) {
      // Final fallback: use existing leaderboard service
      const { getLeaderboard } = await import('./leaderboard');
      const entries = await getLeaderboard(limit);
      return entries.map((entry, index) => ({
        rank: index + 1,
        user_id: entry.user_id,
        username: entry.username,
        co2_saved_kg: entry.total_emissions_lbs * 0.453592, // Convert lbs to kg
        period: 'all_time' as const,
        updated_at: entry.last_update_timestamp,
      }));
    }

    // Determine which field to use based on period
    const getCo2Value = (stat: any) => {
      switch (period) {
        case 'yearly':
          // Support both schema field names
          return parseFloat(stat.current_year_co2 || stat.current_year_co2_saved || 0);
        case 'monthly':
          return parseFloat(stat.current_month_co2 || stat.current_month_co2_saved || 0);
        case 'weekly':
          // Would need weekly calculation - estimate from monthly
          const monthly = parseFloat(stat.current_month_co2 || stat.current_month_co2_saved || 0);
          return monthly / 4;
        default:
          return parseFloat(stat.total_co2_saved_kg || 0);
      }
    };

    return stats
      .map((stat: any, index: number) => ({
        rank: index + 1,
        user_id: stat.user_id,
        username: stat.users?.username || 'Anonymous',
        co2_saved_kg: getCo2Value(stat),
        period,
        updated_at: new Date().toISOString(),
      }))
      .sort((a, b) => b.co2_saved_kg - a.co2_saved_kg);
  } catch (error) {
    console.error('Error calculating leaderboard:', error);
    return [];
  }
}

/**
 * Get recent user action logs - supports production, previous, and legacy schemas
 * Priority: user_action_events > user_action_logs > user_actions
 */
export async function getRecentActionLogs(
  userId?: string,
  limit: number = 50
): Promise<UserActionLog[]> {
  try {
    // Try production schema first: user_action_events
    let query = supabase
      .from('user_action_events')
      .select('*')
      .order('event_timestamp', { ascending: false })
      .limit(limit);

    if (userId) {
      query = query.eq('user_id', userId);
    }

    const { data: eventData, error: eventError } = await query;

    if (!eventError && eventData && eventData.length > 0) {
      return eventData.map((event: any) => ({
        log_id: event.event_id,
        user_id: event.user_id,
        action_id: event.action_id,
        quantity: parseFloat(event.quantity || 1),
        co2_saved_kg: parseFloat(event.co2_saved_kg || 0),
        logged_at: event.event_timestamp,
        action_date: event.action_date,
        notes: null, // Production schema doesn't have notes
        location: null, // Production schema doesn't have location
      }));
    }

    // Try previous schema: user_action_logs
    let prevQuery = supabase
      .from('user_action_logs')
      .select('*')
      .order('logged_at', { ascending: false })
      .limit(limit);

    if (userId) {
      prevQuery = prevQuery.eq('user_id', userId);
    }

    const { data: logData, error: logError } = await prevQuery;

    if (!logError && logData && logData.length > 0) {
      return logData.map((log: any) => ({
        log_id: log.log_id,
        user_id: log.user_id,
        action_id: log.action_id,
        quantity: parseFloat(log.quantity || 1),
        co2_saved_kg: parseFloat(log.co2_saved_kg || 0),
        logged_at: log.logged_at,
        action_date: log.action_date,
        notes: log.notes,
        location: log.location,
      }));
    }

    // Fallback: use existing user_actions
    return await getRecentActionsFromOldSchema(userId, limit);
  } catch (error) {
    console.error('Error getting recent action logs:', error);
    return await getRecentActionsFromOldSchema(userId, limit);
  }
}

/**
 * Get recent actions from old schema (fallback)
 */
async function getRecentActionsFromOldSchema(
  userId?: string,
  limit: number
): Promise<UserActionLog[]> {
  try {
    let query = supabase
      .from('user_actions')
      .select('id, user_id, custom_emissions_saved, logged_at, action_templates(emissions_saved)')
      .order('logged_at', { ascending: false })
      .limit(limit);

    if (userId) {
      query = query.eq('user_id', userId);
    }

    const { data, error } = await query;

    if (error || !data) return [];

    return data.map((action: any, index: number) => {
      const emissionsLbs = action.custom_emissions_saved || 
        ((action.action_templates?.emissions_saved || 0) * 2.20462);
      const emissionsKg = emissionsLbs * 0.453592;

      return {
        log_id: index + 1, // Temporary ID
        user_id: action.user_id,
        action_id: 0, // Would need mapping
        quantity: 1,
        co2_saved_kg: emissionsKg,
        logged_at: action.logged_at,
        action_date: action.logged_at.split('T')[0],
        notes: null,
        location: null,
      };
    });
  } catch (error) {
    console.error('Error getting recent actions:', error);
    return [];
  }
}

/**
 * Convert kg to lbs for display
 */
export function kgToLbs(kg: number): number {
  return kg * 2.20462;
}

/**
 * Convert lbs to kg
 */
export function lbsToKg(lbs: number): number {
  return lbs * 0.453592;
}

