/**
 * Simple Emission Logging Service
 * Works with existing user_actions table structure
 */

import { supabase } from '../lib/supabase';

/**
 * Get user total from user_stats table
 */
export async function getUserTotal(userId: string): Promise<number> {
  try {
    // Try database function first
    const { data: rpcData, error: rpcError } = await supabase.rpc('get_user_total', {
      p_user_id: userId,
    });

    if (!rpcError && rpcData !== null) {
      return parseFloat(rpcData || '0');
    }

    // Fallback: query user_stats table directly
    // Support both total_lbs and total_emissions_lbs column names
    const { data, error } = await supabase
      .from('user_stats')
      .select('total_lbs, total_emissions_lbs')
      .eq('user_id', userId)
      .maybeSingle();

    if (error || !data) {
      // If table doesn't exist, calculate from user_actions
      return await calculateUserTotalFromActions(userId);
    }

    // Use total_lbs if available, otherwise use total_emissions_lbs
    const userTotal = data?.total_lbs ?? data?.total_emissions_lbs ?? 0;
    return parseFloat(userTotal.toString() || '0');
  } catch (error) {
    console.error('Error getting user total:', error);
    return await calculateUserTotalFromActions(userId);
  }
}

/**
 * Calculate user total from user_actions (fallback)
 */
async function calculateUserTotalFromActions(userId: string): Promise<number> {
  try {
    const { data, error } = await supabase
      .from('user_actions')
      .select('custom_emissions_saved, action_template_id, action_templates(emissions_saved)')
      .eq('user_id', userId);

    if (error) return 0;

    let total = 0;
    (data || []).forEach((action: any) => {
      if (action.custom_emissions_saved) {
        total += action.custom_emissions_saved;
      } else if (action.action_templates?.emissions_saved) {
        // Convert kg to lbs
        total += action.action_templates.emissions_saved * 2.20462;
      }
    });

    return total;
  } catch (error) {
    console.error('Error calculating from actions:', error);
    return 0;
  }
}

/**
 * Get global total from global_emissions table
 * Returns the same global total for all users
 * Formula: SUM(user_stats.total_lbs) = global_emissions.total_lbs
 */
export async function getGlobalTotal(): Promise<number> {
  try {
    // Try database function first
    const { data: rpcData, error: rpcError } = await supabase.rpc('get_global_total');

    if (!rpcError && rpcData !== null) {
      return parseFloat(rpcData || '0');
    }

    // Fallback: query global_emissions table directly
    // Support both total_lbs and total_lbs_saved column names
    const { data, error } = await supabase
      .from('global_emissions')
      .select('total_lbs, total_lbs_saved')
      .eq('id', 1)
      .maybeSingle();

    if (error || !data) {
      // If table doesn't exist, calculate from user_stats
      return await calculateGlobalTotalFromUserStats();
    }

    // Use total_lbs if available, otherwise use total_lbs_saved
    const total = data?.total_lbs ?? data?.total_lbs_saved ?? 0;
    return parseFloat(total.toString() || '0');
  } catch (error) {
    console.error('Error getting global total:', error);
    return await calculateGlobalTotalFromUserStats();
  }
}

/**
 * Calculate global total from user_stats (fallback)
 * Formula: SUM(user_stats.total_lbs) = global_emissions.total_lbs
 */
async function calculateGlobalTotalFromUserStats(): Promise<number> {
  try {
    const { data, error } = await supabase
      .from('user_stats')
      .select('total_lbs, total_emissions_lbs');

    if (error) return 0;

    const total = (data || []).reduce((sum: number, stat: any) => {
      // Use total_lbs if available, otherwise use total_emissions_lbs
      const userTotal = stat.total_lbs ?? stat.total_emissions_lbs ?? 0;
      return sum + (parseFloat(userTotal.toString() || '0') || 0);
    }, 0);

    return total;
  } catch (error) {
    console.error('Error calculating from user_stats:', error);
    return 0;
  }
}

/**
 * Recompute all totals from source of truth
 */
export async function recomputeAllTotals(): Promise<{ globalTotal: number }> {
  try {
    const { data, error } = await supabase.rpc('recompute_all_totals');

    if (error) throw error;

    // Get global total after recomputation
    const globalTotal = await getGlobalTotal();

    return { globalTotal };
  } catch (error) {
    console.error('Error recomputing totals:', error);
    throw error;
  }
}

