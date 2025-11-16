import { useEffect, useState } from 'react';
import { useAuth } from '../contexts/AuthContext';
import { supabase, UserAction, ActionTemplate } from '../lib/supabase';
import { TrendingDown, Flame, Target, Globe, BarChart3 } from 'lucide-react';
import { getUserTotal, getGlobalTotal } from '../services/simpleEmissionLogging';
import { TREE_EQUIVALENT_LBS_PER_TREE } from '../constants/emissionFactors';

const kgToLbs = (kg: number): number => kg * 2.20462;

export default function Dashboard() {
  const { profile } = useAuth();
  const [actions, setActions] = useState<UserAction[]>([]);
  const [loading, setLoading] = useState(true);
  const [stats, setStats] = useState({
    totalEmissionsSaved: 0,
    actionsThisMonth: 0,
    currentStreak: 0,
    globalTotalLbs: 0,
    monthlyCo2Kg: 0,
    yearlyCo2Kg: 0,
  });

  useEffect(() => {
    loadDashboardData();
    
    // Subscribe to real-time updates
    const channel = supabase
      .channel('dashboard-updates')
      .on(
        'postgres_changes',
        {
          event: '*',
          schema: 'public',
          table: 'user_stats',
          filter: `user_id=eq.${profile?.id}`,
        },
        () => {
          loadDashboardData();
        }
      )
      .on(
        'postgres_changes',
        {
          event: '*',
          schema: 'public',
          table: 'global_emissions',
        },
        () => {
          loadDashboardData();
        }
      )
      .subscribe();

    return () => {
      supabase.removeChannel(channel);
    };
  }, [profile]);

  const loadDashboardData = async () => {
    if (!profile) return;

    try {
      const [userTotal, globalTotal, actionsResult] = await Promise.all([
        getUserTotal(profile.id),
        getGlobalTotal(),
        supabase
          .from('user_actions')
          .select(`
            *,
            action_templates (*)
          `)
          .eq('user_id', profile.id)
          .order('logged_at', { ascending: false })
          .limit(10),
      ]);

      const { data: actionsData } = actionsResult;

      if (actionsData) {
        setActions(actionsData);
        const thisMonth = new Date();
        thisMonth.setDate(1);
        thisMonth.setHours(0, 0, 0, 0);

        const monthActions = actionsData.filter(
          (a) => new Date(a.logged_at) >= thisMonth
        );

        setStats({
          totalEmissionsSaved: userTotal,
          actionsThisMonth: monthActions.length,
          currentStreak: profile.current_streak,
          globalTotalLbs: globalTotal,
          monthlyCo2Kg: (monthActions.reduce((sum, a) => sum + (a.custom_emissions_saved || 0), 0)) * 0.453592,
          yearlyCo2Kg: userTotal * 0.453592,
        });
      } else {
        setStats({
          totalEmissionsSaved: userTotal,
          actionsThisMonth: 0,
          currentStreak: profile.current_streak,
          globalTotalLbs: globalTotal,
          monthlyCo2Kg: 0,
          yearlyCo2Kg: userTotal * 0.453592,
        });
      }
    } catch (error) {
      console.error('Error loading dashboard data:', error);
      setStats({
        totalEmissionsSaved: 0,
        actionsThisMonth: 0,
        currentStreak: profile.current_streak,
        globalTotalLbs: 0,
        monthlyCo2Kg: 0,
        yearlyCo2Kg: 0,
      });
    } finally {
      setLoading(false);
    }
  };

  if (loading) {
    return (
      <div className="flex justify-center items-center h-64">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-emerald-600"></div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold text-gray-900">
          Welcome back, {profile?.full_name}!
        </h1>
        <p className="text-gray-600 mt-1">
          Track your progress and continue making an impact
        </p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        <div className="bg-gradient-to-br from-emerald-500 to-teal-600 p-6 rounded-xl shadow-lg text-white">
          <div className="flex items-center justify-between mb-4">
            <TrendingDown className="w-8 h-8" />
            <span className="text-sm font-medium bg-white/20 px-3 py-1 rounded-full">
              Total Impact
            </span>
          </div>
          <div className="space-y-1">
            <div className="text-4xl font-bold">
              {stats.totalEmissionsSaved.toFixed(1)} lbs
            </div>
            <p className="text-emerald-100 text-sm">
              {(stats.yearlyCo2Kg).toFixed(2)} kg CO₂ saved
            </p>
          </div>
        </div>

        <div className="bg-gradient-to-br from-teal-500 to-cyan-600 p-6 rounded-xl shadow-lg text-white">
          <div className="flex items-center justify-between mb-4">
            <Flame className="w-8 h-8" />
            <span className="text-sm font-medium bg-white/20 px-3 py-1 rounded-full">
              Streak
            </span>
          </div>
          <div className="space-y-1">
            <div className="text-4xl font-bold">
              {stats.currentStreak}
            </div>
            <p className="text-teal-100 text-sm">days in a row</p>
          </div>
        </div>

        <div className="bg-gradient-to-br from-blue-500 to-indigo-600 p-6 rounded-xl shadow-lg text-white">
          <div className="flex items-center justify-between mb-4">
            <BarChart3 className="w-8 h-8" />
            <span className="text-sm font-medium bg-white/20 px-3 py-1 rounded-full">
              This Month
            </span>
          </div>
          <div className="space-y-1">
            <div className="text-4xl font-bold">
              {kgToLbs(stats.monthlyCo2Kg).toFixed(1)} lbs
            </div>
            <p className="text-blue-100 text-sm">
              {stats.monthlyCo2Kg.toFixed(2)} kg CO₂
            </p>
            <p className="text-blue-100 text-xs mt-2">
              {stats.actionsThisMonth} actions
            </p>
          </div>
        </div>

        <div className="bg-gradient-to-br from-purple-500 to-pink-600 p-6 rounded-xl shadow-lg text-white">
          <div className="flex items-center justify-between mb-4">
            <Target className="w-8 h-8" />
            <span className="text-sm font-medium bg-white/20 px-3 py-1 rounded-full">
              This Year
            </span>
          </div>
          <div className="space-y-1">
            <div className="text-4xl font-bold">
              {kgToLbs(stats.yearlyCo2Kg).toFixed(1)} lbs
            </div>
            <p className="text-purple-100 text-sm">
              {stats.yearlyCo2Kg.toFixed(2)} kg CO₂
            </p>
            {profile?.monthly_goal && (
              <p className="text-purple-100 text-xs mt-2">
                Goal: {profile.monthly_goal * 12} lbs/year
              </p>
            )}
          </div>
        </div>
      </div>

      {/* Global Community Impact Card */}
      {stats.globalTotalLbs > 0 && (
        <div className="bg-gradient-to-r from-emerald-500 via-teal-500 to-cyan-600 rounded-xl shadow-lg p-6 text-white">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
            <div className="flex items-center gap-4">
              <div className="bg-white/20 p-4 rounded-xl">
                <Globe className="w-8 h-8" />
              </div>
              <div>
                <h2 className="text-lg font-semibold mb-1">Global Impact</h2>
                <p className="text-emerald-100 text-sm">Community efforts</p>
              </div>
            </div>
            <div className="text-center md:text-left">
              <div className="text-3xl font-bold mb-1">
                {stats.globalTotalLbs.toLocaleString(undefined, { maximumFractionDigits: 0 })} lbs
              </div>
              <p className="text-emerald-100 text-sm">CO₂ saved globally</p>
              <p className="text-emerald-100 text-xs mt-1">
                {Math.floor(stats.globalTotalLbs / TREE_EQUIVALENT_LBS_PER_TREE).toLocaleString()} trees equivalent
              </p>
            </div>
            <div className="text-center md:text-right">
              <div className="text-2xl font-bold mb-1">
                {stats.globalTotalLbs > 0 ? ((stats.totalEmissionsSaved / stats.globalTotalLbs) * 100).toFixed(2) : '0'}%
              </div>
              <p className="text-emerald-100 text-sm">Your contribution</p>
              <p className="text-emerald-100 text-xs mt-1">
                {stats.totalEmissionsSaved.toFixed(1)} lbs of {stats.globalTotalLbs.toLocaleString(undefined, { maximumFractionDigits: 0 })} lbs
              </p>
            </div>
          </div>
          <div className="mt-4 pt-4 border-t border-white/20">
            <p className="text-emerald-100 text-sm text-center">
              See detailed global statistics and leaderboards on the Global Impact page
            </p>
          </div>
        </div>
      )}

      <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
        <h2 className="text-xl font-bold text-gray-900 mb-4">Recent Actions</h2>
        {actions.length === 0 ? (
          <p className="text-gray-600 text-center py-8">
            No actions yet. Start logging your climate actions!
          </p>
        ) : (
          <div className="space-y-3">
            {actions.map((action) => {
              const template = action.action_templates as ActionTemplate;
              return (
                <div
                  key={action.id}
                  className="flex items-center justify-between p-4 bg-gradient-to-r from-emerald-50 to-teal-50 rounded-lg border border-emerald-100"
                >
                  <div className="flex items-center space-x-4">
                    <span className="text-2xl">{template?.icon || '🌱'}</span>
                    <div>
                      <h3 className="font-semibold text-gray-900">
                        {action.custom_title || template?.title}
                      </h3>
                      <p className="text-sm text-gray-600">
                        {new Date(action.logged_at).toLocaleDateString()}
                      </p>
                    </div>
                  </div>
                  <div className="text-right">
                    <div className="font-bold text-emerald-600">
                      {((action.custom_emissions_saved || (template?.emissions_saved || 0) * 2.20462)).toFixed(1)} lbs CO₂
                    </div>
                    <div className="text-sm text-teal-600">
                      +{template?.points_reward || 10} pts
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
}
