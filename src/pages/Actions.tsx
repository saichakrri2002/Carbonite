import { useEffect, useState } from 'react';
import { useAuth } from '../contexts/AuthContext';
import { supabase, ActionTemplate } from '../lib/supabase';
import { Plus, Check } from 'lucide-react';
import AIActionParser from '../components/AIActionParser';

export default function Actions() {
  const { profile, refreshProfile } = useAuth();
  const [actions, setActions] = useState<ActionTemplate[]>([]);
  const [loading, setLoading] = useState(true);
  const [selectedCategory, setSelectedCategory] = useState<string>('All');
  const [logging, setLogging] = useState<string | null>(null);

  const categories = ['All', 'Transportation', 'Home', 'Food', 'Materials', 'Water', 'Waste'];

  useEffect(() => {
    loadActions();
  }, []);

  const loadActions = async () => {
    try {
      const { data } = await supabase
        .from('action_templates')
        .select('*')
        .eq('is_active', true)
        .order('category');

      if (data) setActions(data);
    } catch (error) {
      console.error('Error loading actions:', error);
    } finally {
      setLoading(false);
    }
  };

  const logAction = async (actionId: string, points: number, emissions: number) => {
    if (!profile) {
      alert('Please log in first');
      return;
    }

    setLogging(actionId);
    try {
      // Convert emissions from kg to lbs for storage
      const emissionsLbs = emissions * 2.20462;
      const { error } = await supabase.from('user_actions').insert({
        user_id: profile.id,
        action_template_id: actionId,
        custom_emissions_saved: emissionsLbs,
      });

      if (error) {
        console.error('Action insert error:', error);
        throw new Error(`Failed to log action: ${error.message}`);
      }

      const { error: updateError } = await supabase
        .from('profiles')
        .update({
          total_points: profile.total_points + points,
          updated_at: new Date().toISOString(),
        })
        .eq('id', profile.id);

      if (updateError) {
        console.error('Profile update error:', updateError);
      }

      await refreshProfile();
      alert('Action logged successfully!');
    } catch (error) {
      const errorMsg = error instanceof Error ? error.message : 'Unknown error';
      console.error('Error logging action:', errorMsg);
      alert(`Failed to log action: ${errorMsg}`);
    } finally {
      setLogging(null);
    }
  };

  const filteredActions = selectedCategory === 'All'
    ? actions
    : actions.filter((a) => a.category === selectedCategory);

  const getDifficultyColor = (level: string) => {
    switch (level) {
      case 'Easy': return 'bg-green-100 text-green-700';
      case 'Medium': return 'bg-yellow-100 text-yellow-700';
      case 'Hard': return 'bg-red-100 text-red-700';
      default: return 'bg-gray-100 text-gray-700';
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
        <h1 className="text-3xl font-bold text-gray-900">Climate Actions</h1>
        <p className="text-gray-600 mt-1">
          Choose actions to reduce your carbon footprint
        </p>
      </div>

      <AIActionParser onActionLogged={loadActions} />

      <div className="flex overflow-x-auto space-x-2 pb-2">
        {categories.map((category) => (
          <button
            key={category}
            onClick={() => setSelectedCategory(category)}
            className={`px-4 py-2 rounded-lg font-medium whitespace-nowrap transition-all ${
              selectedCategory === category
                ? 'bg-gradient-to-r from-emerald-500 to-teal-500 text-white shadow-md'
                : 'bg-white text-gray-700 hover:bg-emerald-50 border border-gray-200'
            }`}
          >
            {category}
          </button>
        ))}
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        {filteredActions.map((action) => (
          <div
            key={action.id}
            className="bg-white rounded-xl shadow-sm border border-gray-200 p-6 hover:shadow-md transition-shadow"
          >
            <div className="flex items-start justify-between mb-4">
              <span className="text-4xl">{action.icon || '🌱'}</span>
              <span className={`px-3 py-1 rounded-full text-xs font-medium ${getDifficultyColor(action.difficulty_level)}`}>
                {action.difficulty_level}
              </span>
            </div>

            <h3 className="text-xl font-bold text-gray-900 mb-2">{action.title}</h3>
            <p className="text-sm text-gray-600 mb-4">{action.description}</p>

            <div className="flex items-center justify-between text-sm mb-4">
              <div>
                <span className="text-gray-500">Saves:</span>
                <span className="ml-1 font-semibold text-emerald-600">
                  {(action.emissions_saved * 2.20462).toFixed(1)} lbs CO₂
                </span>
              </div>
              <div>
                <span className="text-gray-500">Earns:</span>
                <span className="ml-1 font-semibold text-teal-600">
                  {action.points_reward} pts
                </span>
              </div>
            </div>

            <div className="flex items-center justify-between text-xs text-gray-500 mb-4">
              <span>{action.time_commitment}</span>
              <span>{action.cost_impact}</span>
            </div>

            <button
              onClick={() => logAction(action.id, action.points_reward, action.emissions_saved)}
              disabled={logging === action.id}
              className="w-full bg-gradient-to-r from-emerald-500 to-teal-500 text-white py-3 rounded-lg font-semibold hover:from-emerald-600 hover:to-teal-600 transition-all shadow-md hover:shadow-lg disabled:opacity-50 flex items-center justify-center space-x-2"
            >
              {logging === action.id ? (
                <>
                  <div className="animate-spin rounded-full h-5 w-5 border-b-2 border-white"></div>
                  <span>Logging...</span>
                </>
              ) : (
                <>
                  <Check className="w-5 h-5" />
                  <span>Log Action</span>
                </>
              )}
            </button>
          </div>
        ))}
      </div>
    </div>
  );
}
