// ============================================
// CLICK TRACKING QUERIES - Supabase Integration
// ============================================

import { supabase } from '@/lib/db/supabase-client';

export interface ClickData {
  car_id: string;
  user_id?: string | null;
  ip_address?: string;
  referrer?: string;
}

export async function trackCarView(carId: string, userId?: string | null) {
  try {
    // Call Supabase RPC function to track the view
    const { error } = await supabase
      .rpc('track_car_view', { car_id: carId });

    if (error) {
      console.error('Error tracking car view:', error);
    }
  } catch (err) {
    console.error('Error in trackCarView:', err);
  }
}

export async function getTrendingCars(limit: number = 5) {
  try {
    const { data, error } = await supabase
      .rpc('get_trending_cars', { limit_count: limit });

    if (error) {
      console.error('Error getting trending cars:', error);
      return [];
    }

    // Map snake_case to camelCase
    return (data || []).map((car: any) => ({
      brand: car.brand,
      model: car.model,
      displayName: car.display_name || `${car.brand} ${car.model}`,
      clicks: car.clicks || 0,
      bodyType: car.body_type || '',
      fuelType: car.fuel_type || '',
      transmission: car.transmission || ''
    }));
  } catch (err) {
    console.error('Error in getTrendingCars:', err);
    return [];
  }
}
