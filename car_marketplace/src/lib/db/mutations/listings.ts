// ============================================
// LISTING MUTATIONS - Supabase Integration
// ============================================

import { supabase } from '@/lib/db/supabase-client'

export interface CreateListingInput {
  user_id: string;
  brand: string;
  model: string;
  year: number;
  price: number;
  original_price?: number | null;
  mileage?: number | null;
  transmission?: string;
  fuel_type?: string;
  body_type?: string;
  color?: string;
  location: string;
  description?: string;
  images?: string[];
  slug: string;
  status?: string;
}

export interface UpdateListingInput {
  brand?: string;
  model?: string;
  year?: number;
  price?: number;
  status?: 'pending' | 'approved' | 'rejected';
  [key: string]: any;
}

export async function insertListing(listingData: CreateListingInput) {
  const { data, error } = await supabase
    .from('listings')
    .insert(listingData)
    .select()
    .single();

  return { data, error };
}

export async function updateListing(id: string, updates: UpdateListingInput) {
  const { data, error } = await supabase
    .from('listings')
    .update({ ...updates, updated_at: new Date().toISOString() })
    .eq('id', id)
    .select()
    .single();

  return { data, error };
}

export async function deleteListing(id: string) {
  const { error } = await supabase
    .from('listings')
    .delete()
    .eq('id', id);

  return { error };
}
