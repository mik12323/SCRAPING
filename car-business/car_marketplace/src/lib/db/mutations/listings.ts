// ============================================
// LISTING MUTATIONS - Supabase Integration
// ============================================

import { supabase } from '@/lib/db/supabase-client'

// Helper to extract file path from Supabase storage URL
function getStoragePathFromUrl(url: string): string | null {
  // Match patterns like: /storage/v1/object/public/car-images/filename.jpg
  const match = url.match(/\/storage\/v1\/object\/public\/car-images\/(.+)$/);
  return match ? match[1] : null;
}

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
  // First, get the listing to retrieve image URLs
  const { data: listing, error: fetchError } = await supabase
    .from('listings')
    .select('images')
    .eq('id', id)
    .single();

  if (fetchError) {
    return { error: fetchError };
  }

  // Delete images from storage if they exist
  if (listing?.images && listing.images.length > 0) {
    const filePaths = listing.images
      .map((url: string) => getStoragePathFromUrl(url))
      .filter((path: string | null): path is string => path !== null);

    if (filePaths.length > 0) {
      const { error: storageError } = await supabase
        .storage
        .from('car-images')
        .remove(filePaths);

      if (storageError) {
        console.error('Error deleting images from storage:', storageError);
        // Continue with listing deletion even if image deletion fails
      }
    }
  }

  // Delete the listing record
  const { error } = await supabase
    .from('listings')
    .delete()
    .eq('id', id);

  return { error };
}
