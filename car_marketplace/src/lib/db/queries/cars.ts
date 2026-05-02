// =========================================
// CAR QUERIES - Supabase Integration
// =========================================

import type { Car, CarFilters, PaginatedResponse, TrendingCar } from '@/lib/types';
import { supabase } from '@/lib/db/supabase-client';

// Supabase listing table structure
interface SupabaseListing {
  id: string;
  slug: string;
  brand: string;
  model: string;
  year: number;
  price: number;
  original_price?: number;
  body_type?: string;
  images?: string[];
  status: string;
}

// Helper: Transform Supabase listing to Car type
function transformListing(listing: any): Car {
  return {
    id: listing.id,
    slug: listing.slug,
    user_id: listing.user_id,
    brand: listing.brand,
    model: listing.model,
    year: listing.year,
    price: listing.price,
    originalPrice: listing.original_price,
    original_price: listing.original_price,
    bodyType: listing.body_type || '',
    fuelType: listing.fuel_type || '',
    transmission: listing.transmission || '',
    location: listing.location || '',
    description: listing.description || '',
    images: listing.images || [],
    thumbnail: listing.images?.[0] || '',
    status: listing.status as 'pending' | 'approved' | 'rejected',
    createdAt: listing.created_at,
    updatedAt: listing.updated_at,
    created_at: listing.created_at,
    updated_at: listing.updated_at,
  };
}

export async function getCars(filters?: CarFilters, page: number = 1, perPage: number = 12): Promise<PaginatedResponse> {
  try {
    // Add small delay to allow any pending auth operations to complete
    await new Promise(resolve => setTimeout(resolve, 100));

    let query = supabase
      .from('listings')
      .select('id, slug, brand, model, year, price, original_price, body_type, images, status', { count: 'exact' })
      .eq('status', 'approved');

    if (filters?.brand) {
      query = query.ilike('brand', `%${filters.brand}%`);
    }

    if (filters?.model) {
      query = query.ilike('model', `%${filters.model}%`);
    }

    if (filters?.bodyType && filters.bodyType !== 'All') {
      query = query.eq('body_type', filters.bodyType);
    }

    if (filters?.fuelType && filters.fuelType !== 'All') {
      query = query.eq('fuel_type', filters.fuelType);
    }

    if (filters?.transmission && filters.transmission !== 'All') {
      query = query.eq('transmission', filters.transmission);
    }

    if (filters?.priceMin) {
      query = query.gte('price', filters.priceMin);
    }

    if (filters?.priceMax) {
      query = query.lte('price', filters.priceMax);
    }

    if (filters?.year) {
      query = query.eq('year', filters.year);
    }

    if (filters?.sortBy) {
      switch (filters.sortBy) {
        case 'alphabetical':
          query = query.order('brand', { ascending: true }).order('model', { ascending: true });
          break;
        case 'reverse_alphabetical':
          query = query.order('brand', { ascending: false }).order('model', { ascending: false });
          break;
        case 'price_low':
          query = query.order('price', { ascending: true });
          break;
        case 'price_high':
          query = query.order('price', { ascending: false });
          break;
        case 'newest':
          query = query.order('year', { ascending: false });
          break;
        case 'oldest':
          query = query.order('year', { ascending: true });
          break;
      }
    } else {
      query = query.order('brand', { ascending: true }).order('model', { ascending: true });
    }

    const from = (page - 1) * perPage;
    const to = from + perPage - 1;
    query = query.range(from, to);

    const { data, error, count } = await query;

    if (error) {
      console.error('Supabase error in getCars:', error);
      return { cars: [], total: 0, totalPages: 0, currentPage: page };
    }

    const cars = (data || []).map((item: any) => transformListing(item));
    return {
      cars,
      total: count || 0,
      totalPages: Math.ceil((count || 0) / perPage),
      currentPage: page
    };
  } catch (err) {
    console.error('Error in getCars:', err);
    return { cars: [], total: 0, totalPages: 0, currentPage: page };
  }
}

export async function getCarBySlug(slug: string): Promise<Car | undefined> {
  try {
    const { data, error } = await supabase
      .from('listings')
      .select('id, slug, user_id, brand, model, year, price, original_price, body_type, fuel_type, transmission, location, description, images, status, created_at, updated_at')
      .eq('slug', slug)
      .eq('status', 'approved')
      .single();

    if (error || !data) {
      console.error('Supabase error in getCarBySlug - slug:', slug, 'error:', JSON.stringify(error));
      return undefined;
    }

    return transformListing(data as SupabaseListing);
  } catch (err: any) {
    console.error('Error in getCarBySlug:', err?.message || err);
    return undefined;
  }
}

export async function getFeaturedCars(): Promise<Car[]> {
  try {
    const { data, error } = await supabase
      .from('listings')
      .select('id, slug, brand, model, year, price, images')
      .eq('status', 'approved')
      .order('created_at', { ascending: false })
      .limit(6);

    if (error) {
      console.error('Supabase error in getFeaturedCars:', error);
      return [];
    }

    return (data || []).map((item: any) => transformListing(item));
  } catch (err) {
    console.error('Error in getFeaturedCars:', err);
    return [];
  }
}

export async function getTrendingCars(limit: number = 5): Promise<TrendingCar[]> {
  try {
    const { data, error } = await supabase
      .from('listings')
      .select('brand, model, year, body_type, fuel_type, transmission')
      .eq('status', 'approved')
      .order('created_at', { ascending: false })
      .limit(limit);

    if (error) {
      console.error('Supabase error in getTrendingCars:', error);
      return [];
    }

    return (data || []).map((car: { brand: string; model: string; year?: number; body_type?: string; fuel_type?: string; transmission?: string }) => ({
      brand: car.brand,
      model: car.model,
      displayName: `${car.year ? car.year + ' ' : ''}${car.brand} ${car.model}`,
      clicks: 0,
      bodyType: car.body_type || '',
      fuelType: car.fuel_type || '',
      transmission: car.transmission || ''
    }));
  } catch (err) {
    console.error('Error in getTrendingCars:', err);
    return [];
  }
}

export async function getBrands(): Promise<string[]> {
  try {
    const { data, error } = await supabase
      .from('listings')
      .select('brand')
      .eq('status', 'approved')
      .order('brand');

    if (error) {
      console.error('Supabase error in getBrands:', error);
      return [];
    }

    // Get unique brands from sorted results
    const brands = [...new Set((data || []).map((item: { brand: string }) => item.brand))];
    return brands;
  } catch (err) {
    console.error('Error in getBrands:', err);
    return [];
  }
}

export async function getModelsByBrand(brand: string): Promise<string[]> {
  try {
    const { data, error } = await supabase
      .from('listings')
      .select('model')
      .eq('brand', brand)
      .eq('status', 'approved')
      .order('model');

    if (error) {
      console.error('Supabase error in getModelsByBrand:', error);
      return [];
    }

    const models = [...new Set((data || []).map((item: { model: string }) => item.model))];
    return models;
  } catch (err) {
    console.error('Error in getModelsByBrand:', err);
    return [];
  }
}

export async function getRelatedCars(carId: string, limit: number = 3): Promise<Car[]> {
  try {
    const { data: currentCar, error: carError } = await supabase
      .from('listings')
      .select('brand, body_type')
      .eq('id', carId)
      .single();

    if (carError || !currentCar) {
      return [];
    }

    const { data, error } = await supabase
      .from('listings')
      .select('id, slug, brand, model, year, price, images')
      .neq('id', carId)
      .eq('status', 'approved')
      .or(`brand.eq.${currentCar.brand},body_type.eq.${currentCar.body_type}`)
      .limit(limit);

    if (error) {
      console.error('Supabase error in getRelatedCars:', error);
      return [];
    }

    return (data || []).map((item: any) => transformListing(item));
  } catch (err) {
    console.error('Error in getRelatedCars:', err);
    return [];
  }
}
