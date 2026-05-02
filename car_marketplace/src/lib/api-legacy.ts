// =========================================
// LEGACY API - Mock Data Implementations
// =========================================
// This file is now ONLY used as fallback if Supabase is not configured
// Will be removed once Supabase is fully tested.

import { Car, CarFilters, PaginatedResponse, TrendingCar } from './types';
import { mockCars, mockTrendingCars, featuredCars } from './mock-data';

// Helper function to simulate API delay (remove when connecting to real backend)
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

// Check if Supabase is configured
const isSupabaseConfigured = () => {
  return process.env.NEXT_PUBLIC_SUPABASE_URL && process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY;
};

// Get all approved cars with optional filters
export async function getCars(filters?: CarFilters, page: number = 1, perPage: number = 12): Promise<PaginatedResponse> {
  if (isSupabaseConfigured()) {
    // This should not be called if Supabase is configured
    console.warn('api-legacy.ts called but Supabase is configured. Use db/queries/cars.ts instead.');
  }

  await delay(100); // Simulate network delay

  let filteredCars = [...mockCars].filter(car => car.status === 'approved');

  // Apply filters
  if (filters?.brand) {
    filteredCars = filteredCars.filter(car =>
      car.brand.toLowerCase().includes(filters.brand!.toLowerCase())
    );
  }

  if (filters?.model) {
    filteredCars = filteredCars.filter(car =>
      car.model.toLowerCase().includes(filters.model!.toLowerCase())
    );
  }

  if (filters?.bodyType && filters.bodyType !== 'All') {
    filteredCars = filteredCars.filter(car => car.bodyType === filters.bodyType);
  }

  if (filters?.fuelType && filters.fuelType !== 'All') {
    filteredCars = filteredCars.filter(car => car.fuelType === filters.fuelType);
  }

  if (filters?.transmission && filters.transmission !== 'All') {
    filteredCars = filteredCars.filter(car => car.transmission === filters.transmission);
  }

  if (filters?.priceMin) {
    filteredCars = filteredCars.filter(car => car.price >= filters.priceMin!);
  }

  if (filters?.priceMax) {
    filteredCars = filteredCars.filter(car => car.price <= filters.priceMax!);
  }

  if (filters?.year) {
    filteredCars = filteredCars.filter(car => car.year === filters.year);
  }

  // Apply sorting
  if (filters?.sortBy) {
    switch (filters.sortBy) {
      case 'alphabetical':
        filteredCars.sort((a, b) => a.brand.localeCompare(b.brand) || a.model.localeCompare(b.model));
        break;
      case 'reverse_alphabetical':
        filteredCars.sort((a, b) => b.brand.localeCompare(a.brand) || b.model.localeCompare(a.model));
        break;
      case 'price_low':
        filteredCars.sort((a, b) => a.price - b.price);
        break;
      case 'price_high':
        filteredCars.sort((a, b) => b.price - a.price);
        break;
      case 'newest':
        filteredCars.sort((a, b) => b.year - a.year);
        break;
      case 'oldest':
        filteredCars.sort((a, b) => a.year - b.year);
        break;
    }
  } else {
    // Default: alphabetical
    filteredCars.sort((a, b) => a.brand.localeCompare(b.brand) || a.model.localeCompare(b.model));
  }

  // Pagination
  const total = filteredCars.length;
  const totalPages = Math.ceil(total / perPage);
  const start = (page - 1) * perPage;
  const end = start + perPage;
  const paginatedCars = filteredCars.slice(start, end);

  return {
    cars: paginatedCars,
    total,
    totalPages,
    currentPage: page
  };
}

// Get single car by slug
export async function getCarBySlug(slug: string): Promise<Car | undefined> {
  await delay(100);
  return mockCars.find(car => car.slug === slug && car.status === 'approved');
}

// Get featured cars (for homepage)
export async function getFeaturedCars(): Promise<Car[]> {
  await delay(100);
  return featuredCars;
}

// Get trending cars
export async function getTrendingCars(limit: number = 5): Promise<TrendingCar[]> {
  await delay(100);
  return mockTrendingCars.slice(0, limit);
}

// Get all unique brands from mock data
export async function getBrands(): Promise<string[]> {
  await delay(50);
  const brands = [...new Set(mockCars.map(car => car.brand))];
  return brands.sort();
}

// Get models by brand
export async function getModelsByBrand(brand: string): Promise<string[]> {
  await delay(50);
  const models = [...new Set(
    mockCars
      .filter(car => car.brand.toLowerCase() === brand.toLowerCase())
      .map(car => car.model)
  )];
  return models.sort();
}

// Get related cars (same brand or body type)
export async function getRelatedCars(carId: string, limit: number = 3): Promise<Car[]> {
  await delay(100);
  const currentCar = mockCars.find(car => car.id === carId);
  if (!currentCar) return [];

  const related = mockCars
    .filter(car =>
      car.id !== carId &&
      car.status === 'approved' &&
      (car.brand === currentCar.brand || car.bodyType === currentCar.bodyType)
    )
    .slice(0, limit);

  return related;
}

// Format price for display
export function formatPrice(price: number): string {
  if (price >= 1000000) {
    return `₱${(price / 1000000).toFixed(1).replace('.0', '')}M`;
  } else if (price >= 1000) {
    return `₱${(price / 1000).toFixed(0)}K`;
  }
  return `₱${price}`;
}

// Format price for display with full amount
export function formatDisplayPrice(price: number): string {
  return `₱${price.toLocaleString('en-PH')}`;
}
