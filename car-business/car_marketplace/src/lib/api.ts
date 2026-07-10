// ===========================================
// API LAYER - Future Supabase Ready
// ===========================================
// This file serves as the main API interface for the frontend
// Currently uses mock data, structured for easy Supabase migration

// Import from db layer (future Supabase queries)
// Currently, these re-export from mock data via db/queries/*.ts
export {
  getCars,
  getCarBySlug,
  getFeaturedCars,
  getBrands,
  getModelsByBrand,
  getRelatedCars,
} from './db/queries/cars';

export { getTrendingCars } from './db/queries/tracking';

export { formatPrice, formatDisplayPrice } from './api-legacy';

// Re-export types
export type { Car, CarFilters, PaginatedResponse, TrendingCar } from './types';
