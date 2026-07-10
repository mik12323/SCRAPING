export interface Car {
  id: string;
  slug: string;
  user_id?: string;
  brand: string;
  model: string;
  year: number;
  price: number;
  originalPrice?: number;
  original_price?: number;
  color?: string;
  mileage?: number;
  bodyType: string; // Maps to body_type in DB
  fuelType: string; // Maps to fuel_type in DB
  transmission: string; // Maps to transmission in DB
  location: string;
  description: string;
  images: string[];
  thumbnail: string;
  status: 'pending' | 'approved' | 'rejected';
  edit_proposal?: Record<string, { from: any; to: any }> | null;
  admin_remarks?: string | null;
  created_at?: string;
  updated_at?: string;
  createdAt?: string;
  updatedAt?: string;
}

// Helper to convert DB listing to Car type
export function dbListingToCar(dbListing: any): Car {
  return {
    id: dbListing.id,
    slug: dbListing.slug,
    user_id: dbListing.user_id,
    brand: dbListing.brand,
    model: dbListing.model,
    year: dbListing.year,
    price: dbListing.price,
    originalPrice: dbListing.original_price,
    original_price: dbListing.original_price,
    color: dbListing.color || '',
    mileage: dbListing.mileage || undefined,
    bodyType: dbListing.body_type || '',
    fuelType: dbListing.fuel_type || '',
    transmission: dbListing.transmission || '',
    location: dbListing.location,
    description: dbListing.description || '',
    images: dbListing.images || [],
    thumbnail: dbListing.images?.[0] || '',
    status: dbListing.status as 'pending' | 'approved' | 'rejected',
    created_at: dbListing.created_at,
    updated_at: dbListing.updated_at,
    createdAt: dbListing.created_at,
    updatedAt: dbListing.updated_at,
  };
}

export interface CarFilters {
  brand?: string;
  model?: string;
  bodyType?: string;
  fuelType?: string;
  transmission?: string;
  priceMin?: number;
  priceMax?: number;
  year?: number;
  sortBy?: 'alphabetical' | 'reverse_alphabetical' | 'price_low' | 'price_high' | 'newest' | 'oldest';
}

export interface PaginatedResponse {
  cars: Car[];
  total: number;
  totalPages: number;
  currentPage: number;
}

export interface TrendingCar {
  brand: string;
  model: string;
  displayName: string;
  clicks: number;
  bodyType: string;
  fuelType: string;
  transmission: string;
}

export const BODY_TYPES = ["Sedan", "SUV", "Hatchback", "Truck", "Van", "Coupe", "Convertible", "Wagon", "Pickup", "Crossover"];
export const FUEL_TYPES = ["Gas", "Diesel", "Electric", "Hybrid", "Plug-in Hybrid"];
export const TRANSMISSIONS = ["Automatic", "Manual"];

export const CAR_BRANDS = [
  "Acura", "Alfa Romeo", "Aston Martin", "Audi", "BAIC", "Bentley", "BMW",
  "BYD", "Cadillac", "Changan", "Chery", "Chevrolet", "Chrysler",
  "Daewoo", "Daihatsu", "Dodge", "Ferrari",
  "Fiat", "Ford", "Foton", "GAC", "Geely", "Genesis", "GMC", "Great Wall", "Haima",
  "Honda", "Hummer", "Hyundai", "Infiniti", "Isuzu", "JAC",
  "Jaguar", "Jeep", "Jetour", "Kia", "Lamborghini", "Land Rover",
  "Lexus", "Lincoln", "Mahindra", "Maserati", "Mazda",
  "McLaren", "Mercedes-Benz", "MG", "Mini", "Mitsubishi", "Nissan",
  "Opel", "Peugeot", "Pontiac", "Porsche", "Proton", "Renault", "Rolls-Royce",
  "SsangYong", "Subaru", "Suzuki", "Tesla",
  "Tata", "Toyota", "VinFast", "Volkswagen", "Volvo"
];
