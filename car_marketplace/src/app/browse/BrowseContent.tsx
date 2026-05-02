'use client';

import { useState, useEffect } from 'react';
import useSWR from 'swr';
import { useSearchParams, useRouter } from 'next/navigation';
import CarGrid from '@/components/CarGrid';
import FilterSidebar from '@/components/FilterSidebar';
import Pagination from '@/components/Pagination';
import FAQ from '@/components/FAQ';
import {
  getCars,
  getBrands,
  getTrendingCars,
} from '@/lib/api';
import type { Car, CarFilters, TrendingCar } from '@/lib/types';

// Fetch with timeout wrapper
async function fetchWithTimeout<T>(promise: Promise<T>, timeoutMs: number = 10000): Promise<T> {
  const timeout = new Promise<never>((_, reject) =>
    setTimeout(() => reject(new Error('Request timeout')), timeoutMs)
  );
  return Promise.race([promise, timeout]);
}

export default function BrowseContent() {
  const searchParams = useSearchParams();
  const router = useRouter();

  const [brands, setBrands] = useState<string[]>([]);
  const [filters, setFilters] = useState<CarFilters>({});
  const [trendingCars, setTrendingCars] = useState<TrendingCar[]>([]);

  // Load brands once
  useEffect(() => {
    const loadBrands = async () => {
      const data = await getBrands();
      setBrands(data);
    };
    loadBrands();
  }, []);

  // Load trending cars once
  useEffect(() => {
    const loadTrending = async () => {
      const data = await getTrendingCars(5);
      setTrendingCars(data);
    };
    loadTrending();
  }, []);

  // Parse filters from search params
  useEffect(() => {
    const filterData: CarFilters = {};
    const brand = searchParams.get('brand');
    const model = searchParams.get('model');
    const bodyType = searchParams.get('body_type');
    const fuelType = searchParams.get('fuel_type');
    const transmission = searchParams.get('transmission');
    const priceMin = searchParams.get('price_min');
    const priceMax = searchParams.get('price_max');
    const sortBy = (searchParams.get('sort') || searchParams.get('sortBy')) as CarFilters['sortBy'];

    if (brand) filterData.brand = brand;
    if (model) filterData.model = model;
    if (bodyType && bodyType !== 'All') filterData.bodyType = bodyType;
    if (fuelType && fuelType !== 'All') filterData.fuelType = fuelType;
    if (transmission && transmission !== 'All') filterData.transmission = transmission;
    if (priceMin) filterData.priceMin = parseInt(priceMin);
    if (priceMax) filterData.priceMax = parseInt(priceMax);
    if (sortBy) filterData.sortBy = sortBy;

    setFilters(filterData);
  }, [searchParams]);

  // Create SWR key based on filters and page - use stable string key
  const page = parseInt(searchParams.get('page') || '1');
  const swrKey = `cars-${JSON.stringify(filters)}-${page}`;

  const { data, error, isLoading, mutate } = useSWR(
    swrKey,
    async () => {
      try {
        return await fetchWithTimeout(getCars(filters, page), 10000);
      } catch (err: any) {
        if (err.message === 'Request timeout') {
          throw new Error('Request timed out. Please try again.');
        }
        throw err;
      }
    },
    {
      keepPreviousData: true,
      revalidateOnFocus: false,
      revalidateOnReconnect: false,
      shouldRetryOnError: false, // We handle retries manually with Retry button
    }
  );

  const handleFilterChange = (newFilters: Record<string, string>) => {
    const params = new URLSearchParams();
    params.set('page', '1'); // Reset to page 1 on filter change

    Object.entries(newFilters).forEach(([key, value]) => {
      if (value && value !== 'All') {
        // Convert camelCase to snake_case for URL params
        const paramKey = key
          .replace(/([A-Z])/g, '_$1')
          .toLowerCase();
        params.set(paramKey === 'sort_by' ? 'sort' : paramKey, value);
      }
    });

    router.push(`/browse?${params.toString()}`);
  };

  const handlePageChange = (newPage: number) => {
    const params = new URLSearchParams(searchParams.toString());
    params.set('page', newPage.toString());
    router.push(`/browse?${params.toString()}`);
  };

  const handleReset = () => {
    router.push('/browse');
  };

  const cars = data?.cars || [];
  const total = data?.total || 0;
  const totalPages = data?.totalPages || 1;
  const currentPage = data?.currentPage || 1;

  return (
    <div className="container mx-auto px-4 py-8">
      <h1 className="text-3xl font-bold mb-6">Browse Cars</h1>

      <div className="flex flex-col lg:flex-row gap-6">
        <aside className="lg:w-1/4">
          <FilterSidebar
            filters={{
              brand: filters.brand,
              model: filters.model,
              bodyType: filters.bodyType,
              fuelType: filters.fuelType,
              transmission: filters.transmission,
              priceMin: filters.priceMin?.toString(),
              priceMax: filters.priceMax?.toString(),
              sortBy: filters.sortBy,
            }}
            onFilterChange={handleFilterChange}
            onReset={handleReset}
            brands={brands}
          />
        </aside>

        <main className="lg:w-3/4">
          {error ? (
            <div className="text-center py-16">
              <p className="text-red-600 mb-4">{error.message || 'Failed to load cars'}</p>
              <button
                onClick={() => mutate()}
                className="bg-blue-600 text-white px-6 py-3 rounded-lg hover:bg-blue-700 cursor-pointer"
              >
                Retry
              </button>
            </div>
          ) : isLoading ? (
            <CarGridSkeleton />
          ) : cars.length === 0 ? (
            <>
              <div className="text-center py-16 text-gray-600">
                No cars found matching your filters.
              </div>
              {trendingCars.length > 0 && (
                <div className="mt-8">
                  <h3 className="text-xl font-bold text-center mb-4">Trending Today</h3>
                  <div className="flex flex-wrap justify-center gap-3">
                    {trendingCars.map((car: TrendingCar) => (
                      <a
                        key={car.brand + car.model}
                        href={`/browse?brand=${encodeURIComponent(car.brand)}&model=${encodeURIComponent(car.model)}`}
                        className="bg-gradient-to-r from-yellow-400 to-orange-500 text-black px-4 py-2 rounded-full font-semibold text-sm hover:shadow-lg transition-all"
                      >
                        {car.displayName}
                      </a>
                    ))}
                  </div>
                </div>
              )}
            </>
          ) : (
            <>
              <div className="mb-4 text-sm text-gray-600">
                Found {total} car{total !== 1 ? 's' : ''}
              </div>

              <CarGrid cars={cars} />

              {totalPages > 1 && (
                <Pagination
                  currentPage={currentPage}
                  totalPages={totalPages}
                  total={total}
                  onPageChange={handlePageChange}
                />
              )}
            </>
          )}
        </main>
      </div>

      <FAQ />
    </div>
  );
}

// Loading skeleton component
function CarGridSkeleton() {
  return (
    <div>
      <div className="mb-4 text-sm text-gray-400">
        Loading cars...
      </div>
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        {[...Array(6)].map((_, i) => (
          <div key={i} className="bg-white rounded-xl shadow-sm overflow-hidden animate-pulse">
            <div className="aspect-[4/3] bg-gray-200"></div>
            <div className="p-4 space-y-3">
              <div className="h-6 bg-gray-200 rounded w-3/4"></div>
              <div className="h-4 bg-gray-200 rounded w-1/2"></div>
              <div className="h-6 bg-gray-200 rounded w-1/3"></div>
              <div className="flex gap-2">
                <div className="h-6 bg-gray-200 rounded w-16"></div>
                <div className="h-6 bg-gray-200 rounded w-16"></div>
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
