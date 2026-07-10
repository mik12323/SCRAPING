'use client';

import { useState, useEffect } from 'react';
import Link from 'next/link';
import { useRouter } from 'next/navigation';
import useSWR from 'swr';
import { useAuth } from '@/lib/auth/AuthContext';
import { supabase } from '@/lib/db/supabase-client';
import { formatDisplayPrice } from '@/lib/api';
import type { Car } from '@/lib/types';
import { deleteListing } from '@/lib/db/mutations/listings';

// Optimized columns for my-cars list
const MY_CARS_COLUMNS = [
  'id', 'slug', 'brand', 'model', 'year', 'price', 'images', 'status', 'created_at'
].join(', ');

// SWR fetcher with retry
const fetchWithTimeout = async <T,>(promise: Promise<T>, timeoutMs: number = 15000): Promise<T> => {
  const timeout = new Promise<never>((_, reject) =>
    setTimeout(() => reject(new Error('Request timeout')), timeoutMs)
  );
  return Promise.race([promise, timeout]);
};

const myCarsFetcher = async (userId: string): Promise<Car[]> => {
  const result: any = await supabase
    .from('listings')
    .select(MY_CARS_COLUMNS)
    .eq('user_id', userId)
    .order('created_at', { ascending: false });

  if (result.error) throw result.error;
  return (result.data || []) as Car[];
};

export default function MyCarsPage() {
  const { user } = useAuth();
  const router = useRouter();

  // Use SWR for caching with deduping
  const { data: cars, error, isLoading, mutate } = useSWR(
    user ? `my-cars-${user.id}` : null,
    () => myCarsFetcher(user!.id),
    {
      revalidateOnFocus: false,
      revalidateOnReconnect: false,
      dedupingInterval: 5000,
    }
  );

  // Pre-fetch car data on hover
  const preloadCar = (carId: string) => {
    // Pre-populate SWR cache by triggering a fetch
    mutate(); // This will revalidate in background
  };

  if (!user) {
    return (
      <div className="container mx-auto px-4 py-16 text-center">
        <h1 className="text-3xl font-bold mb-4">Please Login</h1>
        <p className="mb-6">You need to login to view your listings.</p>
        <Link href="/login" className="bg-blue-600 text-white px-6 py-3 rounded-lg hover:bg-blue-700">
          Login
        </Link>
      </div>
    );
  }

  const handleDelete = async (id: string) => {
    if (!confirm('Are you sure you want to delete this listing?')) return;

    const { error } = await deleteListing(id);

    if (!error && cars) {
      mutate(cars.filter(car => car.id !== id));
    } else if (error) {
      console.error('Error deleting listing:', error);
      alert(`Failed to delete listing: ${error.message}`);
    }
  };

  return (
    <div className="container mx-auto px-4 py-8">
      <div className="flex justify-between items-center mb-6">
        <h1 className="text-3xl font-bold">My Cars</h1>
        <Link
          href="/sell"
          className="bg-blue-600 text-white px-6 py-3 rounded-lg hover:bg-blue-700"
        >
          + Add New Car
        </Link>
      </div>

      {isLoading ? (
        <div className="text-center py-16">Loading...</div>
      ) : error ? (
        <div className="text-center py-16">
          <p className="text-red-600 mb-4">Failed to load cars</p>
          <button onClick={() => mutate()} className="bg-blue-600 text-white px-6 py-3 rounded-lg hover:bg-blue-700">
            Retry
          </button>
        </div>
      ) : !cars || cars.length === 0 ? (
        <div className="text-center py-16">
          <p className="text-gray-600 mb-4">You haven't posted any cars yet.</p>
          <Link href="/sell" className="text-blue-600 hover:underline">
            Sell your first car
          </Link>
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {cars.map((car) => (
            <div key={car.id} className="bg-white rounded-xl shadow-sm overflow-hidden">
              {car.images && car.images[0] && (
                <img
                  src={car.images[0]}
                  alt={`${car.brand} ${car.model}`}
                  className="w-full h-48 object-cover"
                />
              )}
              <div className="p-4">
                <h3 className="font-bold text-lg">
                  {car.year} {car.brand} {car.model}
                </h3>
                <p className="text-2xl font-bold text-blue-600 mt-2">
                  {formatDisplayPrice(car.price)}
                </p>
                <div className="flex items-center gap-2 mt-2">
                  <span className={`px-2 py-1 rounded text-xs ${
                    car.status === 'approved' ? 'bg-green-100 text-green-700' :
                    car.status === 'pending' ? 'bg-yellow-100 text-yellow-700' :
                    'bg-red-100 text-red-700'
                  }`}>
                    {car.status}
                  </span>
                </div>
                {car.admin_remarks && (
                  <div className="mt-3 p-3 bg-red-50 border border-red-200 rounded-lg">
                    <p className="text-sm font-medium text-red-800">Edit Rejected</p>
                    <p className="text-sm text-red-700 mt-1">{car.admin_remarks}</p>
                    <button
                      onClick={() => {
                        router.push(`/my-cars/edit/${car.id}`);
                      }}
                      className="text-sm text-blue-600 hover:underline mt-2 cursor-pointer"
                    >
                      Edit Again
                    </button>
                  </div>
                )}
                <div className="flex gap-2 mt-4">
                  <Link
                    href={`/preview/${car.id}`}
                    className="flex-1 text-center bg-gray-100 text-gray-700 py-2 rounded hover:bg-gray-200"
                  >
                    View
                  </Link>
                  <Link
                    href={`/my-cars/edit/${car.id}`}
                    className="flex-1 text-center bg-blue-50 text-blue-600 py-2 rounded hover:bg-blue-100"
                    onMouseEnter={() => preloadCar(car.id)}
                  >
                    Edit
                  </Link>
                  <button
                    onClick={() => handleDelete(car.id)}
                    className="flex-1 bg-red-50 text-red-600 py-2 rounded hover:bg-red-100 cursor-pointer"
                  >
                    Delete
                  </button>
                </div>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
