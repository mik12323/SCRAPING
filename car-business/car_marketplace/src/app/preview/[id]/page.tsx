'use client';

import { useState, useEffect } from 'react';
import { useParams } from 'next/navigation';
import Link from 'next/link';
import { useAuth } from '@/lib/auth/AuthContext';
import { supabase } from '@/lib/db/supabase-client';
import { formatDisplayPrice } from '@/lib/api';
import ImageGallery from '@/components/ImageGallery';
import CarOwnerActions from '@/components/CarOwnerActions';

export default function PreviewPage() {
  const params = useParams();
  const { user, isAdmin } = useAuth();
  const [car, setCar] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  useEffect(() => {
    if (!params?.id) return;
    if (!user) return;

    const fetchCar = async () => {
      setLoading(true);
      const { data, error } = await supabase
        .from('listings')
        .select('*')
        .eq('id', params.id)
        .single();

      if (error) {
        setError('Car not found');
        setLoading(false);
        return;
      }

      if (!isAdmin && data.user_id !== user.id) {
        setError('You do not have permission to view this listing');
        setLoading(false);
        return;
      }

      setCar(data);
      setLoading(false);
    };

    fetchCar();
  }, [params?.id, user, isAdmin]);

  if (!user) {
    return (
      <div className="container mx-auto px-4 py-16 text-center">
        <h1 className="text-3xl font-bold mb-4">Please Login</h1>
        <p className="mb-6">You need to login to view this page.</p>
        <Link href="/login" className="bg-blue-600 text-white px-6 py-3 rounded-lg hover:bg-blue-700">
          Login
        </Link>
      </div>
    );
  }

  if (loading) {
    return (
      <div className="container mx-auto px-4 py-16 text-center">
        <div className="inline-block h-8 w-8 animate-spin rounded-full border-4 border-solid border-blue-600 border-r-transparent"></div>
        <p className="mt-4 text-gray-600">Loading listing...</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="container mx-auto px-4 py-16 text-center">
        <h1 className="text-3xl font-bold mb-4">Error</h1>
        <p className="mb-6 text-red-600">{error}</p>
        <div className="flex gap-4 justify-center">
          <Link href={isAdmin ? '/admin' : '/my-cars'} className="bg-blue-600 text-white px-6 py-3 rounded-lg hover:bg-blue-700">
            {isAdmin ? 'Back to Admin' : 'Back to My Cars'}
          </Link>
        </div>
      </div>
    );
  }

  if (!car) {
    return (
      <div className="container mx-auto px-4 py-16 text-center">
        <h1 className="text-3xl font-bold mb-4">Listing Not Found</h1>
        <p className="mb-6">The listing you&apos;re looking for doesn&apos;t exist or has been removed.</p>
        <Link href={isAdmin ? '/admin' : '/my-cars'} className="bg-blue-600 text-white px-6 py-3 rounded-lg hover:bg-blue-700">
          {isAdmin ? 'Back to Admin' : 'Back to My Cars'}
        </Link>
      </div>
    );
  }

  const statusStyles: Record<string, { bg: string; border: string; text: string; icon: string; label: string }> = {
    pending: { bg: 'bg-yellow-50', border: 'border-yellow-300', text: 'text-yellow-800', icon: '⏳', label: 'PENDING' },
    approved: { bg: 'bg-green-50', border: 'border-green-300', text: 'text-green-800', icon: '✅', label: 'APPROVED' },
    rejected: { bg: 'bg-red-50', border: 'border-red-300', text: 'text-red-800', icon: '❌', label: 'REJECTED' },
  };

  const status = statusStyles[car.status as string] || statusStyles.pending;

  return (
    <div className="container mx-auto px-4 py-8">
      <div className={`${status.bg} border ${status.border} ${status.text} px-4 py-3 rounded-lg mb-6 flex items-center justify-between`}>
        <span className="font-semibold">
          {status.icon} {status.label} — This listing is not visible to the public
        </span>
        <Link href={isAdmin ? '/admin' : '/my-cars'} className="underline text-sm">
          {isAdmin ? 'Back to Admin' : 'Back to My Cars'}
        </Link>
      </div>

      <div className="flex items-center gap-4 mb-6">
        <Link href={isAdmin ? '/admin' : '/my-cars'} className="flex items-center gap-2 text-gray-600 hover:text-gray-900 transition-colors">
          <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 19l-7-7m0 0l7-7m-7 7h18" />
          </svg>
          {isAdmin ? 'Back to Admin' : 'Back to My Cars'}
        </Link>
        <span className="text-gray-400">|</span>
        <span className="text-sm text-gray-500">Preview</span>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
        <div className="lg:col-span-2">
          <h1 className="text-3xl font-bold mb-2">
            {car.year} {car.brand} {car.model}
          </h1>

          <div className="flex gap-2 mb-4 flex-wrap">
            <span className="bg-gray-200 text-gray-700 px-3 py-1 rounded-full text-sm">Used</span>
            {car.body_type && (
              <span className="bg-gray-200 text-gray-700 px-3 py-1 rounded-full text-sm">{car.body_type}</span>
            )}
            {car.fuel_type && (
              <span className="bg-blue-100 text-blue-700 px-3 py-1 rounded-full text-sm">{car.fuel_type}</span>
            )}
            {car.transmission && (
              <span className="bg-gray-900 text-white px-3 py-1 rounded-full text-sm">{car.transmission}</span>
            )}
            <span className={`${status.bg} ${status.text} px-3 py-1 rounded-full text-sm font-semibold border ${status.border}`}>
              {status.icon} {status.label}
            </span>
          </div>

          <ImageGallery
            images={car.images || []}
            alt={`${car.year} ${car.brand} ${car.model} for sale Philippines`}
          />

          <div className="mt-8 bg-white p-6 rounded-xl shadow-sm">
            <h2 className="text-xl font-bold mb-4">Vehicle Details</h2>
            <ul className="space-y-2">
              <li><strong>Year:</strong> {car.year}</li>
              <li><strong>Make/Model:</strong> {car.brand} {car.model}</li>
              {car.color && <li><strong>Color:</strong> {car.color}</li>}
              {car.mileage && <li><strong>Mileage:</strong> {parseInt(car.mileage).toLocaleString()} km</li>}
              {car.body_type && <li><strong>Body Type:</strong> {car.body_type}</li>}
              {car.fuel_type && <li><strong>Fuel Type:</strong> {car.fuel_type}</li>}
              {car.transmission && <li><strong>Transmission:</strong> {car.transmission}</li>}
              <li><strong>Location:</strong> {car.location}</li>
            </ul>

            {car.description && (
              <div className="mt-6">
                <h3 className="font-bold mb-2">Description</h3>
                <p className="text-gray-700 whitespace-pre-wrap">{car.description}</p>
              </div>
            )}
          </div>
        </div>

        <div className="lg:col-span-1">
          <div className="bg-white p-6 rounded-xl shadow-sm sticky top-4">
            <CarOwnerActions carId={car.id} carSlug={car.slug} userId={car.user_id} />

            <div className="mb-4">
              <div className="text-3xl font-bold text-red-500">
                {formatDisplayPrice(car.price)}
              </div>
              {car.original_price && car.original_price > car.price && (
                <>
                  <div className="text-lg text-gray-500 line-through">
                    was {formatDisplayPrice(car.original_price)}
                  </div>
                  <span className="inline-block bg-red-500 text-white text-sm font-bold px-3 py-1 rounded mt-2">
                    PRICE REDUCED
                  </span>
                </>
              )}
            </div>

            <div className="space-y-3">
              {car.status === 'approved' ? (
                <Link
                  href={`/car/${car.slug}`}
                  className="block w-full bg-blue-600 text-white text-center px-6 py-3 rounded-lg hover:bg-blue-700 transition-colors"
                >
                  View Public Page
                </Link>
              ) : (
                <div className="block w-full bg-gray-300 text-gray-500 text-center px-6 py-3 rounded-lg">
                  Not Yet Public
                </div>
              )}
              <a
                href="https://www.facebook.com/mikoy.dimaro/"
                target="_blank"
                rel="noopener noreferrer"
                className="block w-full bg-blue-600 text-white text-center px-6 py-3 rounded-lg hover:bg-blue-700 transition-colors"
              >
                Inquire via Messenger
              </a>
              <a
                href="tel:+639970946623"
                className="block w-full bg-green-600 text-white text-center px-6 py-3 rounded-lg hover:bg-green-700 transition-colors"
              >
                Call: +63 997 094 6623
              </a>
            </div>

            <div className="mt-6 pt-6 border-t border-gray-200">
              <p className="text-sm text-gray-600">
                <strong>Location:</strong><br />
                {car.location}
              </p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
