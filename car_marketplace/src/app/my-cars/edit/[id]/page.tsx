'use client';

import { useState, useEffect } from 'react';
import useSWR from 'swr';
import { useRouter } from 'next/navigation';
import { supabase } from '@/lib/db/supabase-client';
import { useAuth } from '@/lib/auth/AuthContext';
import { BODY_TYPES } from '@/lib/types';
import type { Car } from '@/lib/types';
import { generateSlug, makeSlugUnique, shouldRegenerateSlug } from '@/lib/slug-utils';

// Optimized column selection - only fetch what we need
const CAR_EDIT_COLUMNS = [
  'id', 'user_id', 'brand', 'model', 'year', 'price', 'original_price',
  'mileage', 'transmission', 'fuel_type', 'body_type', 'color',
  'location', 'description', 'status'
].join(', ');

// Fetch with timeout wrapper (increased to 15s for PH ISP fluctuations)
async function fetchWithTimeout<T>(promise: Promise<T>, timeoutMs: number = 15000): Promise<T> {
  const timeout = new Promise<never>((_, reject) =>
    setTimeout(() => reject(new Error('Request timeout')), timeoutMs)
  );
  return Promise.race([promise, timeout]);
}

// Retry with exponential backoff (one retry with jitter)
async function fetchWithRetry<T>(
  fn: () => Promise<T>,
  retries: number = 1,
  baseDelay: number = 1000
): Promise<T> {
  try {
    return await fn();
  } catch (err) {
    if (retries <= 0) throw err;

    // Exponential backoff with jitter (500-1500ms)
    const jitter = Math.random() * 1000 + 500;
    await new Promise(resolve => setTimeout(resolve, jitter));

    return fetchWithRetry(fn, retries - 1, baseDelay * 2);
  }
}

// SWR fetcher for car data with retry
const carFetcher = async (id: string): Promise<any> => {
  return fetchWithRetry(async () => {
    const result = await supabase
      .from('listings')
      .select(CAR_EDIT_COLUMNS)
      .eq('id', id)
      .single();

    if (result.error || !result.data) {
      throw new Error('Car not found');
    }

    return result.data;
  });
};

export default function EditCarPage({ params }: { params: Promise<{ id: string }> }) {
  const { user } = useAuth();
  const router = useRouter();
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');
  const [resolvedId, setResolvedId] = useState<string | null>(null);

  // Resolve params once
  useEffect(() => {
    params.then(({ id }) => setResolvedId(id));
  }, [params]);

  // Use SWR for caching and automatic revalidation
  const { data: car, error: swrError, isLoading, mutate } = useSWR(
    resolvedId ? `car-edit-${resolvedId}` : null,
    () => carFetcher(resolvedId!),
    {
      revalidateOnFocus: false,
      revalidateOnReconnect: false,
      shouldRetryOnError: false, // We handle retries manually with exponential backoff
      dedupingInterval: 5000, // Prevent duplicate requests within 5 seconds
    }
  );

  // Form state
  const [formData, setFormData] = useState({
    brand: '',
    model: '',
    year: new Date().getFullYear(),
    price: '',
    originalPrice: '',
    mileage: '',
    transmission: 'Automatic',
    fuelType: 'Gas',
    bodyType: 'Sedan',
    color: '',
    location: '',
    description: '',
  });

  // Initialize form when car data loads
  useEffect(() => {
    if (car) {
      // Check if user owns this car or is admin
      if (user && car.user_id !== user.id && !user.is_admin) {
        setError('You do not have permission to edit this listing');
        return;
      }

      setFormData({
        brand: car.brand || '',
        model: car.model || '',
        year: car.year || new Date().getFullYear(),
        price: car.price?.toString() || '',
        originalPrice: car.original_price?.toString() || '',
        mileage: car.mileage?.toString() || '',
        transmission: car.transmission || 'Automatic',
        fuelType: car.fuel_type || 'Gas',
        bodyType: car.body_type || 'Sedan',
        color: car.color || '',
        location: car.location || '',
        description: car.description || '',
      });
    }
  }, [car, user]);

  // Handle SWR errors
  useEffect(() => {
    if (swrError) {
      setError(swrError.message || 'Failed to load car details');
    }
  }, [swrError]);

  const handleChange = (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>) => {
    const { name, value } = e.target;
    setFormData(prev => ({ ...prev, [name]: value }));
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError('');
    setSaving(true);

    try {
      const { id } = await params;

      // Build edit proposal with only changed fields
      const editProposal: Record<string, { from: any; to: any }> = {};

      if (car) {
        const fieldMappings = [
          { formKey: 'brand', carKey: 'brand', dbKey: 'brand' },
          { formKey: 'model', carKey: 'model', dbKey: 'model' },
          { formKey: 'year', carKey: 'year', dbKey: 'year', transform: (v: any) => parseInt(v) },
          { formKey: 'price', carKey: 'price', dbKey: 'price', transform: (v: any) => parseInt(v) },
          { formKey: 'originalPrice', carKey: 'original_price', dbKey: 'original_price', transform: (v: any) => v ? parseInt(v) : null },
          { formKey: 'mileage', carKey: 'mileage', dbKey: 'mileage', transform: (v: any) => v ? parseInt(v) : null },
          { formKey: 'transmission', carKey: 'transmission', dbKey: 'transmission' },
          { formKey: 'fuelType', carKey: 'fuel_type', dbKey: 'fuel_type' },
          { formKey: 'bodyType', carKey: 'body_type', dbKey: 'body_type' },
          { formKey: 'color', carKey: 'color', dbKey: 'color' },
          { formKey: 'location', carKey: 'location', dbKey: 'location' },
          { formKey: 'description', carKey: 'description', dbKey: 'description' },
        ];

        fieldMappings.forEach(({ formKey, carKey, dbKey, transform }) => {
          const formValue = transform ? transform(formData[formKey as keyof typeof formData]) : formData[formKey as keyof typeof formData];
          const carValue = car[carKey as keyof typeof car];
          if (formValue !== carValue) {
            editProposal[dbKey] = { from: carValue || null, to: formValue || null };
          }
        });

      }

      // Update slug only if relevant fields changed
      const shortId = car.id.replace(/-/g, '').slice(-4);
      const color = formData.color || undefined;
      let slug = car.slug; // Keep original by default

      if (shouldRegenerateSlug(car, formData)) {
        const baseSlug = generateSlug(formData.brand, formData.model, formData.year, parseInt(formData.price), shortId, color);
        // Make sure slug is unique
        slug = await makeSlugUnique(baseSlug, car.id);
      }

      const isAdmin = user?.is_admin;

      const updateData: any = {
        brand: formData.brand,
        model: formData.model,
        year: parseInt(formData.year.toString()),
        price: parseInt(formData.price),
        original_price: formData.originalPrice ? parseInt(formData.originalPrice) : null,
        mileage: formData.mileage ? parseInt(formData.mileage) : null,
        transmission: formData.transmission,
        fuel_type: formData.fuelType,
        body_type: formData.bodyType,
        color: formData.color,
        location: formData.location,
        description: formData.description,
        slug,
        updated_at: new Date().toISOString(),
      };
       if (isAdmin) {
         // Admin edits are auto-approved
         updateData.status = 'approved';
         updateData.edit_proposal = null;
         updateData.admin_remarks = null;
       } else {
         // Regular user edits go to pending for approval
         updateData.status = 'pending';
         // Only add edit_proposal if there are changes
         if (Object.keys(editProposal).length > 0) {
           updateData.edit_proposal = editProposal;
         }
       }

      const { error: updateError } = await supabase
        .from('listings')
        .update(updateData)
        .eq('id', id);

      if (updateError) {
        console.error('Update error:', updateError);
        if (updateError.code === '23505' || updateError.message?.includes('slug_key')) {
          setError('It looks like this exact listing already exists. Please check your active listings or add a unique detail to the title.');
        } else {
          setError('Failed to update listing. Please try again.');
        }
      } else {
        // Invalidate cache after successful update
        await mutate();
        router.push('/my-cars');
      }
    } catch (err: any) {
      console.error('Edit page error:', err);
      setError(err.message === 'Request timeout' ? 'Request timed out. Please try again.' : 'An error occurred. Please try again.');
    } finally {
      setSaving(false);
    }
  };

  if (isLoading) {
    return (
      <div className="container mx-auto px-4 py-16 text-center">
        <div className="inline-block h-8 w-8 animate-spin rounded-full border-4 border-solid border-blue-600 border-r-transparent"></div>
        <p className="mt-4 text-gray-600">Loading car details...</p>
      </div>
    );
  }

  if (error || swrError) {
    return (
      <div className="container mx-auto px-4 py-16 text-center">
        <div className="flex items-center gap-4 mb-6 justify-center">
          <button
            type="button"
            onClick={() => router.back()}
            className="flex items-center gap-2 text-gray-600 hover:text-gray-900 cursor-pointer"
          >
            <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 19l-7-7m0 0l7-7m-7 7h18" />
            </svg>
            Back
          </button>
        </div>
        <h1 className="text-3xl font-bold mb-4">Error</h1>
        <p className="mb-6 text-red-600">{error || 'Failed to load car details'}</p>
        <div className="flex gap-4 justify-center">
          <button
            onClick={() => mutate()}
            className="bg-blue-600 text-white px-6 py-3 rounded-lg hover:bg-blue-700 cursor-pointer"
          >
            Retry
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="container mx-auto px-4 py-8">
      <div className="flex items-center gap-4 mb-6">
        <button
          type="button"
          onClick={() => router.back()}
          className="flex items-center gap-2 text-gray-600 hover:text-gray-900 transition-colors cursor-pointer"
        >
          <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 19l-7-7m0 0l7-7m-7 7h18" />
          </svg>
          Back
        </button>
        <h1 className="text-3xl font-bold">Edit Car Listing</h1>
      </div>

      {error && (
        <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded mb-4">
          {error}
        </div>
      )}

      <form onSubmit={handleSubmit} className="max-w-4xl">
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div>
            <label htmlFor="brand" className="block text-sm font-medium mb-2">Brand *</label>
            <input
              id="brand"
              name="brand"
              type="text"
              value={formData.brand}
              onChange={handleChange}
              required
              className="w-full px-4 py-2 border border-gray-300 rounded-lg"
            />
          </div>

          <div>
            <label htmlFor="model" className="block text-sm font-medium mb-2">Model *</label>
            <input
              id="model"
              name="model"
              type="text"
              value={formData.model}
              onChange={handleChange}
              required
              className="w-full px-4 py-2 border border-gray-300 rounded-lg"
            />
          </div>

          <div>
            <label htmlFor="year" className="block text-sm font-medium mb-2">Year *</label>
            <input
              id="year"
              name="year"
              type="number"
              value={formData.year}
              onChange={handleChange}
              required
              min="1900"
              max={new Date().getFullYear() + 1}
              className="w-full px-4 py-2 border border-gray-300 rounded-lg"
            />
          </div>

          <div>
            <label htmlFor="price" className="block text-sm font-medium mb-2">Price (₱) *</label>
            <input
              id="price"
              name="price"
              type="number"
              value={formData.price}
              onChange={handleChange}
              required
              min="0"
              className="w-full px-4 py-2 border border-gray-300 rounded-lg"
            />
          </div>

          <div>
            <label htmlFor="originalPrice" className="block text-sm font-medium mb-2">Original Price (₱)</label>
            <input
              id="originalPrice"
              name="originalPrice"
              type="number"
              value={formData.originalPrice}
              onChange={handleChange}
              min="0"
              className="w-full px-4 py-2 border border-gray-300 rounded-lg"
            />
          </div>

          <div>
            <label htmlFor="mileage" className="block text-sm font-medium mb-2">Mileage (km)</label>
            <input
              id="mileage"
              name="mileage"
              type="number"
              value={formData.mileage}
              onChange={handleChange}
              min="0"
              className="w-full px-4 py-2 border border-gray-300 rounded-lg"
            />
          </div>

          <div>
            <label htmlFor="transmission" className="block text-sm font-medium mb-2">Transmission *</label>
            <select
              id="transmission"
              name="transmission"
              value={formData.transmission}
              onChange={handleChange}
              className="w-full px-4 py-2 border border-gray-300 rounded-lg"
            >
              <option value="Automatic">Automatic</option>
              <option value="Manual">Manual</option>
            </select>
          </div>

          <div>
            <label htmlFor="fuelType" className="block text-sm font-medium mb-2">Fuel Type *</label>
            <select
              id="fuelType"
              name="fuelType"
              value={formData.fuelType}
              onChange={handleChange}
              className="w-full px-4 py-2 border border-gray-300 rounded-lg"
            >
              <option value="Gas">Gas</option>
              <option value="Diesel">Diesel</option>
              <option value="Electric">Electric</option>
              <option value="Hybrid">Hybrid</option>
              <option value="Plug-in Hybrid">Plug-in Hybrid</option>
            </select>
          </div>

          <div>
            <label htmlFor="bodyType" className="block text-sm font-medium mb-2">Body Type *</label>
            <select
              id="bodyType"
              name="bodyType"
              value={formData.bodyType}
              onChange={handleChange}
              className="w-full px-4 py-2 border border-gray-300 rounded-lg"
            >
              {BODY_TYPES.map((type) => (
                <option key={type} value={type}>{type}</option>
              ))}
            </select>
          </div>

          <div>
            <label htmlFor="color" className="block text-sm font-medium mb-2">Color</label>
            <input
              id="color"
              name="color"
              type="text"
              value={formData.color}
              onChange={handleChange}
              className="w-full px-4 py-2 border border-gray-300 rounded-lg"
            />
          </div>

          <div>
            <label htmlFor="location" className="block text-sm font-medium mb-2">Location *</label>
            <input
              id="location"
              name="location"
              type="text"
              value={formData.location}
              onChange={handleChange}
              required
              placeholder="e.g., Metro Manila"
              className="w-full px-4 py-2 border border-gray-300 rounded-lg"
            />
          </div>
        </div>

        <div className="mt-6">
          <label htmlFor="description" className="block text-sm font-medium mb-2">Description</label>
          <textarea
            id="description"
            name="description"
            value={formData.description}
            onChange={handleChange}
            rows={4}
            className="w-full px-4 py-2 border border-gray-300 rounded-lg"
          />
        </div>

        {!user?.is_admin && (
          <div className="mt-6 p-4 bg-yellow-50 border border-yellow-200 rounded-lg">
            <p className="text-sm text-yellow-800">
              Note: Editing will revert the listing status to "Pending" for re-approval.
            </p>
          </div>
        )}

        <button
          type="submit"
          disabled={saving}
          className="mt-6 w-full bg-blue-600 text-white py-3 rounded-lg hover:bg-blue-700 disabled:bg-gray-400"
        >
          {saving ? 'Saving...' : 'Save Changes'}
        </button>
      </form>
    </div>
  );
}
