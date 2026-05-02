'use client';

import { useState } from 'react';
import { useRouter } from 'next/navigation';
import { useAuth } from '@/lib/auth/AuthContext';
import { insertListing } from '@/lib/db/mutations/listings';
import { supabase } from '@/lib/db/supabase-client';
import { generateSlug, makeSlugUnique } from '@/lib/slug-utils';

export default function SellCarPage() {
  const { user } = useAuth();
  const router = useRouter();

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

  const [images, setImages] = useState<File[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  if (!user) {
    router.push('/login');
    return null;
  }

  const handleChange = (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>) => {
    const { name, value } = e.target;
    setFormData(prev => ({ ...prev, [name]: value }));
  };

  const handleImageChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files) {
      const newImages = Array.from(e.target.files).slice(0, 10 - images.length);
      setImages(prev => [...prev, ...newImages]);
    }
  };

  const removeImage = (index: number) => {
    setImages(prev => prev.filter((_, i) => i !== index));
  };

  const uploadImages = async (): Promise<string[]> => {
    const supabase = (await import('@/lib/db/supabase-client')).supabase;
    const uploadedUrls: string[] = [];

    for (const image of images) {
      const fileName = `${Date.now()}-${image.name}`;
      const { data, error } = await supabase.storage
        .from('car-images')
        .upload(fileName, image);

      if (!error && data) {
        const { data: { publicUrl } } = supabase.storage
          .from('car-images')
          .getPublicUrl(data.path);
        uploadedUrls.push(publicUrl);
      }
    }

    return uploadedUrls;
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError('');
    setLoading(true);

    try {
      // Upload images first
      const imageUrls = await uploadImages();

      if (imageUrls.length === 0) {
        setError('Please upload at least one image');
        setLoading(false);
        return;
      }

      // Create base slug with short ID (will be generated after insert)
      // For new listings, we'll update the slug after we get the ID
      const baseSlug = `${formData.brand.toLowerCase()}-${formData.model.toLowerCase().replace(/\s+/g, '-')}-${formData.year}-${formData.price}`;
      const tempSlug = `${baseSlug}-temp`;

      // Insert listing first to get the ID
      const { data: newListing, error: insertError } = await insertListing({
        user_id: user.id,
        brand: formData.brand,
        model: formData.model,
        year: formData.year,
        price: parseInt(formData.price),
        original_price: formData.originalPrice ? parseInt(formData.originalPrice) : null,
        mileage: formData.mileage ? parseInt(formData.mileage) : null,
        transmission: formData.transmission,
        fuel_type: formData.fuelType,
        body_type: formData.bodyType,
        color: formData.color,
        location: formData.location,
        description: formData.description,
        images: imageUrls,
        slug: tempSlug, // Temporary slug
        status: 'pending',
      });

      if (insertError) {
        console.error('Insert error:', insertError);
        if (insertError.code === '23505' || insertError.message?.includes('slug_key')) {
          setError('It looks like this exact listing already exists. Please check your active listings or add a unique detail to the title.');
        } else {
          setError('Failed to create listing. Please try again.');
        }
        setLoading(false);
        return;
      }

      // Generate proper slug with short ID
      if (newListing?.id) {
        const shortId = newListing.id.replace(/-/g, '').slice(-4);
        const color = formData.color || undefined;
        const properSlug = generateSlug(formData.brand, formData.model, formData.year, parseInt(formData.price), shortId, color);

        // Make sure it's unique
        const uniqueSlug = await makeSlugUnique(properSlug);

        // Update the listing with the proper slug
        const { error: updateError } = await supabase
          .from('listings')
          .update({ slug: uniqueSlug })
          .eq('id', newListing.id);

        if (updateError) {
          console.error('Slug update error:', updateError);
        }
      }

      router.push('/my-cars');
    } catch (err: any) {
      console.error('Sell page error:', err);
      setError('An error occurred. Please try again.');
    } finally {
      setLoading(false);
    }
  };

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
        <h1 className="text-3xl font-bold">Sell Your Car</h1>
      </div>

      {error && (
        <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded mb-4">
          {error}
        </div>
      )}

      <form onSubmit={handleSubmit} className="max-w-4xl">
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          {/* Brand */}
          <div>
            <label className="block text-sm font-medium mb-2">Brand *</label>
            <input
              type="text"
              name="brand"
              value={formData.brand}
              onChange={handleChange}
              required
              className="w-full px-4 py-2 border border-gray-300 rounded-lg"
            />
          </div>

          {/* Model */}
          <div>
            <label className="block text-sm font-medium mb-2">Model *</label>
            <input
              type="text"
              name="model"
              value={formData.model}
              onChange={handleChange}
              required
              className="w-full px-4 py-2 border border-gray-300 rounded-lg"
            />
          </div>

          {/* Year */}
          <div>
            <label className="block text-sm font-medium mb-2">Year *</label>
            <input
              type="number"
              name="year"
              value={formData.year}
              onChange={handleChange}
              required
              min="1900"
              max={new Date().getFullYear() + 1}
              className="w-full px-4 py-2 border border-gray-300 rounded-lg"
            />
          </div>

          {/* Price */}
          <div>
            <label className="block text-sm font-medium mb-2">Price (PHP) *</label>
            <input
              type="number"
              name="price"
              value={formData.price}
              onChange={handleChange}
              required
              min="0"
              className="w-full px-4 py-2 border border-gray-300 rounded-lg"
            />
          </div>

          {/* Original Price */}
          <div>
            <label className="block text-sm font-medium mb-2">Original Price (PHP)</label>
            <input
              type="number"
              name="originalPrice"
              value={formData.originalPrice}
              onChange={handleChange}
              min="0"
              className="w-full px-4 py-2 border border-gray-300 rounded-lg"
            />
          </div>

          {/* Mileage */}
          <div>
            <label className="block text-sm font-medium mb-2">Mileage (km)</label>
            <input
              type="number"
              name="mileage"
              value={formData.mileage}
              onChange={handleChange}
              min="0"
              className="w-full px-4 py-2 border border-gray-300 rounded-lg"
            />
          </div>

          {/* Transmission */}
          <div>
            <label className="block text-sm font-medium mb-2">Transmission *</label>
            <select
              name="transmission"
              value={formData.transmission}
              onChange={handleChange}
              className="w-full px-4 py-2 border border-gray-300 rounded-lg"
            >
              <option value="Automatic">Automatic</option>
              <option value="Manual">Manual</option>
            </select>
          </div>

          {/* Fuel Type */}
          <div>
            <label className="block text-sm font-medium mb-2">Fuel Type *</label>
            <select
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

          {/* Body Type */}
          <div>
            <label className="block text-sm font-medium mb-2">Body Type *</label>
            <select
              name="bodyType"
              value={formData.bodyType}
              onChange={handleChange}
              className="w-full px-4 py-2 border border-gray-300 rounded-lg"
            >
              <option value="Sedan">Sedan</option>
              <option value="SUV">SUV</option>
              <option value="Hatchback">Hatchback</option>
              <option value="Pickup">Pickup</option>
              <option value="Van">Van</option>
              <option value="Coupe">Coupe</option>
              <option value="Convertible">Convertible</option>
            </select>
          </div>

          {/* Color */}
          <div>
            <label className="block text-sm font-medium mb-2">Color</label>
            <input
              type="text"
              name="color"
              value={formData.color}
              onChange={handleChange}
              className="w-full px-4 py-2 border border-gray-300 rounded-lg"
            />
          </div>

          {/* Location */}
          <div>
            <label className="block text-sm font-medium mb-2">Location *</label>
            <input
              type="text"
              name="location"
              value={formData.location}
              onChange={handleChange}
              required
              placeholder="e.g., Metro Manila"
              className="w-full px-4 py-2 border border-gray-300 rounded-lg"
            />
          </div>
        </div>

        {/* Description */}
        <div className="mt-6">
          <label className="block text-sm font-medium mb-2">Description</label>
          <textarea
            name="description"
            value={formData.description}
            onChange={handleChange}
            rows={4}
            className="w-full px-4 py-2 border border-gray-300 rounded-lg"
          />
        </div>

        {/* Image Upload */}
        <div className="mt-6">
          <label className="block text-sm font-medium mb-2">Car Images * (max 10)</label>
          <input
            type="file"
            accept="image/*"
            multiple
            onChange={handleImageChange}
            className="w-full"
          />

          {/* Image Preview */}
          {images.length > 0 && (
            <div className="grid grid-cols-2 md:grid-cols-5 gap-4 mt-4">
              {images.map((image, index) => (
                <div key={index} className="relative">
                  <img
                    src={URL.createObjectURL(image)}
                    alt={`Preview ${index + 1}`}
                    className="w-full h-24 object-cover rounded"
                  />
                   <button
                     type="button"
                     onClick={() => removeImage(index)}
                     className="absolute top-0 right-0 bg-red-500 text-white rounded-full w-6 h-6 flex items-center justify-center cursor-pointer"
                   >
                    ×
                  </button>
                </div>
              ))}
            </div>
          )}
        </div>

        <button
          type="submit"
          disabled={loading}
          className="mt-8 w-full bg-blue-600 text-white py-3 rounded-lg hover:bg-blue-700 transition-colors disabled:bg-gray-400 cursor-pointer"
        >
          {loading ? 'Submitting...' : 'Submit Listing'}
        </button>
      </form>
    </div>
  );
}
