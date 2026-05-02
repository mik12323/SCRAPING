-- ==========================================
-- 02-MANUAL-LISTING.SQL
-- Template for manually adding/updating listings
-- Copy, modify, and run in Supabase SQL Editor
-- ==========================================

-- ==========================================
-- TEMPLATE: Add New Listing
-- ==========================================
/*
INSERT INTO public.listings (
  slug,
  brand,
  model,
  year,
  price,
  original_price,
  body_type,
  fuel_type,
  transmission,
  location,
  description,
  images,
  status,
  user_id
) VALUES (
  'toyota-vios-2024-510000',  -- Generate: brand-model-year-price (lowercase, hyphens)
  'Toyota',
  'Vios',
  2024,
  510000,
  NULL,  -- original_price (if打折)
  'Sedan',  -- Sedan, SUV, Hatchback, Pickup, Van, Coupe, Convertible
  'Gas',  -- Gas, Diesel, Electric, Hybrid, Plug-in Hybrid
  'Automatic',  -- Automatic, Manual
  'Metro Manila, Philippines',
  'Description here...',
  ARRAY['https://vxhmbfdrvdsnyldkslkx.supabase.co/storage/v1/object/public/car-images/image1.jpg'],  -- Image URLs from storage
  'approved',  -- pending, approved, rejected
  (SELECT id FROM public.users LIMIT 1)  -- Assign to admin (first user)
);
*/

-- ==========================================
-- TEMPLATE: Update Existing Listing
-- ==========================================
/*
UPDATE public.listings SET
  price = 490000,  -- New price
  original_price = 510000,  -- If price changed
  slug = 'toyota-vios-2024-490000',  -- Update slug if price changed
  updated_at = NOW()
WHERE slug = 'toyota-vios-2024-510000';  -- Find by slug
*/

-- ==========================================
-- TEMPLATE: Update Listing Status
-- ==========================================
/*
UPDATE public.listings SET
  status = 'approved',  -- pending, approved, rejected
  updated_at = NOW()
WHERE slug = 'toyota-vios-2024-510000';
*/

-- ==========================================
-- CHECK: View All Listings
-- ==========================================
/*
SELECT 
  slug,
  brand,
  model,
  year,
  price,
  status,
  user_id,
  created_at
FROM public.listings
ORDER BY created_at DESC;
*/

-- ==========================================
-- CHECK: Find Listing by Brand
-- ==========================================
/*
SELECT 
  slug,
  brand,
  model,
  year,
  price,
  status
FROM public.listings
WHERE brand ILIKE '%toyota%'
ORDER BY year DESC;
*/

-- ==========================================
-- INSTRUCTIONS:
-- 1. Uncomment the section you need (remove /* and */)
-- 2. Modify the values for your car
-- 3. Copy and paste into Supabase SQL Editor
-- 4. Click "Run"
-- ==========================================
