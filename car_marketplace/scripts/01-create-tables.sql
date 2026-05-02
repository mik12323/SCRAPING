-- ==========================================
-- 01-CREATE-TABLES.SQL
-- Run this FIRST in Supabase SQL Editor
-- ==========================================

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 0. Create users table (for Flask-style username auth)
CREATE TABLE IF NOT EXISTS public.users (
  id uuid REFERENCES auth.users(id) PRIMARY KEY,
  name text,
  phone text NOT NULL,
  is_admin boolean DEFAULT false,
  created_at timestamp with time zone DEFAULT now()
);

-- Enable RLS on users
ALTER TABLE public.users ENABLE ROW LEVEL SECURITY;

-- Users can view their own profile
DROP POLICY IF EXISTS "Users can view own profile" ON public.users;
CREATE POLICY "Users can view own profile"
  ON public.users FOR SELECT
  USING (auth.uid()::text = id::text);

-- Users can update their own profile
DROP POLICY IF EXISTS "Users can update own profile" ON public.users;
CREATE POLICY "Users can update own profile"
  ON public.users FOR UPDATE
  USING (auth.uid()::text = id::text);

-- 1. Create listings table
CREATE TABLE IF NOT EXISTS public.listings (
  id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
  slug text UNIQUE NOT NULL,
  brand text NOT NULL,
  model text NOT NULL,
  year integer NOT NULL,
  price integer NOT NULL,
  original_price integer,
  color text,
  mileage integer,
  body_type text,
  fuel_type text,
  transmission text,
  location text DEFAULT 'Metro Manila, Philippines',
  description text,
  images text[],
  status text DEFAULT 'approved' CHECK (status IN ('pending', 'approved', 'rejected')),
  created_at timestamp with time zone DEFAULT now(),
  updated_at timestamp with time zone DEFAULT now(),
  user_id uuid REFERENCES auth.users(id)
);

-- 2. Create indexes for faster queries
CREATE INDEX IF NOT EXISTS idx_listings_status ON public.listings(status);
CREATE INDEX IF NOT EXISTS idx_listings_brand ON public.listings(brand);
CREATE INDEX IF NOT EXISTS idx_listings_year ON public.listings(year);
CREATE INDEX IF NOT EXISTS idx_listings_price ON public.listings(price);
CREATE INDEX IF NOT EXISTS idx_listings_slug ON public.listings(slug);
CREATE INDEX IF NOT EXISTS idx_listings_user_id ON public.listings(user_id);
CREATE INDEX IF NOT EXISTS idx_listings_body_type ON public.listings(body_type);
CREATE INDEX IF NOT EXISTS idx_listings_fuel_type ON public.listings(fuel_type);
CREATE INDEX IF NOT EXISTS idx_listings_transmission ON public.listings(transmission);
CREATE INDEX IF NOT EXISTS idx_listings_brand_model ON public.listings(brand, model);
CREATE INDEX IF NOT EXISTS idx_listings_approved_created 
  ON public.listings(created_at DESC) 
  WHERE status = 'approved';

-- 3. Enable Row Level Security
ALTER TABLE public.listings ENABLE ROW LEVEL SECURITY;

-- 4. RLS Policies (drop first if exists)
DROP POLICY IF EXISTS "Public can view approved listings" ON public.listings;
DROP POLICY IF EXISTS "Authenticated users can create own listings" ON public.listings;
DROP POLICY IF EXISTS "Users can view own listings" ON public.listings;
DROP POLICY IF EXISTS "Users can update own listings" ON public.listings;
DROP POLICY IF EXISTS "Users can delete own listings" ON public.listings;
DROP POLICY IF EXISTS "Admins can view all listings" ON public.listings;

-- Public can view approved listings
CREATE POLICY "Public can view approved listings" 
  ON public.listings FOR SELECT 
  USING (status = 'approved');

-- Users can view their own listings (all statuses)
CREATE POLICY "Users can view own listings" 
  ON public.listings FOR SELECT 
  USING (auth.uid()::text = user_id::text);

-- Admins can view all listings
CREATE POLICY "Admins can view all listings" 
  ON public.listings FOR SELECT 
  USING (
    EXISTS (
      SELECT 1 FROM public.users 
      WHERE users.id::text = auth.uid()::text 
      AND users.is_admin = true
    )
  );

-- Authenticated users can create own listings
CREATE POLICY "Authenticated users can create own listings" 
  ON public.listings FOR INSERT 
  WITH CHECK (auth.uid()::text = user_id::text);

-- Users can update own listings (FIXED: added WITH CHECK)
CREATE POLICY "Users can update own listings" 
  ON public.listings FOR UPDATE 
  USING (auth.uid()::text = user_id::text)
  WITH CHECK (auth.uid()::text = user_id::text);

-- Users can delete own listings
CREATE POLICY "Users can delete own listings" 
  ON public.listings FOR DELETE 
  USING (auth.uid()::text = user_id::text);

-- 5. Updated_at trigger
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_listings_updated_at ON public.listings;
CREATE TRIGGER update_listings_updated_at 
  BEFORE UPDATE ON public.listings 
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- 6. Create listing_views table (for click tracking)
CREATE TABLE IF NOT EXISTS public.listing_views (
  id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
  listing_id uuid NOT NULL REFERENCES public.listings(id) ON DELETE CASCADE,
  view_date date NOT NULL DEFAULT CURRENT_DATE,
  view_count integer DEFAULT 1,
  UNIQUE(listing_id, view_date)
);

-- Function to track car views
CREATE OR REPLACE FUNCTION public.track_car_view(car_id uuid)
RETURNS void AS $$
BEGIN
  INSERT INTO public.listing_views (listing_id, view_date, view_count)
  VALUES (car_id, CURRENT_DATE, 1)
  ON CONFLICT (listing_id, view_date)
  DO UPDATE SET view_count = listing_views.view_count + 1;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Function to get trending cars
CREATE OR REPLACE FUNCTION public.get_trending_cars(limit_count int DEFAULT 5)
RETURNS TABLE (
  brand text,
  model text,
  display_name text,
  clicks bigint,
  body_type text,
  fuel_type text,
  transmission text
) AS $$
BEGIN
  RETURN QUERY
  SELECT
    l.brand,
    l.model,
    (l.year || ' ' || l.brand || ' ' || l.model)::text as display_name,
    COALESCE(SUM(lv.view_count), 0) as clicks,
    l.body_type,
    l.fuel_type,
    l.transmission
  FROM public.listings l
  LEFT JOIN public.listing_views lv ON l.id = lv.listing_id
  WHERE l.status = 'approved'
  GROUP BY l.id, l.brand, l.model, l.year, l.body_type, l.fuel_type, l.transmission
  ORDER BY clicks DESC
  LIMIT limit_count;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- ==========================================
-- INSTRUCTIONS:
-- 1. Go to Supabase Dashboard > SQL Editor
-- 2. Copy and paste this entire script
-- 3. Click "Run"
-- 4. Then go to Storage > Create Bucket:
--    - Name: car-images
--    - Public: YES
--    - File size limit: 5MB
--    - Allowed MIME types: image/jpeg, image/png, image/webp
-- ==========================================
