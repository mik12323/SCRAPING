-- Create listings table in Supabase
CREATE TABLE IF NOT EXISTS public.listings (
  id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
  slug text UNIQUE NOT NULL,
  brand text NOT NULL,
  model text NOT NULL,
  year integer NOT NULL,
  price integer NOT NULL,
  original_price integer,
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

-- Create index for faster queries
CREATE INDEX IF NOT EXISTS idx_listings_status ON public.listings(status);
CREATE INDEX IF NOT EXISTS idx_listings_brand ON public.listings(brand);
CREATE INDEX IF NOT EXISTS idx_listings_year ON public.listings(year);
CREATE INDEX IF NOT EXISTS idx_listings_price ON public.listings(price);
CREATE INDEX IF NOT EXISTS idx_listings_slug ON public.listings(slug);

-- Enable Row Level Security (read-only for public)
ALTER TABLE public.listings ENABLE ROW LEVEL SECURITY;

-- Allow public to view approved listings only
CREATE POLICY "Public can view approved listings" 
  ON public.listings FOR SELECT 
  USING (status = 'approved');

-- Allow authenticated users to insert their own listings (for future use)
CREATE POLICY "Users can create own listings" 
  ON public.listings FOR INSERT 
  WITH CHECK (auth.uid()::text = user_id::text);

-- Allow users to update own listings (for future use)
CREATE POLICY "Users can update own listings" 
  ON public.listings FOR UPDATE 
  USING (auth.uid()::text = user_id::text);

-- Allow users to delete own listings (for future use)
CREATE POLICY "Users can delete own listings" 
  ON public.listings FOR DELETE 
  USING (auth.uid()::text = user_id::text);

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER update_listings_updated_at 
  BEFORE UPDATE ON public.listings 
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Create storage bucket for car images (run this in Storage > Create Bucket or via SQL)
-- INSERT INTO storage.buckets (id, name, public, file_size_limit, allowed_mime_types)
-- VALUES ('car-images', 'car-images', true, 5242880, ARRAY['image/jpeg', 'image/png', 'image/webp'])
-- ON CONFLICT (id) DO NOTHING;
