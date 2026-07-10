# Supabase Setup Instructions

## Step 1: Create Storage Bucket

1. Go to [Supabase Dashboard](https://app.supabase.com)
2. Select your project
3. Go to **Storage** > **Buckets**
4. Click **Create Bucket**
5. Enter:
   - **Name**: `car-images`
   - **Public bucket**: ✅ Checked
   - **File size limit**: 5MB
   - **Allowed MIME types**: `image/jpeg`, `image/png`, `image/webp`
6. Click **Create Bucket**

## Step 2: Create Listings Table

1. Go to **SQL Editor** in Supabase Dashboard
2. Run this SQL:

```sql
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
  updated_at timestamp with time zone DEFAULT now()
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_listings_status ON public.listings(status);
CREATE INDEX IF NOT EXISTS idx_listings_brand ON public.listings(brand);
CREATE INDEX IF NOT EXISTS idx_listings_year ON public.listings(year);
CREATE INDEX IF NOT EXISTS idx_listings_price ON public.listings(price);
CREATE INDEX IF NOT EXISTS idx_listings_slug ON public.listings(slug);

-- Enable RLS
ALTER TABLE public.listings ENABLE ROW LEVEL SECURITY;

-- Policy: Public can view approved listings
CREATE POLICY "Public can view approved listings" 
  ON public.listings FOR SELECT 
  USING (status = 'approved');

-- Policy: Allow inserts (for import script with service role)
-- NOTE: Service role bypasses RLS automatically
```

## Step 3: Get Service Role Key

1. Go to **Project Settings** > **API**
2. Copy the **service_role** key (NOT the anon key)
3. Add to `car_marketplace/.env.local`:

```env
NEXT_PUBLIC_SUPABASE_URL=https://your-project.supabase.co
NEXT_PUBLIC_SUPABASE_ANON_KEY=your-anon-key
SUPABASE_SERVICE_ROLE_KEY=your-service-role-key-here
```

## Step 4: Run Import

```bash
cd car_marketplace
npm run import
```

## Notes

- The import script uses the **service role key** to bypass RLS
- Images are uploaded to Supabase Storage `car-images` bucket
- The `body_type`, `fuel_type`, and `transmission` fields are left NULL (to be filled manually later)
- All imported listings have `status = 'approved'`
