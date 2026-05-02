-- ==========================================
-- 03-ADD-COLOR-MILEAGE.SQL
-- Run this in Supabase SQL Editor to add missing columns
-- ==========================================

-- Add color column
ALTER TABLE public.listings ADD COLUMN IF NOT EXISTS color TEXT;

-- Add mileage column
ALTER TABLE public.listings ADD COLUMN IF NOT EXISTS mileage INTEGER;

-- Verify the columns were added
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'listings' 
  AND column_name IN ('color', 'mileage');
