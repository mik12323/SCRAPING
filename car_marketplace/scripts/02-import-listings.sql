-- ==========================================
-- 02-IMPORT-LISTINGS.SQL
-- Run this SECOND after 01-create-tables.sql
-- This creates a function to help with importing listings
-- ==========================================

-- Function to import a listing (handles duplicate slugs)
CREATE OR REPLACE FUNCTION public.import_listing(
  p_brand text,
  p_model text,
  p_year integer,
  p_price integer,
  p_original_price integer DEFAULT NULL,
  p_color text DEFAULT NULL,
  p_mileage integer DEFAULT NULL,
  p_body_type text DEFAULT NULL,
  p_fuel_type text DEFAULT NULL,
  p_transmission text DEFAULT NULL,
  p_location text DEFAULT 'Metro Manila, Philippines',
  p_description text DEFAULT NULL,
  p_images text[] DEFAULT NULL,
  p_user_id uuid DEFAULT NULL
)
RETURNS uuid AS $$
DECLARE
  v_slug text;
  v_id uuid;
BEGIN
  -- Generate slug
  v_slug := lower(p_brand || '-' || p_model || '-' || p_year || '-' || p_price);
  v_slug := replace(v_slug, ' ', '-');
  
  -- Insert or update
  INSERT INTO public.listings (
    brand, model, year, price, original_price, color, mileage,
    body_type, fuel_type, transmission, location, description, images, user_id, status
  ) VALUES (
    p_brand, p_model, p_year, p_price, p_original_price, p_color, p_mileage,
    p_body_type, p_fuel_type, p_transmission, p_location, p_description, p_images, p_user_id, 'approved'
  )
  ON CONFLICT (slug) 
  DO UPDATE SET
    price = EXCLUDED.price,
    original_price = EXCLUDED.original_price,
    color = EXCLUDED.color,
    mileage = EXCLUDED.mileage,
    updated_at = now()
  RETURNING id INTO v_id;
  
  RETURN v_id;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- ==========================================
-- INSTRUCTIONS:
-- 1. Run this after 01-create-tables.sql
-- 2. This enables the import script to work properly
-- ==========================================
