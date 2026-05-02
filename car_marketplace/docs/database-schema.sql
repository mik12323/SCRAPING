-- ============================================
-- SUPABASE POSTGRES DATABASE SCHEMA
-- Used Cars Philippines Marketplace
-- ============================================

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================
-- 1. USERS TABLE
-- ============================================
CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255),
    phone VARCHAR(50) NOT NULL,
    email VARCHAR(255),
    is_admin BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes
CREATE INDEX idx_users_email ON users(email) WHERE email IS NOT NULL;
CREATE INDEX idx_users_phone ON users(phone);

-- ============================================
-- 2. LISTINGS TABLE
-- ============================================
CREATE TABLE listings (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    slug VARCHAR(255) NOT NULL UNIQUE,
    brand VARCHAR(100) NOT NULL,
    model VARCHAR(100) NOT NULL,
    year INTEGER NOT NULL CHECK (year >= 1900 AND year <= EXTRACT(YEAR FROM NOW()) + 1),
    price INTEGER NOT NULL CHECK (price > 0),
    original_price INTEGER CHECK (original_price > 0),
    body_type VARCHAR(50),
    fuel_type VARCHAR(50),
    transmission VARCHAR(50),
    location TEXT NOT NULL,
    description TEXT,
    images TEXT[], -- PostgreSQL array type for image URLs
    status VARCHAR(20) NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'approved', 'rejected')),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX idx_listings_slug ON listings(slug);
CREATE INDEX idx_listings_brand ON listings(brand);
CREATE INDEX idx_listings_status ON listings(status);
CREATE INDEX idx_listings_created_at ON listings(created_at DESC);
CREATE INDEX idx_listings_user_id ON listings(user_id);
CREATE INDEX idx_listings_brand_model ON listings(brand, model);
CREATE INDEX idx_listings_status_created ON listings(status, created_at DESC) WHERE status = 'approved';

-- ============================================
-- 3. LISTING VIEWS (CLICKS) TABLE
-- ============================================
CREATE TABLE listing_views (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    listing_id UUID NOT NULL REFERENCES listings(id) ON DELETE CASCADE,
    view_date DATE NOT NULL DEFAULT CURRENT_DATE,
    view_count INTEGER DEFAULT 1,
    UNIQUE(listing_id, view_date)
);

-- Indexes
CREATE INDEX idx_listing_views_listing_id ON listing_views(listing_id);
CREATE INDEX idx_listing_views_date ON listing_views(view_date);

-- ============================================
-- 4. ROW LEVEL SECURITY (RLS)
-- ============================================
ALTER TABLE users ENABLE ROW LEVEL SECURITY;
ALTER TABLE listings ENABLE ROW LEVEL SECURITY;
ALTER TABLE listing_views ENABLE ROW LEVEL SECURITY;

-- Public can read approved listings
CREATE POLICY "Public can view approved listings"
    ON listings FOR SELECT
    USING (status = 'approved');

-- Users can manage their own listings
CREATE POLICY "Users can manage own listings"
    ON listings FOR ALL
    USING (auth.uid() = user_id);

-- ============================================
-- 5. HELPER FUNCTIONS
-- ============================================

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger for updated_at
CREATE TRIGGER update_listings_updated_at
    BEFORE UPDATE ON listings
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ============================================
-- 6. SAMPLE ENUMS (Optional - for data integrity)
-- ============================================
-- Note: Using VARCHAR for flexibility, but can be converted to ENUM later
-- CREATE TYPE body_type_enum AS ENUM ('Sedan', 'SUV', 'Hatchback', 'Truck', 'Van', 'Coupe', 'Convertible', 'Wagon', 'Pickup', 'Crossover');
-- CREATE TYPE fuel_type_enum AS ENUM ('Gas', 'Diesel', 'Electric', 'Hybrid', 'Plug-in Hybrid');
-- CREATE TYPE transmission_enum AS ENUM ('Automatic', 'Manual');
-- CREATE TYPE status_enum AS ENUM ('pending', 'approved', 'rejected');

-- ============================================
-- 7. TRENDING CARS VIEW (Optional - for performance)
-- ============================================
CREATE OR REPLACE VIEW trending_cars AS
SELECT
    l.brand,
    l.model,
    CONCAT(l.brand, ' ', SPLIT_PART(l.model, ' ', 1)) as display_name,
    COALESCE(SUM(lv.view_count), 0) as total_views,
    l.body_type,
    l.fuel_type,
    l.transmission
FROM listings l
LEFT JOIN listing_views lv ON l.id = lv.listing_id
WHERE l.status = 'approved'
GROUP BY l.brand, l.model, l.body_type, l.fuel_type, l.transmission
HAVING COALESCE(SUM(lv.view_count), 0) >= 5
ORDER BY total_views DESC;
