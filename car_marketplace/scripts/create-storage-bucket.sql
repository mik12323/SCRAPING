-- Create storage bucket for car images
INSERT INTO storage.buckets (id, name, public, file_size_limit, allowed_mime_types)
VALUES (
  'car-images', 
  'car-images', 
  true, 
  5242880, -- 5MB limit
  ARRAY['image/jpeg', 'image/png', 'image/webp']
)
ON CONFLICT (id) DO NOTHING;

-- Allow public access to view images
CREATE POLICY "Public can view car images" 
  ON storage.objects FOR SELECT 
  USING (bucket_id = 'car-images');

-- Allow authenticated users to upload images (for future use)
CREATE POLICY "Authenticated users can upload car images" 
  ON storage.objects FOR INSERT 
  WITH CHECK (bucket_id = 'car-images' AND auth.role() = 'authenticated');

-- Allow users to update their own images (for future use)
CREATE POLICY "Users can update own car images" 
  ON storage.objects FOR UPDATE 
  USING (bucket_id = 'car-images' AND auth.uid()::text = (storage.foldername(name))[1]);
