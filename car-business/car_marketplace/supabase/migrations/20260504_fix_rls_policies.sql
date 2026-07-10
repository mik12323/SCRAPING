-- Enable RLS on listings if not already enabled
ALTER TABLE public.listings ENABLE ROW LEVEL SECURITY;

-- Allow admins to update all listings
CREATE POLICY IF NOT EXISTS "Admins can update all listings" ON public.listings
FOR UPDATE USING ((SELECT is_admin FROM public.users WHERE id = auth.uid()) = true);

-- Allow admins to view all listings (for admin dashboard)
CREATE POLICY IF NOT EXISTS "Admins can view all listings" ON public.listings
FOR SELECT USING ((SELECT is_admin FROM public.users WHERE id = auth.uid()) = true OR status = 'approved');

-- Fix notifications delete policy
ALTER TABLE public.notifications ENABLE ROW LEVEL SECURITY;

-- Allow users to delete their own notifications
CREATE POLICY IF NOT EXISTS "Users can delete own notifications" ON public.notifications
FOR DELETE USING (auth.uid()::text = user_id::text);

-- Storage policies for car-images bucket
-- Allow authenticated users to upload images
CREATE POLICY IF NOT EXISTS "Allow authenticated uploads" ON storage.objects
FOR INSERT WITH CHECK (
  bucket_id = 'car-images' AND
  auth.role() = 'authenticated'
);

-- Allow users to delete images from their own listings
-- This uses a subquery to check if the image path is in the user's listings
CREATE POLICY IF NOT EXISTS "Allow users to delete own listing images" ON storage.objects
FOR DELETE USING (
  bucket_id = 'car-images' AND
  EXISTS (
    SELECT 1 FROM public.listings
    WHERE listings.user_id = auth.uid()
    AND listings.images::text LIKE '%' || storage.objects.name || '%'
  )
);

-- Allow admins to delete any image
CREATE POLICY IF NOT EXISTS "Allow admins to delete images" ON storage.objects
FOR DELETE USING (
  bucket_id = 'car-images' AND
  (SELECT is_admin FROM public.users WHERE id = auth.uid()) = true
);

-- Allow public to view images
CREATE POLICY IF NOT EXISTS "Allow public to view images" ON storage.objects
FOR SELECT USING (bucket_id = 'car-images');
