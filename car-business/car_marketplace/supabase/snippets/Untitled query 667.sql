-- Enable RLS on listings if not already enabled
ALTER TABLE public.listings ENABLE ROW LEVEL SECURITY;
-- Drop existing policies if they exist, then recreate
DO $$
BEGIN
  -- Drop existing policies if they exist
  DROP POLICY IF EXISTS "Admins can update all listings" ON public.listings;
  DROP POLICY IF EXISTS "Admins can view all listings" ON public.listings;
  DROP POLICY IF EXISTS "Users can delete own notifications" ON public.notifications;
  DROP POLICY IF EXISTS "Allow authenticated uploads" ON storage.objects;
  DROP POLICY IF EXISTS "Allow users to delete own listing images" ON storage.objects;
  DROP POLICY IF EXISTS "Allow admins to delete images" ON storage.objects;
  DROP POLICY IF EXISTS "Allow public to view images" ON storage.objects;
  
  -- Recreate policies
  CREATE POLICY "Admins can update all listings" ON public.listings
    FOR UPDATE USING ((SELECT is_admin FROM public.users WHERE id = auth.uid()) = true);
  CREATE POLICY "Admins can view all listings" ON public.listings
    FOR SELECT USING ((SELECT is_admin FROM public.users WHERE id = auth.uid()) = true OR status = 'approved');
  -- Fix notifications delete policy
  ALTER TABLE public.notifications ENABLE ROW LEVEL SECURITY;
  CREATE POLICY "Users can delete own notifications" ON public.notifications
    FOR DELETE USING (auth.uid()::text = user_id::text);
  -- Storage policies for car-images bucket
  CREATE POLICY "Allow authenticated uploads" ON storage.objects
    FOR INSERT WITH CHECK (
      bucket_id = 'car-images' AND
      auth.role() = 'authenticated'
    );
  CREATE POLICY "Allow users to delete own listing images" ON storage.objects
    FOR DELETE USING (
      bucket_id = 'car-images' AND
      EXISTS (
        SELECT 1 FROM public.listings
        WHERE listings.user_id = auth.uid()
        AND listings.images::text LIKE '%' || storage.objects.name || '%'
      )
    );
  CREATE POLICY "Allow admins to delete images" ON storage.objects
    FOR DELETE USING (
      bucket_id = 'car-images' AND
      (SELECT is_admin FROM public.users WHERE id = auth.uid()) = true
    );
  CREATE POLICY "Allow public to view images" ON storage.objects
    FOR SELECT USING (bucket_id = 'car-images');
END $$;