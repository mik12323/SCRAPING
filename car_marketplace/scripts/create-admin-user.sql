-- Create admin user in auth.users (this needs to be done via Supabase Dashboard or Auth API)
-- After creating the auth user, insert into public users table:

-- Replace 'YOUR-ADMIN-UUID-HERE' with the actual UUID from auth.users
-- You can create the user via Supabase Dashboard > Authentication > Add User

-- Then run this to set admin privileges:
INSERT INTO public.users (id, phone, is_admin, created_at)
VALUES (
  '00000000-0000-0000-0000-000000000001'::uuid, 
  '+639970946623', 
  true, 
  now()
)
ON CONFLICT (id) DO UPDATE SET is_admin = true;
