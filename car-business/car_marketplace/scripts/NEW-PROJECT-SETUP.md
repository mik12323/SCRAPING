# New Supabase Project Setup Guide

When creating a new Supabase project, follow this sequence:

## Prerequisites
1. Create new project at https://supabase.com
2. Get your project URL and anon key from Project Settings > API
3. Update `.env.local`:
   ```
   NEXT_PUBLIC_SUPABASE_URL=https://YOUR_PROJECT_ID.supabase.co
   NEXT_PUBLIC_SUPABASE_ANON_KEY=your-anon-key
   SUPABASE_SERVICE_ROLE_KEY=your-service-role-key
   ```

4. Update `next.config.ts` - change hostname to your new project:
   ```typescript
   remotePatterns: [
     {
       protocol: 'https',
       hostname: 'YOUR_PROJECT_ID.supabase.co',  // CHANGE THIS
       port: '',
       pathname: '/storage/v1/object/public/car-images/**',
     },
   ]
   ```

## SQL Scripts Execution Order

### 1. Run `01-create-tables.sql` FIRST
- Go to Supabase Dashboard > SQL Editor
- Copy entire contents of `scripts/01-create-tables.sql`
- Click "Run"
- This creates:
  - `users` table with RLS
  - `listings` table with `color` and `mileage` columns
  - All indexes for performance
  - All RLS policies (FIXED - with WITH CHECK)
  - `listing_views` table
  - Helper functions

### 2. Run `02-import-listings.sql` SECOND
- Copy entire contents of `scripts/02-import-listings.sql`
- Click "Run"
- This creates the `import_listing()` function for bulk imports

### 3. Create Storage Bucket
- Go to Storage > New Bucket
- Name: `car-images`
- Public: ✅ YES
- File size limit: `5MB`
- Allowed MIME types: `image/jpeg, image/png, image/webp`

OR run this SQL:
```sql
INSERT INTO storage.buckets (id, name, public, file_size_limit, allowed_mime_types)
VALUES (
  'car-images', 
  'car-images', 
  true, 
  5242880,
  ARRAY['image/jpeg', 'image/png', 'image/webp']
)
ON CONFLICT (id) DO NOTHING;
```

### 4. Create Admin User (Optional)
- Visit `http://localhost:3000/setup-admin`
- Or use the API: `POST /api/setup-admin` with `{ "username": "admin", "password": "password", "phone": "+639123456789" }`

## Verification Checklist
- [ ] Tables `users` and `listings` exist in Table Editor
- [ ] RLS policies visible in Authentication > Policies
- [ ] Storage bucket `car-images` exists and is public
- [ ] Can login and view `/browse`
- [ ] Can view all listings (pending/approved/rejected) on `/my-cars`
- [ ] Admin can view all listings on `/admin`
- [ ] Edit listing works (no RLS error)

## Importing Cars
```bash
cd D:\Programs\Scraping\car_marketplace
# Make sure SUPABASE_SERVICE_ROLE_KEY is in .env.local
npm run import
```

The enhanced import script will:
- Extract `brand`, `model`, `year`, `price` from folder names
- Extract `color` and `mileage` from `details.txt` (various formats supported)
- Upload images to Supabase Storage
- Skip duplicates automatically
