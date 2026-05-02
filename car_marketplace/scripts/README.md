# SQL Scripts Setup Guide

Run these scripts in your **Supabase SQL Editor** in this exact order:

## 1. `01-create-tables.sql` - Main Setup
**Run this FIRST**
- Creates `users` and `listings` tables
- Adds `color` and `mileage` columns
- Creates all indexes for performance
- Sets up Row Level Security (RLS) policies (FIXED - now allows admins to view all listings)
- Creates `listing_views` table for click tracking
- Creates helper functions (`track_car_view`, `get_trending_cars`)

## 2. `02-import-listings.sql` - Import Helper
**Run this SECOND** (after 01)
- Creates `import_listing()` function to help with bulk imports
- Handles duplicate slugs automatically

## 3. `03-add-color-mileage.sql` - Only if upgrading old DB
**Skip this if you ran 01** (already includes these columns)
- Adds `color` and `mileage` columns if they don't exist

## 4. Create Storage Bucket
After running the SQL scripts, go to **Storage > Create Bucket**:
- Name: `car-images`
- Public: **YES** ✓
- File size limit: `5MB`
- Allowed MIME types: `image/jpeg, image/png, image/webp`

## 5. Update Next.js Config
Make sure `next.config.ts` has your Supabase project hostname:
```typescript
remotePatterns: [
  {
    protocol: 'https',
    hostname: 'YOUR_PROJECT_ID.supabase.co', // Change this!
    port: '',
    pathname: '/storage/v1/object/public/car-images/**',
  },
]
```

## Verification Checklist
After running all scripts:
- [ ] Can you see `listings` table in Table Editor?
- [ ] Can you see `users` table?
- [ ] Can you see RLS policies in Authentication > Policies?
- [ ] Storage bucket `car-images` exists and is public?
- [ ] Can login and view approved cars on /browse?
- [ ] Can view pending/rejected cars on /my-cars?
- [ ] Can view all cars on /admin (if admin)?
