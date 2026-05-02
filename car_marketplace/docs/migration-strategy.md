# Migration Strategy: Mock → Supabase

## Overview

This document outlines the step-by-step migration from the current mock data system to a production Supabase backend.

---

## Current Architecture (Mock Data)

```
Frontend Components (React/Next.js)
         ↓
    /src/lib/api.ts
         ↓
  /src/lib/db/queries/*.ts (currently re-exports mock)
         ↓
    /src/lib/mock-data.ts
```

---

## Target Architecture (Supabase)

```
Frontend Components (React/Next.js)
         ↓
    /src/lib/api.ts
         ↓
  /src/lib/db/queries/*.ts (Supabase queries)
         ↓
    Supabase Postgres Database
```

---

## Migration Mapping

### Data Mapping

| Mock System | Supabase System | Notes |
|-------------|-----------------|-------|
| `mockCars[]` | `listings` table | UUIDs replace string IDs |
| `mockCars[].id` | `listings.id` (UUID) | Auto-generated |
| `mockCars[].slug` | `listings.slug` | Unique, SEO-friendly |
| `mockCars[].user_id` | `listings.user_id` → `users.id` | FK relationship |
| `mockTrendingCars[]` | `listing_views` table + view | Aggregated daily |
| `featuredCars` | Query: `ORDER BY created_at LIMIT 6` | First 6 approved |
| `BODY_TYPES`, `FUEL_TYPES` | ENUM or CHECK constraint | Data integrity |

---

## Step-by-Step Migration

### Phase 1: Prepare Supabase Project ✅ (Design Done)
- [x] Create database schema (`docs/database-schema.sql`)
- [x] Define relationships (`docs/schema-relationships.txt`)
- [x] Set up folder structure (`/src/lib/db/`)

### Phase 2: Environment Setup (Future)
```bash
# .env.local
NEXT_PUBLIC_SUPABASE_URL=https://your-project.supabase.co
NEXT_PUBLIC_SUPABASE_ANON_KEY=your-anon-key
SUPABASE_SERVICE_ROLE_KEY=your-service-role-key
```

### Phase 3: Initialize Supabase Client (Future)
```typescript
// /src/lib/db/supabase-client.ts
import { createClient } from '@supabase/supabase-js'

export const supabase = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL!,
  process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!
)
```

### Phase 4: Replace Query Functions (Future)
```typescript
// /src/lib/db/queries/cars.ts

// BEFORE (Mock):
export async function getCars(filters?, page, perPage) {
  // ... mock filtering logic
  return { cars: filteredCars, total, totalPages, currentPage }
}

// AFTER (Supabase):
export async function getCars(filters?: CarFilters, page = 1, perPage = 12) {
  let query = supabase
    .from('listings')
    .select('*', { count: 'exact' })
    .eq('status', 'approved')

  if (filters?.brand) {
    query = query.ilike('brand', `%${filters.brand}%`)
  }
  // ... more filters

  const { data, error, count } = await query.range(
    (page - 1) * perPage,
    page * perPage - 1
  )

  if (error) throw error

  return {
    cars: data as Car[],
    total: count || 0,
    totalPages: Math.ceil((count || 0) / perPage),
    currentPage: page
  }
}
```

### Phase 5: Update API Layer (Future)
```typescript
// /src/lib/api.ts
// No changes needed! It already imports from db/queries/*.ts
// Just ensure db/queries/*.ts uses Supabase instead of mock
```

### Phase 6: Frontend Components (No Changes Needed)
✅ **All frontend components will work WITHOUT any changes**

The components import from `/src/lib/api.ts`, which will seamlessly switch from mock to Supabase.

---

## What Changes vs What Stays

### Will Change:
| Component | Change Required |
|-----------|------------------|
| `/src/lib/db/supabase-client.ts` | Implement real Supabase connection |
| `/src/lib/db/queries/cars.ts` | Replace mock logic with Supabase queries |
| `/src/lib/db/queries/users.ts` | Implement real user queries |
| `/src/lib/db/mutations/listings.ts` | Implement real mutations |
| Environment variables | Add Supabase credentials |

### Will NOT Change:
| Component | Reason |
|-----------|--------|
| All `/src/app/**/*.tsx` | They use api.ts, abstraction works |
| All `/src/components/**/*.tsx` | They receive props, no direct DB access |
| `/src/lib/api.ts` | Already structured correctly |
| `/src/lib/types.ts` | Types match database schema |

---

## Testing Strategy

### Before Migration:
1. Verify all mock data flows work
2. Document current behavior (screenshots/recordings)

### After Migration:
1. **Parallel Testing**: Run both mock and Supabase side-by-side
2. **Data Validation**: Ensure Supabase returns same structure as mock
3. **Feature Parity**: All features work with Supabase

---

## Rollback Plan

If Supabase migration fails:
1. Keep mock implementations in `api-legacy.ts`
2. Revert `/src/lib/api.ts` to import from `api-legacy.ts`
3. All functionality restored in < 5 minutes

---

## Benefits of This Approach

1. **Zero Frontend Changes**: UI components untouched
2. **Incremental Migration**: Can migrate one query at a time
3. **Rollback Ready**: Easy to revert if issues arise
4. **Type Safety**: TypeScript ensures Supabase returns match expectations
5. **Future-Proof**: Clean separation of concerns

---

## Next Steps (After Supabase Setup)

1. Run `database-schema.sql` in Supabase SQL editor
2. Configure environment variables
3. Install `@supabase/supabase-js`
4. Uncomment Supabase client in `db/supabase-client.ts`
5. Replace one query function at a time
6. Test thoroughly after each replacement
7. Remove mock data files when migration complete
