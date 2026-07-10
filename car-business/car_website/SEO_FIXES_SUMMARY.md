# SEO Fixes Summary - Used Cars Philippines

## Issues Found by Semrush
1. Invalid structured data (JSON-LD schema)
2. Duplicate title tags and meta descriptions
3. Robots.txt format issues
4. Sitemap.xml included incorrect pages
5. HTTP/HTTPS and www/non-www duplicate content
6. No mobile/desktop differentiation needed (responsive design)

## Fixes Implemented

### 1. JSON-LD Schema (Structured Data)
**File:** `templates/details.html` (lines 34-60)

Added/updated Car schema with required fields:
- `model`: Extracted from database (`listing['model']`)
- `vehicleIdentificationNumber`: Set to "NOT_AVAILABLE" (placeholder)
- `price`: Numeric value (not string with ₱ sign)
- `priceCurrency`: "PHP"
- `itemCondition`: "https://schema.org/UsedCondition"
- `availability`: "https://schema.org/InStock"
- `brand.name`: Extracted from database (`listing['brand']`)

### 2. Dynamic Title & Meta Description
**File:** `templates/details.html` (lines 6-10)

- Title format: `{{ year }} {{ brand }} {{ model }} - ₱{{ listing.price }} | Used Cars Philippines`
- Description includes: brand, model, year, price, body_type, fuel_type, location

### 3. Robots.txt
**File:** `app.py` (lines 958-964)

Blocks crawlers from:
- `/login`, `/register`, `/sell-car`, `/my-cars`, `/admin`, `/api/`

### 4. Sitemap.xml
**File:** `app.py` (lines 967-998)

- Only includes `status = 'approved'` listings
- Static pages: `/`, `/browse`, `/about`
- Includes `lastmod` dates from `updated_at` column
- Uses correct `https://www.usedcarsphilippines.com.ph` base URL

### 5. HTTPS & WWW Redirects
**File:** `app.py` (lines 18-28)

- `force_https()`: Redirects HTTP → HTTPS (production only)
- `force_www()`: Redirects `usedcarsphilippines.com.ph` → `www.usedcarsphilippines.com.ph`

### 6. Canonical Tags
**Files:** All templates (`index.html`, `browse.html`, `details.html`, `about.html`, `sell_car.html`)

All canonical tags now use:
```html
<link rel="canonical" href="https://www.usedcarsphilippines.com.ph{{ request.path }}">
```

### 7. Organization Schema
**Files:** `index.html`, `browse.html`, `about.html`, `sell_car.html`

Added Organization schema for main pages (not needed for details.html which uses Car schema).

## Database Changes
**File:** `create_db.py`

Updated `listings` table to include:
- `body_type` column
- `fuel_type` column
- `transmission` column

## Testing
- JSON-LD schema validated (numeric price, correct fields)
- Robots.txt blocks correct paths
- Sitemap.xml only shows approved listings
- Canonical tags point to https://www version
- Dynamic meta tags render correctly

## Next Steps
1. Deploy to PythonAnywhere
2. Submit sitemap.xml to Google Search Console
3. Request indexing for car listing pages
4. Monitor Search Console for any remaining issues
5. Consider adding more structured data (Organization, BreadcrumbList)

## Key Files Modified
- `app.py`: SEO routes, redirects, database schema
- `templates/details.html`: JSON-LD schema, dynamic meta tags, canonical tag
- `templates/index.html`: Canonical tag, Organization schema
- `templates/browse.html`: Canonical tag, Organization schema
- `templates/about.html`: Canonical tag, Organization schema
- `templates/sell_car.html`: Canonical tag
- `create_db.py`: Database schema updates
