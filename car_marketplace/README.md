# Used Cars Philippines - Frontend

A modern Next.js car marketplace frontend built with TypeScript and Tailwind CSS.

## Features

- **Homepage** (`/`) - Hero section, trending cars, featured cars, how it works
- **Browse Page** (`/browse`) - Car listings with filters (brand, model, body type, fuel type, transmission, price range) and pagination
- **Car Detail Page** (`/car/[slug]`) - Full car details with image gallery, vehicle specs, and related cars
- **About Page** (`/about`) - Static about page with company info

## Project Structure

```
car_marketplace/
├── src/
│   ├── app/
│   │   ├── about/page.tsx          # About page
│   │   ├── browse/
│   │   │   ├── page.tsx           # Browse page wrapper
│   │   │   └── BrowseContent.tsx # Browse page with filters
│   │   ├── car/[slug]/
│   │   │   └── page.tsx          # Car detail page
│   │   ├── layout.tsx            # Root layout with Navbar/Footer
│   │   ├── globals.css            # Global styles
│   │   └── page.tsx              # Homepage
│   ├── components/
│   │   ├── Navbar.tsx            # Navigation bar
│   │   ├── Footer.tsx            # Footer
│   │   ├── CarCard.tsx           # Individual car card
│   │   ├── CarGrid.tsx           # Grid layout for car cards
│   │   ├── SearchBar.tsx         # Search bar UI
│   │   ├── FilterSidebar.tsx     # Filter sidebar
│   │   ├── Pagination.tsx        # Pagination component
│   │   └── ImageGallery.tsx     # Image gallery with lightbox
│   └── lib/
│       ├── types.ts              # TypeScript interfaces
│       ├── mock-data.ts          # Mock car data
│       └── api.ts                # API functions (mock data)
└── public/
    └── favicon.png
```

## Getting Started

1. Navigate to the project directory:
   ```bash
   cd car_marketplace
   ```

2. Install dependencies (if not already done):
   ```bash
   npm install
   ```

3. Run the development server:
   ```bash
   npm run dev
   ```

4. Open [http://localhost:3000](http://localhost:3000) in your browser.

## Build for Production

```bash
npm run build
npm run start
```

## Notes

- This is **frontend only** - no backend integration
- Uses **mock data** from `src/lib/mock-data.ts`
- Ready for future backend integration via `src/lib/api.ts`
- No authentication, no database, no admin system
- All data is static and stored in the frontend

## Future Integration

The `src/lib/api.ts` file is structured to easily swap mock data with real API calls:

```typescript
// Current: returns mock data
export async function getCars() {
  return mockCars;
}

// Future: connect to backend
export async function getCars() {
  const res = await fetch('https://api.example.com/cars');
  return res.json();
}
```
