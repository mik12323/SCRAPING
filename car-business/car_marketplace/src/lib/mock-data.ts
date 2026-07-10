import { Car, TrendingCar } from './types';

export const mockCars: Car[] = [
  {
    id: '1',
    slug: 'toyota-vios-2020-450k',
    brand: 'Toyota',
    model: 'Vios',
    year: 2020,
    price: 450000,
    originalPrice: 480000,
    bodyType: 'Sedan',
    fuelType: 'Gas',
    transmission: 'Automatic',
    location: 'Quezon City, Metro Manila',
    description: 'Well-maintained Toyota Vios 2020. Single owner, low mileage. Complete papers, recent PMS done. Ideal for city driving and family use.',
    images: [
      'https://images.unsplash.com/photo-1605559424843-94701db5baf?w=800&q=80',
      'https://images.unsplash.com/photo-1552519507-da3b142c53e1?w=800&q=80',
      'https://images.unsplash.com/photo-1503376780353-7e6692767b70?w=800&q=80'
    ],
    thumbnail: 'https://images.unsplash.com/photo-1605559424843-94701db5baf?w=800&q=80',
    status: 'approved',
    createdAt: '2026-04-15T10:00:00Z',
    updatedAt: '2026-04-20T14:30:00Z'
  },
  {
    id: '2',
    slug: 'honda-civic-2021-650k',
    brand: 'Honda',
    model: 'Civic',
    year: 2021,
    price: 650000,
    bodyType: 'Sedan',
    fuelType: 'Gas',
    transmission: 'Automatic',
    location: 'Makati City, Metro Manila',
    description: 'Honda Civic 2021 in pristine condition. Turbo engine, leather seats, backup camera, and more. Must see to appreciate.',
    images: [
      'https://images.unsplash.com/photo-1552519507-da3b142c53e1?w=800&q=80',
      'https://images.unsplash.com/photo-1605559424843-94701db5baf?w=800&q=80',
      'https://images.unsplash.com/photo-1503376780353-7e6692767b70?w=800&q=80'
    ],
    thumbnail: 'https://images.unsplash.com/photo-1552519507-da3b142c53e1?w=800&q=80',
    status: 'approved',
    createdAt: '2026-04-10T08:00:00Z',
    updatedAt: '2026-04-18T11:20:00Z'
  },
  {
    id: '3',
    slug: 'mitsubishi-montero-2019-850k',
    brand: 'Mitsubishi',
    model: 'Montero',
    year: 2019,
    price: 850000,
    bodyType: 'SUV',
    fuelType: 'Diesel',
    transmission: 'Automatic',
    location: 'Pasig City, Metro Manila',
    description: 'Mitsubishi Montero Sport 2019. 4x4, 7-seater, perfect for family trips. Well-maintained, all-terrain tires, roof rack included.',
    images: [
      'https://images.unsplash.com/photo-1533473359331-0135ef1b58bf?w=800&q=80',
      'https://images.unsplash.com/photo-1503376780353-7e6692767b70?w=800&q=80',
      'https://images.unsplash.com/photo-1605559424843-94701db5baf?w=800&q=80'
    ],
    thumbnail: 'https://images.unsplash.com/photo-1533473359331-0135ef1b58bf?w=800&q=80',
    status: 'approved',
    createdAt: '2026-04-05T09:30:00Z',
    updatedAt: '2026-04-16T16:45:00Z'
  },
  {
    id: '4',
    slug: 'nissan-navara-2020-780k',
    brand: 'Nissan',
    model: 'Navara',
    year: 2020,
    price: 780000,
    bodyType: 'Pickup',
    fuelType: 'Diesel',
    transmission: 'Manual',
    location: 'Taguig City, Metro Manila',
    description: 'Nissan Navara 2020 pickup truck. Powerful diesel engine, perfect for work and adventure. Includes bed liner and roll bar.',
    images: [
      'https://images.unsplash.com/photo-1503376780353-7e6692767b70?w=800&q=80',
      'https://images.unsplash.com/photo-1533473359331-0135ef1b58bf?w=800&q=80',
      'https://images.unsplash.com/photo-1552519507-da3b142c53e1?w=800&q=80'
    ],
    thumbnail: 'https://images.unsplash.com/photo-1503376780353-7e6692767b70?w=800&q=80',
    status: 'approved',
    createdAt: '2026-04-08T12:00:00Z',
    updatedAt: '2026-04-19T10:15:00Z'
  },
  {
    id: '5',
    slug: 'ford-ranger-2021-920k',
    brand: 'Ford',
    model: 'Ranger',
    year: 2021,
    price: 920000,
    bodyType: 'Pickup',
    fuelType: 'Diesel',
    transmission: 'Automatic',
    location: 'Paranaque City, Metro Manila',
    description: 'Ford Ranger 2021 Wildtrak edition. Top-of-the-line features, panoramic sunroof, advanced safety tech. Like new condition.',
    images: [
      'https://images.unsplash.com/photo-1503376780353-7e6692767b70?w=800&q=80',
      'https://images.unsplash.com/photo-1552519507-da3b142c53e1?w=800&q=80',
      'https://images.unsplash.com/photo-1533473359331-0135ef1b58bf?w=800&q=80'
    ],
    thumbnail: 'https://images.unsplash.com/photo-1503376780353-7e6692767b70?w=800&q=80',
    status: 'approved',
    createdAt: '2026-04-12T14:20:00Z',
    updatedAt: '2026-04-21T09:00:00Z'
  },
  {
    id: '6',
    slug: 'toyota-fortuner-2020-1.2m',
    brand: 'Toyota',
    model: 'Fortuner',
    year: 2020,
    price: 1200000,
    bodyType: 'SUV',
    fuelType: 'Diesel',
    transmission: 'Automatic',
    location: 'Mandaluyong City, Metro Manila',
    description: 'Toyota Fortuner 2020 4x2. Premium SUV with 7 seats, perfect for large families. Well-maintained, low mileage.',
    images: [
      'https://images.unsplash.com/photo-1533473359331-0135ef1b58bf?w=800&q=80',
      'https://images.unsplash.com/photo-1605559424843-94701db5baf?w=800&q=80',
      'https://images.unsplash.com/photo-1552519507-da3b142c53e1?w=800&q=80'
    ],
    thumbnail: 'https://images.unsplash.com/photo-1533473359331-0135ef1b58bf?w=800&q=80',
    status: 'approved',
    createdAt: '2026-04-14T11:45:00Z',
    updatedAt: '2026-04-22T13:30:00Z'
  },
  {
    id: '7',
    slug: 'honda-cr-v-2019-880k',
    brand: 'Honda',
    model: 'CR-V',
    year: 2019,
    price: 880000,
    bodyType: 'SUV',
    fuelType: 'Gas',
    transmission: 'Automatic',
    location: 'San Juan City, Metro Manila',
    description: 'Honda CR-V 2019. Spacious SUV with excellent fuel economy. Leather interior, sunroof, reverse camera and sensors.',
    images: [
      'https://images.unsplash.com/photo-1533473359331-0135ef1b58bf?w=800&q=80',
      'https://images.unsplash.com/photo-1552519507-da3b142c53e1?w=800&q=80',
      'https://images.unsplash.com/photo-1605559424843-94701db5baf?w=800&q=80'
    ],
    thumbnail: 'https://images.unsplash.com/photo-1533473359331-0135ef1b58bf?w=800&q=80',
    status: 'approved',
    createdAt: '2026-04-09T10:15:00Z',
    updatedAt: '2026-04-17T15:45:00Z'
  },
  {
    id: '8',
    slug: 'toyota-innova-2018-680k',
    brand: 'Toyota',
    model: 'Innova',
    year: 2018,
    price: 680000,
    bodyType: 'Van',
    fuelType: 'Diesel',
    transmission: 'Manual',
    location: 'Caloocan City, Metro Manila',
    description: 'Toyota Innova 2018. Reliable 8-seater MPV, perfect for large families. Fuel-efficient diesel engine, well-maintained.',
    images: [
      'https://images.unsplash.com/photo-1605559424843-94701db5baf?w=800&q=80',
      'https://images.unsplash.com/photo-1503376780353-7e6692767b70?w=800&q=80',
      'https://images.unsplash.com/photo-1533473359331-0135ef1b58bf?w=800&q=80'
    ],
    thumbnail: 'https://images.unsplash.com/photo-1605559424843-94701db5baf?w=800&q=80',
    status: 'approved',
    createdAt: '2026-04-07T09:00:00Z',
    updatedAt: '2026-04-15T12:20:00Z'
  },
  {
    id: '9',
    slug: 'suzuki-dzire-2021-420k',
    brand: 'Suzuki',
    model: 'Dzire',
    year: 2021,
    price: 420000,
    bodyType: 'Sedan',
    fuelType: 'Gas',
    transmission: 'Manual',
    location: 'Marikina City, Metro Manila',
    description: 'Suzuki Dzire 2021. Compact sedan with excellent fuel economy. Perfect for city driving, first-time car owners.',
    images: [
      'https://images.unsplash.com/photo-1605559424843-94701db5baf?w=800&q=80',
      'https://images.unsplash.com/photo-1552519507-da3b142c53e1?w=800&q=80',
      'https://images.unsplash.com/photo-1503376780353-7e6692767b70?w-800&q=80'
    ],
    thumbnail: 'https://images.unsplash.com/photo-1605559424843-94701db5baf?w=800&q=80',
    status: 'approved',
    createdAt: '2026-04-11T13:30:00Z',
    updatedAt: '2026-04-23T08:45:00Z'
  },
  {
    id: '10',
    slug: 'bmw-3-series-2019-1.5m',
    brand: 'BMW',
    model: '3 Series',
    year: 2019,
    price: 1500000,
    bodyType: 'Sedan',
    fuelType: 'Gas',
    transmission: 'Automatic',
    location: 'BGC, Taguig City',
    description: 'BMW 3 Series 2019. Luxury sedan with premium features. Navigation system, leather seats, sport mode. Executive condition.',
    images: [
      'https://images.unsplash.com/photo-1552519507-da3b142c53e1?w=800&q=80',
      'https://images.unsplash.com/photo-1533473359331-0135ef1b58bf?w=800&q=80',
      'https://images.unsplash.com/photo-1605559424843-94701db5baf?w=800&q=80'
    ],
    thumbnail: 'https://images.unsplash.com/photo-1552519507-da3b142c53e1?w=800&q=80',
    status: 'approved',
    createdAt: '2026-04-13T15:00:00Z',
    updatedAt: '2026-04-24T11:10:00Z'
  },
  {
    id: '11',
    slug: 'mitsubishi-mirage-2020-380k',
    brand: 'Mitsubishi',
    model: 'Mirage',
    year: 2020,
    price: 380000,
    bodyType: 'Hatchback',
    fuelType: 'Gas',
    transmission: 'Manual',
    location: 'Las Pinas City, Metro Manila',
    description: 'Mitsubishi Mirage 2020 hatchback. Very fuel-efficient, perfect for daily commute. Low maintenance cost, first owner.',
    images: [
      'https://images.unsplash.com/photo-1605559424843-94701db5baf?w=800&q=80',
      'https://images.unsplash.com/photo-1552519507-da3b142c53e1?w=800&q=80',
      'https://images.unsplash.com/photo-1503376780353-7e6692767b70?w=800&q=80'
    ],
    thumbnail: 'https://images.unsplash.com/photo-1605559424843-94701db5baf?w=800&q=80',
    status: 'approved',
    createdAt: '2026-04-06T08:45:00Z',
    updatedAt: '2026-04-18T14:00:00Z'
  },
  {
    id: '12',
    slug: 'toyota-hilux-2021-1.1m',
    brand: 'Toyota',
    model: 'Hilux',
    year: 2021,
    price: 1100000,
    bodyType: 'Pickup',
    fuelType: 'Diesel',
    transmission: 'Manual',
    location: 'Valenzuela City, Metro Manila',
    description: 'Toyota Hilux 2021. Rugged pickup truck for work and adventure. 4x4 capability, heavy-duty suspension, bed liner included.',
    images: [
      'https://images.unsplash.com/photo-1503376780353-7e6692767b70?w=800&q=80',
      'https://images.unsplash.com/photo-1533473359331-0135ef1b58bf?w=800&q=80',
      'https://images.unsplash.com/photo-1552519507-da3b142c53e1?w=800&q=80'
    ],
    thumbnail: 'https://images.unsplash.com/photo-1503376780353-7e6692767b70?w=800&q=80',
    status: 'approved',
    createdAt: '2026-04-16T10:30:00Z',
    updatedAt: '2026-04-25T09:20:00Z'
  }
];

export const mockTrendingCars: TrendingCar[] = [
  {
    brand: 'Toyota',
    model: 'Vios',
    displayName: 'Toyota Vios',
    clicks: 45,
    bodyType: 'Sedan',
    fuelType: 'Gas',
    transmission: 'Automatic'
  },
  {
    brand: 'Honda',
    model: 'Civic',
    displayName: 'Honda Civic',
    clicks: 38,
    bodyType: 'Sedan',
    fuelType: 'Gas',
    transmission: 'Automatic'
  },
  {
    brand: 'Mitsubishi',
    model: 'Montero',
    displayName: 'Mitsubishi Montero',
    clicks: 32,
    bodyType: 'SUV',
    fuelType: 'Diesel',
    transmission: 'Automatic'
  },
  {
    brand: 'Toyota',
    model: 'Fortuner',
    displayName: 'Toyota Fortuner',
    clicks: 28,
    bodyType: 'SUV',
    fuelType: 'Diesel',
    transmission: 'Automatic'
  },
  {
    brand: 'Ford',
    model: 'Ranger',
    displayName: 'Ford Ranger',
    clicks: 25,
    bodyType: 'Pickup',
    fuelType: 'Diesel',
    transmission: 'Automatic'
  }
];

export const featuredCars = mockCars.slice(0, 6);
