import type { MetadataRoute } from 'next';
import { getCars, getBrands } from '@/lib/api';

export default async function sitemap(): Promise<MetadataRoute.Sitemap> {
  const baseUrl = 'https://usedcars.ph';

  // Static pages
  const staticPages: MetadataRoute.Sitemap = [
    {
      url: `${baseUrl}`,
      lastModified: new Date(),
      changeFrequency: 'daily',
      priority: 1.0,
    },
    {
      url: `${baseUrl}/browse`,
      lastModified: new Date(),
      changeFrequency: 'daily',
      priority: 0.9,
    },
    {
      url: `${baseUrl}/about`,
      lastModified: new Date(),
      changeFrequency: 'monthly',
      priority: 0.5,
    },
  ];

  // Get all approved car listings
  const carsResult = await getCars({}, 1, 1000); // Get up to 1000 cars
  const carPages: MetadataRoute.Sitemap = carsResult.cars.map((car) => ({
    url: `${baseUrl}/car/${car.slug}`,
    lastModified: new Date(car.updatedAt || car.createdAt || Date.now()),
    changeFrequency: 'weekly' as const,
    priority: 0.8,
  }));

  // Get brand pages
  const brands = await getBrands();
  const brandPages: MetadataRoute.Sitemap = brands.map((brand) => ({
    url: `${baseUrl}/browse?brand=${encodeURIComponent(brand)}`,
    lastModified: new Date(),
    changeFrequency: 'weekly' as const,
    priority: 0.7,
  }));

  return [...staticPages, ...carPages, ...brandPages];
}
