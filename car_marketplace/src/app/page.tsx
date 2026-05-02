import Link from 'next/link';
import Image from 'next/image';
import { getFeaturedCars, getTrendingCars, formatDisplayPrice } from '@/lib/api';
import type { Car, TrendingCar } from '@/lib/types';
import Script from 'next/script';

export const metadata = {
  title: 'Used Cars for Sale Philippines | Metro Manila Car Marketplace',
  description: 'Find quality pre-owned cars for sale in the Philippines. Browse Toyota, Honda, Mitsubishi, Ford and more. Best deals on used cars in Metro Manila and Mindanao.',
  keywords: ['used cars Philippines', 'second hand cars Manila', 'car for sale Philippines', 'cheap used cars', 'Toyota Vios for sale', 'Honda Civic Philippines', 'Mitsubishi Montero', 'used cars Metro Manila'],
};

export default async function HomePage() {
  const featuredCars = await getFeaturedCars();
  const trendingCars = await getTrendingCars(5);

  const websiteSchema = {
    '@context': 'https://schema.org',
    '@type': 'WebSite',
    name: 'Used Cars Philippines',
    url: 'https://usedcars.ph',
    description: 'Find quality pre-owned cars for sale in the Philippines',
    potentialAction: {
      '@type': 'SearchAction',
      target: 'https://usedcars.ph/browse?q={search_term_string}',
      'query-input': 'required name=search_term_string',
    },
  };

  return (
    <>
      <Script
        id="website-schema"
        type="application/ld+json"
        dangerouslySetInnerHTML={{ __html: JSON.stringify(websiteSchema) }}
      />
      <div>
        {/* Hero Section */}
        <section className="bg-gradient-to-r from-gray-900 to-gray-800 text-white py-20 px-4">
          <div className="container mx-auto text-center">
            <h1 className="text-4xl md:text-5xl font-extrabold mb-4">
              Find Your Next Car in the Philippines
            </h1>
            <p className="text-xl md:text-2xl mb-8 text-gray-300">
              Browse quality pre-owned cars in Metro Manila and Mindanao
            </p>
            <div className="flex justify-center gap-4 flex-wrap">
              <Link
                href="/browse"
                className="bg-yellow-500 hover:bg-yellow-600 text-black px-8 py-3 rounded-xl font-bold text-lg transition-colors"
              >
                Browse Cars
              </Link>
            </div>
          </div>
        </section>

        {/* Trending Cars */}
        {trendingCars.length > 0 && (
          <section className="py-12 px-4 bg-white">
            <div className="container mx-auto">
              <h2 className="text-2xl font-bold text-center mb-6">Trending Today</h2>
              <div className="flex flex-wrap justify-center gap-3">
                {trendingCars.map((car: TrendingCar, idx: number) => (
                  <Link
                    key={`${car.displayName}-${idx}`}
                    href={`/browse?brand=${car.brand}&model=${car.model}`}
                    className="bg-gradient-to-r from-yellow-400 to-orange-500 text-black px-4 py-2 rounded-full font-semibold text-sm hover:shadow-lg transition-all"
                  >
                    🔥 {car.displayName}
                  </Link>
                ))}
              </div>
            </div>
          </section>
        )}

        {/* Featured Cars */}
        <section className="py-16 px-4">
          <div className="container mx-auto">
            <h2 className="text-3xl font-bold text-center mb-2">Featured Cars</h2>
            <p className="text-center text-gray-600 mb-8">Handpicked quality vehicles for you</p>

            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
              {featuredCars.map((car: Car) => (
                <Link
                  key={car.id}
                  href={`/car/${car.slug}`}
                  className="group"
                >
                  <div className="bg-white rounded-xl shadow-md hover:shadow-xl transition-all duration-300 overflow-hidden h-full flex flex-col">
                    <div className="relative h-64 overflow-hidden bg-gray-100">
                      {car.thumbnail ? (
                        <div className="relative w-full h-full">
                          <Image
                            src={car.thumbnail}
                            alt={`${car.year} ${car.brand} ${car.model} for sale Philippines`}
                            fill
                            className="object-contain group-hover:scale-105 transition-transform duration-300"
                            sizes="(max-width: 768px) 100vw, (max-width: 1200px) 50vw, 33vw"
                          />
                        </div>
                      ) : (
                        <div className="w-full h-full flex items-center justify-center text-gray-400">
                          No Photo
                        </div>
                      )}
                      {car.originalPrice && car.originalPrice > car.price && (
                        <div className="absolute top-3 right-3 bg-red-500 text-white text-xs font-bold px-3 py-1 rounded-full">
                          PRICE REDUCED
                        </div>
                      )}
                    </div>

                    <div className="p-4 flex-1 flex flex-col">
                      <h3 className="font-bold text-lg mb-1 group-hover:text-blue-600 transition-colors">
                        {car.year} {car.brand} {car.model}
                      </h3>
                      <p className="text-2xl font-bold text-red-500 mb-2">
                        {formatDisplayPrice(car.price)}
                      </p>
                      {car.originalPrice && car.originalPrice > car.price && (
                        <p className="text-sm text-gray-500 line-through mb-2">
                          was {formatDisplayPrice(car.originalPrice)}
                        </p>
                      )}
                      <div className="mt-auto text-sm text-gray-500">
                        {car.bodyType} • {car.location}
                      </div>
                    </div>
                  </div>
                </Link>
              ))}
            </div>

            <div className="text-center mt-10">
              <Link
                href="/browse"
                className="bg-blue-600 hover:bg-blue-700 text-white px-8 py-3 rounded-xl font-semibold transition-colors"
              >
                View All Cars
              </Link>
            </div>
          </div>
        </section>

        {/* How It Works */}
        <section className="py-16 px-4 bg-white">
          <div className="container mx-auto">
            <h2 className="text-3xl font-bold text-center mb-10">How It Works</h2>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
              <div className="bg-gray-50 p-8 rounded-xl text-center">
                <div className="text-5xl mb-4">🔍</div>
                <h3 className="font-bold text-xl mb-2">Browse Cars</h3>
                <p className="text-gray-600">
                  Search through our listings of verified pre-owned cars. Filter by brand, model, price, or type.
                </p>
              </div>
              <div className="bg-gray-50 p-8 rounded-xl text-center">
                <div className="text-5xl mb-4">💬</div>
                <h3 className="font-bold text-xl mb-2">Contact Us</h3>
                <p className="text-gray-600">
                  Found a car you like? Reach out via Facebook Messenger or give us a call. We&apos;ll connect you with the owner.
                </p>
              </div>
              <div className="bg-gray-50 p-8 rounded-xl text-center">
                <div className="text-5xl mb-4">🤝</div>
                <h3 className="font-bold text-xl mb-2">Close the Deal</h3>
                <p className="text-gray-600">
                  We can be present at the deal or help coordinate remotely. Either way, we ensure a smooth transaction.
                </p>
              </div>
            </div>
          </div>
        </section>
      </div>
    </>
  );
}
