import ImageGallery from '@/components/ImageGallery';
import CarGrid from '@/components/CarGrid';
import CarOwnerActions from '@/components/CarOwnerActions';
import { getCarBySlug, getRelatedCars, formatDisplayPrice } from '@/lib/api';
import Script from 'next/script';

interface CarPageProps {
  params: Promise<{
    slug: string;
  }>;
}

export default async function CarPage({ params }: CarPageProps) {
  const { slug } = await params;
  const car = await getCarBySlug(slug);

  // Track car view (click tracking)
  if (car) {
    try {
      const { supabase } = await import('@/lib/db/supabase-client');
      await supabase.rpc('track_car_view', { car_id: car.id });
    } catch (err) {
      console.error('Error tracking car view:', err);
    }
  }

  if (!car) {
    return (
      <div className="container mx-auto px-4 py-16 text-center">
        <h1 className="text-3xl font-bold mb-4">Car Not Found</h1>
        <p className="mb-6">The car you&apos;re looking for doesn&apos;t exist or has been removed.</p>
        <a
          href="/browse"
          className="inline-block bg-blue-600 text-white px-6 py-3 rounded-lg hover:bg-blue-700 transition-colors"
        >
          Browse Cars
        </a>
      </div>
    );
  }

  const relatedCars = await getRelatedCars(car.id, 3);

  const carSchema = {
    '@context': 'https://schema.org',
    '@type': 'Vehicle',
    name: `${car.year} ${car.brand} ${car.model}`,
    brand: {
      '@type': 'Brand',
      name: car.brand,
    },
    model: car.model,
    modelDate: car.year.toString(),
    vehicleTransmission: car.transmission,
    fuelType: car.fuelType,
    bodyType: car.bodyType,
    description: car.description,
    image: car.images || [],
    offers: {
      '@type': 'Offer',
      price: car.price,
      priceCurrency: 'PHP',
      availability: 'https://schema.org/InStock',
      itemCondition: 'https://schema.org/UsedCondition',
      seller: {
        '@type': 'Organization',
        name: 'Used Cars Philippines',
      },
    },
    itemLocation: {
      '@type': 'Place',
      name: car.location || 'Metro Manila, Philippines',
    },
  };

  // Breadcrumb navigation
  const breadcrumbs = [
    { label: 'Home', href: '/' },
    { label: 'Browse', href: '/browse' },
    { label: car.brand, href: `/browse?brand=${encodeURIComponent(car.brand)}` },
    { label: `${car.year} ${car.model}`, href: `#` },
  ];

  return (
    <>
      <Script
        id="car-schema"
        type="application/ld+json"
        dangerouslySetInnerHTML={{ __html: JSON.stringify(carSchema) }}
      />
      <div className="container mx-auto px-4 py-8">
        {/* Breadcrumb */}
        <nav className="mb-4 text-sm text-gray-600">
          {breadcrumbs.map((crumb, idx) => (
            <span key={idx}>
              {idx > 0 && <span className="mx-2">/</span>}
              {idx === breadcrumbs.length - 1 ? (
                <span className="text-gray-900">{crumb.label}</span>
              ) : (
                <a href={crumb.href} className="hover:text-blue-600">
                  {crumb.label}
                </a>
              )}
            </span>
          ))}
        </nav>

        <a href="/browse" className="inline-block mb-4 text-blue-600 hover:underline">
          ← Back to Browse
        </a>

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
          <div className="lg:col-span-2">
            <h1 className="text-3xl font-bold mb-2">
              {car.year} {car.brand} {car.model}
            </h1>

            <div className="flex gap-2 mb-4">
              <span className="bg-gray-200 text-gray-700 px-3 py-1 rounded-full text-sm">
                Used
              </span>
              {car.bodyType && (
                <span className="bg-gray-200 text-gray-700 px-3 py-1 rounded-full text-sm">
                  {car.bodyType}
                </span>
              )}
              {car.fuelType && (
                <span className="bg-blue-100 text-blue-700 px-3 py-1 rounded-full text-sm">
                  {car.fuelType}
                </span>
              )}
              {car.transmission && (
                <span className="bg-gray-900 text-white px-3 py-1 rounded-full text-sm">
                  {car.transmission}
                </span>
              )}
            </div>

            <ImageGallery images={car.images} alt={`${car.year} ${car.brand} ${car.model} for sale Philippines`} />

              <div className="mt-8 bg-white p-6 rounded-xl shadow-sm">
                <h2 className="text-xl font-bold mb-4">Vehicle Details</h2>
                <ul className="space-y-2">
                  <li><strong>Year:</strong> {car.year}</li>
                  <li><strong>Make/Model:</strong> {car.brand} {car.model}</li>
                  {car.color && <li><strong>Color:</strong> {car.color}</li>}
                  {car.mileage && <li><strong>Mileage:</strong> {car.mileage.toLocaleString()} km</li>}
                  {car.bodyType && <li><strong>Body Type:</strong> {car.bodyType}</li>}
                  {car.fuelType && <li><strong>Fuel Type:</strong> {car.fuelType}</li>}
                  {car.transmission && <li><strong>Transmission:</strong> {car.transmission}</li>}
                  <li><strong>Location:</strong> {car.location}</li>
                </ul>

              {car.description && (
                <div className="mt-6">
                  <h3 className="font-bold mb-2">Description</h3>
                  <p className="text-gray-700 whitespace-pre-wrap">{car.description}</p>
                </div>
              )}
            </div>
          </div>

          <div className="lg:col-span-1">
            <div className="bg-white p-6 rounded-xl shadow-sm sticky top-4">
              <CarOwnerActions carId={car.id} carSlug={car.slug} userId={car.user_id} />

              <div className="mb-4">
                <div className="text-3xl font-bold text-red-500">
                  {formatDisplayPrice(car.price)}
                </div>
                {car.originalPrice && car.originalPrice > car.price && (
                  <>
                    <div className="text-lg text-gray-500 line-through">
                      was {formatDisplayPrice(car.originalPrice)}
                    </div>
                    <span className="inline-block bg-red-500 text-white text-sm font-bold px-3 py-1 rounded mt-2">
                      PRICE REDUCED
                    </span>
                  </>
                )}
              </div>

              <div className="space-y-3">
                <a
                  href="https://www.facebook.com/mikoy.dimaro/"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="block w-full bg-blue-600 text-white text-center px-6 py-3 rounded-lg hover:bg-blue-700 transition-colors"
                >
                  Inquire via Messenger
                </a>
                <a
                  href="tel:+639970946623"
                  className="block w-full bg-green-600 text-white text-center px-6 py-3 rounded-lg hover:bg-green-700 transition-colors"
                >
                  Call: +63 997 094 6623
                </a>
              </div>

              <div className="mt-6 pt-6 border-t border-gray-200">
                <p className="text-sm text-gray-600">
                  <strong>Location:</strong><br />
                  {car.location}
                </p>
              </div>
            </div>
          </div>
        </div>

        {relatedCars.length > 0 && (
          <section className="mt-16">
            <h2 className="text-2xl font-bold mb-6">Related Cars</h2>
            <CarGrid cars={relatedCars} />
          </section>
        )}
      </div>

      {/* Sticky Mobile Bottom Bar */}
      <div className="fixed bottom-0 left-0 right-0 bg-white border-t border-gray-200 p-4 md:hidden z-40">
        <div className="flex gap-2">
          <a
            href="https://www.facebook.com/mikoy.dimaro/"
            target="_blank"
            rel="noopener noreferrer"
            className="flex-1 bg-blue-600 text-white text-center py-3 rounded-lg hover:bg-blue-700 transition-colors"
          >
            Inquire via Messenger
          </a>
          <a
            href="tel:+639970946623"
            className="flex-1 bg-green-600 text-white text-center py-3 rounded-lg hover:bg-green-700 transition-colors"
          >
            Call: +63 997 094 6623
          </a>
        </div>
      </div>
    </>
  );
}

export async function generateMetadata({ params }: CarPageProps) {
  const { slug } = await params;
  const car = await getCarBySlug(slug);

  if (!car) {
    return {
      title: 'Car Not Found | Used Cars Philippines',
      description: 'The car you are looking for does not exist or has been removed.',
    };
  }

  // Safe access with fallbacks
  const carYear = car?.year || '';
  const carBrand = car?.brand || '';
  const carModel = car?.model || '';
  const carPrice = car?.price || 0;
  const carLocation = car?.location || 'Metro Manila';
  const carBodyType = car?.bodyType || '';
  const carFuelType = car?.fuelType || '';
  const carTransmission = car?.transmission || '';
  const carSlug = car?.slug || slug;
  const carImages = car?.images || [];
  const carDescription = car?.description || '';
  const carOriginalPrice = car?.originalPrice;

  const title = `${carYear} ${carBrand} ${carModel} for Sale - ${formatDisplayPrice(carPrice)} | Philippines`;
  const description = `Buy this used ${carYear} ${carBrand} ${carModel} for ${formatDisplayPrice(carPrice)} in ${carLocation}. ${carBodyType} ${carFuelType} ${carTransmission}. Contact us today!`;
  const canonicalUrl = `https://usedcars.ph/car/${carSlug}`;
  const imageUrl = carImages?.[0] || 'https://usedcars.ph/og-image.jpg';

  return {
    title: title,
    description: carDescription,
    keywords: [
      `${carBrand} ${carModel} for sale`,
      `${carYear} ${carBrand} ${carModel}`,
      `used ${carBrand} ${carModel} Philippines`,
      `second hand ${carBrand} ${carModel}`,
      `buy ${carBrand} ${carModel} ${carLocation}`,
    ],
    alternates: {
      canonical: canonicalUrl,
    },
    openGraph: {
      title: title,
      description: carDescription,
      url: canonicalUrl,
      siteName: 'Used Cars Philippines',
      locale: 'en_PH',
      type: 'website',
      images: [
        {
          url: imageUrl,
          width: 1200,
          height: 630,
          alt: `${carYear} ${carBrand} ${carModel}`,
        },
      ],
    },
    twitter: {
      card: 'summary_large_image',
      title: title,
      description: carDescription,
      images: [imageUrl],
    },
  }
}
