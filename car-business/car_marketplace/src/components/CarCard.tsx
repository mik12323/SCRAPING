import Link from 'next/link';
import Image from 'next/image';
import { formatDisplayPrice } from '@/lib/api';

interface CarCardProps {
  car: {
    id: string;
    slug: string;
    brand: string;
    model: string;
    year: number;
    price: number;
    originalPrice?: number;
    bodyType: string;
    thumbnail: string;
    status: string;
  };
}

export default function CarCard({ car }: CarCardProps) {
  return (
    <Link href={`/car/${car.slug}`} className="group">
      <div className="bg-white rounded-xl shadow-md hover:shadow-xl transition-all duration-300 overflow-hidden h-full flex flex-col">
        <div className="relative aspect-[4/3] overflow-hidden">
          {car.thumbnail ? (
            <Image
              src={car.thumbnail}
              alt={`${car.year} ${car.brand} ${car.model} for sale Philippines`}
              fill
              priority
              className="object-cover group-hover:scale-105 transition-transform duration-300 rounded-t-lg"
              sizes="(max-width: 768px) 100vw, (max-width: 1200px) 50vw, 33vw"
            />
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

          <div className="absolute top-3 left-3 bg-gray-900 bg-opacity-75 text-white text-xs px-2 py-1 rounded">
            Used
          </div>
        </div>

        <div className="p-3 flex-1 flex flex-col">
          <h3 className="font-bold text-lg mb-1 group-hover:text-blue-600 transition-colors">
            {car.year} {car.brand} {car.model}
          </h3>

          <div className="mt-auto">
            <p className="text-2xl font-bold text-red-500 mb-2">
              {formatDisplayPrice(car.price)}
            </p>
            {car.originalPrice && car.originalPrice > car.price && (
              <p className="text-sm text-gray-500 line-through">
                was {formatDisplayPrice(car.originalPrice)}
              </p>
            )}
            <div className="flex items-center text-sm text-gray-500 mt-2">
              <span>{car.bodyType}</span>
            </div>
          </div>
        </div>
      </div>
    </Link>
  );
}
