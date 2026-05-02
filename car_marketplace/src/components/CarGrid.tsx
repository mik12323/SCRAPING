import CarCard from './CarCard';

interface CarGridProps {
  cars: Array<{
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
  }>;
}

export default function CarGrid({ cars }: CarGridProps) {
  if (cars.length === 0) {
    return (
      <div className="text-center py-16">
        <h3 className="text-2xl font-bold text-gray-400 mb-2">No cars found</h3>
        <p className="text-gray-500">Try adjusting your filters or browse our recommendations.</p>
      </div>
    );
  }

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
      {cars.map((car) => (
        <CarCard key={car.id} car={car} />
      ))}
    </div>
  );
}
