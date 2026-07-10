import { Suspense } from 'react';
import BrowseContent from './BrowseContent';
import type { Metadata } from 'next';

export const metadata: Metadata = {
  title: 'Browse Used Cars for Sale Philippines | Search by Brand & Price',
  description: 'Browse all used cars for sale in the Philippines. Filter by brand, model, price, body type, fuel type and more. Find Toyota, Honda, Mitsubishi, Ford and more.',
  keywords: ['browse used cars Philippines', 'search cars for sale Manila', 'filter cars by brand', 'used cars by price', 'Toyota for sale', 'Honda for sale', 'Mitsubishi for sale'],
};

export default function BrowsePage() {
  return (
    <Suspense fallback={
      <div className="container mx-auto px-4 py-16 text-center">
        <div className="inline-block h-8 w-8 animate-spin rounded-full border-4 border-solid border-blue-600 border-r-transparent"></div>
        <p className="mt-4 text-gray-600">Loading...</p>
      </div>
    }>
      <BrowseContent />
    </Suspense>
  );
}
