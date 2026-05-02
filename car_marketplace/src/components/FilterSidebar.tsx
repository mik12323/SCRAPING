'use client';

import { useState, useRef, useEffect } from 'react';
import { BODY_TYPES, FUEL_TYPES, TRANSMISSIONS } from '@/lib/types';

interface FilterSidebarProps {
  filters?: {
    brand?: string;
    model?: string;
    bodyType?: string;
    fuelType?: string;
    transmission?: string;
    priceMin?: string;
    priceMax?: string;
    sortBy?: string;
  };
  onFilterChange?: (filters: Record<string, string>) => void;
  onReset?: () => void;
  brands?: string[];
}

export default function FilterSidebar({
  filters = {},
  onFilterChange,
  onReset,
  brands = []
}: FilterSidebarProps) {
  const [brandSearch, setBrandSearch] = useState(filters.brand || '');
  const [showBrandDropdown, setShowBrandDropdown] = useState(false);
  const brandDropdownRef = useRef<HTMLDivElement>(null);

  const handleChange = (field: string, value: string) => {
    onFilterChange?.({ ...filters, [field]: value });
  };

  // Filter brands based on search
  const filteredBrands = brands.filter(brand =>
    brand.toLowerCase().includes(brandSearch.toLowerCase())
  );

  // Close dropdown on click outside
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (brandDropdownRef.current && !brandDropdownRef.current.contains(event.target as Node)) {
        setShowBrandDropdown(false);
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  // Update search when brand filter changes externally
  useEffect(() => {
    setBrandSearch(filters.brand || '');
  }, [filters.brand]);

  return (
    <div className="bg-white p-6 rounded-xl shadow-sm space-y-4">
      <h3 className="font-bold text-lg mb-2">Filters</h3>

      <div ref={brandDropdownRef}>
        <label className="block text-sm font-semibold mb-2">Brand</label>
        <div className="relative">
          <input
            type="text"
            value={brandSearch}
            onChange={(e) => {
              setBrandSearch(e.target.value);
              handleChange('brand', e.target.value);
              setShowBrandDropdown(true);
            }}
            onFocus={() => setShowBrandDropdown(true)}
            placeholder="Search brand..."
            className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 outline-none"
          />
          {showBrandDropdown && brandSearch && (
            <div className="absolute z-10 w-full mt-1 bg-white border border-gray-300 rounded-lg shadow-lg max-h-48 overflow-y-auto">
              {filteredBrands.length > 0 ? (
                filteredBrands.map((brand) => (
                  <div
                    key={brand}
                    onClick={() => {
                      handleChange('brand', brand);
                      setBrandSearch(brand);
                      setShowBrandDropdown(false);
                    }}
                    className="px-3 py-2 hover:bg-gray-100 cursor-pointer"
                  >
                    {brand}
                  </div>
                ))
              ) : (
                <div className="px-3 py-2 text-gray-500">No brands found</div>
              )}
            </div>
          )}
        </div>
      </div>

      <div>
        <label className="block text-sm font-semibold mb-2">Model</label>
        <input
          type="text"
          value={filters.model || ''}
          onChange={(e) => handleChange('model', e.target.value)}
          placeholder="e.g. Civic"
          className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 outline-none"
        />
      </div>

      <div>
        <label className="block text-sm font-semibold mb-2">Body Type</label>
        <select
          value={filters.bodyType || 'All'}
          onChange={(e) => handleChange('bodyType', e.target.value)}
          className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 outline-none"
        >
          <option value="All">All Body Types</option>
          {BODY_TYPES.map((type) => (
            <option key={type} value={type}>{type}</option>
          ))}
        </select>
      </div>

      <div>
        <label className="block text-sm font-semibold mb-2">Fuel Type</label>
        <select
          value={filters.fuelType || 'All'}
          onChange={(e) => handleChange('fuelType', e.target.value)}
          className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 outline-none"
        >
          <option value="All">All Fuel Types</option>
          {FUEL_TYPES.map((type) => (
            <option key={type} value={type}>{type}</option>
          ))}
        </select>
      </div>

      <div>
        <label className="block text-sm font-semibold mb-2">Transmission</label>
        <select
          value={filters.transmission || 'All'}
          onChange={(e) => handleChange('transmission', e.target.value)}
          className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 outline-none"
        >
          <option value="All">All Transmissions</option>
          {TRANSMISSIONS.map((type) => (
            <option key={type} value={type}>{type}</option>
          ))}
        </select>
      </div>

      <div>
        <label className="block text-sm font-semibold mb-2">Min Price</label>
        <input
          type="number"
          value={filters.priceMin || ''}
          onChange={(e) => handleChange('priceMin', e.target.value)}
          placeholder="e.g. 500000"
          className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 outline-none"
        />
      </div>

      <div>
        <label className="block text-sm font-semibold mb-2">Max Price</label>
        <input
          type="number"
          value={filters.priceMax || ''}
          onChange={(e) => handleChange('priceMax', e.target.value)}
          placeholder="e.g. 1500000"
          className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 outline-none"
        />
      </div>

        <div>
          <label className="block text-sm font-semibold mb-2">Sort By</label>
          <select
            value={filters.sortBy || 'alphabetical'}
            onChange={(e) => handleChange('sortBy', e.target.value)}
            className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 outline-none"
          >
            <option value="alphabetical">A-Z (Alphabetical)</option>
            <option value="reverse_alphabetical">Z-A (Reverse)</option>
            <option value="price_low">Price: Low to High</option>
            <option value="price_high">Price: High to Low</option>
            <option value="newest">Year: Newest First</option>
            <option value="oldest">Year: Oldest First</option>
          </select>
        </div>

      <div className="flex gap-2 pt-4">
        <button
          type="button"
          onClick={onReset}
          className="flex-1 px-4 py-2 border border-gray-300 rounded-lg hover:bg-gray-50 transition-colors"
        >
          Reset
        </button>
        <button
          type="button"
          onClick={() => onFilterChange?.(filters)}
          className="flex-1 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
        >
          Apply
        </button>
      </div>
    </div>
  );
}
