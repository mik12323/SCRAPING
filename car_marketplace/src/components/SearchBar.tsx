'use client';

import { useState } from 'react';

interface SearchBarProps {
  onSearch?: (query: string) => void;
}

export default function SearchBar({ onSearch }: SearchBarProps) {
  const [query, setQuery] = useState('');

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    onSearch?.(query);
  };

  return (
    <form onSubmit={handleSubmit} className="w-full max-w-2xl mx-auto">
      <div className="flex gap-2">
        <input
          type="text"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="Search by brand, model, or keyword..."
          className="flex-1 px-4 py-3 rounded-l-xl border-0 focus:ring-2 focus:ring-yellow-400 outline-none text-gray-900"
        />
        <button
          type="submit"
          className="bg-yellow-500 hover:bg-yellow-600 text-black px-8 py-3 rounded-r-xl font-semibold transition-colors"
        >
          Search
        </button>
      </div>
    </form>
  );
}
