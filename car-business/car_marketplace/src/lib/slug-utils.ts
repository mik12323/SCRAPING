// ============================================
// SLUG UTILITY FUNCTIONS
// ============================================

import { supabase } from '@/lib/db/supabase-client';

/**
 * Extracts last 4 characters of a UUID as short ID
 */
export function getShortId(id: string): string {
  return id.replace(/-/g, '').slice(-4);
}

/**
 * Cleans a string for URL use: lowercase, replace spaces with hyphens
 */
function cleanForSlug(input: string): string {
  return input
    .toLowerCase()
    .replace(/\s+/g, '-')
    .replace(/[^\w\-]+/g, '')
    .replace(/\-\-+/g, '-')
    .trim();
}

/**
 * Generates slug in format: [brand]-[model]-[year]-[color]-[price]-[short-id]
 * If color is empty, it's omitted from the slug
 */
export function generateSlug(
  brand: string,
  model: string,
  year: number,
  price: number,
  shortId: string,
  color?: string
): string {
  const parts = [
    cleanForSlug(brand),
    cleanForSlug(model),
    year.toString(),
  ];

  if (color && color.trim()) {
    parts.push(cleanForSlug(color));
  }

  parts.push(
    price.toString(),
    shortId
  );

  return parts.join('-');
}

/**
 * Checks if slug-relevant fields changed
 */
export function shouldRegenerateSlug(
  originalCar: any,
  formData: any
): boolean {
  const fieldsToCheck = [
    [originalCar.brand, formData.brand],
    [originalCar.model, formData.model],
    [originalCar.year, parseInt(formData.year)],
  ];

  return fieldsToCheck.some(([original, current]) => original !== current);
}

/**
 * Ensures slug is unique by checking database
 * Appends -1, -2, etc. if duplicate found
 */
export async function makeSlugUnique(
  baseSlug: string,
  currentId?: string
): Promise<string> {
  let slug = baseSlug;
  let counter = 0;

  while (true) {
    const query = supabase
      .from('listings')
      .select('id')
      .eq('slug', slug);

    if (currentId) {
      query.neq('id', currentId);
    }

    const { data } = await query.maybeSingle();

    if (!data) {
      return slug; // Slug is unique
    }

    counter++;
    slug = `${baseSlug}-${counter}`;
  }
}
