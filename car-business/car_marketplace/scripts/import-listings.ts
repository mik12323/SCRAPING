#!/usr/bin/env node
import { createClient } from '@supabase/supabase-js'
import * as fs from 'fs'
import * as path from 'path'
import * as dotenv from 'dotenv'

// Load environment variables
dotenv.config({ path: path.resolve(__dirname, '../.env.local') })

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!

if (!supabaseUrl || !supabaseKey) {
  console.error('Missing Supabase credentials. Please check .env.local')
  console.error('For import, add SUPABASE_SERVICE_ROLE_KEY to .env.local')
  process.exit(1)
}

const supabase = createClient(supabaseUrl, supabaseKey, {
  auth: {
    autoRefreshToken: false,
    persistSession: false
  }
})

const DIST_DIR = 'D:\\carsfsale\\dist'

// Parse folder name: "Cash Honda Civic FC 2017 685k" or "Toyota Vios XLE CVT 2026 720k"
function parseFolderName(folderName: string): { brand: string; model: string; year: number; price: number } | null {
  // Skip folders that don't match the pattern (must have a 4-digit year)
  if (!/\d{4}/.test(folderName)) {
    return null
  }
  
  // Remove "Cash " prefix if present
  const cleaned = folderName.replace(/^Cash\s+/i, '')
  
  // Try to match: Brand ModelName Year Price
  // Last number before end is year, last item is price
  const parts = cleaned.trim().split(/\s+/)
  
  // Find year (4 digit number)
  let yearIndex = -1
  for (let i = 0; i < parts.length; i++) {
    if (/^\d{4}$/.test(parts[i])) {
      yearIndex = i
      break
    }
  }
  
  if (yearIndex < 2) return null // Need at least brand + model + year + price
  
  const year = parseInt(parts[yearIndex])
  const priceStr = parts[parts.length - 1]
  const brand = parts[0]
  const model = parts.slice(1, yearIndex).join(' ')
  
  // Parse price
  let price: number = 0
  if (priceStr.toLowerCase().endsWith('m')) {
    price = Math.round(parseFloat(priceStr.slice(0, -1)) * 1000000
  } else if (priceStr.toLowerCase().endsWith('k')) {
    price = Math.round(parseFloat(priceStr.slice(0, -1)) * 1000
  } else {
    price = parseInt(priceStr)
  }
  
  if (isNaN(year) || isNaN(price)) return null
  
  return { brand, model, year, price }
}

// Generate slug from car details
function generateSlug(brand: string, model: string, year: number, price: number): string {
  const slugBase = `${brand} ${model} ${year} ${price}`.toLowerCase()
  return slugBase
    .replace(/[^\w\s-]/g, '')
    .replace(/\s+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-+|-+$/g, '')
}

// Upload image to Supabase Storage
async function uploadImage(filePath: string, fileName: string): Promise<string | null> {
  try {
    const fileBuffer = fs.readFileSync(filePath)
    const fileExt = path.extname(fileName).slice(1).toLowerCase()
    const storagePath = `${Date.now()}-${fileName}`
    
    const contentType = fileExt === 'jpg' ? 'image/jpeg' : `image/${fileExt}`
    
    const { data, error } = await supabase.storage
      .from('car-images')
      .upload(storagePath, fileBuffer, {
        contentType,
        upsert: false
      })
    
    if (error) {
      console.error(`Error uploading ${fileName}:`, error.message)
      return null
    }
    
    // Get public URL
    const { data: { publicUrl } } = supabase.storage
      .from('car-images')
      .getPublicUrl(storagePath)
    
    return publicUrl
  } catch (err) {
    console.error(`Error processing ${fileName}:`, err)
    return null
  }
}

// Main import function
async function importListings() {
  console.log('Starting import from:', DIST_DIR)
  
  if (!fs.existsSync(DIST_DIR)) {
    console.error('Dist directory not found:', DIST_DIR)
    process.exit(1)
  }
  
  const folders = fs.readdirSync(DIST_DIR).filter(f => {
    const fullPath = path.join(DIST_DIR, f)
    return fs.statSync(fullPath).isDirectory()
  })
  
  console.log(`Found ${folders.length} folders`)
  
  let imported = 0
  let skipped = 0
  let duplicates = 0
  
  for (const folder of folders) {
    const folderPath = path.join(DIST_DIR, folder)
     
    // Parse folder name
    const parsed = parseFolderName(folder)
    if (!parsed) {
      console.warn(`Skipping (could not parse): ${folder}`)
      skipped++
      continue
    }
     
    const { brand, model, year, price } = parsed
    const slug = generateSlug(brand, model, year, price)
     
    // Check for duplicate slug
    const { data: existing } = await supabase
      .from('listings')
      .select('id')
      .eq('slug', slug)
      .single()
     
    if (existing) {
      console.log(`Duplicate, skipping: ${folder}`)
      duplicates++
      continue
    }
     
    // Read description from details.txt if exists
    let description = ''
    const detailsPath = path.join(folderPath, 'details.txt')
    if (fs.existsSync(detailsPath)) {
      description = fs.readFileSync(detailsPath, 'utf-8').trim()
    }
     
    // Upload images
    const files = fs.readdirSync(folderPath).filter(f => 
      /\.(jpg|jpeg|png|webp)$/i.test(f)
    )
     
    const imageUrls: string[] = []
    for (const file of files) {
      const filePath = path.join(folderPath, file)
      const url = await uploadImage(filePath, file)
      if (url) {
        imageUrls.push(url)
      }
    }
     
    if (imageUrls.length === 0) {
      console.warn(`No images found for: ${folder}`)
    }
     
    // Insert into database
    const { data, error } = await supabase
      .from('listings')
      .insert({
        slug,
        brand,
        model,
        year,
        price,
        location: 'Metro Manila, Philippines',
        description,
        images: imageUrls,
        status: 'approved'
      })
      .select()
      .single()
     
    if (error) {
      console.error(`Error inserting ${folder}:`, error.message)
      skipped++
      continue
    }
     
    console.log(`Imported: ${brand} ${model} ${year} - ₱${price.toLocaleString()}`)
    imported++
  }
   
  console.log(`\nImport complete!`)
  console.log(`Imported: ${imported}`)
  console.log(`Duplicates: ${duplicates}`)
  console.log(`Skipped: ${skipped}`)
}

importListings().catch(console.error)
