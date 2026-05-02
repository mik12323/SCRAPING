#!/usr/bin/env node
const { createClient } = require('@supabase/supabase-js')
const fs = require('fs')
const path = require('path')
require('dotenv').config({ path: path.resolve(__dirname, '../.env.local') })

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY

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
function parseFolderName(folderName) {
  if (!/\d{4}.*/.test(folderName)) {
    return null
  }
  
  const cleaned = folderName.replace(/^Cash\s+/i, '')
  const parts = cleaned.trim().split(/\s+/)
  
  let yearIndex = -1
  for (let i = 0; i < parts.length; i++) {
    if (/^\d{4}$/.test(parts[i])) {
      yearIndex = i
      break
    }
  }
  
  if (yearIndex < 2) return null
  
  const year = parseInt(parts[yearIndex])
  const priceStr = parts[parts.length - 1]
  const brand = parts[0]
  const model = parts.slice(1, yearIndex).join(' ')
  
  let price = 0
  if (priceStr.toLowerCase().endsWith('m')) {
    price = Math.round(parseFloat(priceStr.slice(0, -1)) * 1000000)
  } else if (priceStr.toLowerCase().endsWith('k')) {
    price = Math.round(parseFloat(priceStr.slice(0, -1)) * 1000)
  } else {
    price = parseInt(priceStr)
  }
  
  if (isNaN(year) || isNaN(price)) return null
  
  return { brand, model, year, price }
}

// Extract mileage from description text
// Handles: "58,000 km", "58k odo", "58000 mileage", "58000km", etc.
function extractMileage(text) {
  if (!text) return null;
  
  const patterns = [
    /(\d{1,3}(?:,\d{3})+)\s*(?:km|kilometers?)\b/i,
    /(\d+)\s*k(?:m|m\s*(?:odo|odometer)?)\b/i,
    /(\d{4,6})\s*(?:km|kilometers?|mileage)\b/i,
  ];
  
  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (match) {
      // Remove commas and convert to number
      const numStr = match[1].replace(/,/g, '');
      const mileage = parseInt(numStr);
      // Sanity check: mileage should be reasonable (less than 1 million km)
      if (mileage > 0 && mileage < 1000000) {
        return mileage;
      }
    }
  }
  return null;
}

// Extract color from description text
// Handles: "Red", "Color: Red", "Ext color: Midnight Blue", "Red exterior", etc.
function extractColor(text) {
  if (!text) return null;
  
  const patterns = [
    /(?:color:?\s*|ext(?:erior)?\s*color:?\s*)([\w\s]+?)(?:\n|,|\.|km|odo|$)/i,
    /(?:exterior|ext):\s*([\w\s]+?)(?:\n|,|\.|km|odo|$)/i,
    /^(?:color|colour):\s*([\w\s]+)$/im,
  ];
  
  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (match && match[1]) {
      const color = match[1].trim();
      // Sanity check: color should be short (less than 30 chars)
      if (color.length > 0 && color.length < 30) {
        return color;
      }
    }
  }
  return null;
}

function generateSlug(brand, model, year, price) {
  const slugBase = `${brand} ${model} ${year} ${price}`.toLowerCase()
  return slugBase
    .replace(/[^\w\s-]/g, '')
    .replace(/\s+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-+|-+$/g, '')
}

async function uploadImage(filePath, fileName) {
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
    
    const { data: { publicUrl } } = supabase.storage
      .from('car-images')
      .getPublicUrl(storagePath)
    
    return publicUrl
  } catch (err) {
    console.error(`Error processing ${fileName}:`, err)
    return null
  }
}

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
  
  // Get admin user (first user)
  const { data: adminData } = await supabase
    .from('users')
    .select('id')
    .limit(1)
    .single()
  
  if (!adminData) {
    console.error('No users found. Please create admin user first via /setup-admin')
    process.exit(1)
  }
  
  console.log('Admin user ID:', adminData.id)
  
  let imported = 0
  let skipped = 0
  let duplicates = 0
  
  for (const folder of folders) {
    const folderPath = path.join(DIST_DIR, folder)
    
    const parsed = parseFolderName(folder)
    if (!parsed) {
      console.warn(`Skipping (could not parse): ${folder}`)
      skipped++
      continue
    }
    
    const { brand, model, year, price } = parsed
    const slug = generateSlug(brand, model, year, price)
    
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
    
    let description = ''
    let color = null
    let mileage = null
    
    const detailsPath = path.join(folderPath, 'details.txt')
    if (fs.existsSync(detailsPath)) {
      description = fs.readFileSync(detailsPath, 'utf-8').trim()
      
      // Extract color and mileage from description
      color = extractColor(description)
      mileage = extractMileage(description)
    }
    
    const files = fs.readdirSync(folderPath).filter(f => 
      /\.(jpg|jpeg|png|webp)$/i.test(f)
    )
    
    const imageUrls = []
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
    
    const { data, error } = await supabase
      .from('listings')
      .insert({
        slug,
        brand,
        model,
        year,
        price,
        color,
        mileage,
        location: 'Metro Manila, Philippines',
        description,
        images: imageUrls,
        status: 'approved',
        user_id: adminData.id  // Assign to admin user
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
