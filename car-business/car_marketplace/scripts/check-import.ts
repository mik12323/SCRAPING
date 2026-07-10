#!/usr/bin/env node
import * as dotenv from 'dotenv'
import { createClient } from '@supabase/supabase-js'
import * as path from 'path'

// Load environment variables
dotenv.config({ path: path.resolve(__dirname, '../.env.local') })

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!
const supabaseKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!

if (!supabaseUrl || !supabaseKey) {
  console.error('Missing Supabase credentials. Please check .env.local')
  process.exit(1)
}

const supabase = createClient(supabaseUrl, supabaseKey)

async function checkImport() {
  console.log('Checking imported listings...\n')
  
  const { count, error } = await supabase
    .from('listings')
    .select('*', { count: 'exact', head: true })
  
  if (error) {
    console.error('Error:', error.message)
    return
  }
  
  console.log(`Total listings in database: ${count}`)
  
  const { data, error: fetchError } = await supabase
    .from('listings')
    .select('brand, model, year, price, slug, status')
    .order('created_at', { ascending: false })
    .limit(10)
  
  if (fetchError) {
    console.error('Error fetching:', fetchError.message)
    return
  }
  
  console.log('\nRecent listings:')
  data?.forEach((car: any) => {
    console.log(`- ${car.year} ${car.brand} ${car.model} - ₱${car.price?.toLocaleString()} [${car.status}]`)
  })
  
  // Check images
  const { data: withImages, error: imgError } = await supabase
    .from('listings')
    .select('id, images')
    .not('images', 'eq', '{}')
  
  if (!imgError) {
    console.log(`\nListings with images: ${withImages?.length || 0}`)
  }
}

checkImport().catch(console.error)
