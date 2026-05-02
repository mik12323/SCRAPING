import { createClient, SupabaseClient } from '@supabase/supabase-js'

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL
const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY

if (!supabaseUrl || !supabaseAnonKey) {
  throw new Error('Missing Supabase environment variables. Ensure NEXT_PUBLIC_SUPABASE_URL and NEXT_PUBLIC_SUPABASE_ANON_KEY are set in .env.local')
}

export const supabase: SupabaseClient = createClient(supabaseUrl, supabaseAnonKey, {
  auth: {
    lock: async (name: string, acquireTimeout: number, fn: () => Promise<any>) => {
      // Bypass Web Locks API to avoid lock stealing issues during React Strict Mode
      return await fn();
    },
    autoRefreshToken: true,
    persistSession: true,
    detectSessionInUrl: true,
  }
})
