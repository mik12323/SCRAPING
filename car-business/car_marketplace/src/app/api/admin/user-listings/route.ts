import { NextRequest, NextResponse } from 'next/server';
import { createClient } from '@supabase/supabase-js';

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY!;
const adminSupabase = createClient(supabaseUrl, supabaseServiceKey);

export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    const userId = searchParams.get('userId');
    const status = searchParams.get('status');

    if (!userId) {
      return NextResponse.json({ error: 'Missing userId parameter' }, { status: 400 });
    }

    let query = adminSupabase
      .from('listings')
      .select('id, slug, brand, model, year, price, images, status, created_at, location')
      .eq('user_id', userId)
      .order('created_at', { ascending: false });

    if (status) {
      query = query.eq('status', status);
    }

    const { data: listings, error } = await query;

    if (error) {
      console.error('Error fetching user listings:', error);
      return NextResponse.json({ error: 'Failed to fetch listings' }, { status: 500 });
    }

    return NextResponse.json({ listings: listings || [] });
  } catch (err: any) {
    console.error('Error in user-listings API:', err);
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 });
  }
}
