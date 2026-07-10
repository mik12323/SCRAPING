import { NextRequest, NextResponse } from 'next/server';
import { createClient } from '@supabase/supabase-js';

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY!;
const adminSupabase = createClient(supabaseUrl, supabaseServiceKey);

export async function POST(request: NextRequest) {
  try {
    const { userId, listingId, type, title, message } = await request.json();

    if (!userId || !type || !title) {
      return NextResponse.json({ error: 'Missing required fields' }, { status: 400 });
    }

    // Check if target user is admin (admins don't get notifications about their own actions)
    const { data: userData } = await adminSupabase
      .from('users')
      .select('is_admin')
      .eq('id', userId)
      .single();

    if (userData?.is_admin) {
      return NextResponse.json({ success: true, skipped: true });
    }

    const { error: insertError } = await adminSupabase
      .from('notifications')
      .insert({
        user_id: userId,
        listing_id: listingId || null,
        type,
        title,
        message: message || null,
      });

    if (insertError) {
      console.error('Error inserting notification:', insertError);
      return NextResponse.json({ error: 'Failed to create notification' }, { status: 500 });
    }

    return NextResponse.json({ success: true });
  } catch (err: any) {
    console.error('Error in notify-user:', err);
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 });
  }
}
