import { NextRequest, NextResponse } from 'next/server';
import { createClient } from '@supabase/supabase-js';

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY!;
const adminSupabase = createClient(supabaseUrl, supabaseServiceKey);

export async function POST(request: NextRequest) {
  try {
    const { listingId, type, title, message } = await request.json();

    if (!type || !title) {
      return NextResponse.json(
        { error: 'Missing required fields' },
        { status: 400 }
      );
    }

    const { data: admins, error: adminError } = await adminSupabase
      .from('users')
      .select('id')
      .eq('is_admin', true);

    if (adminError) {
      console.error('Error fetching admins:', adminError);
      return NextResponse.json(
        { error: 'Failed to fetch admin users' },
        { status: 500 }
      );
    }

    if (!admins || admins.length === 0) {
      return NextResponse.json({ success: true, notified: 0 });
    }

    const notifications = admins.map(admin => ({
      user_id: admin.id,
      listing_id: listingId || null,
      type,
      title,
      message: message || null,
    }));

    const { error: insertError } = await adminSupabase
      .from('notifications')
      .insert(notifications);

    if (insertError) {
      console.error('Error inserting notifications:', insertError);
      return NextResponse.json(
        { error: 'Failed to create notifications' },
        { status: 500 }
      );
    }

    return NextResponse.json({ success: true, notified: admins.length });
  } catch (error: any) {
    console.error('Error in notify-admins:', error);
    return NextResponse.json(
      { error: 'Internal server error' },
      { status: 500 }
    );
  }
}
