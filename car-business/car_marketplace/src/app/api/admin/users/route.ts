import { NextRequest, NextResponse } from 'next/server';
import { createClient } from '@supabase/supabase-js';

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY!;
const adminSupabase = createClient(supabaseUrl, supabaseServiceKey);

export async function GET() {
  try {
    const { data: users, error } = await adminSupabase
      .from('users')
      .select('id, name, phone, is_admin, created_at')
      .order('created_at', { ascending: false });

    if (error) {
      console.error('Error fetching users:', error);
      return NextResponse.json({ error: 'Failed to fetch users' }, { status: 500 });
    }

    // Get car counts for all users
    const { data: listings } = await adminSupabase
      .from('listings')
      .select('user_id');

    const countMap: Record<string, number> = {};
    if (listings) {
      listings.forEach((item: { user_id: string }) => {
        countMap[item.user_id] = (countMap[item.user_id] || 0) + 1;
      });
    }

    const usersWithCounts = users.map(u => ({
      ...u,
      car_count: countMap[u.id] || 0,
    }));

    return NextResponse.json({ users: usersWithCounts });
  } catch (err: any) {
    console.error('Error in admin users API:', err);
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 });
  }
}

export async function PATCH(request: NextRequest) {
  try {
    const { userId, is_admin } = await request.json();

    if (!userId) {
      return NextResponse.json({ error: 'Missing userId' }, { status: 400 });
    }

    const { error } = await adminSupabase
      .from('users')
      .update({ is_admin })
      .eq('id', userId);

    if (error) {
      console.error('Error updating user:', error);
      return NextResponse.json({ error: 'Failed to update user' }, { status: 500 });
    }

    return NextResponse.json({ success: true });
  } catch (err: any) {
    console.error('Error in admin users API:', err);
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 });
  }
}

export async function DELETE(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    const userId = searchParams.get('userId');

    if (!userId) {
      return NextResponse.json({ error: 'Missing userId' }, { status: 400 });
    }

    const { error } = await adminSupabase
      .from('users')
      .delete()
      .eq('id', userId);

    if (error) {
      console.error('Error deleting user:', error);
      return NextResponse.json({ error: 'Failed to delete user' }, { status: 500 });
    }

    return NextResponse.json({ success: true });
  } catch (err: any) {
    console.error('Error in admin users API:', err);
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 });
  }
}
