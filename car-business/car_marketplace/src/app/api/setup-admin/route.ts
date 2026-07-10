import { NextRequest, NextResponse } from 'next/server';
import { createClient, SupabaseClient } from '@supabase/supabase-js';

// Use service role key for admin operations
const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY!;

const adminSupabase: SupabaseClient = createClient(supabaseUrl, supabaseServiceKey);

export async function POST(request: NextRequest) {
  try {
    const { username, password, phone } = await request.json();

    if (!username || !password) {
      return NextResponse.json(
        { error: 'Username and password are required' },
        { status: 400 }
      );
    }

    if (password.length < 6) {
      return NextResponse.json(
        { error: 'Password must be at least 6 characters' },
        { status: 400 }
      );
    }

    // Create synthetic email from username (for Supabase Auth)
    const email = `${username}@usedcars.ph`;

    // 1. Create auth user using service role
    const { data: authData, error: authError } = await adminSupabase.auth.admin.createUser({
      email,
      password,
      email_confirm: true,
    });

    if (authError) {
      console.error('Auth error:', authError);
      return NextResponse.json(
        { error: 'Failed to create auth user' },
        { status: 500 }
      );
    }

    // 2. Insert into public.users table (using username as 'name')
    const { error: dbError } = await adminSupabase
      .from('users')
      .insert([
        {
          id: authData.user.id,
          name: username,
          phone: phone || '+639970946623',
          is_admin: true,
          created_at: new Date().toISOString(),
        }
      ]);

    if (dbError) {
      console.error('DB error:', dbError);
      return NextResponse.json(
        { error: 'Failed to create user profile' },
        { status: 500 }
      );
    }

    return NextResponse.json({
      success: true,
      message: 'Admin user created successfully',
      username: username,
      password: password,
      userId: authData.user.id,
    });
  } catch (error: any) {
    console.error('Error setting up admin:', error);
    return NextResponse.json(
      { error: 'Failed to setup admin' },
      { status: 500 }
    );
  }
}
