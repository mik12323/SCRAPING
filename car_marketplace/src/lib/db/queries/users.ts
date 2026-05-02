// ============================================
// USER QUERIES - Supabase Integration
// ============================================

import { supabase } from '@/lib/db/supabase-client'

export interface UserProfile {
  id: string;
  name?: string; // This stores the username
  phone: string;
  email?: string;
  is_admin: boolean;
  created_at: string;
}

// For compatibility, name = username
export const username = (profile: UserProfile) => profile.name || '';

export async function getUserById(id: string): Promise<UserProfile | null> {
  try {
    const { data, error } = await supabase
      .from('users')
      .select('*')
      .eq('id', id)
      .single();

    if (error || !data) {
      console.error('Supabase error in getUserById:', error);
      return null;
    }

    return data as UserProfile;
  } catch (err) {
    console.error('Error in getUserById:', err);
    return null;
  }
}

export async function getUserByUsername(username: string): Promise<UserProfile | null> {
  try {
    const { data, error } = await supabase
      .from('users')
      .select('*')
      .eq('name', username)
      .single();

    if (error || !data) {
      console.error('Supabase error in getUserByUsername:', error);
      return null;
    }

    return data as UserProfile;
  } catch (err) {
    console.error('Error in getUserByUsername:', err);
    return null;
  }
}

export async function getUserByPhone(phone: string): Promise<UserProfile | null> {
  try {
    const { data, error } = await supabase
      .from('users')
      .select('*')
      .eq('phone', phone)
      .single();

    if (error || !data) {
      console.error('Supabase error in getUserByPhone:', error);
      return null;
    }

    return data as UserProfile;
  } catch (err) {
    console.error('Error in getUserByPhone:', err);
    return null;
  }
}
