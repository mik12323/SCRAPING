// ============================================
// NOTIFICATION QUERIES - Supabase Integration
// ============================================

import { supabase } from '@/lib/db/supabase-client';

export interface Notification {
  id: string;
  user_id: string;
  listing_id?: string;
  type: 'edit_rejected' | 'edit_approved' | 'listing_approved' | 'listing_rejected' | 'listing_pending' | 'edit_pending' | 'user_registered';
  title: string;
  message?: string;
  is_read: boolean;
  created_at: string;
  listings?: {
    brand: string;
    model: string;
    year: number;
  };
}

export async function createNotification(
  userId: string,
  listingId: string | null,
  type: Notification['type'],
  title: string,
  message?: string
) {
  // Skip notification if user is admin (admins don't need notifications about their own actions)
  const { data: userData } = await supabase
    .from('users')
    .select('is_admin')
    .eq('id', userId)
    .single();

  if (userData?.is_admin) {
    return { data: null, error: null }; // Skip notification for admins
  }

  const { data, error } = await supabase
    .from('notifications')
    .insert({
      user_id: userId,
      listing_id: listingId,
      type,
      title,
      message,
    })
    .select()
    .single();

  return { data, error };
}

export async function createAdminNotification(
  listingId: string | null,
  type: Notification['type'],
  title: string,
  message?: string
) {
  // Get all admin users
  const { data: admins, error: adminError } = await supabase
    .from('users')
    .select('id')
    .eq('is_admin', true);

  if (adminError || !admins || admins.length === 0) {
    return { error: adminError };
  }

  // Create notification for each admin
  const notifications = admins.map(admin => ({
    user_id: admin.id,
    listing_id: listingId,
    type,
    title,
    message,
  }));

  const { data, error } = await supabase
    .from('notifications')
    .insert(notifications)
    .select();

  return { data, error };
}

export async function getNotifications(userId: string): Promise<Notification[]> {
  const { data, error } = await supabase
    .from('notifications')
    .select(`
      *,
      listings:listing_id (
        brand,
        model,
        year
      )
    `)
    .eq('user_id', userId)
    .order('created_at', { ascending: false })
    .limit(50);

  if (error) {
    console.error('Error fetching notifications:', error);
    return [];
  }

  return data || [];
}

export async function getUnreadCount(userId: string): Promise<number> {
  const { count, error } = await supabase
    .from('notifications')
    .select('*', { count: 'exact', head: true })
    .eq('user_id', userId)
    .eq('is_read', false);

  if (error) {
    console.error('Error counting notifications:', error);
    return 0;
  }

  return count || 0;
}

export async function markNotificationRead(notificationId: string) {
  const { error } = await supabase
    .from('notifications')
    .update({ is_read: true })
    .eq('id', notificationId);

  return { error };
}

export async function markAllRead(userId: string) {
  const { error } = await supabase
    .from('notifications')
    .update({ is_read: true })
    .eq('user_id', userId)
    .eq('is_read', false);

  return { error };
}

export async function deleteNotification(notificationId: string) {
  const { error } = await supabase
    .from('notifications')
    .delete()
    .eq('id', notificationId);

  return { error };
}
