// ============================================
// NOTIFICATION QUERIES - Supabase Integration
// ============================================

import { supabase } from '@/lib/db/supabase-client';

export interface Notification {
  id: string;
  user_id: string;
  listing_id?: string;
  type: 'edit_rejected' | 'edit_approved' | 'listing_approved' | 'listing_rejected';
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
