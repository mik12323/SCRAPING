-- Migration: Add admin_remarks column to listings
ALTER TABLE public.listings ADD COLUMN IF NOT EXISTS admin_remarks TEXT;

-- Migration: Create notifications table
CREATE TABLE IF NOT EXISTS public.notifications (
  id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
  user_id uuid REFERENCES auth.users(id) ON DELETE CASCADE NOT NULL,
  listing_id uuid REFERENCES public.listings(id) ON DELETE CASCADE,
  type TEXT NOT NULL CHECK (type IN ('edit_rejected', 'edit_approved', 'listing_approved', 'listing_rejected')),
  title TEXT NOT NULL,
  message TEXT,
  is_read BOOLEAN DEFAULT false,
  created_at timestamp with time zone DEFAULT now()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_notifications_user_id ON public.notifications(user_id);
CREATE INDEX IF NOT EXISTS idx_notifications_is_read ON public.notifications(is_read);
CREATE INDEX IF NOT EXISTS idx_notifications_created_at ON public.notifications(created_at DESC);

-- RLS: Users can only see their own notifications
ALTER TABLE public.notifications ENABLE ROW LEVEL SECURITY;

-- Drop existing policies if any
DROP POLICY IF EXISTS "Users can view own notifications" ON public.notifications;
DROP POLICY IF EXISTS "Users can update own notifications" ON public.notifications;
DROP POLICY IF EXISTS "Authenticated users can insert notifications" ON public.notifications;

-- Create policies
CREATE POLICY "Users can view own notifications" ON public.notifications 
  FOR SELECT USING (auth.uid()::text = user_id::text);

CREATE POLICY "Users can update own notifications" ON public.notifications 
  FOR UPDATE USING (auth.uid()::text = user_id::text);

-- Allow authenticated users to insert notifications (for admin creating notifications)
CREATE POLICY "Authenticated users can insert notifications" ON public.notifications 
  FOR INSERT WITH CHECK (auth.role() = 'authenticated');
