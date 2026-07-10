-- Migration: Add pending notification types to notifications table
ALTER TABLE public.notifications
  DROP CONSTRAINT IF EXISTS notifications_type_check,
  ADD CONSTRAINT notifications_type_check
    CHECK (type IN ('edit_rejected', 'edit_approved', 'listing_approved', 'listing_rejected', 'listing_pending', 'edit_pending'));
