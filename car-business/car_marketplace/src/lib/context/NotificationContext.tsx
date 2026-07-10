'use client';

import { createContext, useContext, useState, useEffect, useCallback } from 'react';
import { useAuth } from '@/lib/auth/AuthContext';
import { getUnreadCount } from '@/lib/db/queries/notifications';

interface NotificationContextType {
  unreadCount: number;
  refreshUnreadCount: () => Promise<void>;
}

const NotificationContext = createContext<NotificationContextType | undefined>(undefined);

export function NotificationProvider({ children }: { children: React.ReactNode }) {
  const { user } = useAuth();
  const [unreadCount, setUnreadCount] = useState(0);

  const refreshUnreadCount = useCallback(async () => {
    if (!user) {
      setUnreadCount(0);
      return;
    }
    const count = await getUnreadCount(user.id);
    setUnreadCount(count);
  }, [user]);

  useEffect(() => {
    refreshUnreadCount();

    // Poll every 30 seconds
    const interval = setInterval(refreshUnreadCount, 30000);
    return () => clearInterval(interval);
  }, [refreshUnreadCount]);

  return (
    <NotificationContext.Provider value={{ unreadCount, refreshUnreadCount }}>
      {children}
    </NotificationContext.Provider>
  );
}

export function useNotification() {
  const context = useContext(NotificationContext);
  if (context === undefined) {
    throw new Error('useNotification must be used within a NotificationProvider');
  }
  return context;
}
