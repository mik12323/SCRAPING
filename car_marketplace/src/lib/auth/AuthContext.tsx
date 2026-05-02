'use client';

import { createContext, useContext, useEffect, useState, ReactNode } from 'react';
import { supabase } from '@/lib/db/supabase-client';
import { UserProfile, getUserById, getUserByUsername } from '@/lib/db/queries/users';
import { createAdminNotification } from '@/lib/db/queries/notifications';

interface AuthContextType {
  user: UserProfile | null;
  loading: boolean;
  signIn: (username: string, password: string) => Promise<{ error: any }>;
  signUp: (username: string, password: string, phone: string) => Promise<{ error: any }>;
  signOut: () => Promise<void>;
  isAdmin: boolean;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<UserProfile | null>(null);
  const [loading, setLoading] = useState(true);
  const [isAdmin, setIsAdmin] = useState(false);

  useEffect(() => {
    // Check active session
    const checkSession = async () => {
      try {
        const { data: { session }, error } = await supabase.auth.getSession();
        if (error) {
          console.error('Session error (lock may have been stolen):', error);
          // Clear any stale state
          await supabase.auth.signOut();
          setLoading(false);
          return;
        }
        if (session?.user) {
          const profile = await getUserById(session.user.id);
          setUser(profile);
          setIsAdmin(profile?.is_admin || false);
        }
      } catch (error) {
        console.error('Error checking session:', error);
        // Reset state on lock errors
        setUser(null);
        setIsAdmin(false);
      } finally {
        setLoading(false);
      }
    };

    checkSession();

    // Listen for auth changes
    const { data: { subscription } } = supabase.auth.onAuthStateChange(async (event, session) => {
      if (event === 'TOKEN_REFRESHED') {
        // Ignore token refresh events that might cause lock issues
        return;
      }
      if (session?.user) {
        const profile = await getUserById(session.user.id);
        setUser(profile);
        setIsAdmin(profile?.is_admin || false);
      } else {
        setUser(null);
        setIsAdmin(false);
      }
      setLoading(false);
    });

    return () => subscription.unsubscribe();
  }, []);

  const signIn = async (username: string, password: string) => {
    // In Supabase, we need email to sign in. We use the synthetic email format
    const email = `${username}@usedcars.ph`;

    const { data, error } = await supabase.auth.signInWithPassword({
      email,
      password,
    });

    if (error) {
      return { error };
    }

    return { error: null };
  };

  const signUp = async (username: string, password: string, phone: string) => {
    // Create auth user with a synthetic email
    const email = `${username}@usedcars.ph`;
    const { data, error } = await supabase.auth.signUp({ email, password });
    if (!error && data.user) {
      // Create user profile (name field stores the username)
      const { error: profileError } = await supabase
        .from('users')
        .insert([{ id: data.user.id, name: username, phone, is_admin: false }]);
      if (profileError) {
        console.error('Profile creation error:', profileError);
        return { error: { message: 'Failed to create profile. Please try again.' } };
      }

      // Notify admins about new user registration
      await createAdminNotification(
        null,
        'listing_approved', // Reuse type or create 'user_registered' if preferred
        'New User Registered',
        `New user registered: ${username}`
      );

      return { error: null };
    }
    return { error };
  };

  const signOut = async () => {
    await supabase.auth.signOut();
    setUser(null);
    setIsAdmin(false);
    // Force page reload to clear any cached state
    if (typeof window !== 'undefined') {
      window.location.href = '/';
    }
  };

  return (
    <AuthContext.Provider value={{ user, loading, signIn, signUp, signOut, isAdmin }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (context === undefined) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
}
