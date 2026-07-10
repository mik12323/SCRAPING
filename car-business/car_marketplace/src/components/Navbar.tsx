'use client';

import Link from 'next/link';
import { useAuth } from '@/lib/auth/AuthContext';
import { useNotification } from '@/lib/context/NotificationContext';

export default function Navbar() {
  const { user, isAdmin, signOut } = useAuth();
  const { unreadCount } = useNotification();

  return (
    <nav className="bg-gray-900 text-white shadow-lg">
      <div className="container mx-auto px-4">
        <div className="flex items-center justify-between h-16">
          <Link href="/" className="flex items-center space-x-2">
            <span className="text-2xl">🚗</span>
            <span className="font-bold text-xl">Used Cars Philippines</span>
          </Link>

          <div className="hidden md:flex items-center space-x-6">
            <Link href="/" className="hover:text-yellow-400 transition-colors cursor-pointer">
              Home
            </Link>
            <Link href="/browse" className="hover:text-yellow-400 transition-colors cursor-pointer">
              Browse Cars
            </Link>
            <Link href="/sell" className="hover:text-yellow-400 transition-colors cursor-pointer">
              Sell Your Car
            </Link>
            <Link href="/about" className="hover:text-yellow-400 transition-colors cursor-pointer">
              About
            </Link>

            {user ? (
              <>
                <Link href="/my-cars" className="hover:text-yellow-400 transition-colors cursor-pointer">
                  My Cars
                </Link>
                <Link href="/notifications" className="hover:text-yellow-400 transition-colors cursor-pointer relative">
                  Notifications
                  {unreadCount > 0 && (
                    <span className="absolute -top-2 -right-3 bg-red-600 text-white text-xs rounded-full h-5 w-5 flex items-center justify-center">
                      {unreadCount > 9 ? '9+' : unreadCount}
                    </span>
                  )}
                </Link>
                {isAdmin && (
                  <Link href="/admin" className="hover:text-yellow-400 transition-colors cursor-pointer">
                    Admin
                  </Link>
                )}
                <button
                  onClick={() => signOut()}
                  className="hover:text-yellow-400 transition-colors cursor-pointer"
                >
                  Logout
                </button>
              </>
            ) : (
              <>
                <Link href="/login" className="hover:text-yellow-400 transition-colors cursor-pointer">
                  Login
                </Link>
                <Link href="/register" className="bg-blue-600 hover:bg-blue-700 px-4 py-2 rounded transition-colors cursor-pointer">
                  Register
                </Link>
              </>
            )}
          </div>
        </div>
      </div>
    </nav>
  );
}
