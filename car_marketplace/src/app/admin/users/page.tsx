'use client';

import { useState, useEffect } from 'react';
import Link from 'next/link';
import { supabase } from '@/lib/db/supabase-client';
import { useAuth } from '@/lib/auth/AuthContext';
import type { UserProfile } from '@/lib/db/queries/users';

interface UserWithCount extends UserProfile {
  car_count: number;
}

export default function AdminUsersPage() {
  const { user, isAdmin } = useAuth();
  const [users, setUsers] = useState<UserWithCount[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!user || !isAdmin) return;

    const fetchUsers = async () => {
      // Get all users
      const { data: usersData, error: usersError } = await supabase
        .from('users')
        .select('id, name, phone, is_admin, created_at')
        .order('created_at', { ascending: false });

      if (usersError || !usersData) {
        setLoading(false);
        return;
      }

      // Get car counts in a single query using RPC or aggregation
      const { data: countsData, error: countsError } = await supabase
        .from('listings')
        .select('user_id')
        .in('user_id', usersData.map(u => u.id));

      // Count cars per user
      const countMap: Record<string, number> = {};
      if (!countsError && countsData) {
        countsData.forEach((item: { user_id: string }) => {
          countMap[item.user_id] = (countMap[item.user_id] || 0) + 1;
        });
      }

      const usersWithCounts = usersData.map(userData => ({
        ...userData,
        car_count: countMap[userData.id] || 0,
      }));

      setUsers(usersWithCounts);
      setLoading(false);
    };

    fetchUsers();
  }, [user, isAdmin]);

  if (!user || !isAdmin) {
    return (
      <div className="container mx-auto px-4 py-16 text-center">
        <h1 className="text-3xl font-bold mb-4">Access Denied</h1>
        <p className="mb-6">You don't have permission to access this page.</p>
        <Link href="/" className="bg-blue-600 text-white px-6 py-3 rounded-lg hover:bg-blue-700">
          Go Home
        </Link>
      </div>
    );
  }

  const toggleAdmin = async (userId: string, currentStatus: boolean) => {
    const { error } = await supabase
      .from('users')
      .update({ is_admin: !currentStatus })
      .eq('id', userId);

    if (!error) {
      setUsers(prev =>
        prev.map(u =>
          u.id === userId ? { ...u, is_admin: !currentStatus } : u
        )
      );
    }
  };

  const handleDeleteUser = async (userId: string) => {
    if (userId === user.id) {
      alert("You cannot delete your own account!");
      return;
    }

    if (!confirm('Are you sure you want to delete this user? This will also delete all their listings.')) return;

    const { error } = await supabase
      .from('users')
      .delete()
      .eq('id', userId);

    if (!error) {
      setUsers(prev => prev.filter(u => u.id !== userId));
    }
  };

  return (
    <div className="container mx-auto px-4 py-8">
      <div className="flex justify-between items-center mb-6">
        <h1 className="text-3xl font-bold">User Management</h1>
        <Link
          href="/admin"
          className="bg-gray-200 text-gray-700 px-4 py-2 rounded hover:bg-gray-300"
        >
          ← Back to Dashboard
        </Link>
      </div>

      {/* Stats */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-8">
        <div className="bg-white p-6 rounded-xl shadow-sm">
          <div className="text-3xl font-bold text-blue-600">{users.length}</div>
          <div className="text-gray-600">Total Users</div>
        </div>
        <div className="bg-white p-6 rounded-xl shadow-sm">
          <div className="text-3xl font-bold text-green-600">
            {users.filter(u => u.is_admin).length}
          </div>
          <div className="text-gray-600">Admins</div>
        </div>
        <div className="bg-white p-6 rounded-xl shadow-sm">
          <div className="text-3xl font-bold text-purple-600">
            {users.reduce((sum, u) => sum + u.car_count, 0)}
          </div>
          <div className="text-gray-600">Total Listings</div>
        </div>
      </div>

      {loading ? (
        <div className="text-center py-16">Loading...</div>
      ) : (
        <div className="bg-white rounded-xl shadow-sm overflow-hidden">
          <table className="w-full">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">User</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Phone</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Admin</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Cars</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Joined</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200">
              {users.map((u) => (
                <tr key={u.id}>
                  <td className="px-6 py-4">
                    <div className="font-medium">{u.name || 'N/A'}</div>
                    <div className="text-sm text-gray-500">{u.id}</div>
                  </td>
                  <td className="px-6 py-4 text-sm">{u.phone}</td>
                  <td className="px-6 py-4">
                    <button
                      onClick={() => toggleAdmin(u.id, u.is_admin)}
                      className={`px-3 py-1 rounded-full text-xs font-medium ${
                        u.is_admin
                          ? 'bg-green-100 text-green-700 hover:bg-green-200'
                          : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                      }`}
                    >
                      {u.is_admin ? 'Yes' : 'No'}
                    </button>
                  </td>
                  <td className="px-6 py-4 text-sm">{u.car_count}</td>
                  <td className="px-6 py-4 text-sm text-gray-500">
                    {new Date(u.created_at).toLocaleDateString()}
                  </td>
                  <td className="px-6 py-4">
                    <div className="flex gap-2">
                      <Link
                        href={`/admin/users/${u.id}`}
                        className="text-blue-600 hover:underline text-sm"
                      >
                        View
                      </Link>
                      {u.id !== user.id && (
                        <button
                          onClick={() => handleDeleteUser(u.id)}
                          className="text-red-600 hover:underline text-sm"
                        >
                          Delete
                        </button>
                      )}
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
