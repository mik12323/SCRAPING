'use client';

import { useState, useEffect } from 'react';
import { useParams } from 'next/navigation';
import Link from 'next/link';
import { useAuth } from '@/lib/auth/AuthContext';
import { formatDisplayPrice } from '@/lib/api';

export default function AdminUserDetailPage() {
  const params = useParams();
  const { user, isAdmin } = useAuth();
  const [userInfo, setUserInfo] = useState<any>(null);
  const [listings, setListings] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [filterStatus, setFilterStatus] = useState<string>('');

  useEffect(() => {
    if (!params?.id || !user || !isAdmin) return;

    const fetchData = async () => {
      setLoading(true);

      const [usersRes, listingsRes] = await Promise.all([
        fetch('/api/admin/users'),
        fetch(`/api/admin/user-listings?userId=${params.id}${filterStatus ? `&status=${filterStatus}` : ''}`),
      ]);

      const usersData = await usersRes.json();
      const listingsData = await listingsRes.json();

      const foundUser = usersData.users?.find((u: any) => u.id === params.id);
      setUserInfo(foundUser || null);
      setListings(listingsData.listings || []);
      setLoading(false);
    };

    fetchData();
  }, [params?.id, user, isAdmin, filterStatus]);

  if (!user || !isAdmin) {
    return (
      <div className="container mx-auto px-4 py-16 text-center">
        <h1 className="text-3xl font-bold mb-4">Access Denied</h1>
        <p className="mb-6">You don't have permission to access this page.</p>
        <Link href="/" className="bg-blue-600 text-white px-6 py-3 rounded-lg hover:bg-blue-700">Go Home</Link>
      </div>
    );
  }

  return (
    <div className="container mx-auto px-4 py-8">
      <div className="flex items-center gap-4 mb-6">
        <Link href="/admin/users" className="flex items-center gap-2 text-gray-600 hover:text-gray-900 transition-colors">
          <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 19l-7-7m0 0l7-7m-7 7h18" />
          </svg>
          Back to Users
        </Link>
      </div>

      {loading ? (
        <div className="text-center py-16">Loading...</div>
      ) : !userInfo ? (
        <div className="text-center py-16">
          <p className="text-gray-600 mb-4">User not found.</p>
          <Link href="/admin/users" className="text-blue-600 hover:underline">Back to Users</Link>
        </div>
      ) : (
        <>
          {/* User Info Card */}
          <div className="bg-white p-6 rounded-xl shadow-sm mb-8">
            <h1 className="text-2xl font-bold mb-4">User Details</h1>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <span className="text-sm text-gray-500">Username</span>
                <p className="font-medium">{userInfo.name || 'N/A'}</p>
              </div>
              <div>
                <span className="text-sm text-gray-500">Phone</span>
                <p className="font-medium">{userInfo.phone}</p>
              </div>
              <div>
                <span className="text-sm text-gray-500">Admin</span>
                <p className={`font-medium ${userInfo.is_admin ? 'text-green-600' : 'text-gray-700'}`}>
                  {userInfo.is_admin ? 'Yes' : 'No'}
                </p>
              </div>
              <div>
                <span className="text-sm text-gray-500">Joined</span>
                <p className="font-medium">{new Date(userInfo.created_at).toLocaleDateString()}</p>
              </div>
              <div>
                <span className="text-sm text-gray-500">User ID</span>
                <p className="font-mono text-sm text-gray-600 break-all">{userInfo.id}</p>
              </div>
              <div>
                <span className="text-sm text-gray-500">Total Listings</span>
                <p className="font-medium">{listings.length}</p>
              </div>
            </div>
          </div>

          {/* Listings Section */}
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-xl font-bold">Car Listings</h2>
            <div className="flex gap-2">
              {['', 'pending', 'approved', 'rejected'].map(s => (
                <button
                  key={s}
                  onClick={() => setFilterStatus(s)}
                  className={`px-3 py-1 rounded-lg text-sm capitalize ${
                    filterStatus === s
                      ? 'bg-blue-600 text-white'
                      : 'bg-gray-200 text-gray-700 hover:bg-gray-300'
                  }`}
                >
                  {s || 'All'}
                </button>
              ))}
            </div>
          </div>

          {listings.length === 0 ? (
            <div className="text-center py-16 text-gray-600">No listings found.</div>
          ) : (
            <div className="bg-white rounded-xl shadow-sm overflow-hidden">
              <table className="w-full">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Car</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Price</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Status</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Date</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Actions</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-200">
                  {listings.map((listing) => (
                    <tr key={listing.id}>
                      <td className="px-6 py-4">
                        <div className="flex items-center">
                          {listing.images?.[0] && (
                            <img
                              src={listing.images[0]}
                              alt=""
                              className="w-12 h-12 object-cover rounded mr-3"
                            />
                          )}
                          <div>
                            <div className="font-medium">{listing.year} {listing.brand} {listing.model}</div>
                            <div className="text-sm text-gray-500">{listing.location || ''}</div>
                          </div>
                        </div>
                      </td>
                      <td className="px-6 py-4 font-medium">{formatDisplayPrice(listing.price)}</td>
                      <td className="px-6 py-4">
                        <span className={`px-2 py-1 rounded text-xs ${
                          listing.status === 'approved' ? 'bg-green-100 text-green-700' :
                          listing.status === 'pending' ? 'bg-yellow-100 text-yellow-700' :
                          'bg-red-100 text-red-700'
                        }`}>
                          {listing.status}
                        </span>
                      </td>
                      <td className="px-6 py-4 text-sm text-gray-500">
                        {listing.created_at ? new Date(listing.created_at).toLocaleDateString() : 'N/A'}
                      </td>
                      <td className="px-6 py-4">
                        <Link
                          href={`/preview/${listing.id}`}
                          className="text-blue-600 hover:underline text-sm"
                        >
                          View
                        </Link>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </>
      )}
    </div>
  );
}
