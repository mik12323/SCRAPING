'use client';

import React, { useState, useEffect } from 'react';
import Link from 'next/link';
import { useAuth } from '@/lib/auth/AuthContext';
import { supabase } from '@/lib/db/supabase-client';
import { formatDisplayPrice } from '@/lib/api';
import type { Car } from '@/lib/types';
import ConfirmationModal from '@/components/ConfirmationModal';
import { createAdminNotification, createNotification } from '@/lib/db/queries/notifications';

export const dynamic = 'force-dynamic'; // Prevent static generation - location is browser-only

interface EditProposal {
  from: any;
  to: any;
}

interface CarWithProposal extends Car {
  edit_proposal?: Record<string, EditProposal> | null;
}

export default function AdminPage() {
  const { user, isAdmin } = useAuth();
  const [cars, setCars] = useState<CarWithProposal[]>([]);
  const [loading, setLoading] = useState(true);
  const [activeTab, setActiveTab] = useState<'pending' | 'approved' | 'rejected'>('pending');
  const [stats, setStats] = useState({ pending: 0, approved: 0, rejected: 0 });
  const [selectedIds, setSelectedIds] = useState<string[]>([]);
  const [showProposalFor, setShowProposalFor] = useState<string | null>(null);
  const [bulkActionLoading, setBulkActionLoading] = useState(false);

  // Confirmation modal state
  const [confirmModal, setConfirmModal] = useState<{
    isOpen: boolean;
    title: string;
    message: string;
    confirmText: string;
    isDanger: boolean;
    onConfirm: () => void;
    onClose: () => void;
  } | null>(null);

  // Reject edit modal state
  const [rejectEditModal, setRejectEditModal] = useState<{
    isOpen: boolean;
    listingId: string | null;
    carInfo: string;
  }>({ isOpen: false, listingId: null, carInfo: '' });
  const [adminRemarks, setAdminRemarks] = useState('');

  // Fetch stats once
  useEffect(() => {
    if (!user || !isAdmin) return;

    const fetchStats = async () => {
      const { data, error } = await supabase
        .from('listings')
        .select('status')
        .in('status', ['pending', 'approved', 'rejected']);

      if (!error && data) {
        const counts = { pending: 0, approved: 0, rejected: 0 };
        data.forEach((item: { status: string }) => {
          if (item.status === 'pending') counts.pending++;
          if (item.status === 'approved') counts.approved++;
          if (item.status === 'rejected') counts.rejected++;
        });
        setStats(counts);
      }
    };

    fetchStats();
  }, [user, isAdmin]);

  useEffect(() => {
    if (!user || !isAdmin) return;

    const fetchCars = async () => {
      setLoading(true);
      setSelectedIds([]);

      console.log('Fetching cars with status:', activeTab);

      // Fetch with edit_proposal column
       const { data, error } = await supabase
         .from('listings')
         .select('id, slug, brand, model, year, price, images, user_id, created_at, status, edit_proposal, location')
         .eq('status', activeTab)
         .order('created_at', { ascending: false });

       if (!error && data) {
         console.log('Fetched cars:', data.length);

         // Fetch user names for all unique user_ids
         const userIds = [...new Set(data.map(car => car.user_id).filter(Boolean))];
         const { data: users } = await supabase
           .from('users')
           .select('id, name, phone')
           .in('id', userIds);

         const userMap = new Map(users?.map(u => [u.id, u.name || u.phone || 'Unknown']) || []);

         // Add user_name to each car
         const carsWithUsers = data.map(car => ({
           ...car,
           user_name: userMap.get(car.user_id) || 'N/A'
         }));

         setCars(carsWithUsers as any);
       } else if (error) {
         console.error('Error fetching cars:', error);
       }
      setLoading(false);
    };

    fetchCars();
  }, [user, isAdmin, activeTab]);

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

  const handleStatusChange = async (id: string, newStatus: 'approved' | 'rejected') => {
    // Check if this listing has an edit_proposal (meaning it's an edit being reviewed)
    const car = cars.find(c => c.id === id);

    if (car?.edit_proposal && newStatus === 'rejected') {
      // Show modal to reject edit (revert to original)
      setRejectEditModal({ isOpen: true, listingId: id, carInfo: `${car.year} ${car.brand} ${car.model}` });
      return;
    }

    // For regular approval/rejection or rejecting new listings
    if (car?.edit_proposal && newStatus === 'approved') {
      // Approve the edit - apply the proposed changes
      const updates: any = {
        status: 'approved',
        updated_at: new Date().toISOString(),
        edit_proposal: null,
        admin_remarks: null,
      };

      // Apply the proposed changes from edit_proposal
      if (car.edit_proposal) {
        Object.entries(car.edit_proposal).forEach(([field, change]: [string, any]) => {
          updates[field] = change.to;
        });
      }

      const { error } = await supabase
        .from('listings')
        .update(updates)
        .eq('id', id);

      if (!error && car.user_id) {
        // Create notification for user (skip if user is admin - handled in createNotification)
        await createNotification(
          car.user_id,
          id,
          'edit_approved',
          'Edit Approved',
          `Your edit to ${car.year} ${car.brand} ${car.model} has been approved.`
        );
      }

      if (!error) {
        setCars(prev => prev.filter(c => c.id !== id));
        setSelectedIds(prev => prev.filter(item => item !== id));
        setStats(prev => ({
          ...prev,
          [activeTab]: prev[activeTab] - 1,
          approved: prev.approved + 1,
        }));
      }
      return;
    }

    // Regular status change (new listings) - notify ADMINS about approval/rejection
    const { error } = await supabase
      .from('listings')
      .update({ status: newStatus, updated_at: new Date().toISOString(), edit_proposal: null })
      .eq('id', id);

    if (!error) {
      setCars(prev => prev.filter(car => car.id !== id));
      setSelectedIds(prev => prev.filter(item => item !== id));
      setStats(prev => ({
        ...prev,
        [activeTab]: prev[activeTab] - 1,
        [newStatus]: prev[newStatus] + 1,
      }));

      // Notify ADMINS about new listing approval/rejection
      if (car) {
        const type = newStatus === 'approved' ? 'listing_approved' : 'listing_rejected';
        const title = newStatus === 'approved' ? 'New Listing Approved' : 'New Listing Rejected';
        const message = newStatus === 'approved'
          ? `A new listing ${car.year} ${car.brand} ${car.model} has been approved.`
          : `A new listing ${car.year} ${car.brand} ${car.model} was rejected.`;

        await createAdminNotification(
          id,
          type,
          title,
          message
        );
      }
    }
  };

  const handleRejectEdit = async () => {
    if (!rejectEditModal.listingId) return;

    const car = cars.find(c => c.id === rejectEditModal.listingId);
    if (!car || !car.edit_proposal) return;

    // Revert to original values from edit_proposal
    const updates: any = {
      status: 'approved', // Revert to approved
      updated_at: new Date().toISOString(),
      edit_proposal: null,
    };

    // Revert each field to its original value
    Object.entries(car.edit_proposal).forEach(([field, change]: [string, any]) => {
      updates[field] = change.from;
    });

    // Add admin remarks if provided
    if (adminRemarks.trim()) {
      updates.admin_remarks = adminRemarks.trim();
    } else {
      updates.admin_remarks = null;
    }

    const { error } = await supabase
      .from('listings')
      .update(updates)
      .eq('id', rejectEditModal.listingId);

    if (!error && car.user_id) {
      // Create notification for user (skip if user is admin - handled in createNotification)
      const message = adminRemarks.trim()
        ? `Reason: ${adminRemarks.trim()}`
        : `Your edit to ${car.year} ${car.brand} ${car.model} was rejected and reverted to original.`;

      await createNotification(
        car.user_id,
        rejectEditModal.listingId,
        'edit_rejected',
        'Edit Rejected',
        message
      );
    }

    if (!error) {
      setCars(prev => prev.filter(c => c.id !== rejectEditModal.listingId));
      setSelectedIds(prev => prev.filter(item => item !== rejectEditModal.listingId));
      setStats(prev => ({
        ...prev,
        [activeTab]: prev[activeTab] - 1,
        approved: prev.approved + 1, // Reverted to approved
      }));
    }

    // Reset modal
    setRejectEditModal({ isOpen: false, listingId: null, carInfo: '' });
    setAdminRemarks('');
  };

  const handleDelete = async (id: string) => {
    if (!confirm('Are you sure you want to delete this listing?')) return;

    const { error } = await supabase
      .from('listings')
      .delete()
      .eq('id', id);

    if (!error) {
      setCars(prev => prev.filter(car => car.id !== id));
      setSelectedIds(prev => prev.filter(item => item !== id));
      setStats(prev => ({ ...prev, [activeTab]: prev[activeTab] - 1 }));
    }
  };

  // Bulk actions
  const handleSelectAll = () => {
    if (selectedIds.length === cars.length) {
      setSelectedIds([]);
    } else {
      setSelectedIds(cars.map(car => car.id));
    }
  };

  const handleBulkApprove = () => {
    if (selectedIds.length === 0) return;
    setConfirmModal({
      isOpen: true,
      title: 'Approve All Selected',
      message: `Are you sure you want to approve ${selectedIds.length} listing(s)? This will approve all selected listings and they will be visible to the public.`,
      confirmText: 'Approve All',
      isDanger: false,
      onConfirm: async () => {
        setBulkActionLoading(true);
        setConfirmModal(null);
        const { error } = await supabase
          .from('listings')
          .update({ status: 'approved', updated_at: new Date().toISOString(), edit_proposal: null })
          .in('id', selectedIds);

        if (!error) {
          setCars(prev => prev.filter(car => !selectedIds.includes(car.id)));
          setStats(prev => ({
            ...prev,
            [activeTab]: prev[activeTab] - selectedIds.length,
            approved: prev.approved + selectedIds.length,
          }));
          setSelectedIds([]);
        }
        setBulkActionLoading(false);
      },
      onClose: () => setConfirmModal(null),
    });
  };

  const handleBulkDelete = () => {
    if (selectedIds.length === 0) return;
    setConfirmModal({
      isOpen: true,
      title: 'Delete All Selected',
      message: `Are you sure you want to delete ${selectedIds.length} listing(s)? This action cannot be undone.`,
      confirmText: 'Delete All',
      isDanger: true,
      onConfirm: async () => {
        setBulkActionLoading(true);
        setConfirmModal(null);
        const { error } = await supabase
          .from('listings')
          .delete()
          .in('id', selectedIds);

        if (!error) {
          setCars(prev => prev.filter(car => !selectedIds.includes(car.id)));
          setStats(prev => ({ ...prev, [activeTab]: prev[activeTab] - selectedIds.length }));
          setSelectedIds([]);
        }
        setBulkActionLoading(false);
      },
      onClose: () => setConfirmModal(null),
    });
  };

  // Helper to display field names nicely
  const fieldDisplayNames: Record<string, string> = {
    brand: 'Brand',
    model: 'Model',
    year: 'Year',
    price: 'Price',
    original_price: 'Original Price',
    mileage: 'Mileage',
    transmission: 'Transmission',
    fuel_type: 'Fuel Type',
    body_type: 'Body Type',
    color: 'Color',
    location: 'Location',
    description: 'Description',
  };

  return (
    <div className="container mx-auto px-4 py-8">
      <div className="flex justify-between items-center mb-6">
        <h1 className="text-3xl font-bold">Admin Dashboard</h1>
        <Link
          href="/admin/users"
          className="bg-gray-200 text-gray-700 px-4 py-2 rounded hover:bg-gray-300"
        >
          Manage Users
        </Link>
      </div>

      {/* Stats */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-8">
        <div className="bg-white p-6 rounded-xl shadow-sm">
          <div className="text-3xl font-bold text-blue-600">{stats[activeTab]}</div>
          <div className="text-gray-600 capitalize">{activeTab} Listings</div>
        </div>
        <div className="bg-white p-6 rounded-xl shadow-sm">
          <div className="text-3xl font-bold text-green-600">
            {stats.approved}
          </div>
          <div className="text-gray-600">Approved</div>
        </div>
        <div className="bg-white p-6 rounded-xl shadow-sm">
          <div className="text-3xl font-bold text-yellow-600">
            {stats.pending}
          </div>
          <div className="text-gray-600">Pending</div>
        </div>
        <div className="bg-white p-6 rounded-xl shadow-sm">
          <div className="text-3xl font-bold text-red-600">
            {stats.rejected}
          </div>
          <div className="text-gray-600">Rejected</div>
        </div>
      </div>

      {/* Tabs */}
      <div className="flex gap-4 mb-6">
        {(['pending', 'approved', 'rejected'] as const).map(tab => (
          <button
            key={tab}
            onClick={() => {
              setActiveTab(tab);
              setLoading(true);
            }}
            className={`px-4 py-2 rounded-lg capitalize ${
              activeTab === tab
                ? 'bg-blue-600 text-white'
                : 'bg-gray-200 text-gray-700 hover:bg-gray-300'
            }`}
          >
            {tab}
          </button>
        ))}
      </div>

      {loading ? (
        <div className="text-center py-16">Loading...</div>
      ) : cars.length === 0 ? (
        <div className="text-center py-16 text-gray-600">
          No {activeTab} listings found.
        </div>
      ) : (
        <>
          {/* Bulk Actions Bar */}
          {selectedIds.length > 0 && (
            <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 mb-4 flex items-center justify-between">
              <span className="text-blue-800 font-medium">
                {selectedIds.length} listing(s) selected
              </span>
              <div className="flex gap-2">
                <button
                  onClick={handleBulkApprove}
                  disabled={bulkActionLoading}
                  className="bg-green-600 text-white px-4 py-2 rounded hover:bg-green-700 disabled:bg-gray-400 text-sm"
                >
                  {bulkActionLoading ? 'Processing...' : 'Approve All'}
                </button>
                <button
                  onClick={handleBulkDelete}
                  disabled={bulkActionLoading}
                  className="bg-red-600 text-white px-4 py-2 rounded hover:bg-red-700 disabled:bg-gray-400 text-sm"
                >
                  {bulkActionLoading ? 'Processing...' : 'Delete All'}
                </button>
                <button
                  onClick={() => setSelectedIds([])}
                  className="bg-gray-300 text-gray-700 px-4 py-2 rounded hover:bg-gray-400 text-sm"
                >
                  Clear Selection
                </button>
              </div>
            </div>
          )}

          <div className="bg-white rounded-xl shadow-sm overflow-hidden">
            <table className="w-full">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-6 py-3 text-left">
                    <input
                      type="checkbox"
                      checked={selectedIds.length === cars.length && cars.length > 0}
                      onChange={handleSelectAll}
                      className="w-4 h-4 text-blue-600 rounded focus:ring-blue-500"
                    />
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Car</th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Price</th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Seller</th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Date</th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Actions</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-200">
                {cars.map((car) => (
                  <React.Fragment key={car.id}>
                    <tr className={car.edit_proposal && showProposalFor !== car.id ? 'bg-yellow-50' : ''}>
                    <td className="px-6 py-4">
                      <input
                        type="checkbox"
                        checked={selectedIds.includes(car.id)}
                        onChange={() => {
                          if (selectedIds.includes(car.id)) {
                            setSelectedIds(prev => prev.filter(id => id !== car.id));
                          } else {
                            setSelectedIds(prev => [...prev, car.id]);
                          }
                        }}
                        className="w-4 h-4 text-blue-600 rounded focus:ring-blue-500 cursor-pointer"
                      />
                    </td>
                    <td className="px-6 py-4">
                      <div className="flex items-center">
                        {car.images && car.images[0] && (
                          <img
                            src={car.images[0]}
                            alt=""
                            className="w-12 h-12 object-cover rounded mr-3"
                          />
                        )}
                        <div>
                          <div className="font-medium">
                            {car.year} {car.brand} {car.model}
                            {car.edit_proposal && (
                              <span className="ml-2 px-2 py-1 text-xs bg-yellow-200 text-yellow-800 rounded">
                                Edited
                              </span>
                            )}
                          </div>
                          <div className="text-sm text-gray-500">{car.location}</div>
                        </div>
                      </div>
                    </td>
                    <td className="px-6 py-4 font-medium">
                      {formatDisplayPrice(car.price)}
                    </td>
                    <td className="px-6 py-4 text-sm text-gray-500">
                      {(car as any).user_name || car.user_id || 'N/A'}
                    </td>
                    <td className="px-6 py-4 text-sm text-gray-500">
                      {car.created_at ? new Date(car.created_at).toLocaleDateString() : 'N/A'}
                    </td>
                    <td className="px-6 py-4">
                      <div className="flex gap-2 flex-wrap">
                        <Link
                          href={`/car/${car.slug}`}
                          className="text-blue-600 hover:underline text-sm cursor-pointer"
                        >
                          View
                        </Link>
                        {activeTab === 'pending' && (
                          <>
                            <button
                              onClick={() => handleStatusChange(car.id, 'approved')}
                              className="text-green-600 hover:underline text-sm cursor-pointer"
                            >
                              Approve
                            </button>
                            <button
                              onClick={() => handleStatusChange(car.id, 'rejected')}
                              className="text-red-600 hover:underline text-sm cursor-pointer"
                            >
                              Reject
                            </button>
                          </>
                        )}
                        {car.edit_proposal && (
                          <button
                            onClick={() => setShowProposalFor(showProposalFor === car.id ? null : car.id)}
                            className="text-yellow-600 hover:underline text-sm cursor-pointer"
                          >
                            {showProposalFor === car.id ? 'Hide Changes' : 'View Changes'}
                          </button>
                        )}
                        <button
                          onClick={() => handleDelete(car.id)}
                          className="text-red-600 hover:underline text-sm cursor-pointer"
                        >
                          Delete
                        </button>
                      </div>
                    </td>
                  </tr>
                  {/* Edit Proposal Details Row */}
                  {showProposalFor === car.id && car.edit_proposal && (
                    <tr className="bg-yellow-50">
                      <td colSpan={6} className="px-6 py-4">
                        <div className="bg-white rounded-lg p-4 border border-yellow-200">
                          <h4 className="font-bold text-sm mb-2">Proposed Changes:</h4>
                          <div className="space-y-1">
                            {Object.entries(car.edit_proposal).map(([field, change]) => (
                              <div key={field} className="text-sm">
                                <span className="font-medium">{fieldDisplayNames[field] || field}:</span>
                                {' '}
                                <span className="text-red-600 line-through">{String((change as EditProposal).from || '(empty)')}</span>
                                {' → '}
                                <span className="text-green-600">{String((change as EditProposal).to || '(empty)')}</span>
                              </div>
                            ))}
                          </div>
                        </div>
                      </td>
                    </tr>
                  )}
                  </React.Fragment>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}

      {/* Confirmation Modal */}
      <ConfirmationModal
        isOpen={!!confirmModal}
        title={confirmModal?.title || ''}
        message={confirmModal?.message || ''}
        confirmText={confirmModal?.confirmText || 'Confirm'}
        cancelText="Cancel"
        isDanger={confirmModal?.isDanger || false}
        isLoading={bulkActionLoading}
        onConfirm={confirmModal?.onConfirm || (() => {})}
        onClose={confirmModal?.onClose || (() => setConfirmModal(null))}
      />

      {/* Reject Edit Modal */}
      {rejectEditModal.isOpen && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
          <div className="bg-white rounded-xl p-6 max-w-md w-full mx-4">
            <h3 className="text-xl font-bold mb-4">Reject Edit</h3>
            <p className="text-gray-600 mb-4">
              Rejecting the edit for <span className="font-semibold">{rejectEditModal.carInfo}</span>.
              The car will be reverted to its original content and remain approved.
            </p>
            <div className="mb-4">
              <label htmlFor="adminRemarks" className="block text-sm font-medium mb-2">
                Remarks (Optional)
              </label>
              <textarea
                id="adminRemarks"
                value={adminRemarks}
                onChange={(e) => setAdminRemarks(e.target.value)}
                rows={3}
                placeholder="Explain why the edit was rejected..."
                className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              />
            </div>
            <div className="flex gap-3 justify-end">
              <button
                onClick={() => {
                  setRejectEditModal({ isOpen: false, listingId: null, carInfo: '' });
                  setAdminRemarks('');
                }}
                className="px-4 py-2 border border-gray-300 rounded-lg hover:bg-gray-50"
              >
                Cancel
              </button>
              <button
                onClick={handleRejectEdit}
                className="px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700"
              >
                Reject Edit
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}


