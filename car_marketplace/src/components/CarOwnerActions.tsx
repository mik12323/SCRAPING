'use client';

import { useState } from 'react';
import { useAuth } from '@/lib/auth/AuthContext';
import { deleteListing } from '@/lib/db/mutations/listings';
import { useRouter } from 'next/navigation';

interface CarOwnerActionsProps {
  carId: string;
  carSlug: string;
  userId: string | undefined;
}

export default function CarOwnerActions({ carId, carSlug, userId }: CarOwnerActionsProps) {
  const { user, isAdmin } = useAuth();
  const router = useRouter();
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);
  const [deleting, setDeleting] = useState(false);

  if (!user || (user.id !== userId && !isAdmin)) {
    return null;
  }

  const handleDelete = async () => {
    setDeleting(true);
    const { error } = await deleteListing(carId);
    setDeleting(false);

    if (!error) {
      router.push('/my-cars');
    } else {
      console.error('Delete error:', error);
      alert('Failed to delete listing. Please try again.');
    }
  };

  return (
    <>
      <div className="flex gap-2 mb-4">
        <a
          href={`/my-cars/edit/${carId}`}
          className="flex-1 bg-blue-600 text-white text-center py-2 px-4 rounded-lg hover:bg-blue-700 transition-colors text-sm font-medium"
        >
          Edit Listing
        </a>
        <button
          onClick={() => setShowDeleteConfirm(true)}
          className="flex-1 bg-red-600 text-white py-2 px-4 rounded-lg hover:bg-red-700 transition-colors text-sm font-medium"
        >
          Delete
        </button>
      </div>

      {showDeleteConfirm && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50" onClick={() => setShowDeleteConfirm(false)}>
          <div className="bg-white p-6 rounded-lg max-w-sm mx-4" onClick={(e) => e.stopPropagation()}>
            <h3 className="text-lg font-bold mb-4">Delete Listing?</h3>
            <p className="text-gray-600 mb-6">Are you sure you want to delete this listing? This action cannot be undone.</p>
            <div className="flex gap-3">
              <button
                onClick={() => setShowDeleteConfirm(false)}
                className="flex-1 bg-gray-200 text-gray-800 py-2 px-4 rounded-lg hover:bg-gray-300 transition-colors"
              >
                Cancel
              </button>
              <button
                onClick={handleDelete}
                disabled={deleting}
                className="flex-1 bg-red-600 text-white py-2 px-4 rounded-lg hover:bg-red-700 transition-colors disabled:opacity-50"
              >
                {deleting ? 'Deleting...' : 'Delete'}
              </button>
            </div>
          </div>
        </div>
      )}
    </>
  );
}
