'use client';

interface ConfirmationModalProps {
  isOpen: boolean;
  title: string;
  message: string;
  confirmText?: string;
  cancelText?: string;
  isDanger?: boolean;
  isLoading?: boolean;
  onConfirm: () => void;
  onClose: () => void;
}

export default function ConfirmationModal({
  isOpen,
  title,
  message,
  confirmText = 'Confirm',
  cancelText = 'Cancel',
  isDanger = false,
  isLoading = false,
  onConfirm,
  onClose,
}: ConfirmationModalProps) {
  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-50 overflow-y-auto">
      {/* Backdrop with blur */}
      <div
        className="fixed inset-0 bg-gray-900/40 backdrop-blur-md transition-opacity duration-300"
        onClick={onClose}
        style={{ animation: 'fadeIn 0.3s ease-out' }}
      />

      {/* Modal */}
      <div className="flex min-h-full items-center justify-center p-4">
        <div className="relative transform overflow-hidden rounded-2xl bg-white text-left shadow-2xl transition-all duration-300 sm:my-8 sm:w-full sm:max-w-lg" style={{ animation: 'slideUp 0.3s ease-out' }}>
          {/* Header */}
          <div className="bg-white px-6 pt-6 pb-4">
            <div className="flex items-center gap-4">
              {/* Icon */}
              <div className={`mx-auto flex h-12 w-12 flex-shrink-0 items-center justify-center rounded-full ${
                isDanger
                  ? 'bg-red-100'
                  : 'bg-blue-100'
              } sm:mx-0 sm:h-10 sm:w-10`}>
                {isDanger ? (
                  <svg className="h-6 w-6 text-red-600" fill="none" viewBox="0 0 24 24" strokeWidth="1.5" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" d="M12 9v3.75m-9.303 3.379a1.5 1.5 0 0 1-1.2.9.9 2.625 2.625 0 0 1-.6.75.9 2.625 2.625 0 0 1-1.2-.9 1.5 1.5 0 0 1 .9-1.2 1.5 1.5 0 0 1 1.2-.9.9-2.625.9-2.625 0 0 1 .6-.75.9-2.625.9-2.625 0 0 1 1.2-.9 1.5-1.5 0 0 1 .9 1.2 1.5 1.5 0 0 1-1.2.9.9 2.625.9 2.625 0 0 1-.6.75.9 2.625 2.625 0 0 1-1.2-.9 1.5-1.5 0 0 1-.9 1.2-1.5 1.5 0 0 1 1.2.9-2.625 2.625 0 0 1 .6.75-2.625 2.625 0 0 1 1.2.9z" />
                    <path strokeLinecap="round" strokeLinejoin="round" d="M12 12.75h.008v.008h-.008v-.008z" />
                  </svg>
                ) : (
                  <svg className="h-6 w-6 text-blue-600" fill="none" viewBox="0 0 24 24" strokeWidth="1.5" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" d="M9 12.75L11.25 15 15 9.75M21 12a9 9 0 1 1-18 0 9 9 0 0 1 18 0z" />
                  </svg>
                )}
              </div>

              <div className="text-left">
                <h3 className="text-lg font-semibold leading-6 text-gray-900">
                  {title}
                </h3>
              </div>
            </div>

            {/* Message */}
            <div className="mt-3 ml-14">
              <p className="text-sm text-gray-600">
                {message}
              </p>
            </div>
          </div>

          {/* Actions */}
          <div className="bg-gray-50 px-6 py-4 flex flex-row-reverse gap-3">
            <button
              type="button"
              disabled={isLoading}
              onClick={onConfirm}
              className={`inline-flex justify-center rounded-lg px-4 py-2 text-sm font-semibold text-white shadow-sm ${
                isDanger
                  ? 'bg-red-600 hover:bg-red-500 disabled:bg-red-400'
                  : 'bg-blue-600 hover:bg-blue-500 disabled:bg-blue-400'
              } transition-colors`}
            >
              {isLoading ? (
                <span className="flex items-center gap-2">
                  <svg className="animate-spin h-4 w-4" viewBox="0 0 24 24">
                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" fill="none" />
                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 0 1 8-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 0 1 4 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
                  </svg>
                  Processing...
                </span>
              ) : (
                confirmText
              )}
            </button>
            <button
              type="button"
              onClick={onClose}
              className="inline-flex justify-center rounded-lg bg-white px-4 py-2 text-sm font-semibold text-gray-900 shadow-sm ring-1 ring-inset ring-gray-300 hover:bg-gray-50 transition-colors"
            >
              {cancelText}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
