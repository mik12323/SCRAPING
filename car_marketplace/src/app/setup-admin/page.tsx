'use client';

import { useState } from 'react';
import { useRouter } from 'next/navigation';

export default function SetupAdminPage() {
  const [username, setUsername] = useState('admin');
  const [password, setPassword] = useState('Admin123!');
  const [confirmPassword, setConfirmPassword] = useState('');
  const [phone, setPhone] = useState('+639970946623');
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState('');
  const router = useRouter();

  const handleSetup = async () => {
    setLoading(true);
    setResult('');

    // Validate passwords match
    if (password !== confirmPassword) {
      setResult('Error: Passwords do not match');
      setLoading(false);
      return;
    }

    if (password.length < 6) {
      setResult('Error: Password must be at least 6 characters');
      setLoading(false);
      return;
    }

    try {
      const response = await fetch('/api/setup-admin', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username, password, phone }),
      });

      const data = await response.json();

      if (response.ok) {
        setResult(`Success! Admin user created.
You can now login with:
Username: ${data.username}
Password: ${data.password}`);
      } else {
        setResult(`Error: ${data.error}`);
      }
    } catch (error: any) {
      setResult(`Error: ${error.message}`);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="container mx-auto px-4 py-16">
      <div className="max-w-md mx-auto bg-white p-8 rounded-xl shadow-sm">
        <h1 className="text-3xl font-bold mb-6 text-center">Setup Admin Account</h1>

        {result && (
          <div className={`p-4 rounded mb-4 ${result.includes('Success') ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-700'}`}>
            {result.split('\n').map((line, i) => (
              <div key={i}>{line}</div>
            ))}
          </div>
        )}

        <div className="mb-4">
          <label className="block text-sm font-medium mb-2">Username</label>
          <input
            type="text"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            minLength={3}
            className="w-full px-4 py-2 border border-gray-300 rounded-lg"
          />
        </div>

        <div className="mb-4">
          <label className="block text-sm font-medium mb-2">Password</label>
          <input
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            minLength={6}
            className="w-full px-4 py-2 border border-gray-300 rounded-lg"
          />
        </div>

        <div className="mb-6">
          <label className="block text-sm font-medium mb-2">Confirm Password</label>
          <input
            type="password"
            value={confirmPassword}
            onChange={(e) => setConfirmPassword(e.target.value)}
            minLength={6}
            className="w-full px-4 py-2 border border-gray-300 rounded-lg"
          />
        </div>

        <div className="mb-6">
          <label className="block text-sm font-medium mb-2">Phone</label>
          <input
            type="text"
            value={phone}
            onChange={(e) => setPhone(e.target.value)}
            className="w-full px-4 py-2 border border-gray-300 rounded-lg"
          />
        </div>

        <button
          onClick={handleSetup}
          disabled={loading}
          className="w-full bg-blue-600 text-white py-3 rounded-lg hover:bg-blue-700 disabled:bg-gray-400"
        >
          {loading ? 'Setting up...' : 'Setup Admin Account'}
        </button>

        <p className="mt-4 text-sm text-gray-600 text-center">
          This will create an admin user with username-based login.
        </p>
      </div>
    </div>
  );
}
