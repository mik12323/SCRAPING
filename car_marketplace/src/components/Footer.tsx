import Link from 'next/link';

export default function Footer() {
  return (
    <footer className="bg-gray-900 text-gray-300 pt-12 pb-6 mt-16">
      <div className="container mx-auto px-4">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-8 mb-8">
          <div>
            <h5 className="text-white font-bold mb-3">Used Cars Philippines</h5>
            <p className="text-sm">
              Your trusted car agent in Metro Manila and Mindanao. We connect buyers and sellers with transparency and reliability.
            </p>
          </div>

          <div>
            <h6 className="text-white font-bold mb-3">Quick Links</h6>
            <ul className="space-y-2">
              <li>
                <Link href="/" className="hover:text-white transition-colors">Home</Link>
              </li>
              <li>
                <Link href="/browse" className="hover:text-white transition-colors">Browse Cars</Link>
              </li>
              <li>
                <Link href="/sell" className="hover:text-white transition-colors">Sell Your Car</Link>
              </li>
              <li>
                <Link href="/about" className="hover:text-white transition-colors">About</Link>
              </li>
            </ul>
          </div>

          <div>
            <h6 className="text-white font-bold mb-3">Contact Us</h6>
            <a
              href="https://www.facebook.com/mikoy.dimaro/"
              target="_blank"
              rel="noopener noreferrer"
              className="inline-block bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded mb-3 transition-colors"
            >
              Facebook
            </a>
            <p className="text-sm mb-2">Call: +63 997 094 6623</p>
            <p className="text-sm">Email: mikoy.dimaro@gmail.com</p>
          </div>
        </div>

        <hr className="border-gray-700 mb-6" />

        <div className="text-center text-sm">
          <p>&copy; 2026 Used Cars Philippines. All rights reserved.</p>
        </div>
      </div>
    </footer>
  );
}
