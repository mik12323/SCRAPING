import Link from 'next/link';
import type { Metadata } from 'next';

export const metadata: Metadata = {
  title: 'About Us | Used Cars Philippines',
  description: 'Learn about Used Cars Philippines - your trusted partner for buying and selling pre-owned vehicles in Metro Manila and Mindanao. 2+ years of experience in the used car market.',
  keywords: ['about used cars Philippines', 'car agent Manila', 'used car marketplace about', 'Metro Manila car dealer'],
};

export default function AboutPage() {
  return (
    <div className="container mx-auto px-4 py-16">
      <div className="max-w-3xl mx-auto">
        <h1 className="text-4xl font-bold mb-8 text-center">About Used Cars Philippines</h1>

        <div className="bg-white p-8 rounded-xl shadow-sm mb-8">
          <h2 className="text-2xl font-bold mb-4">Our Mission</h2>
          <p className="text-gray-700 mb-4">
            Used Cars Philippines is your trusted partner in buying and selling pre-owned vehicles in Metro Manila and Mindanao. We act as an agent connecting car owners with potential buyers, ensuring transparent and reliable transactions.
          </p>
          <p className="text-gray-700">
            With over 2 years in the used car market, we&apos;ve helped hundreds of clients find the right car at the right price, or sell their vehicle quickly and fairly.
          </p>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
          <div className="bg-white p-6 rounded-xl shadow-sm text-center">
            <div className="text-4xl mb-3">📄</div>
            <h3 className="font-bold text-lg mb-2">Transparent Process</h3>
            <p className="text-gray-600 text-sm">
              We connect buyers directly with car owners. All listings include clear pricing so you know exactly what you&apos;re getting.
            </p>
          </div>

          <div className="bg-white p-6 rounded-xl shadow-sm text-center">
            <div className="text-4xl mb-3">🏆</div>
            <h3 className="font-bold text-lg mb-2">2+ Years Experience</h3>
            <p className="text-gray-600 text-sm">
              With over 5 years in the used car market and 2 years as a dedicated agent, we know what to look for and how to get you the best deal.
            </p>
          </div>

          <div className="bg-white p-6 rounded-xl shadow-sm text-center">
            <div className="text-4xl mb-3">📍</div>
            <h3 className="font-bold text-lg mb-2">Metro Manila & Mindanao</h3>
            <p className="text-gray-600 text-sm">
              We cover deals across Metro Manila and can also coordinate transactions in Mindanao. Location is not a barrier.
            </p>
          </div>
        </div>

        <div className="bg-white p-8 rounded-xl shadow-sm mb-8">
          <h2 className="text-2xl font-bold mb-4">How We Work</h2>
          <div className="space-y-6">
            <div className="flex gap-4">
              <div className="shrink-0 w-10 h-10 bg-blue-600 text-white rounded-full flex items-center justify-center font-bold">
                1
              </div>
              <div>
                <h3 className="font-bold mb-1">Browse Cars</h3>
                <p className="text-gray-600">
                  Search through our listings of verified pre-owned cars. Filter by brand, model, price, or payment type.
                </p>
              </div>
            </div>

            <div className="flex gap-4">
              <div className="shrink-0 w-10 h-10 bg-blue-600 text-white rounded-full flex items-center justify-center font-bold">
                2
              </div>
              <div>
                <h3 className="font-bold mb-1">Contact Us</h3>
                <p className="text-gray-600">
                  Found a car you like? Reach out via Facebook Messenger or give us a call. We&apos;ll connect you with the owner and negotiate on your behalf.
                </p>
              </div>
            </div>

            <div className="flex gap-4">
              <div className="shrink-0 w-10 h-10 bg-blue-600 text-white rounded-full flex items-center justify-center font-bold">
                3
              </div>
              <div>
                <h3 className="font-bold mb-1">Close the Deal</h3>
                <p className="text-gray-600">
                  We can be present at the deal or help coordinate remotely. Either way, we make sure the transaction goes smoothly.
                </p>
              </div>
            </div>
          </div>
        </div>

        <div className="bg-white p-8 rounded-xl shadow-sm text-center">
          <h2 className="text-2xl font-bold mb-4">Get in Touch</h2>
          <p className="text-gray-700 mb-6">
            Ready to buy or sell a car? Contact us today!
          </p>
          <div className="flex justify-center gap-4 flex-wrap">
            <a
              href="https://www.facebook.com/mikoy.dimaro/"
              target="_blank"
              rel="noopener noreferrer"
              className="inline-block bg-blue-600 hover:bg-blue-700 text-white px-6 py-3 rounded-lg transition-colors"
            >
              Message on Facebook
            </a>
            <a
              href="tel:+639970946623"
              className="inline-block bg-green-600 hover:bg-green-700 text-white px-6 py-3 rounded-lg transition-colors"
            >
              Call: +63 997 094 6623
            </a>
            <Link
              href="/browse"
              className="inline-block bg-blue-600 hover:bg-blue-700 text-white px-6 py-3 rounded-lg transition-colors"
            >
              Browse Cars
            </Link>
          </div>
        </div>
      </div>
    </div>
  );
}
