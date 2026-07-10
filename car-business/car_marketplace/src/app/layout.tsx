import type { Metadata } from 'next'
import './globals.css'
import Navbar from '@/components/Navbar'
import Footer from '@/components/Footer'
import { AuthProvider } from '@/lib/auth/AuthContext'
import { NotificationProvider } from '@/lib/context/NotificationContext'

export const metadata: Metadata = {
  title: {
    default: 'Used Cars for Sale Philippines | Metro Manila Car Marketplace',
    template: '%s | Used Cars Philippines'
  },
  description: 'Find quality pre-owned cars for sale in the Philippines. Browse Toyota, Honda, Mitsubishi, Ford and more. Best deals on used cars in Metro Manila.',
  keywords: ['used cars Philippines', 'second hand cars Manila', 'car for sale Philippines', 'Toyota Vios', 'Honda Civic', 'Mitsubishi Montero', 'used cars Metro Manila'],
  authors: [{ name: 'Used Cars Philippines' }],
  creator: 'Used Cars Philippines',
  publisher: 'Used Cars Philippines',
  formatDetection: {
    email: false,
    address: false,
    telephone: false,
  },
  metadataBase: new URL('https://usedcars.ph'),
  alternates: {
    canonical: '/',
  },
  openGraph: {
    type: 'website',
    locale: 'en_PH',
    url: 'https://usedcars.ph',
    title: 'Used Cars for Sale Philippines | Metro Manila Car Marketplace',
    description: 'Find quality pre-owned cars for sale in the Philippines. Browse Toyota, Honda, Mitsubishi, Ford and more.',
    siteName: 'Used Cars Philippines',
    images: [
      {
        url: 'https://usedcars.ph/og-image.jpg',
        width: 1200,
        height: 630,
        alt: 'Used Cars Philippines Marketplace',
      },
    ],
  },
  twitter: {
    card: 'summary_large_image',
    title: 'Used Cars for Sale Philippines | Metro Manila Car Marketplace',
    description: 'Find quality pre-owned cars for sale in the Philippines.',
    images: ['https://usedcars.ph/og-image.jpg'],
  },
  robots: {
    index: true,
    follow: true,
    googleBot: {
      index: true,
      follow: true,
      'max-video-preview': -1,
      'max-image-preview': 'large',
      'max-snippet': -1,
    },
  },
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en-PH">
      <body>
        <AuthProvider>
          <NotificationProvider>
            <Navbar />
            <main className="min-h-screen">
              {children}
            </main>
            <Footer />
          </NotificationProvider>
        </AuthProvider>
      </body>
    </html>
  )
}
