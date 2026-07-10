import { NextResponse, type NextRequest } from 'next/server';

// Paths that don't need session checks (static assets, images, etc.)
const PUBLIC_PATHS = [
  '/_next/',
  '/favicon.ico',
  '/images/',
  '/assets/',
  '.svg',
  '.png',
  '.jpg',
  '.jpeg',
  '.gif',
  '.webp',
  '.ico',
  '.css',
  '.js',
  '.json',
  '.woff',
  '.woff2',
  '.ttf',
  '.eot',
];

export function middleware(request: NextRequest) {
  const { pathname } = request.nextUrl;

  // Skip session checks for static assets and API routes
  const isPublicPath = PUBLIC_PATHS.some(path =>
    pathname.startsWith(path) || pathname.endsWith(path)
  );

  const isStaticImage = pathname.match(/\.(png|jpg|jpeg|gif|webp|svg|ico|avif)$/i);
  const isStaticAsset = pathname.match(/\.(css|js|woff|woff2|ttf|eot|json)$/i);

  if (isPublicPath || isStaticImage || isStaticAsset) {
    return NextResponse.next();
  }

  // For all other routes, let the request pass through
  // Session checking is handled client-side by AuthContext
  return NextResponse.next();
}

export const config = {
  matcher: [
    /*
     * Match all request paths except:
     * - _next/static (static files)
     * - _next/image (image optimization files)
     * - favicon.ico (favicon file)
     */
    '/((?!_next/static|_next/image|favicon.ico).*)',
  ],
};
