import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  images: {
    remotePatterns: [
      {
        protocol: 'https',
        hostname: 'aryqfkgyxfprcenahfmz.supabase.co',  // New Supabase project
        port: '',
        pathname: '/storage/v1/object/public/car-images/**',
      },
    ],
  },
};

export default nextConfig;