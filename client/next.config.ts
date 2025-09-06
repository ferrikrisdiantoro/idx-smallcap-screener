import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  reactStrictMode: true,

  // >>> PENTING: sesuaikan dengan prefix tempat app kamu di-hosting
  // Jika Apache/Nginx mem-proxy di /app, set basePath ke "/app"
  basePath: "/app",

  // Jangan set assetPrefix kalau proxy sudah meneruskan /app/_next/*
  // assetPrefix: "/app", // HANYA kalau proxy/CDN kamu butuh ini
};

export default nextConfig;
