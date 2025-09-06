import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  reactStrictMode: true,

  // PENTING: karena app diakses di http://...:8080/app/
  basePath: "/app",
};

export default nextConfig;
