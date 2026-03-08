import { fileURLToPath } from "node:url";

/** @type {import('next').NextConfig} */
const nextConfig = {
  typedRoutes: true,
  async rewrites() {
    return [
      {
        source: "/favicon.ico",
        destination: "/icon.svg"
      }
    ];
  },
  outputFileTracingRoot: fileURLToPath(new URL("../../", import.meta.url))
};

export default nextConfig;
