/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  eslint: { ignoreDuringBuilds: true },
  // Les exports PDF/PPTX/XLS (jspdf / jszip / xlsx) sont rendus CÔTÉ CLIENT.
  // Ces libs référencent des modules Node (« node:… », fs, …) dans des branches
  // inutilisées au navigateur : on les neutralise côté client.
  webpack: (config, { isServer, webpack }) => {
    if (!isServer) {
      config.plugins.push(
        new webpack.NormalModuleReplacementPlugin(/^node:/, (resource) => {
          resource.request = resource.request.replace(/^node:/, "");
        }),
      );
      config.resolve.fallback = {
        ...config.resolve.fallback,
        fs: false, https: false, http: false, os: false, path: false,
        "image-size": false, stream: false, zlib: false, crypto: false,
      };
    }
    return config;
  },
};

export default nextConfig;
