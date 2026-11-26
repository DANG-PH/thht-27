import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  reactStrictMode: true,
  // Bản build tự chứa (server.js + node_modules tối thiểu): chép sang VPS là chạy, không cần npm install ở đó.
  output: "standalone",
  // Tắt nén: nén gzip làm luồng Server-Sent Events bị dồn lại, trình duyệt nhận chậm.
  compress: false,
};

export default nextConfig;
