import type { Metadata } from "next";
import { Be_Vietnam_Pro } from "next/font/google";
import "./globals.css";

// Phông thiết kế cho tiếng Việt: dấu thanh rõ, không bị dính dòng.
const sans = Be_Vietnam_Pro({
  subsets: ["latin", "vietnamese"],
  weight: ["400", "500", "600", "700"],
  variable: "--font-sans",
  display: "swap",
});

export const metadata: Metadata = {
  title: "Chủ đề 07 · Đăng ký học phần qua RabbitMQ",
  description: "Demo Message Queue và Publish/Subscribe: Academic – Finance – Notification – Analytics qua RabbitMQ",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="vi" className={sans.variable}>
      <body>{children}</body>
    </html>
  );
}
