import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Chủ đề 07 · Message Queue & Pub/Sub",
  description: "Demo tích hợp Academic – Finance – Notification – Analytics qua RabbitMQ",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="vi">
      <body>{children}</body>
    </html>
  );
}
