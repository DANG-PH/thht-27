"use client";

import { useMemo } from "react";
import { CampusMap } from "@/components/CampusMap";
import { EventFeed } from "@/components/EventFeed";
import { PubSubBoard } from "@/components/PubSubBoard";
import { SendPanel } from "@/components/SendPanel";
import { WorkBoard } from "@/components/WorkBoard";
import { useEventStream } from "@/lib/useEventStream";

// Mặc định gọi /api/... trên chính trang web; Next.js chuyển tiếp sang Dashboard API (app/api/*/route.ts).
const API = process.env.NEXT_PUBLIC_API_URL ?? "";

const CONN_TEXT = {
  connecting: "Đang kết nối Dashboard API…",
  open: "Trực tiếp",
  error: "Mất kết nối Dashboard API – đang thử lại",
} as const;

export default function Dashboard() {
  const { conn, records, queues, queuesError } = useEventStream(API);
  const queueMap = useMemo(() => new Map(queues.map((q) => [q.name, q])), [queues]);
  const dlq = queueMap.get("academic.dead-letter");

  return (
    <main className="page">
      <header className="top">
        <div>
          <p className="eyebrow">Nhập môn Tích hợp hệ thống · Chủ đề 07</p>
          <h1>Message Queue và Publish/Subscribe với RabbitMQ</h1>
        </div>
        <div className="top-status">
          <span className={`pill ${conn}`}>{CONN_TEXT[conn]}</span>
          <span className={dlq && dlq.messages_ready > 0 ? "pill dlq" : "pill"} title="academic.dead-letter">
            Dead-letter: <b>{dlq?.messages_ready ?? 0}</b>
          </span>
        </div>
      </header>
      {queuesError && <p className="error banner">{queuesError}</p>}
      <SendPanel api={API} />
      <div className="grid">
        <WorkBoard records={records} queues={queueMap} />
        <PubSubBoard records={records} queues={queueMap} />
      </div>
      <CampusMap records={records} />
      <EventFeed records={records} />
      <footer className="foot">
        Dữ liệu giả lập · Trình duyệt không kết nối RabbitMQ: trang web chuyển tiếp /api sang Dashboard API, Dashboard đọc
        logs/*.jsonl và RabbitMQ Management API.
      </footer>
    </main>
  );
}
