"use client";

import { useEffect, useMemo, useState } from "react";
import { AnalyticsApp, FinanceApp, NotificationApp } from "@/components/apps";
import { CampusMap } from "@/components/CampusMap";
import { EventFeed } from "@/components/EventFeed";
import { FlowDiagram, type Scenario } from "@/components/FlowDiagram";
import { PubSubBoard } from "@/components/PubSubBoard";
import { RegistrationPortal } from "@/components/RegistrationPortal";
import { WorkBoard } from "@/components/WorkBoard";
import { useCatalog } from "@/lib/catalog";
import { useEventStream } from "@/lib/useEventStream";

// Mặc định gọi /api/... trên chính trang web; Next.js chuyển tiếp sang Dashboard API (app/api/*/route.ts).
const API = process.env.NEXT_PUBLIC_API_URL ?? "";
const REPO = "https://github.com/DANG-PH/thht-27";

const CONN_TEXT = {
  connecting: "Đang kết nối…",
  open: "Trực tiếp",
  error: "Mất kết nối – đang thử lại",
} as const;

interface Fault {
  title: string;
  how: string;
  vps?: string;
  local?: string;
}

const SCENARIOS: Record<Scenario, { tab: string; desc: string; observe: string[]; faults: Fault[] }> = {
  work: {
    tab: "Kịch bản (i) · Message Queue",
    desc: "1 queue, 3 Finance Worker chia tải",
    observe: [
      "Mỗi gói tin từ queue chỉ chạy tới đúng một worker – không worker nào lập trùng hóa đơn.",
      "Unacked là số thông điệp đang xử lý. Prefetch = 1 nên mỗi worker giữ tối đa một thông điệp.",
      "Bấm 12 lượt: ba worker chia nhau, mỗi hóa đơn ghi rõ worker nào lập.",
    ],
    faults: [
      {
        title: "Worker chết khi đang xử lý",
        how: "Bấm “12 lượt”, rồi ngay lập tức giết W1. Thông điệp W1 đang giữ quay lại queue và chạy sang worker khác (gói tin vàng ↻).",
        vps: "pm2 sendSignal SIGKILL thht27-W1",
        local: "scripts/stop.sh -9 W1",
      },
      {
        title: "Thông điệp sai hợp đồng",
        how: "Bấm “Gửi thông điệp lỗi”: worker từ chối, thông điệp sang dead-letter (gói tin đỏ), không thử lại vô ích.",
      },
      {
        title: "Chưa có worker nào",
        how: "Tắt cả ba worker rồi gửi: Ready của finance.work tăng dần. Bật lại: worker xử lý hết phần tồn.",
        vps: "pm2 stop thht27-W1 thht27-W2 thht27-W3",
        local: "scripts/stop.sh W1 W2 W3",
      },
    ],
  },
  pubsub: {
    tab: "Kịch bản (ii) · Publish/Subscribe",
    desc: "4 ứng dụng, mỗi bên một bản sao",
    observe: [
      "Một sự kiện tới exchange rồi tách thành bốn gói tin – mỗi queue riêng nhận một bản sao.",
      "Bốn ứng dụng xử lý độc lập: hóa đơn, hộp thư, thống kê, bản đồ trường.",
      "Academic không biết có bao nhiêu subscriber: Game được thêm sau mà không sửa Academic.",
    ],
    faults: [
      {
        title: "Subscriber tạm tắt",
        how: "Tắt Notification rồi đăng ký vài lượt: ô Notification chuyển màu, Ready của notification.events tăng. Bật lại: hộp thư nhận đủ.",
        vps: "pm2 stop thht27-notification",
        local: "scripts/stop.sh notification",
      },
      {
        title: "Thông điệp sai hợp đồng",
        how: "Bấm “Gửi thông điệp lỗi”: mỗi subscriber từ chối bản sao của mình, dead-letter tăng 4.",
      },
      {
        title: "Thêm subscriber mới",
        how: "Game là subscriber thứ tư, chỉ tự tạo queue game.events rồi bind vào exchange – commit thêm Game không đổi file nào của Academic.",
      },
    ],
  },
};

export default function Dashboard() {
  const { conn, records, queues, queuesError, version } = useEventStream(API);
  const catalog = useCatalog(API);
  const [scenario, setScenario] = useState<Scenario>("work");
  useEffect(() => {
    if (window.location.hash === "#ii") setScenario("pubsub");
  }, []);
  const choose = (s: Scenario) => {
    setScenario(s);
    window.history.replaceState(null, "", s === "pubsub" ? "#ii" : "#i");
  };

  const queueMap = useMemo(() => new Map(queues.map((q) => [q.name, q])), [queues]);
  const dlq = queueMap.get("academic.dead-letter")?.messages_ready ?? 0;
  const s = SCENARIOS[scenario];

  return (
    <div className="shell">
      <header className="topbar">
        <div className="brand">
          <span className="logo" aria-hidden="true">
            <svg viewBox="0 0 32 32">
              <rect x="5" y="9" width="5" height="14" rx="1.5" />
              <rect x="13.5" y="9" width="5" height="14" rx="1.5" opacity="0.75" />
              <rect x="22" y="9" width="5" height="14" rx="1.5" opacity="0.5" />
            </svg>
          </span>
          <div>
            <p className="eyebrow">Nhập môn Tích hợp hệ thống · Chủ đề 07</p>
            <h1>Đăng ký học phần qua RabbitMQ</h1>
          </div>
        </div>
        <div className="top-actions">
          <span className={`pill pill-${conn}`}>
            <span className="dot" />
            {CONN_TEXT[conn]}
          </span>
          <span className={dlq > 0 ? "pill pill-bad" : "pill"} title="Queue academic.dead-letter">
            Dead-letter <b>{dlq}</b>
          </span>
          <a className="toplink" href={REPO} target="_blank" rel="noreferrer">
            Mã nguồn
          </a>
          <a className="toplink" href={`${REPO}/blob/main/docs/bao-cao.md`} target="_blank" rel="noreferrer">
            Báo cáo
          </a>
        </div>
      </header>

      {queuesError && <p className="banner bad">{queuesError}</p>}

      <nav className="tabs" role="tablist" aria-label="Chọn kịch bản">
        {(["work", "pubsub"] as const).map((key) => (
          <button
            key={key}
            type="button"
            role="tab"
            aria-selected={scenario === key}
            className={scenario === key ? "tab on" : "tab"}
            onClick={() => choose(key)}
          >
            <span className="tab-title">{SCENARIOS[key].tab}</span>
            <span className="tab-desc">{SCENARIOS[key].desc}</span>
          </button>
        ))}
      </nav>

      <section className="stage">
        <div className="panel flow-panel">
          <header className="panel-head row">
            <div>
              <span className="role role-broker">RabbitMQ</span>
              <h2>Luồng thông điệp trực tiếp</h2>
            </div>
            <div className="packet-legend" aria-label="Chú thích gói tin">
              <span>
                <i className="pk pk-pub" />
                gửi
              </span>
              <span>
                <i className="pk pk-recv" />
                giao cho consumer
              </span>
              <span>
                <i className="pk pk-again" />
                giao lại
              </span>
              <span>
                <i className="pk pk-reject" />
                bị từ chối
              </span>
            </div>
          </header>
          <FlowDiagram scenario={scenario} records={records} version={version} queues={queueMap} />
          <p className="flow-note">
            Gói tin và số “đã xử lý” lấy từ nhật ký của từng ứng dụng (tức thời). Ready / Unacked / Consumer lấy từ RabbitMQ
            Management API nên chậm hơn vài giây.
          </p>
        </div>

        <aside className="panel guide">
          <div>
            <h2>Quan sát gì?</h2>
            <ol className="observe">
              {s.observe.map((o) => (
                <li key={o}>{o}</li>
              ))}
            </ol>
          </div>
          <div>
            <h2>Thử tình huống lỗi</h2>
            <ul className="faults">
              {s.faults.map((f) => (
                <li key={f.title}>
                  <b>{f.title}</b>
                  <p>{f.how}</p>
                  {f.vps && (
                    <p className="cmd">
                      <span>VPS</span>
                      <code>{f.vps}</code>
                    </p>
                  )}
                  {f.local && (
                    <p className="cmd">
                      <span>Máy cá nhân</span>
                      <code>{f.local}</code>
                    </p>
                  )}
                </li>
              ))}
            </ul>
          </div>
        </aside>
      </section>

      <section className={scenario === "work" ? "apps two" : "apps"}>
        <RegistrationPortal api={API} scenario={scenario} catalog={catalog} />
        <FinanceApp records={records} scenario={scenario} catalog={catalog} />
        {scenario === "pubsub" && (
          <>
            <NotificationApp records={records} catalog={catalog} queue={queueMap.get("notification.events")} />
            <AnalyticsApp records={records} catalog={catalog} />
          </>
        )}
      </section>

      {scenario === "pubsub" && <CampusMap records={records} />}

      <details className="evidence" open>
        <summary>
          Bằng chứng kỹ thuật <span className="muted">– cùng dữ liệu và quy tắc với cmd/verify</span>
        </summary>
        <div className="evidence-grid">
          {scenario === "work" ? <WorkBoard records={records} /> : <PubSubBoard records={records} />}
          <EventFeed records={records} />
        </div>
      </details>

      <footer className="foot">
        Dữ liệu giả lập · Trình duyệt không kết nối RabbitMQ: trang web chuyển tiếp /api sang Dashboard API (Go, chỉ đọc),
        Dashboard đọc nhật ký logs/*.jsonl và RabbitMQ Management API.
      </footer>
    </div>
  );
}
