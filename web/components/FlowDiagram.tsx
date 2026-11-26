"use client";

// Sơ đồ luồng thông điệp theo thời gian thực: mỗi bản ghi MỚI trong nhật ký sinh ra một "gói tin"
// chạy trên đường tương ứng (Academic → broker, broker → consumer, queue → dead-letter).
// Số liệu trên các ô lấy từ nhật ký (đã xử lý) và từ RabbitMQ Management API (Ready / Unacked / Consumer).

import { useEffect, useMemo, useRef, useState } from "react";
import type { EvRecord, QueueInfo } from "@/lib/types";

export type Scenario = "work" | "pubsub";

type Pt = readonly [number, number];
interface Box {
  x: number;
  y: number;
  w: number;
  h: number;
}
const L = (b: Box): Pt => [b.x, b.y + b.h / 2];
const R = (b: Box): Pt => [b.x + b.w, b.y + b.h / 2];
const T = (b: Box): Pt => [b.x + b.w / 2, b.y];
const B = (b: Box): Pt => [b.x + b.w / 2, b.y + b.h];
const line = (a: Pt, b: Pt) => `M ${a[0]} ${a[1]} L ${b[0]} ${b[1]}`;
const curve = (a: Pt, b: Pt) => {
  const dx = (b[0] - a[0]) / 2;
  return `M ${a[0]} ${a[1]} C ${a[0] + dx} ${a[1]}, ${b[0] - dx} ${b[1]}, ${b[0]} ${b[1]}`;
};

const DEFAULT_WORKERS = ["W1", "W2", "W3"];
const DEFAULT_SUBS = ["finance", "notification", "analytics", "game"];
const SUB_LABEL: Record<string, string> = {
  finance: "Finance",
  notification: "Notification",
  analytics: "Analytics",
  game: "Game",
};

type PacketKind = "pub" | "recv" | "again" | "reject";
interface Packet {
  id: number;
  path: string;
  kind: PacketKind;
}

interface Layout {
  W: number;
  H: number;
  academic: Box;
  broker: Box; // queue finance.work hoặc exchange academic.events
  dlq: Box;
  consumers: { name: string; queue?: Box; box: Box }[];
}

function layoutWork(workers: string[]): Layout {
  const rowH = 96;
  const H = Math.max(330, workers.length * rowH + 30);
  const mid = H / 2;
  return {
    W: 1000,
    H,
    academic: { x: 16, y: mid - 50, w: 190, h: 100 },
    broker: { x: 310, y: mid - 70, w: 250, h: 120 },
    dlq: { x: 330, y: H - 66, w: 210, h: 52 },
    consumers: workers.map((name, i) => ({
      name,
      box: { x: 760, y: 15 + i * rowH + (H - 30 - workers.length * rowH) / 2, w: 224, h: 80 },
    })),
  };
}

function layoutPubSub(subs: string[]): Layout {
  const rowH = 92;
  const H = Math.max(380, subs.length * rowH + 30);
  const mid = H / 2;
  const top = (H - subs.length * rowH) / 2;
  return {
    W: 1000,
    H,
    academic: { x: 16, y: mid - 50, w: 190, h: 100 },
    broker: { x: 250, y: mid - 48, w: 180, h: 96 },
    dlq: { x: 230, y: H - 62, w: 220, h: 50 },
    consumers: subs.map((name, i) => ({
      name,
      queue: { x: 480, y: top + i * rowH + 10, w: 230, h: 66 },
      box: { x: 760, y: top + i * rowH + 10, w: 224, h: 66 },
    })),
  };
}

/** Đường đi của gói tin cho một bản ghi, hoặc null nếu bản ghi không cần vẽ. */
function pathFor(r: EvRecord, scenario: Scenario, lay: Layout): { path: string; kind: PacketKind } | null {
  if (r.scenario !== scenario) return null;
  if (r.service === "academic") {
    return r.status === "published" ? { path: line(R(lay.academic), L(lay.broker)), kind: "pub" } : null;
  }
  const key = scenario === "work" ? r.instance : r.service;
  const c = lay.consumers.find((x) => x.name === key);
  if (!c) return null;
  if (r.status === "received") {
    if (scenario === "work") {
      return { path: curve(R(lay.broker), L(c.box)), kind: r.redelivered ? "again" : "recv" };
    }
    const q = c.queue!;
    return {
      path: `${curve(R(lay.broker), L(q))} L ${R(q)[0]} ${R(q)[1]} L ${L(c.box)[0]} ${L(c.box)[1]}`,
      kind: r.redelivered ? "again" : "recv",
    };
  }
  if (r.status === "rejected" || r.status === "dead-lettered") {
    const from = scenario === "work" ? B(lay.broker) : L(c.queue!);
    return { path: line(from, T(lay.dlq)), kind: "reject" };
  }
  return null;
}

let packetSeq = 0;

export function FlowDiagram({
  scenario,
  records,
  version,
  queues,
}: {
  scenario: Scenario;
  records: EvRecord[];
  version: number;
  queues: Map<string, QueueInfo>;
}) {
  // Các consumer có trên sơ đồ: mặc định + những cái đã xuất hiện trong nhật ký.
  const names = useMemo(() => {
    const seen = new Set(scenario === "work" ? DEFAULT_WORKERS : DEFAULT_SUBS);
    for (const r of records) {
      if (r.scenario !== scenario || r.service === "academic") continue;
      if (scenario === "work" && r.service === "finance-worker") seen.add(r.instance);
      if (scenario === "pubsub") seen.add(r.service);
    }
    const order = scenario === "work" ? DEFAULT_WORKERS : DEFAULT_SUBS;
    return [...seen].sort((a, b) => {
      const ia = order.indexOf(a);
      const ib = order.indexOf(b);
      return (ia < 0 ? 99 : ia) - (ib < 0 ? 99 : ib) || a.localeCompare(b);
    });
  }, [records, scenario]);
  const lay = useMemo(() => (scenario === "work" ? layoutWork(names) : layoutPubSub(names)), [scenario, names]);

  // ---- Số liệu hiển thị trên các ô ----
  const stats = useMemo(() => {
    let published = 0;
    const done = new Map<string, number>();
    const last = new Map<string, EvRecord>();
    const doneBy = new Map<string, string>(); // event_id → consumer đã xử lý xong (kịch bản i)
    for (const r of records) {
      if (r.scenario !== scenario) continue;
      if (r.service === "academic") {
        if (r.status === "published") published++;
        continue;
      }
      const key = scenario === "work" ? r.instance : r.service;
      last.set(key, r);
      if (r.status === "processed") {
        done.set(key, (done.get(key) ?? 0) + 1);
        doneBy.set(r.event_id, key);
      }
    }
    return { published, done, last, doneBy };
  }, [records, scenario]);

  // ---- Gói tin: chỉ cho bản ghi mới tới sau khi đã có lịch sử ----
  const [packets, setPackets] = useState<Packet[]>([]);
  const seen = useRef<{ key: string; last?: EvRecord }>({ key: "" });
  const timers = useRef<ReturnType<typeof setTimeout>[]>([]);
  useEffect(() => {
    const key = `${version}-${scenario}`;
    if (seen.current.key !== key) {
      seen.current = { key, last: records[records.length - 1] };
      setPackets([]);
      return;
    }
    const idx = seen.current.last ? records.lastIndexOf(seen.current.last) : -1;
    const fresh = records.slice(idx + 1).slice(-40);
    seen.current.last = records[records.length - 1];
    fresh.forEach((r, i) => {
      const p = pathFor(r, scenario, lay);
      if (!p) return;
      const packet: Packet = { id: ++packetSeq, ...p };
      // Rải các gói tin cách nhau một chút để nhìn ra từng thông điệp.
      timers.current.push(setTimeout(() => setPackets((ps) => [...ps, packet].slice(-80)), i * 70));
      timers.current.push(setTimeout(() => setPackets((ps) => ps.filter((x) => x.id !== packet.id)), i * 70 + 1100));
    });
  }, [records, version, scenario, lay]);
  useEffect(() => () => timers.current.forEach(clearTimeout), []);

  const brokerQueue = queues.get("finance.work");
  const dlq = queues.get("academic.dead-letter");

  return (
    <div className="flow-wrap">
      <svg className="flow" viewBox={`0 0 ${lay.W} ${lay.H}`} role="img" aria-label="Sơ đồ luồng thông điệp">
        <defs>
          <marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto">
            <path d="M 0 0 L 10 5 L 0 10 z" className="arrow-head" />
          </marker>
        </defs>

        {/* ---- Đường nối ---- */}
        <path d={line(R(lay.academic), L(lay.broker))} className="edge" markerEnd="url(#arrow)" />
        {lay.consumers.map((c) =>
          scenario === "work" ? (
            <path key={c.name} d={curve(R(lay.broker), L(c.box))} className="edge" markerEnd="url(#arrow)" />
          ) : (
            <g key={c.name}>
              <path d={curve(R(lay.broker), L(c.queue!))} className="edge" markerEnd="url(#arrow)" />
              <path d={line(R(c.queue!), L(c.box))} className="edge" markerEnd="url(#arrow)" />
            </g>
          ),
        )}
        {scenario === "work" && <path d={line(B(lay.broker), T(lay.dlq))} className="edge edge-dlq" />}

        {/* ---- Academic ---- */}
        <NodeBox box={lay.academic} kind="producer" title="Academic" tag="PRODUCER"
          sub={scenario === "work" ? "key: finance.work" : "key: student.enrolled"}>
          <tspan className="node-big">{stats.published}</tspan>
          <tspan className="node-dim"> sự kiện đã gửi</tspan>
        </NodeBox>

        {/* ---- Broker ---- */}
        {scenario === "work" ? (
          <QueueBox box={lay.broker} name="finance.work" sub="queue · durable · 1 queue chung" q={brokerQueue} big />
        ) : (
          <ExchangeShape box={lay.broker} />
        )}

        {/* ---- Consumers ---- */}
        {lay.consumers.map((c) => {
          const lastRec = stats.last.get(c.name);
          const holding = lastRec?.status === "received" ? lastRec.event_id : undefined;
          // Sự kiện tiến trình này đang giữ đã được consumer KHÁC xử lý xong → tiến trình này đã chết trước khi ack.
          const takenBy = holding ? stats.doneBy.get(holding) : undefined;
          const lost = takenBy !== undefined && takenBy !== c.name;
          const busy = holding !== undefined && !lost;
          if (scenario === "work") {
            const seq = holding?.split("-")[2];
            return (
              <NodeBox key={c.name} box={c.box} kind="consumer" title={`Finance Worker ${c.name}`}
                sub={lost ? `dừng? #${seq} đã chuyển cho ${takenBy}` : "consumer · queue chung"}
                state={lost ? "off" : busy ? "busy" : undefined}>
                <tspan className="node-big">{stats.done.get(c.name) ?? 0}</tspan>
                <tspan className="node-dim"> đã xử lý{busy ? ` · đang làm #${seq}` : ""}</tspan>
              </NodeBox>
            );
          }
          const queueName = `${c.name}.events`;
          const q = queues.get(queueName);
          const off = q ? q.consumers === 0 : false;
          return (
            <g key={c.name}>
              <QueueBox box={c.queue!} name={queueName} sub="queue riêng" q={q} />
              <NodeBox box={c.box} kind="consumer" title={SUB_LABEL[c.name] ?? c.name}
                tag={off ? "ĐANG TẮT" : "SUBSCRIBER"} state={off ? "off" : busy ? "busy" : undefined}>
                <tspan className="node-big">{stats.done.get(c.name) ?? 0}</tspan>
                <tspan className="node-dim"> đã xử lý</tspan>
              </NodeBox>
            </g>
          );
        })}

        {/* ---- Dead-letter ---- */}
        <NodeBox box={lay.dlq} kind="dlq" title="academic.dead-letter" compact>
          <tspan className="node-dim">thông điệp bị từ chối: </tspan>
          <tspan className="node-strong">{dlq?.messages_ready ?? 0}</tspan>
        </NodeBox>

        {/* ---- Gói tin đang chạy ---- */}
        {packets.map((p) => (
          <circle key={p.id} r={p.kind === "pub" ? 6 : 5.5} className={`packet packet-${p.kind}`}>
            <animateMotion
              dur="0.9s"
              fill="freeze"
              path={p.path}
              begin="indefinite"
              ref={(el) => {
                // Bắt đầu chạy đúng lúc gói tin xuất hiện (một lần duy nhất).
                if (el && !el.dataset.started) {
                  el.dataset.started = "1";
                  (el as unknown as SVGAnimationElement).beginElement();
                }
              }}
            />
          </circle>
        ))}
      </svg>
    </div>
  );
}

function NodeBox({
  box,
  kind,
  title,
  tag,
  sub,
  state,
  compact,
  children,
}: {
  box: Box;
  kind: "producer" | "consumer" | "dlq";
  title: string;
  tag?: string;
  sub?: string;
  state?: "busy" | "off";
  compact?: boolean;
  children?: React.ReactNode;
}) {
  return (
    <g transform={`translate(${box.x} ${box.y})`} className={`node node-${kind}${state ? ` is-${state}` : ""}`}>
      <rect width={box.w} height={box.h} rx={12} className="node-bg" />
      <rect x={0} y={10} width={4} height={box.h - 20} rx={2} className="node-accent" />
      <text x={16} y={compact ? 21 : 26} className={compact ? "node-title mono-title small-title" : "node-title"}>
        {title}
      </text>
      {tag && (
        <text x={box.w - 12} y={22} textAnchor="end" className="node-tag">
          {tag}
        </text>
      )}
      {sub && (
        <text x={16} y={45} className="node-sub">
          {sub}
        </text>
      )}
      <text x={16} y={compact ? 40 : sub ? box.h - 16 : box.h - 14} className="node-line">
        {children}
      </text>
    </g>
  );
}

/** Ô queue: số Ready (ô đặc) và Unacked (ô viền) vẽ thành các khe như một hàng đợi. */
function QueueBox({ box, name, sub, q, big }: { box: Box; name: string; sub: string; q?: QueueInfo; big?: boolean }) {
  const ready = q?.messages_ready ?? 0;
  const unacked = q?.messages_unacknowledged ?? 0;
  const slots = big ? 12 : 8;
  const slotW = (box.w - 32 - (slots - 1) * 4) / slots;
  const cells = Array.from({ length: slots }, (_, i) => (i < unacked ? "unacked" : i < unacked + ready ? "ready" : "empty"));
  return (
    <g transform={`translate(${box.x} ${box.y})`} className="node node-broker">
      <rect width={box.w} height={box.h} rx={12} className="node-bg" />
      <rect x={0} y={10} width={4} height={box.h - 20} rx={2} className="node-accent" />
      <text x={16} y={22} className="node-title mono-title">
        {name}
      </text>
      {big && (
        <text x={box.w - 12} y={22} textAnchor="end" className="node-tag">
          BROKER
        </text>
      )}
      {big && (
        <text x={16} y={40} className="node-sub">
          {sub}
        </text>
      )}
      {cells.map((c, i) => (
        <rect key={i} x={16 + i * (slotW + 4)} y={big ? 54 : 29} width={slotW} height={big ? 22 : 11} rx={3} className={`slot slot-${c}`} />
      ))}
      <text x={16} y={box.h - (big ? 12 : 9)} className="node-line">
        <tspan className="node-dim">Ready </tspan>
        <tspan className="node-strong">{ready}</tspan>
        <tspan className="node-dim"> · Unacked </tspan>
        <tspan className="node-strong">{unacked}</tspan>
        <tspan className="node-dim"> · Consumer </tspan>
        <tspan className={q && q.consumers === 0 ? "node-warn" : "node-strong"}>{q?.consumers ?? "–"}</tspan>
      </text>
    </g>
  );
}

function ExchangeShape({ box }: { box: Box }) {
  const { x, y, w, h } = box;
  const k = 22;
  const pts = `${x + k},${y} ${x + w - k},${y} ${x + w},${y + h / 2} ${x + w - k},${y + h} ${x + k},${y + h} ${x},${y + h / 2}`;
  return (
    <g className="node node-broker">
      <polygon points={pts} className="node-bg" />
      <text x={x + w / 2} y={y + h / 2 - 4} textAnchor="middle" className="node-title mono-title">
        academic.events
      </text>
      <text x={x + w / 2} y={y + h / 2 + 18} textAnchor="middle" className="node-sub">
        exchange · topic
      </text>
    </g>
  );
}
