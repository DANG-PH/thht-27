import type { CSSProperties } from "react";
import { hue } from "@/lib/derive";
import type { EvRecord } from "@/lib/types";

const W = 760;
const H = 300;
const GATE = { x: 24, y: 282 };
const DEFAULT_COURSES = ["TH301", "CS201", "CS102", "MA101"];
const ROWS = 3;

/**
 * Bản đồ trường của Game subscriber: mỗi sự kiện Game đã xử lý → một nhân vật đi từ cổng
 * tới trước tòa nhà của học phần. Hình vẽ đơn giản, tự vẽ bằng SVG.
 */
export function CampusMap({ records }: { records: EvRecord[] }) {
  const seen = new Set<string>();
  const walkers = records
    .filter((r) => r.service === "game" && r.status === "processed" && r.course_id)
    .filter((r) => (seen.has(r.event_id) ? false : (seen.add(r.event_id), true)));

  const courses = [...DEFAULT_COURSES];
  for (const w of walkers) if (!courses.includes(w.course_id!)) courses.push(w.course_id!);

  const slot = (W - 40) / courses.length;
  const bw = Math.min(150, slot - 24);
  const cols = Math.max(1, Math.floor((bw - 8) / 22));
  const cap = cols * ROWS;
  const buildings = courses.map((c, i) => ({ course: c, x: 20 + i * slot + (slot - bw) / 2 }));

  const placed: { r: EvRecord; x: number; y: number }[] = [];
  const overflow = new Map<string, number>();
  for (const b of buildings) {
    const mine = walkers.filter((w) => w.course_id === b.course);
    const shown = mine.slice(-cap);
    overflow.set(b.course, mine.length - shown.length);
    shown.forEach((r, k) => {
      placed.push({ r, x: b.x + 14 + (k % cols) * 22, y: 172 + Math.floor(k / cols) * 34 });
    });
  }

  return (
    <section className="card">
      <header className="card-head">
        <h2>Game subscriber · bản đồ trường</h2>
        <p className="hint">
          Subscriber thứ tư, thêm vào sau mà <b>không sửa Academic</b>: tự tạo queue <code>game.events</code> và bind vào
          exchange. Mỗi sự kiện → một nhân vật tới trước tòa nhà học phần.
        </p>
      </header>
      {walkers.length === 0 && (
        <p className="empty">
          Chưa có nhân vật nào. Chạy <code>go run ./cmd/game-sub</code> rồi gửi sự kiện ở kịch bản (ii).
        </p>
      )}
      <svg className="campus" viewBox={`0 0 ${W} ${H}`} role="img" aria-label="Bản đồ trường">
        <rect x={0} y={0} width={W} height={H} className="ground" />
        <path d={`M ${GATE.x} ${GATE.y} L ${W - 20} ${GATE.y}`} className="road" />
        <g transform={`translate(${GATE.x} ${GATE.y})`}>
          <rect x={-14} y={-26} width={6} height={26} className="gate" />
          <rect x={8} y={-26} width={6} height={26} className="gate" />
          <text y={14} textAnchor="middle" className="map-label">
            Cổng
          </text>
        </g>
        {buildings.map((b) => (
          <g key={b.course} transform={`translate(${b.x} 0)`}>
            <polygon points={`-6,52 ${bw / 2},22 ${bw + 6},52`} className="roof" />
            <rect x={0} y={52} width={bw} height={90} className="building" />
            {[0, 1, 2].map((row) =>
              [0, 1, 2, 3].map((col) => (
                <rect
                  key={`${row}-${col}`}
                  x={12 + col * ((bw - 24) / 4)}
                  y={62 + row * 20}
                  width={(bw - 24) / 4 - 8}
                  height={11}
                  className="window"
                />
              )),
            )}
            <rect x={bw / 2 - 10} y={122} width={20} height={20} className="door" />
            <text x={bw / 2} y={44} textAnchor="middle" className="map-title">
              {b.course}
            </text>
            {(overflow.get(b.course) ?? 0) > 0 && (
              <text x={bw} y={160} textAnchor="end" className="map-label">
                +{overflow.get(b.course)}
              </text>
            )}
          </g>
        ))}
        {placed.map(({ r, x, y }) => {
          const h = hue(r.student_id ?? r.event_id);
          const style = { "--fx": `${GATE.x - x}px`, "--fy": `${GATE.y - y}px` } as CSSProperties;
          return (
            <g key={r.event_id} transform={`translate(${x} ${y})`}>
              <g className="walker" style={style}>
                <title>{`${r.student_id} → ${r.course_id} (${r.event_id})`}</title>
                <circle cy={-17} r={5} fill={`hsl(${h} 65% 58%)`} />
                <rect x={-5} y={-11} width={10} height={13} rx={3} fill={`hsl(${h} 55% 42%)`} />
                <text y={11} textAnchor="middle" className="walker-label">
                  {r.student_id}
                </text>
              </g>
            </g>
          );
        })}
      </svg>
    </section>
  );
}
