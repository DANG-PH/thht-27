// Các "ứng dụng" phía nhận, dựng lại từ nhật ký của chính chúng (logs/*.jsonl):
// Finance → hóa đơn học phí, Notification → hộp thư sinh viên, Analytics → thống kê.

import { type Catalog, courseOf, feeOf, initials, money, studentName } from "@/lib/catalog";
import { clock, hue } from "@/lib/derive";
import type { EvRecord, QueueInfo } from "@/lib/types";
import type { Scenario } from "./FlowDiagram";

/** Bản ghi "processed" của một ứng dụng, mỗi sự kiện một lần, mới nhất trước. */
function processedBy(records: EvRecord[], scenario: Scenario, match: (r: EvRecord) => boolean): EvRecord[] {
  const seen = new Set<string>();
  const out: EvRecord[] = [];
  for (let i = records.length - 1; i >= 0; i--) {
    const r = records[i];
    if (r.scenario !== scenario || r.status !== "processed" || !match(r) || seen.has(r.event_id)) continue;
    seen.add(r.event_id);
    out.push(r);
  }
  return out;
}

function invoiceNo(eventId: string): string {
  const [, run, seq] = eventId.split("-");
  return `HĐ-${(run ?? "").toUpperCase()}-${seq}`;
}

export function FinanceApp({ records, scenario, catalog }: { records: EvRecord[]; scenario: Scenario; catalog: Catalog | null }) {
  const rows = processedBy(records, scenario, (r) =>
    scenario === "work" ? r.service === "finance-worker" : r.service === "finance",
  );
  const total = rows.reduce((s, r) => s + feeOf(catalog, r.course_id), 0);
  return (
    <section className="panel">
      <header className="panel-head">
        <span className="role role-consumer">{scenario === "work" ? "Finance Worker × 3" : "Finance · Subscriber"}</span>
        <h2>Hóa đơn học phí</h2>
        <p className="hint">
          {scenario === "work"
            ? "Ba worker cùng lập hóa đơn từ một queue; mỗi hóa đơn ghi rõ worker nào lập."
            : "Finance nhận bản sao của mỗi sự kiện và lập nghĩa vụ học phí."}
        </p>
      </header>
      <div className="kpis">
        <div className="kpi">
          <span className="kpi-label">Số hóa đơn</span>
          <span className="kpi-value">{rows.length}</span>
        </div>
        <div className="kpi">
          <span className="kpi-label">Tổng phải thu</span>
          <span className="kpi-value money">{money(total)}</span>
        </div>
      </div>
      {rows.length === 0 ? (
        <p className="empty">Chưa có hóa đơn. Đăng ký một học phần ở cổng bên cạnh.</p>
      ) : (
        <ul className="invoices">
          {rows.slice(0, 7).map((r) => {
            const course = courseOf(catalog, r.course_id);
            return (
              <li key={r.event_id} className="invoice">
                <div className="invoice-main">
                  <span className="invoice-no">{invoiceNo(r.event_id)}</span>
                  <span className="invoice-who">
                    {studentName(catalog, r.student_id)} · {course?.name ?? r.course_id}
                  </span>
                </div>
                <div className="invoice-side">
                  <span className="invoice-amount">{money(feeOf(catalog, r.course_id))}</span>
                  <span className="invoice-meta">
                    {scenario === "work" && <span className="tag">{r.instance}</span>}
                    {r.redelivered && <span className="tag tag-warn">↻ giao lại</span>}
                    <span className="tag tag-pending">Chờ thanh toán</span>
                  </span>
                </div>
              </li>
            );
          })}
        </ul>
      )}
    </section>
  );
}

export function NotificationApp({
  records,
  catalog,
  queue,
}: {
  records: EvRecord[];
  catalog: Catalog | null;
  queue?: QueueInfo;
}) {
  const rows = processedBy(records, "pubsub", (r) => r.service === "notification");
  const off = queue ? queue.consumers === 0 : false;
  return (
    <section className="panel">
      <header className="panel-head">
        <span className="role role-consumer">Notification · Subscriber</span>
        <h2>Hộp thư sinh viên</h2>
        <p className="hint">Thông báo xác nhận gửi cho sinh viên (giả lập, không gửi thật).</p>
      </header>
      {off && (
        <p className="banner warn">
          Notification đang tắt – <b>{queue?.messages_ready ?? 0}</b> thông báo đang chờ trong queue{" "}
          <code>notification.events</code>. Bật lại sẽ gửi đủ.
        </p>
      )}
      {rows.length === 0 ? (
        <p className="empty">Chưa có thông báo nào.</p>
      ) : (
        <ul className="inbox">
          {rows.slice(0, 6).map((r) => {
            const name = studentName(catalog, r.student_id);
            const course = courseOf(catalog, r.course_id);
            return (
              <li key={r.event_id} className="mail">
                <span className="avatar" style={{ background: `hsl(${hue(r.student_id ?? "")} 55% 42%)` }}>
                  {initials(name)}
                </span>
                <div className="mail-body">
                  <div className="mail-top">
                    <b>{name}</b>
                    <span className="muted small mono">{clock(r.ts).slice(0, 8)}</span>
                  </div>
                  <p>
                    Đăng ký <b>{course?.name ?? r.course_id}</b> thành công ({course?.credits ?? "?"} tín chỉ). Học phí{" "}
                    {money(feeOf(catalog, r.course_id))}.
                  </p>
                </div>
              </li>
            );
          })}
        </ul>
      )}
    </section>
  );
}

export function AnalyticsApp({ records, catalog }: { records: EvRecord[]; catalog: Catalog | null }) {
  const rows = processedBy(records, "pubsub", (r) => r.service === "analytics");
  const perCourse = new Map<string, number>();
  for (const r of rows) if (r.course_id) perCourse.set(r.course_id, (perCourse.get(r.course_id) ?? 0) + 1);
  const credits = rows.reduce((s, r) => s + (courseOf(catalog, r.course_id)?.credits ?? 0), 0);
  const revenue = rows.reduce((s, r) => s + feeOf(catalog, r.course_id), 0);
  const courses = catalog?.courses.map((c) => c.id) ?? [...perCourse.keys()].sort();
  const max = Math.max(1, ...perCourse.values());
  return (
    <section className="panel">
      <header className="panel-head">
        <span className="role role-consumer">Analytics · Subscriber</span>
        <h2>Thống kê đăng ký</h2>
        <p className="hint">Số liệu dẫn xuất từ luồng sự kiện mà Analytics đã nhận.</p>
      </header>
      <div className="kpis three">
        <div className="kpi">
          <span className="kpi-label">Lượt đăng ký</span>
          <span className="kpi-value">{rows.length}</span>
        </div>
        <div className="kpi">
          <span className="kpi-label">Tín chỉ</span>
          <span className="kpi-value">{credits}</span>
        </div>
        <div className="kpi">
          <span className="kpi-label">Học phí dự kiến</span>
          <span className="kpi-value money">{money(revenue)}</span>
        </div>
      </div>
      <div className="bars">
        {courses.map((id) => {
          const n = perCourse.get(id) ?? 0;
          return (
            <div key={id} className="bar-row">
              <span className="bar-label">
                <code>{id}</code>
                <span className="muted small">{courseOf(catalog, id)?.name}</span>
              </span>
              <div className="bar">
                <div className="bar-fill" style={{ width: `${(n / max) * 100}%` }} />
              </div>
              <b className="bar-value">{n}</b>
            </div>
          );
        })}
      </div>
    </section>
  );
}
