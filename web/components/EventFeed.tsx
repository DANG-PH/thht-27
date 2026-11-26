import { clock } from "@/lib/derive";
import type { EvRecord } from "@/lib/types";

/** Nhật ký gần nhất từ logs/*.jsonl – cùng nguồn dữ liệu với cmd/verify. */
export function EventFeed({ records }: { records: EvRecord[] }) {
  const recent = records.slice(-60).reverse();
  return (
    <section className="card">
      <header className="card-head">
        <h2>Nhật ký sự kiện</h2>
        <p className="hint">60 bản ghi mới nhất từ logs/*.jsonl, mới nhất ở trên.</p>
      </header>
      {recent.length === 0 ? (
        <p className="empty">Chưa có bản ghi nào.</p>
      ) : (
        <div className="table-wrap feed">
          <table>
            <thead>
              <tr>
                <th>Giờ</th>
                <th>Tiến trình</th>
                <th>Trạng thái</th>
                <th>Event ID</th>
                <th>SV · học phần</th>
              </tr>
            </thead>
            <tbody>
              {recent.map((r, i) => (
                <tr key={`${r.ts}-${r.instance}-${r.event_id}-${r.status}-${i}`}>
                  <td className="mono">{clock(r.ts)}</td>
                  <td>{r.service === r.instance ? r.service : `${r.service} ${r.instance}`}</td>
                  <td>
                    <span className={`status ${r.status}`}>{r.status}</span>
                    {r.redelivered && <span className="tag">↻ giao lại</span>}
                  </td>
                  <td>
                    <code>{r.event_id}</code>
                  </td>
                  <td className="muted">{r.student_id ? `${r.student_id} · ${r.course_id}` : r.note ?? ""}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}
