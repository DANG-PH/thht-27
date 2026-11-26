import { latestRun, subscribers, trailsOfRun } from "@/lib/derive";
import type { EvRecord, QueueInfo } from "@/lib/types";

/** Kịch bản (ii): mỗi subscriber một queue riêng; ma trận sự kiện × subscriber giống cmd/verify. */
export function PubSubBoard({ records, queues }: { records: EvRecord[]; queues: Map<string, QueueInfo> }) {
  const run = latestRun(records, "pubsub");
  const subs = subscribers(records);
  const trails = run ? trailsOfRun(records, "pubsub", run) : [];
  const normal = trails.filter((t) => !t.malformed);
  const malformed = trails.filter((t) => t.malformed);

  const got = new Map<string, number>();
  // Một ô của ma trận: ✓ đã xử lý, … đã nhận chưa xong, ✗ chưa nhận (giống cmd/verify).
  const cell = (stepsOf: EvRecord[], service: string): { cls: string; text: string } => {
    const mine = stepsOf.filter((s) => s.service === service);
    const processed = mine.filter((s) => s.status === "processed").length;
    if (processed > 0) {
      got.set(service, (got.get(service) ?? 0) + 1);
      return { cls: "ok", text: processed > 1 ? `✓×${processed}` : "✓" };
    }
    if (mine.length > 0) return { cls: "pending", text: "…" };
    return { cls: "miss", text: "✗" };
  };
  const rows = normal.map((t) => (
    <tr key={t.id}>
      <td>
        <code>{t.id}</code>
      </td>
      <td className="muted">
        {t.studentId} · {t.courseId}
      </td>
      {subs.map((s) => {
        const c = cell(t.steps, s.service);
        return (
          <td key={s.service} className={c.cls}>
            {c.text}
          </td>
        );
      })}
    </tr>
  ));
  const pass = normal.length > 0 && subs.length > 0 && subs.every((s) => got.get(s.service) === normal.length);

  // Analytics: số lượt đã xử lý theo học phần (từ nhật ký của Analytics, mọi lần gửi).
  const perCourse = new Map<string, number>();
  for (const r of records) {
    if (r.service === "analytics" && r.status === "processed" && r.course_id) {
      perCourse.set(r.course_id, (perCourse.get(r.course_id) ?? 0) + 1);
    }
  }
  const maxCount = Math.max(1, ...perCourse.values());

  return (
    <section className="card">
      <header className="card-head">
        <h2>(ii) Publish/Subscribe</h2>
        <p className="hint">
          Academic publish lên exchange <code>academic.events</code>. Mỗi subscriber có queue riêng và nhận{" "}
          <b>một bản sao</b>. Tắt một subscriber: Ready của queue đó tăng; bật lại: nhận đủ.
        </p>
      </header>
      {subs.length === 0 ? (
        <p className="empty">Chưa có subscriber nào. Chạy finance-sub, notification-sub, analytics-sub.</p>
      ) : (
        <div className="subs">
          {subs.map((s) => {
            const q = queues.get(s.queue);
            const off = !q || q.consumers === 0;
            return (
              <div key={s.service} className={off ? "sub off" : "sub"}>
                <b>{s.service}</b>
                <code>{s.queue}</code>
                <span>
                  Ready <b>{q?.messages_ready ?? "–"}</b> · Unacked <b>{q?.messages_unacknowledged ?? "–"}</b>
                </span>
                <span className={off ? "tag warn" : "tag"}>{off ? "đang tắt" : "đang chạy"}</span>
              </div>
            );
          })}
        </div>
      )}
      {run ? (
        <>
          <p className="runline">
            <span>
              Lần gửi <code>{run}</code>: {normal.length} sự kiện, mỗi subscriber phải có đủ một bản sao
            </span>
            <span className={pass ? "verdict ok" : "verdict pending"}>{pass ? "ĐẠT" : "chưa đủ"}</span>
          </p>
          <div className="table-wrap">
            <table className="matrix">
              <thead>
                <tr>
                  <th>Event ID</th>
                  <th>SV · học phần</th>
                  {subs.map((s) => (
                    <th key={s.service}>{s.service}</th>
                  ))}
                </tr>
              </thead>
              <tbody>{rows}</tbody>
              <tfoot>
                <tr>
                  <td colSpan={2}>Đã xử lý</td>
                  {subs.map((s) => (
                    <td key={s.service}>
                      {got.get(s.service) ?? 0}/{normal.length}
                    </td>
                  ))}
                </tr>
              </tfoot>
            </table>
          </div>
          {malformed.map((t) => (
            <p key={t.id} className="muted small">
              <code>{t.id}</code> (sai hợp đồng):{" "}
              {t.steps.map((s) => `${s.service}: ${s.status}`).join(", ") || "chưa có subscriber nào xử lý"}
            </p>
          ))}
        </>
      ) : (
        <p className="empty">Chưa có lần gửi nào ở kịch bản (ii).</p>
      )}
      {perCourse.size > 0 && (
        <div className="analytics">
          <h3>Analytics: số lượt đăng ký theo học phần</h3>
          {[...perCourse.entries()]
            .sort((a, b) => a[0].localeCompare(b[0]))
            .map(([course, count]) => (
              <div key={course} className="bar-row">
                <code>{course}</code>
                <div className="bar">
                  <div className="bar-fill" style={{ width: `${(count / maxCount) * 100}%` }} />
                </div>
                <b>{count}</b>
              </div>
            ))}
        </div>
      )}
    </section>
  );
}
