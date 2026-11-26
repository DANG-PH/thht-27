import { latestRun, subscribers, trailsOfRun } from "@/lib/derive";
import type { EvRecord } from "@/lib/types";

/** Kịch bản (ii): ma trận sự kiện × subscriber của lần gửi gần nhất – cùng quy tắc với cmd/verify. */
export function PubSubBoard({ records }: { records: EvRecord[] }) {
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

  return (
    <section className="card">
      <header className="card-head">
        <h3>Ma trận sự kiện × subscriber – lần gửi gần nhất</h3>
        <p className="hint">
          ✓ đã xử lý · … đã nhận, chưa xong · ✗ chưa nhận (subscriber đang tắt, hoặc được thêm sau lần gửi này).
        </p>
      </header>
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
    </section>
  );
}
