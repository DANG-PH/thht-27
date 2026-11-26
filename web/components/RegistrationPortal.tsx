"use client";

// Cổng đăng ký học phần – giao diện của Academic (Producer).
// Mỗi lần bấm "Đăng ký" = Academic phát MỘT sự kiện student.enrolled lên RabbitMQ.

import { useEffect, useState } from "react";
import { type Catalog, money } from "@/lib/catalog";
import type { Scenario } from "./FlowDiagram";

interface SendResult {
  run: string;
  target: string;
  event_ids: string[];
}

export function RegistrationPortal({ api, scenario, catalog }: { api: string; scenario: Scenario; catalog: Catalog | null }) {
  const [student, setStudent] = useState("");
  const [busy, setBusy] = useState<string | null>(null);
  const [msg, setMsg] = useState<{ ok: boolean; text: string } | null>(null);

  useEffect(() => {
    if (!student && catalog?.students.length) setStudent(catalog.students[0].id);
  }, [catalog, student]);

  async function send(key: string, body: Record<string, unknown>) {
    setBusy(key);
    setMsg(null);
    try {
      const res = await fetch(`${api}/api/send`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ mode: scenario, ...body }),
      });
      const data = await res.json();
      if (!res.ok) {
        setMsg({ ok: false, text: data.error ?? `HTTP ${res.status}` });
      } else {
        const r = data as SendResult;
        const ids = r.event_ids;
        const label = ids.length === 1 ? ids[0] : `${ids.length} thông điệp (${ids[0]} … ${ids[ids.length - 1]})`;
        setMsg({ ok: true, text: `Đã phát ${label} → ${r.target}. Broker đã xác nhận.` });
      }
    } catch (e) {
      setMsg({ ok: false, text: `Không gọi được máy chủ: ${String(e)}` });
    } finally {
      setBusy(null);
    }
  }

  return (
    <section className="panel">
      <header className="panel-head">
        <span className="role role-producer">Academic · Producer</span>
        <h2>Cổng đăng ký học phần</h2>
        <p className="hint">
          Mỗi lượt đăng ký phát một sự kiện <code>student.enrolled</code>{" "}
          {scenario === "work" ? (
            <>
              vào queue <code>finance.work</code>.
            </>
          ) : (
            <>
              lên exchange <code>academic.events</code>.
            </>
          )}
        </p>
      </header>

      {!catalog ? (
        <p className="empty">Đang tải danh mục từ Academic…</p>
      ) : (
        <>
          <label className="select-row">
            <span>Sinh viên</span>
            <select value={student} onChange={(e) => setStudent(e.target.value)}>
              {catalog.students.map((s) => (
                <option key={s.id} value={s.id}>
                  {s.id} · {s.name}
                </option>
              ))}
            </select>
          </label>

          <div className="courses">
            {catalog.courses.map((c) => (
              <article key={c.id} className="course">
                <div className="course-top">
                  <code className="course-code">{c.id}</code>
                  <span className="muted small">{c.credits} tín chỉ</span>
                </div>
                <h3>{c.name}</h3>
                <div className="course-bottom">
                  <span className="price">{money(c.fee)}</span>
                  <button
                    type="button"
                    className="btn btn-primary"
                    disabled={busy !== null}
                    onClick={() => send(c.id, { student_id: student, course_id: c.id })}
                  >
                    {busy === c.id ? "Đang gửi…" : "Đăng ký"}
                  </button>
                </div>
              </article>
            ))}
          </div>

          <div className="bulk">
            <span className="muted small">Mô phỏng nhiều sinh viên cùng đăng ký:</span>
            {[6, 12].map((n) => (
              <button key={n} type="button" className="btn" disabled={busy !== null} onClick={() => send(`n${n}`, { n })}>
                {busy === `n${n}` ? "Đang gửi…" : `${n} lượt`}
              </button>
            ))}
            <button
              type="button"
              className="btn btn-danger-ghost"
              disabled={busy !== null}
              onClick={() => send("bad", { bad_only: true })}
              title="Gửi 1 thông điệp thiếu trường bắt buộc: consumer từ chối, thông điệp sang dead-letter queue"
            >
              {busy === "bad" ? "Đang gửi…" : "Gửi thông điệp lỗi"}
            </button>
          </div>
        </>
      )}

      {msg && <p className={msg.ok ? "toast ok" : "toast bad"}>{msg.text}</p>}
    </section>
  );
}
