"use client";

import { useState } from "react";

interface SendResult {
  run: string;
  mode: string;
  target: string;
  event_ids: string[];
}

/** Gửi yêu cầu tới Dashboard API → Academic publish sự kiện lên RabbitMQ. */
export function SendPanel({ api }: { api: string }) {
  const [mode, setMode] = useState<"work" | "pubsub">("work");
  const [n, setN] = useState(12);
  const [bad, setBad] = useState(false);
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState<SendResult | null>(null);
  const [error, setError] = useState<string | null>(null);

  async function submit() {
    setBusy(true);
    setError(null);
    try {
      const res = await fetch(`${api}/api/send`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ mode, n, bad }),
      });
      const data = await res.json();
      if (!res.ok) {
        setError(data.error ?? `HTTP ${res.status}`);
      } else {
        setResult(data as SendResult);
      }
    } catch (e) {
      setError(`Không gọi được Dashboard API tại ${api}: ${String(e)}`);
    } finally {
      setBusy(false);
    }
  }

  return (
    <section className="card send">
      <div className="send-row">
        <div className="segmented" role="radiogroup" aria-label="Kịch bản">
          <button
            type="button"
            role="radio"
            aria-checked={mode === "work"}
            className={mode === "work" ? "on" : ""}
            onClick={() => setMode("work")}
          >
            Kịch bản (i) · queue finance.work
          </button>
          <button
            type="button"
            role="radio"
            aria-checked={mode === "pubsub"}
            className={mode === "pubsub" ? "on" : ""}
            onClick={() => setMode("pubsub")}
          >
            Kịch bản (ii) · exchange academic.events
          </button>
        </div>
        <label className="field">
          Số sự kiện
          <input
            type="number"
            min={1}
            max={200}
            value={n}
            onChange={(e) => setN(Math.max(1, Math.min(200, Number(e.target.value) || 1)))}
          />
        </label>
        <label className="check">
          <input type="checkbox" checked={bad} onChange={(e) => setBad(e.target.checked)} />
          Kèm 1 thông điệp sai hợp đồng
        </label>
        <button type="button" className="primary" onClick={submit} disabled={busy}>
          {busy ? "Đang gửi…" : "Academic: gửi student.enrolled"}
        </button>
      </div>
      {error && <p className="error">{error}</p>}
      {result && !error && (
        <p className="result">
          Lần gửi <code>{result.run}</code>: {result.event_ids.length} thông điệp → {result.target}. Broker đã xác nhận
          (publisher confirm).
        </p>
      )}
    </section>
  );
}
