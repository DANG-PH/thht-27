// Các phép tính trên nhật ký, cùng quy tắc với cmd/verify bên Go.

import type { EvRecord } from "./types";

/** Lấy mã lần gửi từ event ID dạng ev-<run>-<số thứ tự>. */
export function runOf(eventId: string): string {
  const parts = eventId.split("-");
  return parts.length >= 3 && parts[0] === "ev" ? parts[1] : "";
}

/** Mã lần gửi gần nhất của một kịch bản. */
export function latestRun(records: EvRecord[], scenario: string): string | null {
  for (let i = records.length - 1; i >= 0; i--) {
    const r = records[i];
    if (r.service === "academic" && r.scenario === scenario && r.status === "published") {
      return runOf(r.event_id);
    }
  }
  return null;
}

export interface Trail {
  id: string;
  studentId?: string;
  courseId?: string;
  malformed: boolean;
  steps: EvRecord[]; // bản ghi phía consumer, theo thời gian
}

/** Các sự kiện của một lần gửi, theo thứ tự gửi, kèm diễn biến phía consumer. */
export function trailsOfRun(records: EvRecord[], scenario: string, run: string): Trail[] {
  const byId = new Map<string, Trail>();
  const order: string[] = [];
  for (const r of records) {
    if (r.scenario !== scenario || runOf(r.event_id) !== run) continue;
    if (r.service === "academic") {
      if (r.status !== "published" || byId.has(r.event_id)) continue;
      byId.set(r.event_id, {
        id: r.event_id,
        studentId: r.student_id,
        courseId: r.course_id,
        malformed: r.note === "malformed",
        steps: [],
      });
      order.push(r.event_id);
    }
  }
  for (const r of records) {
    if (r.service === "academic" || r.scenario !== scenario) continue;
    byId.get(r.event_id)?.steps.push(r);
  }
  return order.map((id) => byId.get(id)!);
}

/** Tên các ứng dụng đã từng ghi log ở kịch bản (ii), kèm tên queue riêng của chúng. */
export function subscribers(records: EvRecord[]): { service: string; queue: string }[] {
  const seen = new Map<string, string>();
  for (const r of records) {
    if (r.scenario === "pubsub" && r.service !== "academic" && !seen.has(r.service)) {
      seen.set(r.service, r.queue ?? "");
    }
  }
  return [...seen.entries()]
    .map(([service, queue]) => ({ service, queue }))
    .sort((a, b) => a.service.localeCompare(b.service));
}

/** Giờ trong nhật ký, dạng HH:MM:SS.mmm (lấy thẳng từ chuỗi, giữ múi giờ của máy chạy service). */
export function clock(ts: string): string {
  const m = /T(\d\d:\d\d:\d\d)(\.\d{1,3})?/.exec(ts);
  return m ? m[1] + (m[2] ?? "") : ts;
}

/** Màu ổn định theo chuỗi (dùng cho nhân vật trên bản đồ). */
export function hue(s: string): number {
  let h = 0;
  for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) >>> 0;
  return h % 360;
}
